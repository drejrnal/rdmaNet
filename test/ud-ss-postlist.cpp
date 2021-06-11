//
// Created by luoxiYun on 2021/5/30.
//

#include "../ib_comm.h"
#include "../sock.h"

#define DefaultPort 13211
static constexpr size_t kRdsniNumQPs = 1;
static constexpr size_t kRdsniBufSize = 4096;
static constexpr size_t kRdsniUnsigBatch = 64;
static constexpr size_t kRdsniMaxLID = 4; //根据struct ibv_wc内的slid识别对端lid,并创建对应的ah,同该端传输数据
static constexpr int service_port = DefaultPort;

static constexpr int FLAG_SIZE = 16; //for receive request
static constexpr size_t RESPONSE_BUF_SIZE = 16;//response buffer size

/*
 * 可作为命令行参数
 */
static constexpr size_t postListSize = 32;
/*
 * batch send-recv 采用batch发送后，相比于Non-batch吞吐量提升效果明显
 * 进一步增加batch大小，提升幅度较小
 * （batchSize变成64，相应的其他参数也变化，缓冲区大小由4096变成8192、相应的修改ib_comm.h的参数kRdsniSQDepth到512）
 */
void run_server(){
    int srv_gid = 0;
    size_t ib_port_index = 0;

    rdsni_dgram_config_t dgram_config;
    dgram_config.num_qps = kRdsniNumQPs;
    dgram_config.buf_size = kRdsniBufSize;

    //datagram-oriented so @Param conn_config is NULL
    auto *cb = rdsni_resources_init(srv_gid, ib_port_index, nullptr, &dgram_config);

    //buffer to send responses from
    uint8_t *response_buffer = static_cast<uint8_t *>( malloc(RESPONSE_BUF_SIZE) );
    if( response_buffer == NULL ) {
        fprintf( stderr, "malloc failed\n");
        exit(1);
    }
    memset( response_buffer, 1, RESPONSE_BUF_SIZE );
    /*
     * fill cb->dgram_qp with RECVs before sync qp info with clients
     * ibv_post_recv() posts a linked list of WRs to a queue pair’s (QP) receive queue.
     * At least one receive buffer should be posted to the receive queue to transition the QP to RTR.
     *
     * '40'的含义
     * When global routing is used on UD QPs, there will be a GRH contained in the first 40 bytes of
     * the receive buffer. This area is used to store global routing information, so an appropriate
     * address vector can be generated to respond to the received packet. If GRH is used with UD,
     * the RR should always have extra 40 bytes available for this GRH.
     */
    for( int i = 0; i < kRdsniRQDepth; i++ )
        rdsni_post_dgram_recv( cb->dgram_qp[0], const_cast<char *>(cb->dgram_buf),
                               FLAG_SIZE+40, cb->dgram_buf_mr->lkey );

    rdsni_qp_attr_t *clt_qp = (struct rdsni_qp_attr_t *)
            malloc(sizeof(struct rdsni_qp_attr_t));

    //server端等待client端连接，并获取client qp信息 存储到clt_qp中
    connQP_connect_server(cb, service_port, clt_qp);
    rdsni_msgx("run_server():remote qp info: lid-%d,qpn-%d, buf_addr-%u",
               clt_qp->lid, clt_qp->qpn, clt_qp->buf_addr);

    struct ibv_ah *ah[kRdsniMaxLID];
    memset( ah, 0, sizeof(ah) );

    struct ibv_send_wr wr[kRdsniPostlist], *bad_send_wr;
    memset( wr, 0, sizeof(wr) );
    struct ibv_recv_wr recv_wr[kRdsniPostlist], *bad_recv_wr;
    struct ibv_wc wc[kRdsniPostlist];
    struct ibv_sge sgl[kRdsniPostlist]; //for batch recv after polling cq returns

    size_t rolling_iter = 0;

    size_t recv_offset = 0;
    while ( recv_offset < FLAG_SIZE + 40 ) recv_offset +=64;
    assert( recv_offset * postListSize < dgram_config.buf_size );
    rdsni_msgx("receive offset for each WR in receive queue:%d", recv_offset );

    struct timespec start, end;
    clock_gettime( CLOCK_REALTIME, &start);

    double tput = 0.0;
    /*
     * server端等待client端send/write with IMM
     * server端轮询receive qp对应的completion qp，直到有wc在cq中产生
     */
    while (1){
        if( rolling_iter >= 1024 ){
            //性能测试
            clock_gettime(CLOCK_REALTIME, &end);
            double seconds = (end.tv_sec - start.tv_sec) +
                             (end.tv_nsec - start.tv_nsec) / 1000000000.0;
            tput = rolling_iter / seconds/1000000;
            printf("main: Server: %.3f Mops.\n",tput);
            break;
        }
        size_t num_comps = ibv_poll_cq( cb->dgram_recv_cq[0], postListSize, wc );

        assert( num_comps >= 0 );
        if( num_comps == 0 )
            continue;
        //rdsni_msgx("get %d completion receive request from client's qp No.%d ", num_comps, wc[0].src_qp );
        /*
         * 接收到client端消息后，执行batch post recv
         * receive queue内的recv request被消耗，
         * 重新往receive queue内增加receive request
        */
        for( size_t w_i = 0; w_i < num_comps; w_i++ ){
            sgl[w_i].length = FLAG_SIZE + 40;
            sgl[w_i].lkey = cb->dgram_buf_mr->lkey;
            sgl[w_i].addr = reinterpret_cast<uint64_t>(
                            &cb->dgram_buf[recv_offset * w_i]);
            recv_wr[w_i].num_sge = 1;
            recv_wr[w_i].sg_list = &sgl[w_i];
            recv_wr->next = ( w_i == num_comps - 1 ) ? nullptr : &recv_wr[w_i+1];
        }

        if(ibv_post_recv(cb->dgram_qp[0], &recv_wr[0], &bad_recv_wr)){
            rdsni_msgx("Error:%s,in run_server()[ibv_post_recv()]", strerror(errno));
            exit(-1);
        }

        for( size_t w_i = 0; w_i < num_comps; w_i++ ){
            int s_lid = wc[w_i].slid;
            if (ah[s_lid] == nullptr) {
                rdsni_msgx("client IB local identifier: %d", s_lid );
                struct ibv_ah_attr ah_attr;
                memset(&ah_attr, 0, sizeof(ah_attr));
                ah_attr.is_global = 0;
                ah_attr.dlid = s_lid;
                ah_attr.sl = 0;
                ah_attr.src_path_bits = 0;
                ah_attr.port_num = cb->device_info.dev_port_id;

                ah[s_lid] = ibv_create_ah(cb->pd, &ah_attr);
                assert(ah[s_lid] != nullptr);
            }
            wr[w_i].wr.ud.ah = ah[s_lid];
            wr[w_i].wr.ud.remote_qpn = wc[w_i].src_qp;
            wr[w_i].wr.ud.remote_qkey = kRdsniDefaultQKey;

            wr[w_i].opcode = IBV_WR_SEND;
            wr[w_i].num_sge = 1;
            wr[w_i].next = (w_i == num_comps - 1) ? nullptr : &wr[w_i + 1];
            wr[w_i].sg_list = &sgl[w_i];

            wr[w_i].send_flags =
                    ( rolling_iter % kRdsniUnsigBatch == 0 ) ? IBV_SEND_SIGNALED : 0;
            if( rolling_iter % kRdsniUnsigBatch == (kRdsniUnsigBatch - 1) ){
                struct ibv_wc signal_wc;
                //rdsni_msgx("server: Before polling send completion queue");
                rdsni_poll_cq( cb->dgram_send_cq[0], 1, &signal_wc );
                //rdsni_msgx("server: After polling send completion queue");
            }
            wr[w_i].send_flags |= IBV_SEND_INLINE;
            sgl[w_i].addr = reinterpret_cast<uint64_t>(response_buffer);
            sgl[w_i].length = RESPONSE_BUF_SIZE;

            rolling_iter++;
        }

        if( ibv_post_send(cb->dgram_qp[0], &wr[0], &bad_send_wr) ){
            rdsni_msgx("Error:%s,in run_server()[ibv_post_send()]", strerror(errno));
            exit(-1);
        }
        //rdsni_msgx("before polling cq for send qp");
    }
    rdsni_resources_destroy(cb);
}

void run_client(char *server){

    size_t clt_gid = 0;
    size_t srv_gid = clt_gid;
    size_t ib_port_index = 0;

    rdsni_dgram_config_t dgram_config;
    dgram_config.num_qps = kRdsniNumQPs;
    dgram_config.buf_size = kRdsniBufSize;

    //connected-oriented so @dgram_config parameter is NULL
    auto *cb = rdsni_resources_init(srv_gid, ib_port_index, nullptr, &dgram_config);
    //sprintf(server_name, "server-%d",srv_gid);

    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr;
    ibv_query_qp(cb->dgram_qp[0], &attr, IBV_QP_STATE | IBV_QP_DEST_QPN, &init_attr);
    rdsni_msgx("run_client()- qp state:%d, dest qp num:%d,  qp number %d",
               attr.qp_state,attr.dest_qp_num, cb->dgram_qp[0]->qp_num);

    uint8_t *request_buffer = static_cast<uint8_t *>(malloc(RESPONSE_BUF_SIZE));
    memset( request_buffer, 1, RESPONSE_BUF_SIZE );

    struct rdsni_qp_attr_t *server_qp = (struct rdsni_qp_attr_t *)
            malloc(sizeof(struct rdsni_qp_attr_t));

    connQP_connect_client(cb, server, service_port, server_qp);

    rdsni_msgx("client %d prepared, server qpn:%d", clt_gid, server_qp->qpn);
    /*
     * we need only one address handle because we
     * only contact exactly one server
     */
    struct ibv_ah_attr ah_attr;
    memset( &ah_attr, 0, sizeof(ah_attr) );
    ah_attr.is_global = 0;
    ah_attr.dlid = server_qp->lid;
    ah_attr.sl = 0;
    ah_attr.src_path_bits = 0;
    ah_attr.port_num = cb->device_info.dev_port_id;

    struct ibv_ah *ah = ibv_create_ah(cb->pd, &ah_attr);

    //client端发送writting request
    struct ibv_send_wr wr[kRdsniPostlist], *bad_send_wr;
    memset( wr, 0, sizeof(wr) );
    struct ibv_wc wc[kRdsniPostlist];
    struct ibv_sge sgl[kRdsniPostlist];
    size_t rolling_iter = 0;  // For throughput measurement

    struct timespec start, end;
    clock_gettime(CLOCK_REALTIME, &start);

    while (1) {
        if (rolling_iter >= 1024 ) {
            //性能测试
            clock_gettime(CLOCK_REALTIME, &end);
            double seconds = (end.tv_sec - start.tv_sec) +
                             (end.tv_nsec - start.tv_nsec) / 1000000000.0;
            printf("main: Client: %.3f Mops.\n", rolling_iter / seconds/1000000);
            break;
        }
        for (size_t w_i = 0; w_i < postListSize; w_i++) {
            rdsni_post_dgram_recv(cb->dgram_qp[0], const_cast<char *>(cb->dgram_buf),
                                  kRdsniBufSize, cb->dgram_buf_mr->lkey);

            wr[w_i].wr.ud.ah = ah;
            wr[w_i].wr.ud.remote_qpn = server_qp->qpn;
            wr[w_i].wr.ud.remote_qkey = kRdsniDefaultQKey;

            wr[w_i].opcode = IBV_WR_SEND;
            wr[w_i].num_sge = 1;
            wr[w_i].next = (w_i == postListSize - 1) ? nullptr : &wr[w_i + 1];
            wr[w_i].sg_list = &sgl[w_i];

            wr[w_i].send_flags = (w_i == 0) ? IBV_SEND_SIGNALED : 0;
            wr[w_i].send_flags |= IBV_SEND_INLINE;

            sgl[w_i].addr = reinterpret_cast<uint64_t>(cb->dgram_buf);
            sgl[w_i].length = RESPONSE_BUF_SIZE;

            rolling_iter++;
        }

        int ret = ibv_post_send(cb->dgram_qp[0], &wr[0], &bad_send_wr);
        CPE(ret, "ibv_post_send_error", ret);
        //rdsni_msgx("before polling completion qp");
        rdsni_poll_cq(cb->dgram_send_cq[0], 1, wc);
        //rdsni_msgx( "send wr consumed by send cq");

        /*
         * server端发送send response,client端阻塞在recv cq上
         */
        rdsni_poll_cq(cb->dgram_recv_cq[0], postListSize, wc);
        //rdsni_msgx( "recv wr consumed by recv cq");
    }
    rdsni_resources_destroy(cb);
}

int main(int argc, char *argv[]){
    if(argc < 2){
        run_server();
    }else{
        run_client(argv[1]);
    }
    return 0;
}