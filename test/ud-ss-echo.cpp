//
// Created by luoxiYun on 2021/6/4.
//

#include "../ib_comm.h"
#include "../sock.h"

#define DefaultPort 13211
static constexpr size_t kRdsniNumQPs = 1;
static constexpr size_t kRdsniBufSize = 4096;
static constexpr int service_port = DefaultPort;

static constexpr int FLAG_SIZE = 16; //for receive request

static constexpr size_t RESPONSE_BUF_SIZE = 32;//response buffer size

void run_server(){
    int srv_gid = 0;
    size_t ib_port_index = 0;

    rdsni_dgram_config_t dgram_config;
    dgram_config.num_qps = kRdsniNumQPs;
    dgram_config.buf_size = kRdsniBufSize;

    //connected-oriented so @dgram_config parameter is NULL
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

    /*
     * server端等待client端send/write with IMM
     * server端轮询receive qp对应的completion qp，直到有wc在cq中产生
     */
    struct ibv_wc wc;
    size_t num_comps = 0;
    while ( (num_comps = ibv_poll_cq(cb->dgram_recv_cq[0],1, &wc )) == 0 );
    //rdsni_poll_cq( cb->dgram_recv_cq[0], 1, &wc );

    printf( "client qp No.%d send request No.%d with completion : %d\n",wc.src_qp, wc.wr_id, num_comps );
    //注意下面dgram_buf接收到的数据内容从偏移量40以后出现，offset 40前无法看不到client端发的数据。
    printf("server dgram_qp_buf content: %d", cb->dgram_buf[40] );
    /*
     * receive queue内的recv request被消耗，
     * 重新往receive queue内增加receive request
     */
    struct ibv_ah_attr ah_attr;
    memset( &ah_attr, 0, sizeof(ah_attr) );
    ah_attr.is_global = 0;
    ah_attr.dlid = clt_qp->lid;
    ah_attr.sl = 0;
    ah_attr.src_path_bits = 0;
    ah_attr.port_num = cb->device_info.dev_port_id;

    struct ibv_ah *ah = ibv_create_ah( cb->pd, &ah_attr );
    struct ibv_send_wr wr, *bad_wr;
    struct ibv_sge sgl;

    wr.opcode = IBV_WR_SEND;
    wr.num_sge = 1;
    wr.sg_list = &sgl;
    wr.next = NULL; //if the value of wr.next is invalid value, then ibv_post_send would cause SIGSEGV

    wr.send_flags =  IBV_SEND_SIGNALED;
    wr.send_flags |= IBV_SEND_INLINE;
    sgl.addr = reinterpret_cast<uint64_t>(response_buffer);
    //sgl.addr = reinterpret_cast<uint64_t>(cb->dgram_buf);
    //todo::确定大小
    sgl.length = FLAG_SIZE;
    //todo::ah
    wr.wr.ud.ah = ah;
    wr.wr.ud.remote_qpn = clt_qp->qpn;
    wr.wr.ud.remote_qkey = kRdsniDefaultQKey;

    int ret = ibv_post_send(cb->dgram_qp[0], &wr, &bad_wr);
    CPE( ret, "ibv_post_send_error", ret );

    rdsni_poll_cq( cb->dgram_send_cq[0], 1, &wc );

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

    memset( const_cast<char *>(cb->dgram_buf), 'a', cb->dgram_buf_size );

    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr;
    ibv_query_qp(cb->dgram_qp[0], &attr, IBV_QP_STATE | IBV_QP_DEST_QPN, &init_attr);
    rdsni_msgx("run_client()- qp state:%d, dest qp num:%d,  qp number %d",
               attr.qp_state,attr.dest_qp_num, cb->dgram_qp[0]->qp_num);
    //client端作为接收端，往receive queue中添加WR

    for( int i = 0; i < kRdsniRQDepth; i++ )
        rdsni_post_dgram_recv( cb->dgram_qp[0], const_cast<char *>(cb->dgram_buf),
                               FLAG_SIZE+40, cb->dgram_buf_mr->lkey );

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
    struct ibv_send_wr wr, *bad_wr;
    struct ibv_sge sgl;
    struct ibv_wc wc;

    wr.wr_id = 32;
    wr.opcode = IBV_WR_SEND;
    //wr.opcode = IBV_WR_SEND_WITH_IMM;
    //wr.imm_data = 3185;
    wr.num_sge = 1;
    //wr.next = (w_i == params.window - 1) ? NULL : &wr[w_i + 1];
    wr.sg_list = &sgl;
    wr.next = NULL; //if the value of wr.next is invalid value, then ibv_post_send would cause SIGSEGV

    wr.send_flags =  IBV_SEND_SIGNALED;
    /*
     *  IBV_SEND_INLINE: send data in sge_list as inline data
     *  using inline data usually provides better performance.
     */
    wr.send_flags |= IBV_SEND_INLINE;

    sgl.addr = reinterpret_cast<uint64_t>(cb->dgram_buf);

    sgl.length = FLAG_SIZE;

    wr.wr.ud.ah = ah;
    wr.wr.ud.remote_qpn = server_qp->qpn;
    wr.wr.ud.remote_qkey = kRdsniDefaultQKey;

    int ret = ibv_post_send(cb->dgram_qp[0], &wr, &bad_wr);
    CPE( ret, "ibv_post_send_error", ret );

    rdsni_poll_cq( cb->dgram_send_cq[0], 1, &wc );

    struct ibv_wc wc2;
    rdsni_poll_cq( cb->dgram_recv_cq[0], 1, &wc2 );
    /*
     * 注意此处 接收到的数据从偏移量40开始，40之前是ibv_gh相关数据
     */
    printf( "client receive response from server qp : %d,Content: %d %d %d\n",
            wc2.src_qp, cb->dgram_buf[40], cb->dgram_buf[55],cb->dgram_buf[57] );

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
