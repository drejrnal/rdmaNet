//
// Created by luoxiYun on 2021/6/6.
//
#include "../ib_comm.h"
#include "../sock.h"
#include "../log.h"

#define DefaultPort 13211
static constexpr size_t kRdsniNumQPs = 1;
static constexpr size_t kRdsniBufSize = 4096;
static constexpr int service_port = DefaultPort;

static constexpr size_t RESPONSE_BUF_SIZE = 16;
static constexpr size_t REQUEST_BUF_SIZE = 16;

static constexpr size_t postListSize = 32;
/*
 *  测试server端同client端 reliable write方式的点对点数据传输
 *  server端进行batch post,每隔postListSize个wqe，执行poll cq
 *  测量server端和client端各自吞吐量
 */
void run_server(){
    int srv_gid = 0;
    size_t ib_port_index = 0;

    rdsni_conn_config_t conn_config;
    conn_config.num_qps = kRdsniNumQPs;
    conn_config.use_uc = false;
    conn_config.buf_size = kRdsniBufSize;

    /*
     * connected-oriented so @dgram_config parameter is NULL
     * server端MR区域占据的内存初始化为0
     */
    auto *cb = rdsni_resources_init(srv_gid, ib_port_index, &conn_config, nullptr);
    //memset(const_cast<uint8_t *>(cb->conn_buf), static_cast<uint8_t>(srv_gid)+1, kRdsniBufSize);
    /*
     * buffer to send response from
     */
    uint8_t *response_buffer = static_cast<uint8_t *>( malloc(RESPONSE_BUF_SIZE) );
    if( response_buffer == NULL ) {
        fprintf( stderr, "malloc failed\n");
        exit(1);
    }
    memset( response_buffer, 1, RESPONSE_BUF_SIZE );

    //rdsni_msgx("conn_buf  content %s", cb->conn_buf );
    rdsni_msgx("response from server : %d", response_buffer[0] );
    rdsni_qp_attr_t *clt_qp = (struct rdsni_qp_attr_t *)
            malloc(sizeof(struct rdsni_qp_attr_t));

    //server端等待client端连接，并获取client qp信息 存储到clt_qp中
    connQP_connect_server(cb, service_port, clt_qp);
    rdsni_msgx("run_server():remote qp info: lid-%d,qpn-%d, buf_addr-%u",
               clt_qp->lid, clt_qp->qpn, clt_qp->buf_addr);

    rdsni_connect_qps(cb, 0, clt_qp);
    rdsni_msgx("run_server():server %u connected", srv_gid);
    /*
     * server---client端rdma write方式通信
     */
    struct ibv_send_wr wr[kRdsniPostlist], *bad_send_wr;
    memset( wr, 0, sizeof(wr) );
    struct ibv_sge sgl[kRdsniPostlist];
    struct ibv_wc wc; //server端

    size_t offset = 64;
    while ( offset < RESPONSE_BUF_SIZE ) offset +=64;
    assert( offset * postListSize < cb->conn_config.buf_size );
    rdsni_msgx("receive offset for each WR in receive queue:%d", offset );

    size_t rolling_iter = 0;
    struct timespec start, end;
    clock_gettime(CLOCK_REALTIME, &start);

    while(1){
        if( rolling_iter >= 512 ){
            clock_gettime( CLOCK_REALTIME, &end );
            double seconds = ( end.tv_sec - start.tv_sec )+
                    ( end.tv_nsec - start.tv_nsec )/1000000000.0;
            printf("server: %.3f Mops\n", rolling_iter / seconds / 1000000 );
            break;
        }
        while ( cb->conn_buf[0] == 0 ){
            //do nothing
        }
        /*
         * cb->conn_buf[64]和cb->conn_buf[128]应该也是1,但是输出却是0 不知为何？
         * 说明client端的数据并未全部发到server端，这也是write吞吐量比send高的原因之一，因为
         * write的话不需要接收全部请求即可发送回复，而send需要等recv cq达到一定数量后才可send
         * 当然上述情形适用于双端通信
         * rdsni_msgx( "server get request from client: %d %d %d",
                    cb->conn_buf[0],cb->conn_buf[64], cb->conn_buf[128] );
         */
        cb->conn_buf[0] = 0;
        for( int w_i = 0; w_i < postListSize; w_i++ ){
            wr[w_i].opcode = IBV_WR_RDMA_WRITE;
            wr[w_i].num_sge = 1;
            wr[w_i].sg_list = &sgl[w_i];

            wr->next = (w_i == postListSize - 1) ? NULL : &wr[w_i+1];
            wr->send_flags |= ( w_i == 0  ) ? IBV_SEND_SIGNALED : 0;
            wr->send_flags |= IBV_SEND_INLINE;

            sgl[w_i].addr = (uint64_t)(uintptr_t)(response_buffer);
            sgl[w_i].length = RESPONSE_BUF_SIZE;

            wr[w_i].wr.rdma.remote_addr = clt_qp->buf_addr + w_i * offset;
            wr[w_i].wr.rdma.rkey = clt_qp->rkey;

            rolling_iter++;
        }
        int ret = ibv_post_send( cb->conn_qp[0], &wr[0], &bad_send_wr );
        CPE( ret, "ibv_post_send_error", ret );

        //rdsni_msgx( "Before polling completion event for rdma write");
        rdsni_poll_cq(cb->conn_cq[0],1, &wc);
        //rdsni_msgx( "After polling completion event for rdma write");
    }

    rdsni_resources_destroy(cb);
}

void run_client(char *server){

    size_t clt_gid = 0;
    size_t srv_gid = clt_gid;
    size_t ib_port_index = 0;

    rdsni_conn_config_t conn_config;
    conn_config.num_qps = kRdsniNumQPs;
    conn_config.use_uc = false;
    conn_config.buf_size = kRdsniBufSize;

    //connected-oriented so @dgram_config parameter is NULL
    /*
     * client端 接收server端的response的缓冲区cb->conn_buf初始化为0
     */
    auto *cb = rdsni_resources_init(srv_gid, ib_port_index, &conn_config, nullptr);

    //char *server_name;
    //sprintf(server_name, "server-%d",srv_gid);
    /*
     * buffer to send requests from
     */
    uint8_t *req_buf = static_cast<uint8_t *>(malloc( REQUEST_BUF_SIZE ) );
    if( req_buf == NULL ) {
        fprintf( stderr, "malloc failed\n");
        exit(1);
    }
    memset( req_buf, 1, REQUEST_BUF_SIZE );

    struct rdsni_qp_attr_t *server_qp = (struct rdsni_qp_attr_t *)
            malloc(sizeof(struct rdsni_qp_attr_t));

    connQP_connect_client(cb, server, service_port, server_qp);

    rdsni_connect_qps(cb, 0, server_qp);
    rdsni_msgx("client %d prepared:", clt_gid);

    /*
     * client---server端rdma write方式通信
     */
    struct ibv_send_wr wr[kRdsniPostlist], *bad_send_wr;
    memset( wr, 0, sizeof(wr) );
    struct ibv_sge sgl[kRdsniPostlist];
    struct ibv_wc wc; //server端

    size_t offset = 64;
    while ( offset < RESPONSE_BUF_SIZE ) offset +=64;
    assert( offset * postListSize < cb->conn_config.buf_size );
    rdsni_msgx("receive offset for each WR in receive queue:%d", offset );

    size_t rolling_iter = 0;
    struct timespec start, end;
    clock_gettime(CLOCK_REALTIME, &start);

    while(1){
        if( rolling_iter >= 512 ){
            clock_gettime( CLOCK_REALTIME, &end );
            double seconds = ( end.tv_sec - start.tv_sec )+
                             ( end.tv_nsec - start.tv_nsec )/1000000000.0;
            printf("server: %.3f Mops\n", rolling_iter / seconds / 1000000 );
            break;
        }

        for( int w_i = 0; w_i < postListSize; w_i++ ){
            wr[w_i].opcode = IBV_WR_RDMA_WRITE;
            wr[w_i].num_sge = 1;
            wr[w_i].sg_list = &sgl[w_i];

            wr->next = (w_i == postListSize - 1) ? NULL : &wr[w_i+1];
            wr->send_flags |= ( w_i == 0  ) ? IBV_SEND_SIGNALED : 0;
            wr->send_flags |= IBV_SEND_INLINE;

            sgl[w_i].addr = (uint64_t)(uintptr_t)(req_buf);
            sgl[w_i].length = REQUEST_BUF_SIZE;

            wr[w_i].wr.rdma.remote_addr = server_qp->buf_addr + w_i * offset;
            wr[w_i].wr.rdma.rkey = server_qp->rkey;

            rolling_iter++;
        }
        int ret = ibv_post_send( cb->conn_qp[0], &wr[0], &bad_send_wr );
        CPE( ret, "ibv_post_send_error", ret );

        //rdsni_msgx( "Before polling completion event for rdma write");
        rdsni_poll_cq(cb->conn_cq[0],1, &wc);
        //rdsni_msgx( "After polling completion event for rdma write");

        while( cb->conn_buf[0] == 0 ){
            //printf("client waiting message from server\n");
            /*
             * do-nothing
             */
        }
        /*rdsni_msgx("run_client(): client-%lu, response from server: %s",
                   clt_gid, cb->conn_buf );*/
        cb->conn_buf[0] = 0;
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

