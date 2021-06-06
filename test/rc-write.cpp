#include "../ib_comm.h"
#include "../sock.h"
#include "../log.h"

#define DefaultPort 13211
static constexpr size_t kRdsniNumQPs = 1;
static constexpr size_t kRdsniBufSize = 4096;
static constexpr int service_port = DefaultPort;

static constexpr size_t RESPONSE_BUF_SIZE = 16;
static constexpr size_t REQUEST_BUF_SIZE = 16;

/*
 *  测试server端同client端 reliable write方式的点对点数据传输
 *  server端等待client端数据传输，client端发送字符串后
 *  server端收到后回复client端
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
    char *response_buffer = static_cast<char *>( malloc(RESPONSE_BUF_SIZE) );
    if( response_buffer == NULL ) {
        fprintf( stderr, "malloc failed\n");
        exit(1);
    }
    snprintf(response_buffer, RESPONSE_BUF_SIZE, "hello,client" );

    //rdsni_msgx("conn_buf  content %s", cb->conn_buf );
    rdsni_msgx("response from server : %s", response_buffer );

    rdsni_qp_attr_t *clt_qp = (struct rdsni_qp_attr_t *)
                                    malloc(sizeof(struct rdsni_qp_attr_t));

    //server端等待client端连接，并获取client qp信息 存储到clt_qp中
    connQP_connect_server(cb, service_port, clt_qp);
    rdsni_msgx("run_server():remote qp info: lid-%d,qpn-%d, buf_addr-%u", 
                clt_qp->lid, clt_qp->qpn, clt_qp->buf_addr);

    rdsni_connect_qps(cb, 0, clt_qp);
    rdsni_msgx("run_server():server %u connected", srv_gid);


    /*
     * server端等待client端发送信息，相当于recvfrom
     */
    while ( cb->conn_buf[0] == 0 ){
        //do nothing
    }
    rdsni_msgx( "server get request from client: %s", cb->conn_buf );
    cb->conn_buf[0] = 0;
    //发送response
    struct ibv_send_wr wr, *bad_wr;
    struct ibv_sge sgl;
    struct ibv_wc wc;

    wr.opcode = IBV_WR_RDMA_WRITE;
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

    sgl.addr = (uint64_t)(uintptr_t)response_buffer;
    sgl.length = RESPONSE_BUF_SIZE;

    wr.wr.rdma.remote_addr = clt_qp->buf_addr;
    wr.wr.rdma.rkey = clt_qp->rkey;

    int ret = ibv_post_send(cb->conn_qp[0], &wr, &bad_wr);
    CPE( ret, "ibv_post_send_error", ret );
    //rdsni_post_write(cb,0, clt_qp, 0);

    //poll for send completion
    rdsni_poll_cq(cb->conn_cq[0],1, &wc);
    rdsni_msgx( "Operation: %d ,write completion from QP %d at server.",
                wc.opcode, wc.qp_num, wc.src_qp );

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
    char *req_buf = static_cast<char *>(malloc( REQUEST_BUF_SIZE ) );
    if( req_buf == NULL ) {
        fprintf( stderr, "malloc failed\n");
        exit(1);
    }
    //memset( req_buf, 1, REQUEST_BUF_SIZE );
    snprintf( req_buf, RESPONSE_BUF_SIZE, "request");

    struct rdsni_qp_attr_t *server_qp = (struct rdsni_qp_attr_t *) 
                                        malloc(sizeof(struct rdsni_qp_attr_t));

    connQP_connect_client(cb, server, service_port, server_qp);

    rdsni_connect_qps(cb, 0, server_qp);
    rdsni_msgx("client %d prepared:", clt_gid);

    /*
     * client端发送req_buffer中的内容
     */
    struct ibv_send_wr wr, *bad_wr;
    struct ibv_sge sgl;
    struct ibv_wc wc;

    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.num_sge = 1;
    //wr.next = (w_i == params.window - 1) ? NULL : &wr[w_i + 1];
    wr.sg_list = &sgl;
    wr.next = NULL;

    wr.send_flags =  IBV_SEND_SIGNALED;
    wr.send_flags |= IBV_SEND_INLINE;

    sgl.addr = (uint64_t)(uintptr_t)req_buf;
    sgl.length = REQUEST_BUF_SIZE;

    wr.wr.rdma.remote_addr = server_qp->buf_addr;
    wr.wr.rdma.rkey = server_qp->rkey;

    rdsni_msgx("request content:%s", req_buf );

    int ret = ibv_post_send(cb->conn_qp[0], &wr, &bad_wr);
    CPE( ret, "ibv_post_send_error", ret );
    /*
     * 轮询conn_buf的内容，获取从对端传输过来的数据
     * while(1){
     *      printf("run_client(): client-%lu, content %s\n",
     *      clt_gid, cb->conn_buf );
     *      sleep(3);
     * }
     *
     */

    rdsni_poll_cq( cb->conn_cq[0], 1, &wc );
    rdsni_msgx( "Operation: %d ,write completion from QP %d at client.",
                wc.opcode, wc.qp_num );

    while( cb->conn_buf[0] == 0 ){
        //printf("client waiting message from server\n");
        /*
         * do-nothing
         */
    }
    rdsni_msgx("run_client(): client-%lu, response from server: %s",
           clt_gid, cb->conn_buf );

    cb->conn_buf[0] = 0;

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
