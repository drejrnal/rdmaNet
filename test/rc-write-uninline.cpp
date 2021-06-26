//
// Created by luoxiYun on 2021/6/25.
//
#include "../ib_comm.h"

#define DefaultPort 13211

static constexpr size_t rdsni_qp_num = 1;
static constexpr size_t rdsni_mr_size = 1<<15; //memory region大小
static constexpr int service_port = DefaultPort;

void run_server(){
    int srv_gid = 0;
    size_t ib_port_index = 0;

    rdsni_conn_config_t conn_config;
    conn_config.num_qps = rdsni_qp_num;
    conn_config.use_uc = false;
    conn_config.buf_size = rdsni_mr_size;

    /*
     * connected-oriented so @dgram_config parameter is NULL
     * server端MR区域占据的内存初始化为0
     */

    auto *cb = rdsni_resources_init(srv_gid, ib_port_index, &conn_config, nullptr);

    rdsni_qp_attr_t *clt_qp = (struct rdsni_qp_attr_t *)
            malloc(sizeof(struct rdsni_qp_attr_t));

    //server端等待client端连接，并获取client qp信息 存储到clt_qp中
    connQP_connect_server(cb, service_port, clt_qp);
    rdsni_msgx("run_server():remote qp info: lid-%d,qpn-%d, buf_addr-%u",
               clt_qp->lid, clt_qp->qpn, clt_qp->buf_addr);

    rdsni_connect_qps(cb, 0, clt_qp);
    rdsni_msgx("run_server():server %u connected", srv_gid);


    while (cb->conn_buf[cb->conn_config.buf_size-1] == 0 ){

    }

    //TODO::打印不出完整的数据 但调式的时候却能看到完整的数据
    rdsni_msgx("server fetch content from client:%s",
               cb->conn_buf );
    rdsni_msgx("server fetch content from client:%c",
               cb->conn_buf[cb->conn_config.buf_size-1] );
    rdsni_msgx("content length %d", strlen(const_cast<char *>(cb->conn_buf)) );

    rdsni_resources_destroy(cb);
}

void run_client(char *server){
    size_t clt_gid = 0;
    size_t srv_gid = clt_gid;
    size_t ib_port_index = 0;

    rdsni_conn_config_t conn_config;
    conn_config.num_qps = rdsni_qp_num;
    conn_config.use_uc = false;
    conn_config.buf_size = rdsni_mr_size;

    auto *cb = rdsni_resources_init(srv_gid, ib_port_index, &conn_config, nullptr);
    //memset(const_cast<char *>(cb->conn_buf), 'a', rdsni_mr_size);

    struct rdsni_qp_attr_t *server_qp = (struct rdsni_qp_attr_t *)
            malloc(sizeof(struct rdsni_qp_attr_t));

    connQP_connect_client(cb, server, service_port, server_qp);

    rdsni_connect_qps(cb, 0, server_qp);
    rdsni_msgx("client %d prepared:", clt_gid);

    for( int i = 0; i < cb->conn_config.buf_size; i +=512 ){
        for( int j = 0; j < 512; j++ )
            cb->conn_buf[i+j] = static_cast<char>('a' + (i / 512)%26);
    }

    rdsni_msgx("content from client: %c %c %c",
               cb->conn_buf[0], cb->conn_buf[512], cb->conn_buf[1024]);

    /*
     * client端通过batch write方式发送memory region中的内容
     * 每个wr携带数据大小byte_per_wr,每轮发送byte_per_iter数据
     */
    struct ibv_wc wc;
    int byte_per_wr = 256;
    int byte_per_iter = 2<<10; //2048
    int batch_size = byte_per_iter / byte_per_wr; // 8
    int total_iter = cb->conn_config.buf_size / byte_per_iter; // 16
    int signal_batch = batch_size * 2;
    int counter = 0;
    for( int iter = 0; iter < total_iter; iter++ ){
        rdsni_post_batch_write( cb, 0, server_qp, byte_per_wr, &counter,
                                signal_batch, batch_size,0 );
    }
    rdsni_msgx("total send number %d", counter);
    if( counter % ( signal_batch ) != 0 )
        rdsni_poll_cq( cb->conn_cq[0], 1, &wc);

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