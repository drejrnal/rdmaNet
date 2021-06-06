#include "ib_comm.h"
#include "log.h"
#include "sock.h"
#include <sstream>
#include <assert.h>

/*
 * to fill the device info into dev_info of rdsni_resources_blk 
 * @phy_port : port num on one device
 */
void rdsni_resolve_port_index(rdsni_resources_blk *resources, size_t phy_port){
    std::ostringstream exceptionMsg;
    auto& resolve = resources->device_info;

    int num_devices = 0;
    struct ibv_device **dev_list = ibv_get_device_list(&num_devices);
    assert( dev_list != nullptr );

    int ports_to_discover = phy_port;

    for(int dev_i = 0; dev_i < num_devices; dev_i++){
        struct ibv_context *ib_ctx = ibv_open_device(dev_list[dev_i]);
        struct ibv_device_attr device_attr;
        memset(&device_attr, 0, sizeof(device_attr));

        if(ibv_query_device(ib_ctx, &device_attr) != 0 ){
            exceptionMsg<<" Failed to query Infiniband device "<<std::to_string(dev_i);
            throw std::runtime_error(exceptionMsg.str());
        }
        for(uint8_t port_i = 1; port_i <= device_attr.phys_port_cnt; port_i++){
            struct ibv_port_attr port_attr;
            if( ibv_query_port(ib_ctx, port_i, &port_attr) != 0 ){
                exceptionMsg<<" Failed to query Port "<<std::to_string(port_i) 
                                <<" on device "<<ib_ctx->device->name;
                throw std::runtime_error(exceptionMsg.str());
            }
            if( port_attr.phys_state != IBV_PORT_ACTIVE &&
                    port_attr.phys_state != IBV_PORT_ACTIVE_DEFER){
                continue;
            }
            if( ports_to_discover == 0 ){
                printf("Rdsni::port index %zu resolved to device %d, port %d, Name %s.\n",
                        phy_port,dev_i, port_i, dev_list[dev_i]->name);
                resolve.device_id = dev_i;
                resolve.ib_context = ib_ctx;
                resolve.dev_port_id = port_i;
                resolve.port_lid = port_attr.lid;
                return;
            }
            ports_to_discover--;

        }
        if(ibv_close_device(ib_ctx) != 0){
            exceptionMsg<<" Failed to close device "<<ib_ctx->device->name;
            throw std::runtime_error(exceptionMsg.str());
        }

    }
}

/*
 * 调用sock_write传递当前qp信息
 */ 
int sock_set_qp_info(int sockfd, struct rdsni_qp_attr_t *qp_info){

    struct rdsni_qp_attr_t tmp_qp_info;
    strcpy(tmp_qp_info.name, qp_info->name);
    tmp_qp_info.lid = htons(qp_info->lid);
    tmp_qp_info.qpn = htonl(qp_info->qpn);

    tmp_qp_info.buf_addr = qp_info->buf_addr;
    tmp_qp_info.buf_size = htonl(qp_info->buf_size);
    tmp_qp_info.rkey = htonl(qp_info->rkey);

    int n = sock_write(sockfd, &tmp_qp_info, sizeof(tmp_qp_info));
    assert( n == sizeof(struct rdsni_qp_attr_t) );

    return 0;
}
/*
 * 调用sock_read读取对端qp信息
 */
int sock_get_qp_info(int sockfd, struct rdsni_qp_attr_t *qp_info){

    struct rdsni_qp_attr_t tmp_qp_info;
    rdsni_msgx("sock_get_qp_info");
    int n = sock_read(sockfd, (char *)&tmp_qp_info, sizeof(struct rdsni_qp_attr_t));
    assert( n == sizeof(struct rdsni_qp_attr_t));

    strcpy(tmp_qp_info.name ,qp_info->name);
    qp_info->lid = ntohs(tmp_qp_info.lid);
    qp_info->qpn = ntohl(tmp_qp_info.qpn);
    qp_info->buf_addr = tmp_qp_info.buf_addr;
    qp_info->buf_size = ntohl(tmp_qp_info.buf_size);
    qp_info->rkey = ntohl(tmp_qp_info.rkey);

    return 0;
}

/*
 * 通过tcp协议获取client端的qp信息
 * 由rc read rc/uc write server端调用
 * server端同client端qp一一对应
 * followed by void rdsni_connect_qps(rdsni_resources_blk *cb, size_t conn_qp_index, 
                            rdsni_qp_attr_t *remote_qp_attr);
    return: 0 on success, filled with remote_qp_info
*/ 
int connQP_connect_server(struct rdsni_resources_blk *cb, 
                                int port, struct rdsni_qp_attr_t *remote_qp_info)
{
    int num_qps = cb->conn_config.num_qps;
    int sockfd;
   
    int num_peers = 1;
    struct sockaddr_in peer_addr;
    socklen_t peer_addr_len = sizeof(struct sockaddr_in);

    int peer_sockfd; //server-client连接套接字 
    struct rdsni_qp_attr_t *local_qp_info = nullptr;

    //char server_name[num_qps][128];

    sockfd = tcp_server_listen(port);
 
   // peer_sockfd = (int *)calloc(num_qps, sizeof(int));

    peer_sockfd = accept(sockfd, (struct sockaddr *)(&peer_addr), &peer_addr_len);
    assert(peer_sockfd > 0);

    rdsni_msgx("connQP_connect_server()- new connection established %d", sockfd);

    local_qp_info = (struct rdsni_qp_attr_t *)malloc(sizeof(struct rdsni_qp_attr_t));
    
    /*for(int i = 0; i < num_qps; i++){
        sprintf(local_qp_info[i].name, "server-qp-%d", i);
        local_qp_info[i].lid = cb->device_info.port_lid;
        local_qp_info[i].qpn = cb->conn_qp[i]->qp_num; //server端第i个qp对应的qp number
        local_qp_info[i].buf_addr = reinterpret_cast<uint64_t>(cb->conn_buf);
        local_qp_info[i].buf_size = cb->conn_config.buf_size;
        local_qp_info[i].rkey = cb->conn_buf_mr->rkey; //从memory region属性中获取rkey
    }*/
    local_qp_info->lid = cb->device_info.port_lid;
    local_qp_info->qpn = (cb->num_dgram_qps>=1) ? cb->dgram_qp[0]->qp_num 
                                    : cb->conn_qp[0]->qp_num; //server端第i个qp对应的qp number
    local_qp_info->buf_addr =(cb->num_dgram_qps>=1) ? reinterpret_cast<uint64_t>(cb->dgram_buf)
                                    : reinterpret_cast<uint64_t>(cb->conn_buf);
    local_qp_info->buf_size =(cb->num_dgram_qps>=1) ? cb->dgram_buf_size 
                                    : cb->conn_config.buf_size;
    local_qp_info->rkey = (cb->num_dgram_qps >= 1) ? cb->dgram_buf_mr->rkey
                                    : cb->conn_buf_mr->rkey; //从memory region属性中获取rkey

    //remote_qp_info = (struct rdsni_qp_attr_t *)malloc(sizeof(struct rdsni_qp_attr_t));

    int ret = 0;
    //获取每个client端对应的QP信息
    ret = sock_get_qp_info(peer_sockfd, remote_qp_info);
    assert( ret == 0 );

    rdsni_msgx("connQP_server()- get remote qp info lid:%d,qpn:%d, rkey:%d, bufaddr:%u",
                    remote_qp_info->lid, remote_qp_info->qpn,
                        remote_qp_info->rkey, remote_qp_info->buf_addr );
    //send QP info to client
    //需要保证第@i个qp信息对应的是peer_sockfd[i]连接套接字
    //在client端，确保peer_sockfd
    rdsni_msgx("connQP_server()- send local qp to remote lid:%d, qpn:%d, rkey:%d,bufaddr:%u",
               local_qp_info->lid, local_qp_info->qpn,
               local_qp_info->rkey, local_qp_info->buf_addr );
    ret = sock_set_qp_info(peer_sockfd, local_qp_info);

    free(local_qp_info);
    close(peer_sockfd);
    //free(peer_sockfd);
    close(sockfd);

    return 0;
}

/*
 * 由client端发起调用，指定server段的IP地址 端口
 * followed by void rdsni_connect_qps(rdsni_resources_blk *cb, size_t conn_qp_index, 
                            rdsni_qp_attr_t *remote_qp_attr);
   return 0 on success, filled with remote_qp_info
 */ 
int connQP_connect_client(struct rdsni_resources_blk *cb, char *server_name, int port, struct rdsni_qp_attr_t *remote_qp_info) {
    
    int sockfd = tcp_client_connect(server_name, port);
    struct rdsni_qp_attr_t *local_qp_info = nullptr;
     
    local_qp_info = (struct rdsni_qp_attr_t *)malloc(sizeof(struct rdsni_qp_attr_t));
    int num_qps = cb->conn_config.num_qps;

    //sprintf(local_qp_info->name, "client-qp-%d", i);
    local_qp_info->lid = cb->device_info.port_lid;
    local_qp_info->qpn = (cb->num_dgram_qps>=1) ? cb->dgram_qp[0]->qp_num 
                                    : cb->conn_qp[0]->qp_num; //client端第i个qp对应的qp number
    local_qp_info->buf_addr =(cb->num_dgram_qps>=1) ? reinterpret_cast<uint64_t>(cb->dgram_buf)
                                    : reinterpret_cast<uint64_t>(cb->conn_buf);
    local_qp_info->buf_size =(cb->num_dgram_qps>=1) ? cb->dgram_buf_size 
                                    : cb->conn_config.buf_size;
    local_qp_info->rkey = (cb->num_dgram_qps >= 1) ? cb->dgram_buf_mr->rkey
                                    : cb->conn_buf_mr->rkey; //从memory region属性中获取rkey

    //remote_qp_info = (struct rdsni_qp_attr_t *)malloc(sizeof(struct rdsni_qp_attr_t));
     rdsni_msgx("connQP_client- send local qp to remote info lid:%d, qpn:%d, rkey:%d, bufaddr:%u",
             local_qp_info->lid,local_qp_info->qpn, 
             local_qp_info->rkey, local_qp_info->buf_addr);
    int ret = 0;
    //send QP info to server
    //需要保证第@i个qp信息对应的是peer_sockfd[i]连接套接字
    ret = sock_set_qp_info(sockfd, local_qp_info);
    assert( ret == 0 );

    //获取每个server端对应的QP信息
    ret = sock_get_qp_info(sockfd, remote_qp_info);
    assert( ret == 0 );

    rdsni_msgx("connQP_client- get remote qp info qpn:%d, rkey:%d, bufaddr:%u",
                  remote_qp_info->qpn, remote_qp_info->rkey, remote_qp_info->buf_addr);

    free(local_qp_info);
    close(sockfd);
    return 0;
}