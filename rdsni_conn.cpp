#include "ib_comm.h"

struct rdsni_resources_blk *rdsni_resources_init(size_t local_hid, size_t port_index,
                                                    rdsni_conn_config_t *conn_config,
                                                    rdsni_dgram_config_t *dgram_config){
    struct rdsni_resources_blk *resources = new rdsni_resources_blk();
    memset( resources, 0, sizeof(rdsni_resources_blk) );

    resources->local_hid = local_hid;
    resources->port_index = port_index;
    
    if( conn_config != nullptr ){
        resources->conn_config = *conn_config;
    }
    if( dgram_config != nullptr ){
        resources->num_dgram_qps = dgram_config->num_qps;
        resources->dgram_buf_size = dgram_config->buf_size;
    }
    
    rdsni_resolve_port_index(resources, port_index);
    resources->pd = ibv_alloc_pd(resources->device_info.ib_context);

    assert(resources->pd != nullptr);

    int ib_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

    //创建dgram qps,并且其状态变为ready to send
    //创建dgram buffer供dgram 的memory region使用
    if( resources->num_dgram_qps >= 1 ){
        rdsni_create_dgram_qps(resources);
        size_t reg_size = 0;

        reg_size = resources->dgram_buf_size;
        //resources->dgram_buf = reinterpret_cast<volatile uint8_t*>(memalign(4096, reg_size));
        resources->dgram_buf = static_cast<char *>( malloc(reg_size) );

        assert(resources->dgram_buf != nullptr);
        //memset(const_cast<uint8_t*>(resources->dgram_buf), 0, reg_size);
        memset( const_cast<char *>(resources->dgram_buf), 0,  reg_size );

        //resources->dgram_buf_mr = ibv_reg_mr(resources->pd,const_cast<uint8_t *>(resources->dgram_buf), reg_size, ib_flags);
        resources->dgram_buf_mr = ibv_reg_mr(resources->pd,const_cast<char *>(resources->dgram_buf),
                                             reg_size, ib_flags);

        assert(resources->dgram_buf_mr != nullptr);
    }
    //创建conn_qps 使其变为ready to send状态
    //创建connected qp使用的buffer并利用它注册conn_qps对应的memory region
    if( resources->conn_config.num_qps >= 1 ){

        resources->conn_qp = new ibv_qp *[resources->conn_config.num_qps];
        resources->conn_cq = new ibv_cq *[resources->conn_config.num_qps];
        rdsni_create_conn_qps(resources);
        
        size_t buffer_size = 0;
        buffer_size = resources->conn_config.buf_size;
        //resources->conn_buf = reinterpret_cast<volatile uint8_t*>(memalign(4096, buffer_size));
        resources->conn_buf = static_cast<volatile char *>( malloc(buffer_size) );

        assert(resources->conn_buf != nullptr);
        //memset( const_cast<uint8_t*>(resources->conn_buf), 0, buffer_size );
        memset( const_cast<char *>(resources->conn_buf), 0,  buffer_size);

        //resources->conn_buf_mr = ibv_reg_mr(resources->pd,const_cast<uint8_t*>(resources->conn_buf),buffer_size, ib_flags);
        resources->conn_buf_mr = ibv_reg_mr( resources->pd, const_cast<char *>(resources->conn_buf),
                                             buffer_size, ib_flags);

        assert(resources->conn_buf_mr != nullptr);
    }
    return resources;
}

/*
 * free up resources
 *  return -1 on error
 *          0 on success
 */ 
int rdsni_resources_destroy(rdsni_resources_blk *resources){
    // destroy QPs and CQs. QPs must be destroyed before CQs 
    for(size_t i = 0; i < resources->num_dgram_qps; i++){
        ibv_destroy_qp(resources->dgram_qp[i]);
        ibv_destroy_cq(resources->dgram_send_cq[i]);
        ibv_destroy_cq(resources->dgram_recv_cq[i]);
    }

    for(size_t i = 0; i < resources->conn_config.num_qps;i++){
         ibv_destroy_qp(resources->conn_qp[i]);
         ibv_destroy_cq(resources->conn_cq[i]);
    }
    
    //destroy memory regions
    if( resources->num_dgram_qps > 0){
        assert( resources->dgram_buf_mr != nullptr && resources->dgram_buf != nullptr );
        if(ibv_dereg_mr(resources->dgram_buf_mr)){
            fprintf(stderr, "Failed to deregister conn MR");
            exit(-1);
        }
        //free(const_cast<uint8_t *>(resources->dgram_buf));
        free( const_cast<char *>(resources->dgram_buf) );
    }

    if( resources->conn_config.num_qps > 0 ){
        assert(resources->conn_buf_mr != nullptr);
        if( ibv_dereg_mr(resources->conn_buf_mr) ){
            fprintf(stderr, "Failed to deregister conn MR");
            exit(-1);
        }
        //free(const_cast<uint8_t *>(resources->conn_buf));
        free( const_cast<char *>(resources->conn_buf) );
    }


    //deallocate protection domain
    ibv_dealloc_pd(resources->pd);
    ibv_close_device(resources->device_info.ib_context);

    return 0;

}



/*
 * qp创建(connected qp and dgram qp)
 *  @Param cb control block(resources struct)
*/
void rdsni_create_conn_qps(rdsni_resources_blk *cb){

    for(int i = 0; i < cb->conn_config.num_qps; i++){
        cb->conn_cq[i] = ibv_create_cq(cb->device_info.ib_context, cb->conn_config.sq_depth,
                                        nullptr, nullptr, 0);

        struct ibv_qp_init_attr create_attr;
        memset(&create_attr, 0, sizeof(create_attr) );
        create_attr.send_cq = cb->conn_cq[i];
        create_attr.recv_cq = cb->conn_cq[i];
        create_attr.qp_type = cb->conn_config.use_uc ? IBV_QPT_UC : IBV_QPT_RC;
        //if this value is set to 1, all send request will generate CQE, if set to 0, only wrs that are flagged will generate CQE
        create_attr.sq_sig_all = 0;

        create_attr.cap.max_send_wr = cb->conn_config.sq_depth;
        create_attr.cap.max_recv_wr = 1; // connectted qps不执行post_recv操作
        create_attr.cap.max_send_sge = 1;
        create_attr.cap.max_recv_sge = 1;
        create_attr.cap.max_inline_data = kRdsniMaxInline;

        cb->conn_qp[i] = ibv_create_qp( cb->pd, &create_attr);
        assert( cb->conn_qp[i] != nullptr );

        //QP state to INIT
        struct ibv_qp_attr qp_attr;
        memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
        qp_attr.qp_state = IBV_QPS_INIT;
        qp_attr.pkey_index = 0;
        qp_attr.port_num = cb->device_info.dev_port_id;
        qp_attr.qp_access_flags = cb->conn_config.use_uc 
                                    ? IBV_ACCESS_REMOTE_WRITE
                                    : (IBV_ACCESS_REMOTE_WRITE |
                                            IBV_ACCESS_REMOTE_READ |
                                                IBV_ACCESS_REMOTE_ATOMIC);
        if( ibv_modify_qp(cb->conn_qp[i], &qp_attr, 
                            IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT
                            | IBV_QP_ACCESS_FLAGS) ){
            fprintf(stderr, "Failed to modify conn qp to INIT\n");
            exit(-1);
        }
    }

}

void rdsni_create_dgram_qps(rdsni_resources_blk *cb){

    for(size_t i = 0; i < cb->num_dgram_qps; i++){

        cb->dgram_send_cq[i] = ibv_create_cq(cb->device_info.ib_context, kRdsniSQDepth,
                                                nullptr, nullptr, 0);
        assert(cb->dgram_send_cq[i] != nullptr);

        size_t recv_queue_depth = ( i == 0 ) ? kRdsniRQDepth : 1;
        cb->dgram_recv_cq[i] = ibv_create_cq(cb->device_info.ib_context, recv_queue_depth,
                                                nullptr, nullptr, 0);
        assert(cb->dgram_recv_cq[i] != nullptr);

        struct ibv_qp_init_attr create_attr;
        memset( &create_attr, 0, sizeof(create_attr));
        
        create_attr.send_cq = cb->dgram_send_cq[i];
        create_attr.recv_cq = cb->dgram_recv_cq[i];
        create_attr.cap.max_send_wr = kRdsniSQDepth;
        create_attr.cap.max_send_sge = 1;
        create_attr.cap.max_inline_data = kRdsniMaxInline;

        create_attr.srq = nullptr;
        create_attr.cap.max_recv_wr = recv_queue_depth;
        create_attr.cap.max_recv_sge = 1;
        //if this value is set to 1, all send request will generate CQE, if set to 0, only wrs that are flagged will generate CQE
        create_attr.sq_sig_all = 0;
        create_attr.qp_type = IBV_QPT_UD;
        cb->dgram_qp[i] = ibv_create_qp( cb->pd, &create_attr );
        assert(cb->dgram_qp[i] != nullptr);

        struct ibv_qp_attr qp_attr;
        memset(&qp_attr, 0, sizeof(qp_attr));
        qp_attr.qp_state = IBV_QPS_INIT;
        qp_attr.pkey_index = 0;
        qp_attr.port_num = cb->device_info.dev_port_id;
        qp_attr.qkey = kRdsniDefaultQKey;
        uint64_t attr_comp_mask = 
             IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY;
        ibv_modify_qp(cb->dgram_qp[i], &qp_attr, attr_comp_mask);

        //RTR state
        struct ibv_qp_attr rtr_attr;
        memset(&rtr_attr, 0, sizeof(rtr_attr));
        rtr_attr.qp_state = IBV_QPS_RTR;
        ibv_modify_qp( cb->dgram_qp[i], &rtr_attr, IBV_QP_STATE );

        //RTS state
        struct ibv_qp_attr rts_attr;
        memset(&rts_attr, 0, sizeof(rts_attr));
        rts_attr.qp_state = IBV_QPS_RTS;
        rts_attr.sq_psn = kRdsniDefaultPSN;

        ibv_modify_qp( cb->dgram_qp[i], &rts_attr, IBV_QP_STATE | IBV_QP_SQ_PSN );

    }
}

/*
  * 队列连接 the @conn_qp_index th qp of @cb connected to desnition qp @remote_qp_attr 
  * only for reliable connected qps 
  * 用于rc/uc send/write rc read
  * server端与client端qp一对一连接 ibv_ah describe the route from local node to remote node
*/
void rdsni_connect_qps(rdsni_resources_blk *cb, size_t conn_qp_index, 
        rdsni_qp_attr_t *remote_qp_attr){
    
    assert( conn_qp_index < cb->conn_config.num_qps );
    assert( cb->conn_qp[conn_qp_index] != nullptr );
    assert( cb->device_info.dev_port_id >= 1 );

    struct ibv_qp_attr conn_attr;
    memset( &conn_attr, 0, sizeof(struct ibv_qp_attr));
    conn_attr.qp_state = IBV_QPS_RTR;
    conn_attr.path_mtu = IBV_MTU_4096;
    /*
     * A 24 bits value of the remote QP number of RC and UC QPs;
     * when sending data, packets will be sent to this QP number and
     * when receiving data, packets will be accepted only from this QP number
     *
     */
    conn_attr.dest_qp_num = remote_qp_attr->qpn;
    conn_attr.rq_psn = kRdsniDefaultPSN; //packet sequence number for receive queue, should match remote QP's sq_psn
    conn_attr.ah_attr.is_global = 0;
    conn_attr.ah_attr.dlid = remote_qp_attr->lid;
    conn_attr.ah_attr.sl = 0;
    conn_attr.ah_attr.src_path_bits = 0;
    conn_attr.ah_attr.port_num = cb->device_info.dev_port_id; //local physical port the packet will be sent from

    int rtr_flags = IBV_QP_STATE | IBV_QP_AV 
                | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN 
                | IBV_QP_RQ_PSN;
    if( !cb->conn_config.use_uc ){
        conn_attr.max_dest_rd_atomic = cb->conn_config.max_rd_atomic;
        conn_attr.min_rnr_timer = 12;
        rtr_flags |= (IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
    }
    if( ibv_modify_qp( cb->conn_qp[conn_qp_index], &conn_attr, rtr_flags)){
        fprintf(stderr, "Failed to modify conn QP to RTR\n");
        assert(false);
    }

    memset(&conn_attr, 0, sizeof(conn_attr));
    conn_attr.qp_state = IBV_QPS_RTS;
    conn_attr.sq_psn = kRdsniDefaultPSN;

    int rts_flags = IBV_QP_STATE | IBV_QP_SQ_PSN;

    if( !cb->conn_config.use_uc ){
        conn_attr.timeout = 14;
        conn_attr.retry_cnt = 7;
        conn_attr.rnr_retry = 7;
        conn_attr.max_rd_atomic = cb->conn_config.max_rd_atomic;
        conn_attr.max_dest_rd_atomic = cb->conn_config.max_rd_atomic;
        rts_flags |= (IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT 
                | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC);
    }
    if( ibv_modify_qp(cb->conn_qp[conn_qp_index], &conn_attr, rts_flags) ){
        fprintf(stderr, "Failed to modify conn QP to RTS\n");
        assert(false);
    }
}
