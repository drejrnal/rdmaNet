#include "ib_comm.h"
#include "log.h"

static constexpr size_t MAX_POST_LIST = 128; //往send/receive queue一次batch操作
static constexpr size_t BUF_SIZE = 4096; //注册到MR的缓冲区大小
static constexpr size_t SIGNAL_BATCH = 16; //每隔SIGNAL_BATCH个WR向对应的CQ写入CQE

int rdsni_post_send(struct rdsni_resources_blk *cb, int qp_i,  
        struct rdsni_qp_attr_t *remote_qp_info){
    struct ibv_send_wr wr, *bad_send_wr;
    struct ibv_sge sgl;
    size_t inline_data_size = 40;


    //uint8_t *source_buf = new uint8_t[inline_data_size];
    //memset(source_buf,1, inline_data_size);

    struct ibv_ah *ah; //associated with each WR in send queue
    struct ibv_ah_attr ah_attr;
    memset(&ah_attr, 0, sizeof(ah_attr));
    ah_attr.is_global = 0;
    ah_attr.dlid = remote_qp_info->lid; //destinition lid
    ah_attr.sl = 0;
    ah_attr.src_path_bits = 0;
    ah_attr.port_num = cb->device_info.dev_port_id; //physical port number to use to reach destination

    ah = ibv_create_ah(cb->pd, &ah_attr);
    assert( ah != nullptr );


    memset(&wr, 0, sizeof(wr));
    memset(&sgl, 0, sizeof(sgl));

    wr.opcode = IBV_WR_SEND;
    //wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.num_sge = 1;

    char *Response = static_cast<char *>(malloc( cb->dgram_buf_size ));
    snprintf( Response, BUF_SIZE, " response from server\n" );

    //sgl.addr = reinterpret_cast<uint64_t>(source_buf);
    sgl.addr = reinterpret_cast<uint64_t>( Response );
    sgl.length = cb->dgram_buf_size;
    sgl.lkey = cb->dgram_buf_mr->lkey;

    wr.sg_list = &sgl;
    //wr.imm_data = 3185;
    //wr.send_flags = IBV_SEND_SIGNALED;
    //wr.send_flags |= IBV_SEND_INLINE;

    wr.wr.ud.ah = ah;
    wr.wr.ud.remote_qpn = remote_qp_info->qpn;
    wr.wr.ud.remote_qkey = kRdsniDefaultQKey;


    int ret = ibv_post_send(cb->dgram_qp[qp_i], &wr, &bad_send_wr);
    assert( ret == 0 );

    return 0;
}


/*
 *  ud-send from qp in @cb to qp refered by remote_qp_info
 *  sender端第@qp_i个qp向@remote_qp_info对应的qp发送send
 *  @peer_counter: indicator for whether using signaled WR 
 *  return 0 on success
 */
int rdsni_post_batch_send(struct rdsni_resources_blk *cb, 
        int qp_i, struct rdsni_qp_attr_t *remote_qp_info, 
        int *peer_counter, int batch_size){

    char *source_buf = (char *)malloc(BUF_SIZE);

    struct ibv_ah *ah; //associated with each WR in send queue
    struct ibv_ah_attr ah_attr;
    memset(&ah_attr, 0, sizeof(ah_attr));
    ah_attr.is_global = 0;
    ah_attr.dlid = remote_qp_info->lid; //destinition lid
    ah_attr.sl = 0;
    ah_attr.src_path_bits = 0;
    ah_attr.port_num = cb->device_info.dev_port_id; //physical port number to use to reach destination

    ah = ibv_create_ah(cb->pd, &ah_attr);
    assert( ah != nullptr );

    struct ibv_send_wr wr[MAX_POST_LIST], *bad_send_wr;
    struct ibv_sge sgl[MAX_POST_LIST];
    //struct ibv_wc wc[MAX_POST_LIST];
    struct ibv_wc wc;

    for(int w_i = 0; w_i < batch_size; w_i++){
        wr[w_i].wr.ud.ah = ah;
        wr[w_i].wr.ud.remote_qpn = remote_qp_info->qpn;
        wr[w_i].wr.ud.remote_qkey = kRdsniDefaultQKey;

        wr[w_i].opcode = IBV_WR_SEND;
        wr[w_i].num_sge = 1;
        
        sgl[w_i].addr = reinterpret_cast<uint64_t>(source_buf);
        sgl[w_i].length = BUF_SIZE;
        sgl[w_i].lkey = cb->dgram_buf_mr->lkey;

        wr[w_i].sg_list = &sgl[w_i];
        wr[w_i].next = (w_i == (batch_size - 1) ? nullptr : &wr[w_i+1]);
        wr[w_i].send_flags = ( (*peer_counter & (SIGNAL_BATCH) == 0) ? IBV_SEND_SIGNALED : 0);
        
        if( (*peer_counter % SIGNAL_BATCH) == (SIGNAL_BATCH - 1) ){
            rdsni_msgx("server: Before polling send completion queue");
            rdsni_poll_cq(cb->dgram_send_cq[qp_i], 1, &wc);
            rdsni_msgx("server: After polling send completion queue");
        }
        (*peer_counter)++;
    }
    int ret = ibv_post_send(cb->dgram_qp[qp_i], &wr[0], &bad_send_wr);
    assert( ret == 0 );

    return 0;

}

int rdsni_post_read(struct rdsni_resources_blk *cb, int qp_i,  
        struct rdsni_qp_attr_t *remote_qp_info){
    struct ibv_send_wr wr, *bad_send_wr;
    struct ibv_sge sgl;
    
    memset(&wr, 0, sizeof(wr));
    memset(&sgl, 0, sizeof(sgl));

    wr.opcode = IBV_WR_RDMA_READ;
    wr.num_sge = 1;

    sgl.addr = reinterpret_cast<uint64_t>(cb->conn_buf);
    sgl.length = BUF_SIZE;
    sgl.lkey = cb->conn_buf_mr->lkey;

    wr.sg_list = &sgl;

    wr.send_flags = IBV_SEND_SIGNALED;

    wr.wr.rdma.remote_addr = remote_qp_info->buf_addr;
    wr.wr.rdma.rkey = remote_qp_info->rkey;

    wr.send_flags = IBV_SEND_SIGNALED;

    int ret = ibv_post_send(cb->conn_qp[qp_i], &wr, &bad_send_wr);
    assert( ret == 0 );

    return 0;
}


int rdsni_post_batch_read(struct rdsni_resources_blk *cb, int qp_i,
        struct rdsni_qp_attr_t *remote_qp_info, int *peer_counter, int batch_size){
    
    struct ibv_send_wr wr[MAX_POST_LIST], *bad_send_wr;
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_wc wc;

    for(int w_i = 0; w_i < batch_size; w_i++){
        
        wr[w_i].opcode = IBV_WR_RDMA_READ;
        wr[w_i].num_sge = 1;

        sgl[w_i].addr = reinterpret_cast<uint64_t>(cb->conn_buf + w_i * 8);
        sgl[w_i].length = BUF_SIZE;
        sgl[w_i].lkey = cb->conn_buf_mr->lkey;

        wr[w_i].sg_list = &sgl[w_i];
        wr[w_i].next = (w_i == (batch_size - 1) ? nullptr : &wr[w_i+1]);

        wr[w_i].wr.rdma.remote_addr = remote_qp_info->buf_addr + w_i * 8;
        wr[w_i].wr.rdma.rkey = remote_qp_info->rkey;

        wr[w_i].send_flags = ( (*peer_counter % SIGNAL_BATCH == 0) ? IBV_SEND_SIGNALED : 0);
        
        if( (*peer_counter%SIGNAL_BATCH == 0) && *peer_counter > 0 )
            rdsni_poll_cq(cb->conn_cq[qp_i], 1, &wc);

        *peer_counter++;
    }
    int ret = ibv_post_send(cb->conn_qp[qp_i], &wr[0], &bad_send_wr);
    assert( ret == 0 );

    return 0;
}

int rdsni_post_write(struct rdsni_resources_blk *cb, int qp_i,  
        struct rdsni_qp_attr_t *remote_qp_info, uint32_t  imm){

    struct ibv_send_wr wr, *bad_send_wr;
    struct ibv_sge sgl;
    
    memset(&wr, 0, sizeof(wr));
    memset(&sgl, 0, sizeof(sgl));

    if( imm != 0 ){
        wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        wr.imm_data = imm;
    }else{
        wr.opcode = IBV_WR_RDMA_WRITE;
    }
    wr.num_sge = 1;

    sgl.addr = reinterpret_cast<uint64_t>(cb->conn_buf);
    sgl.length = BUF_SIZE;
    sgl.lkey = cb->conn_buf_mr->lkey;

    wr.sg_list = &sgl;

    wr.send_flags = IBV_SEND_SIGNALED;

    rdsni_msgx("post_write()- remote_addr %u, remote qpn:%d,remote rkey %d", 
            remote_qp_info->buf_addr, remote_qp_info->qpn,remote_qp_info->rkey);
    wr.wr.rdma.remote_addr = remote_qp_info->buf_addr;
    wr.wr.rdma.rkey = remote_qp_info->rkey;

    wr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_qp_attr qp_attr;
    struct ibv_qp_init_attr init_qp_attr;
    int query_flags = IBV_QP_STATE | IBV_QP_DEST_QPN;
    if(ibv_query_qp(cb->conn_qp[qp_i],&qp_attr, query_flags,&init_qp_attr) != 0){
        rdsni_msgx("rdsni_post_write()- query qp failed");
    }else{
        rdsni_msgx("rdsni_post_write()- qp state:%d, dest qpn:%d", 
                            qp_attr.qp_state, qp_attr.dest_qp_num);
    }
    int ret = ibv_post_send(cb->conn_qp[qp_i], &wr, &bad_send_wr);
    assert( ret == 0 );

    return 0;
}

/**
 * @Param qp_i connected qp number from which write request send
 * @Param remote_qp_info qp and MR attribute from another end
 * @Param byte_per_wr 每个wr携带的数据量
 * @Param counter 目前已经send的次数
 * @Param batch_size 调用者计算好，传入函数内，比如如果想传输4K大小的数据，
 *        每个wr携带数据量为512，则可将batch size设置为8次
 */

int rdsni_post_batch_write(struct rdsni_resources_blk *cb, int qp_i,  
        struct rdsni_qp_attr_t *remote_qp_info, int byte_per_wr, int *counter,
        int signal_batch, int batch_size, uint32_t imm){
    struct ibv_send_wr wr[MAX_POST_LIST], *bad_send_wr;
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_wc wc;

    memset(wr, 0, sizeof(wr) );
    memset(sgl, 0, sizeof(sgl) );

    for(int w_i = 0; w_i < batch_size; w_i++){
        
        if( imm != 0 ){
            wr[w_i].opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            wr[w_i].imm_data = imm;
        }else{
            wr[w_i].opcode = IBV_WR_RDMA_WRITE;
        }
        wr[w_i].num_sge = 1;

        sgl[w_i].addr = reinterpret_cast<uint64_t>(cb->conn_buf + (*counter) * byte_per_wr);
        sgl[w_i].length = byte_per_wr;
        sgl[w_i].lkey = cb->conn_buf_mr->lkey;

        wr[w_i].sg_list = &sgl[w_i];
        wr[w_i].next = (w_i == (batch_size - 1) ? nullptr : &wr[w_i+1]);

        wr[w_i].wr.rdma.remote_addr = remote_qp_info->buf_addr + *(counter) * byte_per_wr;
        wr[w_i].wr.rdma.rkey = remote_qp_info->rkey;

        wr[w_i].send_flags = ((*counter)%signal_batch==0) ? IBV_SEND_SIGNALED : 0;

        (*counter)++;
    }
    int ret = ibv_post_send(cb->conn_qp[qp_i], &wr[0], &bad_send_wr);
    /*
        * 比如编号为0-7 8-15的wr完成send后，此时*counter=15，poll from send cq
        * 后续send增加*counter的值 需要满足SIGNAL_BATCH被batch_size整除
        */
    if( *counter > 0 && (*counter%(signal_batch) == 0) )
        rdsni_poll_cq(cb->conn_cq[qp_i], 1, &wc);
    assert( ret == 0 );

    return 0;
}

/*
 *  Post 1/batch_size RECV for this dgram qp
 *  参数为receive queue sgl list 的属性
 *  当前，每个recv request对应一个ibv_sge, 后续可使用ibv_sge数组扩大接收缓冲空间
 *  buf_addr需转成uint64_t
 */
void rdsni_post_batch_recv(struct ibv_qp *qp, uint64_t  buf_addr, size_t len, uint32_t lkey,int batch_size){
    
    struct ibv_recv_wr recv_wr[MAX_POST_LIST], *bad_recv_wr;
    struct ibv_sge sgl[MAX_POST_LIST];

    size_t recv_offset = 0;
    while ( recv_offset < len + 40 ) recv_offset +=64;
    rdsni_msgx("receive offset for each WR in receive queue:%d", recv_offset );

    for(int i = 0; i < batch_size; i++){
        sgl[i].addr = buf_addr + i * recv_offset;
        sgl[i].length = len + 40;
        sgl[i].lkey = lkey;

        recv_wr[i].sg_list = &sgl[i];
        recv_wr[i].num_sge = 1;
        recv_wr[i].next = (i == batch_size - 1)? NULL : &recv_wr[i+1];
    }

    if(ibv_post_recv(qp, &recv_wr[0],&bad_recv_wr)){
        rdsni_msgx("rdsni_post_batch_recv()- post recevie opt failed");
        exit(-1);
    }
}
/*
 * 注意此处buf_addr需要是ibv_reg_mr过的内存区域，否则recv qp对应的completion qp
 * 返回IBV_WC_LOC_PROT_ERR错误
 */
void rdsni_post_dgram_recv(struct ibv_qp *qp, void *buf_addr, size_t len, uint32_t lkey){
    struct ibv_sge sgl;
    struct ibv_recv_wr recv_wr;
    struct ibv_recv_wr *bad_wr;

    memset(&recv_wr, 0, sizeof(recv_wr));

    recv_wr.wr_id = 128; //为调试CQ是否完成该receive request
    sgl.addr = reinterpret_cast<uint64_t>(buf_addr);
    sgl.length = len;
    sgl.lkey = lkey;

    recv_wr.sg_list = &sgl;
    recv_wr.num_sge = 1;

    if(ibv_post_recv(qp, &recv_wr, &bad_wr)){
        rdsni_msgx("Error:%s, for rdsni_post_dgram_recv()", strerror(errno));
        exit(-1);
    }
}

