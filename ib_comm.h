#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <infiniband/verbs.h>
#include <unistd.h>
#include <assert.h>
#include "log.h"

static constexpr size_t kRdsniMaxUDQPs = 256;
static constexpr size_t kRdsniQPNameSize = 64;
static constexpr size_t kRdsniSQDepth = 128; // depth of all send queues
/*
 * 该参数决定可以往receive queue中发送recv请求数量的最大值
 */
static constexpr size_t kRdsniRQDepth = 2048; //depth of all receive queues
static constexpr size_t kRdsniPostlist = 64; //for batch send

static constexpr uint32_t kRdsniMaxInline = 16; //一次send inline操作inline最大数据大小
static constexpr uint32_t kRdsniDefaultQKey =  0x11111111;
static constexpr uint32_t kRdsniDefaultPSN =  3185;

#define CPE(val, msg, err_code)                \
  if (val) {                                   \
    fprintf(stderr, msg);                      \
    fprintf(stderr, " Error %d, %s \n", err_code,strerror(err_code)); \
    exit(err_code);                            \
  }

//remote direct storage and network interface

/*for connected qp configuration （由应用层提供的参数）
 * @Param num_qps 节点可创建的qp数量
 * @Param use_uc 该qp的类型rc/uc，用于修改qp_attr时作为判断依据
 * @Param buf_size 节点内所有qp关联的内存缓冲区的大小
*/
struct rdsni_conn_config_t{
    size_t num_qps;
    bool use_uc;

    size_t buf_size;

    uint8_t max_rd_atomic = 16;
    size_t sq_depth = kRdsniSQDepth;
};

struct rdsni_dgram_config_t{
    size_t num_qps;
    size_t buf_size;
};

/*registry info about a qp
 * .name name of this qp
 * .lid If the destination is in same subnet, the LID of the port to which the subnet delivers the packets to
 * .qpn qp number of this node
 */
struct rdsni_qp_attr_t{
    char name[kRdsniQPNameSize];
    uint16_t lid;
    uint32_t qpn;

    //Info about the RDMA buffer assosiated with this qp
    uintptr_t buf_addr;
    uint32_t buf_size;
    uint32_t rkey;
};


struct rdsni_resources_blk{
    size_t local_hid; //local ID on the machine this process runs on
    size_t port_index; //0-based across all devices

    struct{
        int device_id;
        uint8_t dev_port_id; //physical port id 1-based
        uint16_t port_lid; //lid of physical port 1-based
        struct ibv_context *ib_context;
    }device_info;

    struct ibv_pd *pd; //该节点对应的protection domain

    //创建conn_qp需要的条件
    struct rdsni_conn_config_t conn_config;
    struct ibv_qp **conn_qp; 
    struct ibv_cq **conn_cq;
    //volatile uint8_t *conn_buf;
    volatile char *conn_buf;
    struct ibv_mr *conn_buf_mr;//conn_qp执行操作所在的memory region

    //创建dgram_qp需要的条件
    size_t num_dgram_qps;
    struct ibv_qp *dgram_qp[kRdsniMaxUDQPs];
    struct ibv_cq *dgram_send_cq[kRdsniMaxUDQPs], *dgram_recv_cq[kRdsniMaxUDQPs];
    //volatile uint8_t *dgram_buf;
    volatile char *dgram_buf;
    size_t dgram_buf_size;
    struct ibv_mr *dgram_buf_mr;//dgram_qp执行操作所在的memory region

    uint8_t pad[64];

};

struct rdsni_resources_blk *rdsni_resources_init(size_t local_hid, size_t port_index,
                                                    rdsni_conn_config_t *conn_config,
                                                    rdsni_dgram_config_t *dgram_config);

int rdsni_resources_destroy(rdsni_resources_blk *resources);

/*
 * to fill the device info into dev_info of rdsni_resources_blk 
 */
void rdsni_resolve_port_index(rdsni_resources_blk *cb, size_t port_index);


/* qp创建(connected qp and dgram qp)
 *  @cb control block(resources struct) 
*/
void rdsni_create_conn_qps(rdsni_resources_blk *cb);

void rdsni_create_dgram_qps(rdsni_resources_blk *cb);

/*
  * 队列连接 the @conn_qp_index th qp of @cb connected to desnition qp @remote_qp_attr 
  * only for reliable connected qps 
*/
void rdsni_connect_qps(rdsni_resources_blk *cb, size_t conn_qp_index, 
                            rdsni_qp_attr_t *remote_qp_attr);

int connQP_connect_server( struct rdsni_resources_blk *cb, int port, 
        struct rdsni_qp_attr_t *remote_qp_info);

int connQP_connect_client(struct rdsni_resources_blk *cb,char *server_name, int port, 
        struct rdsni_qp_attr_t *remote_qp_info);

/*
 * 调用sock_write传递当前qp信息
 */ 
int sock_set_qp_info(int sockfd, struct rdsni_qp_attr_t *qp_info);

/*
 * 调用sock_read读取对端qp信息
 */
int sock_get_qp_info(int sockfd, struct rdsni_qp_attr_t *qp_info); 

/*
 * rdma read/write/send with batch_size wrs sending to a send queue
 */ 
int rdsni_post_send(struct rdsni_resources_blk *cb, int qp_i,struct rdsni_qp_attr_t *remote_qp_info);

int rdsni_post_read(struct rdsni_resources_blk *cb, int qp_i,struct rdsni_qp_attr_t *remote_qp_info);

int rdsni_post_write(struct rdsni_resources_blk *cb, int qp_i,struct rdsni_qp_attr_t *remote_qp_info, uint32_t imm);

/*
 * send read write batch processing
 */ 
int rdsni_post_batch_send(struct rdsni_resources_blk *cb,
           int qp_i, struct rdsni_qp_attr_t *remote_qp_info,
           int *peer_counter, int batch_size);

int rdsni_post_batch_read(struct rdsni_resources_blk *cb,int qp_i, struct rdsni_qp_attr_t *remote_qp_info, int *peer_counter, int batch_size);

int rdsni_post_batch_write(struct rdsni_resources_blk *cb,int qp_i, struct rdsni_qp_attr_t *remote_qp_info, int *peer_counter, int batch_size, uint32_t imm);


/*
 *  Post 1/batch_size RECV for this dgram qp 
 *  参数为receive queue sgl list 的属性
 *  buf_addr需转成uint64_t
 */
void rdsni_post_batch_recv(struct ibv_qp *qp, uint64_t buf_addr, size_t len, uint32_t lkey,int batch_size);

void rdsni_post_dgram_recv(struct ibv_qp *qp, void *buf_addr, size_t len, uint32_t lkey);


/*static inline void rdsni_poll_cq(struct ibv_cq *cq, int num_comps, struct ibv_wc *wc){

    int comps = 0;
    while( comps < num_comps ){
        int tmp_comps = ibv_poll_cq(cq, num_comps - comps, &wc[comps]);
        if( tmp_comps != 0 ){
            if(wc[comps].status != 0){
                rdsni_msgx("rdsni_poll_cq()- poll completion end with status %d", 
                                                                        wc[comps].status);
                exit(-1);
            }
        }
        comps +=tmp_comps;
    }
}*/

// Fill @wc with @num_comps comps from this @cq. Exit on error.
static inline void rdsni_poll_cq(struct ibv_cq* cq, int num_comps,
                               struct ibv_wc* wc) {
    int comps = 0;
    while (comps < static_cast<int>(num_comps)) {
        int new_comps = ibv_poll_cq(cq, num_comps - comps, &wc[comps]);
        if (new_comps != 0) {
            // Ideally, we should check from comps -> new_comps - 1
            if (wc[comps].status != 0) {
                fprintf(stderr, "Bad wc status %d\n", wc[comps].status);
                exit(0);
            }
            comps += new_comps;
        }
    }

}

