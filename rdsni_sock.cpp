#include "sock.h"
#include "log.h"


/*
 *  server端调用
 *  等待客户端的连接 获取客户端qp信息 同client端同步
 */
int tcp_server_listen(int port){

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(port);

    int on = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    int rt1 = bind(listen_fd, (struct sockaddr *)(&server_addr), sizeof(server_addr));
    if(rt1 < 0 ){
        close(listen_fd);
        fprintf(stderr, "failed to bind on server\n");
        exit(-1);
    }

    int rt2 = listen(listen_fd, 5);
    if( rt2 < 0 ){
        close(listen_fd);
        fprintf(stderr, "failed to listen on server\n");
        exit(-1);
    }
    
    return listen_fd;
}

/*
 *  client端调用
 *  @server_name: IP addr of server
 *  @port: service name of server
 */ 
int tcp_client_connect(char *server_name, int port){

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, server_name, &server_addr.sin_addr);

    rdsni_msgx("client- server_name para %s %u", server_name, sizeof(server_addr));

    int connect_rt = connect(sockfd,(struct sockaddr*)&server_addr,sizeof(server_addr));
    
    rdsni_msgx("client connected to server server_name %s socket %d", server_name, sockfd);
    if( connect_rt < 0 ){
        close(sockfd);
        fprintf(stderr, "client failed to connect\n");
        exit(-1);
    }
    return sockfd;
}

/*
 * 1、用于交换server-client间的qp信息，填充到struct rdsni_qp_attr中
 * 2、用于server-client端同步
 */
ssize_t sock_read(int sock_fd, void *buffer, size_t len){
    
    ssize_t nr, tot_read;
    char *buf = (char *)buffer; // avoid pointer arithmetic on void pointer

    tot_read = 0;
    while( len != 0 && (nr = read(sock_fd, buf, len))!=0){
        if(nr < 0){
            if( errno == EINTR ){
                continue;
            }else{
                return -1; // read error
            }
        }
            buf +=nr;
            tot_read += nr;
            len -=nr;
    }

    return tot_read;

}

ssize_t sock_write(int sock_fd, void *buffer, size_t len){
    
    ssize_t nw, tot_written;
    const char *buf = (const char *)buffer;

    for(tot_written = 0; tot_written < len; ){
        nw = write(sock_fd, buf,len - tot_written);

        if( nw <= 0 ){
            if( nw == -1 && errno == EINTR ){
                continue;
            }else{
                return -1;
            }
        }
        tot_written +=nw;
        buf +=nw;
    }
    return tot_written;
}


