#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/socket.h>
#include <strings.h>
#include <netinet/in.h>



int tcp_server_listen(int port);

int tcp_client_connect(char *server_name, int port);

ssize_t sock_read(int sock_fd, void *buffer, size_t len);

ssize_t sock_write(int sock_fd, void *buffer, size_t len);


