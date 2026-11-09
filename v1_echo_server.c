#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

/*
 * v1：阻塞单连接回声服务器
 * 模型：一次只服务一个客户端（内层 while 把它服务到底才回头 accept 下一个）
 * 这是演进起点，目的是熟悉完整的 socket 编程流程。
 */
int main() {
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd == -1) {
        perror("socket");
        exit(1);
    }

    struct sockaddr_in ser_addr;
    memset(&ser_addr, 0, sizeof(ser_addr));
    ser_addr.sin_family = AF_INET;
    ser_addr.sin_port = htons(8888);
    ser_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    int opt = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    int ret = bind(sfd, (struct sockaddr *)&ser_addr, sizeof(ser_addr));
    if (ret == -1) {
        perror("bind");
        exit(1);
    }

    ret = listen(sfd, 5);
    if (ret == -1) {
        perror("listen");
        exit(1);
    }

    printf("服务器已启动,开始监听8888端口...\n");

    while (1) {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int newfd = accept(sfd, (struct sockaddr *)&cli_addr, &cli_len);
        if (newfd == -1) {
            perror("accept");
            continue;
        }

        char ip[20];
        inet_ntop(AF_INET, &cli_addr.sin_addr, ip, sizeof(ip));
        printf("new client fd=%d,ip=%s,port=%d join\n", newfd, ip, ntohs(cli_addr.sin_port));

        char buf[1024];
        while (1) {
            memset(buf, 0, sizeof(buf));
            /* 留 1 字节给 '\0'，避免越界 */
            ssize_t r = recv(newfd, buf, sizeof(buf) - 1, 0);
            if (r < 0) {
                if (errno == EINTR) continue;   /* 被信号打断，重试 */
                perror("recv");
                break;
            } else if (r == 0) {
                printf("客户端%d退出\n", newfd);
                break;
            }

            buf[r] = '\0';
            printf("服务端收到信息:%s\n", buf);

            /* 循环 send 保证把数据完整发回（处理短写） */
            int total = 0;
            while (total < r) {
                ssize_t s = send(newfd, buf + total, r - total, 0);
                if (s < 0) {
                    perror("send");
                    break;
                }
                total += s;
            }
        }
        close(newfd);
    }

    close(sfd);
    return 0;
}
