#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/socket.h>
#include <errno.h>
#include <signal.h>

/*
 * v2：多线程聊天室服务器
 * 模型：每连接一线程（pthread_create + pthread_detach 自回收）
 *  - 全局客户端表 g_clients[]，用互斥锁保护，线程安全
 *  - broadcast：把任意客户端的消息转发给其他所有在线客户端
 *  - fd 用 malloc 拷值传入线程，避免栈变量竞争
 *  - 忽略 SIGPIPE，避免对端断开时进程被信号杀死
 */
#define MAX_CLIENTS 64
int g_clients[MAX_CLIENTS];
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

int add_cli(int fd) {
    int ret = pthread_mutex_lock(&mutex);
    if (ret != 0) {
        fprintf(stderr, "add lock error:%s\n", strerror(ret));
        return 1;
    }

    int idx = -1;
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (g_clients[i] == -1) {
            g_clients[i] = fd;
            idx = i;
            break;
        }
    }

    ret = pthread_mutex_unlock(&mutex);
    if (ret != 0) {
        fprintf(stderr, "add unlock error:%s\n", strerror(ret));
        return 1;
    }
    return idx;
}

void del_cli(int fd) {
    int ret = pthread_mutex_lock(&mutex);
    if (ret != 0) {
        fprintf(stderr, "del lock error:%s\n", strerror(ret));
        return;
    }

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (g_clients[i] == fd) {
            g_clients[i] = -1;
            break;
        }
    }

    ret = pthread_mutex_unlock(&mutex);
    if (ret != 0) {
        fprintf(stderr, "add unlock error:%s\n", strerror(ret));
        return;
    }
}

void broadcast(int fd, const char *buf, int len) {
    int ret = pthread_mutex_lock(&mutex);
    if (ret != 0) {
        fprintf(stderr, "broad lock error:%s\n", strerror(ret));
        return;
    }

    for (int i = 0; i < MAX_CLIENTS; i++) {
        int cfd = g_clients[i];
        if (cfd == -1 || cfd == fd) {
            continue;   /* 跳过自己与空位 */
        }
        int total = 0;
        while (total < len) {
            ssize_t s = send(cfd, buf + total, len - total, 0);
            if (s < 0) {
                perror("send");
                break;
            }
            total += s;
        }
    }

    ret = pthread_mutex_unlock(&mutex);
    if (ret != 0) {
        fprintf(stderr, "broad unlock error:%s\n", strerror(ret));
        return;
    }
}

void *worker(void *arg) {
    int fd = *(int *)arg;
    free(arg);   /* 线程接管 fd 的拷贝，立即释放传入的内存 */

    int idx = add_cli(fd);
    if (idx == -1) {
        printf("聊天室已满,等待下次连接..\n");
        close(fd);
        return NULL;
    }
    printf("new client %d join\n", fd);

    char buf[1024];
    while (1) {
        memset(buf, 0, sizeof(buf));
        ssize_t r = recv(fd, buf, sizeof(buf) - 1, 0);
        if (r < 0) {
            if (errno == EINTR) continue;
            perror("recv");
            del_cli(fd);
            close(fd);
            break;
        } else if (r == 0) {
            printf("客户端%d退出\n", fd);
            del_cli(fd);
            close(fd);
            break;
        }

        buf[r] = '\0';
        printf("服务端收到客户端%d消息:%s\n", fd, buf);
        broadcast(fd, buf, r);
    }
    return NULL;   /* 所有退出路径都已在上文 del_cli+close，避免重复关闭 */
}

int main() {
    signal(SIGPIPE, SIG_IGN);   /* 对端断开时 send 会触发 SIGPIPE，忽略以免进程被杀 */
    memset(g_clients, -1, sizeof(g_clients));

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
    printf("服务器已启动,监听8888端口...\n");

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

        int *pfd = (int *)malloc(sizeof(int));
        if (pfd == NULL) {
            perror("malloc");
            continue;
        }
        *pfd = newfd;

        pthread_t tid;
        ret = pthread_create(&tid, NULL, worker, (void *)pfd);
        if (ret != 0) {
            fprintf(stderr, "pthread_create error:%s\n", strerror(ret));
            close(newfd);
            free(pfd);
            continue;
        }

        ret = pthread_detach(tid);   /* 线程结束自动回收，不堆积僵尸 */
        if (ret != 0) {
            fprintf(stderr, "pthread_detach error:%s\n", strerror(ret));
            close(newfd);
            free(pfd);
            continue;
        }
    }

    close(sfd);
    return 0;
}
