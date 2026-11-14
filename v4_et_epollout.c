#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <fcntl.h>

#define WORKER_NUM 4    // 预创建的worker工作线程数量
#define QUEUE_MAX 1024  // 环形任务队列最大容量
#define EVENTS_MAX 1024  //epoll_wait 一次最多处理就绪事件
#define OUTBUF_MAX 8192  //单个连接的用户层写缓冲区最大字节 8192

typedef struct{
    int fd;
}task_t;

typedef struct{
    char buf[OUTBUF_MAX];   // 存放待发送字节
    int len;    // 待发总长度（有效数据总大小）
    int sent;   // 已经发送出去的偏移量：下一次send从 buf+sent 开始发
    int used;   // 1：这个缓冲区有pending数据；0：空闲、无待发数据
}outbuf_t;

pthread_mutex_t mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond=PTHREAD_COND_INITIALIZER;
int stop=0;     // 线程池停止标志：0运行，1停止
task_t *queue[QUEUE_MAX];   // 环形任务队列：存放malloc出来的fd
int qhead=0,qtail=0;    // 队列头下标(取任务)、队列尾下标(放任务)
pthread_t g_workers[WORKER_NUM];    // 存放所有worker线程ID的数组
int epfd;       //全局epoll fd，worker需要修改epoll事件
outbuf_t g_outbuf[65536];   //用 fd 作下标（demo 假设 fd < 65536）

void err_pth(int ret,const char *str){
    if(ret!=0){
        fprintf(stderr,"%s : %s\n",str,strerror(ret));
        pthread_exit (NULL);
    }
}

void set_nonblock(int fd){
    int flags=fcntl(fd,F_GETFL,0);
    if(flags==-1){
        perror("flags fcntl");
        return;
    }

    int ret=fcntl(fd,F_SETFL,flags | O_NONBLOCK);
    if(ret==-1){
        perror("ret fcntl");
        return;
    }
}

void enqueue(int fd){
    task_t* t=(task_t *)malloc(sizeof(task_t));
    if(t==NULL){
        perror("malloc");
        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        return;
    }
    t->fd=fd;

    int ret=pthread_mutex_lock(&mutex);
    err_pth(ret,"enqueue lock error");

    if(((qtail+1)%QUEUE_MAX)==qhead){   // 队列满判断：(qtail+1)%QUEUE_MAX == qhead
        ret=pthread_mutex_unlock(&mutex);
        err_pth(ret,"enqueue full unlock error");
        fprintf(stderr,"task queue full\n");
        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        free(t);
        return ;
    }

    queue[qtail]=t;
    qtail=(qtail+1)%QUEUE_MAX;

    ret=pthread_cond_signal(&cond);
    err_pth(ret,"enqueue signal error");
    ret=pthread_mutex_unlock(&mutex);
    err_pth(ret,"enqueue unlock error");
}

// 重新ARM ONESHOT：with_out=1 带上EPOLLOUT
void epoll_rearm(int fd,int with_out){
    struct epoll_event ev;
    ev.data.fd=fd;
    ev.events=EPOLLIN | EPOLLET | EPOLLONESHOT;
    if(with_out){
        ev.events |=EPOLLOUT;
    }

    int ret=epoll_ctl(epfd,EPOLL_CTL_MOD,fd,&ev);
    if(ret==-1){
        perror("rearm mod epoll_ctl");
        epoll_ctl(epfd,EPOLL_CTL_DEL,fd,NULL);
        close(fd);
        memset(&g_outbuf[fd],0,sizeof(outbuf_t));
        return;
    }
}

// 尝试发送；返回 0=发完 1=出错关闭 2=挂起等可写
int try_send(int fd,const char *data,int len){
    int total=0;
    while(total<len){
        ssize_t s=send(fd,data+total,len-total,0);
        if(s==-1){
            if(errno==EINTR)    continue;
            if(errno==EAGAIN){
                int rem=len-total;  // rem：还没发出去的剩余字节
                outbuf_t *ob=&g_outbuf[fd];

                if(ob->used){   // 已有pending数据 → 追加到写缓冲尾部
                    if(ob->len +rem >OUTBUF_MAX){
                        perror("outbuf full send");
                        epoll_ctl(epfd,EPOLL_CTL_DEL,fd,NULL);
                        close(fd);
                        memset(ob,0,sizeof(outbuf_t));
                        return 1;
                    }
                    memcpy(ob->buf+ob->len,data+total,rem);
                    ob->len+=rem;
                }
                else{           // 首次挂起：初始化写缓冲
                    memcpy(ob->buf,data+total,rem);
                    ob->len=rem;
                    ob->sent=0;
                    ob->used=1;
                }
                epoll_rearm(fd,1);
                return 2;
            }

            perror("try send");
            epoll_ctl(epfd,EPOLL_CTL_DEL,fd,NULL);
            close(fd);
            memset(&g_outbuf[fd],0,sizeof(outbuf_t));
            return 1;
        }
        total+=s;
    }
    return 0;
}

// 刷新写缓冲（EPOLLOUT 触发时）；返回 0=发完 1=关闭 2=仍等可写
int flush_out(int fd){
    outbuf_t *ob=&g_outbuf[fd];
    if(!ob->used){
        return 0;
    }
    while(ob->sent < ob->len){
        ssize_t s=send(fd,ob->buf+ob->sent,ob->len-ob->sent,0);
        if(s==-1){
            if(errno==EINTR)    continue;
            if(errno==EAGAIN){   // 注意是 == 比较，不是 = 赋值！
                epoll_rearm(fd,1);
                return 2;
            }
            perror("flush send");
            epoll_ctl(epfd,EPOLL_CTL_DEL,fd,NULL);
            close(fd);
            memset(ob,0,sizeof(outbuf_t));
            return 1;
        }
        ob->sent+=s;
    }
    memset(ob, 0, sizeof(outbuf_t));   // 发完，清空写缓冲
    return 0;
}

void *worker(void *arg){
    (void )arg;
    while(1){
        int ret=pthread_mutex_lock(&mutex);
        err_pth(ret,"worker lock error");

        while(qhead==qtail && stop==0){
            ret=pthread_cond_wait(&cond,&mutex);
            err_pth(ret,"worker wait error");
        }
        if(qhead==qtail && stop==1){
            ret=pthread_mutex_unlock(&mutex);
            err_pth(ret,"worker unlock error");
            break;
        }

        task_t *p=queue[qhead];
        qhead=(qhead+1)%QUEUE_MAX;
        ret=pthread_mutex_unlock(&mutex);
        err_pth(ret,"worker unlock error");

        int fd=p->fd;
        free(p);

        // 1) 若该 fd 写缓冲还有待发数据（EPOLLOUT 触发），先 flush
        if(g_outbuf[fd].used){
            int fr=flush_out(fd);
            if(fr==1 || fr==2){
                continue;   // 关闭 或 仍等可写 → 回池
            }
            // fr==0：发完了，继续往下处理新可读数据
        }

        // 2) ET：循环 recv 到 EAGAIN
        char buf[1024];
        memset(buf,0,sizeof(buf));
        int closed=0;    // 本任务是否已 DEL+close
        while(1){
            ssize_t r=recv(fd,buf,sizeof(buf)-1,0);
            if(r>0){
                buf[r]='\0';
                printf("服务端已收到客户端%d信息:%s\n",fd,buf);

                int n=try_send(fd,buf,r);
                if(n==1){
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                    close(fd);
                    closed=1;
                    break;
                }
                if(n==2){
                    // send 遇 EAGAIN 挂起：不能 break，否则会漏读同批次其他数据。
                    // 继续循环把 recv 也读干净（非阻塞，最终 recv 返回 EAGAIN 自然退出），
                    // 所有数据都已挂进写缓冲，等 EPOLLOUT 再续发。
                    continue;
                }
                // n==0：本次读完回声，继续读下一段
            }
            else if(r == 0){
                printf("客户端%d退出\n",fd);
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                close(fd);
                closed=1;
                break;
            }
            else {  // r < 0
                if(errno==EINTR) continue;          // 信号打断 → 重试
                if(errno==EAGAIN) break;            // ET 已读干净 → 退出循环
                perror("recv");
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                close(fd);
                closed=1;
                break;
            }
        }
        if(closed) continue;

        // 存活：按是否还有 pending 写，决定是否带 EPOLLOUT 重新 ARM
        epoll_rearm(fd, g_outbuf[fd].used);
    }
    return NULL;
}

void handler(int sig)
{
    (void)sig;
    stop = 1;
}

int main(){
    signal(SIGPIPE,SIG_IGN);
    signal(SIGINT, handler);   // Ctrl+C 优雅退出
    int lfd=socket(AF_INET,SOCK_STREAM,0);
    if(lfd==-1){
        perror("socket");
        return 1;
    }

    struct sockaddr_in ser_addr;
    memset(&ser_addr,0,sizeof(ser_addr));
    ser_addr.sin_family=AF_INET;
    ser_addr.sin_port=htons(8888);
    ser_addr.sin_addr.s_addr=htonl(INADDR_ANY);

    int opt=1;
    setsockopt(lfd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

    int ret=bind(lfd,(struct sockaddr *)&ser_addr,sizeof(ser_addr));
    if(ret==-1){
        perror("bind");
        return 1;
    }

    ret=listen(lfd,128);
    if(ret==-1){
        perror("listen");
        return 1;
    }
    printf("服务器已启动,端口8888正在监听...\n");

    epfd=epoll_create1(0);
    if(epfd==-1){
        perror("epoll_create1");
        return 1;
    }
    struct epoll_event ev,events[EVENTS_MAX];
    ev.data.fd=lfd;
    ev.events=EPOLLIN;
    ret=epoll_ctl(epfd,EPOLL_CTL_ADD,lfd,&ev);
    if(ret==-1){
        perror("lfd add epoll_ctl");
        return 1;
    }

    for(int i=0;i<WORKER_NUM;i++){
        ret=pthread_create(&g_workers[i],NULL,worker,NULL);
        err_pth(ret,"pthread create error");
        printf("worker=%d\n",i);
    }

    while(!stop){
        int n=epoll_wait(epfd,events,EVENTS_MAX,-1);
        if(n==-1){
            if(stop) break;   // 被信号打断且已置退出标志 → 退出主循环
            perror("epoll_wait");
            continue;
        }

        for(int i=0;i<n;i++){
            int fd=events[i].data.fd;
            if(fd==lfd){
                struct sockaddr_in cli_addr;
                socklen_t cli_len=sizeof(cli_addr);
                int newfd=accept(lfd,(struct sockaddr *)&cli_addr,&cli_len);
                if(newfd==-1){
                    perror("accept");
                    continue;
                }

                char ip[20];
                inet_ntop(AF_INET,&cli_addr.sin_addr,ip,sizeof(ip));
                printf("new client fd=%d,ip=%s,port=%d join\n",newfd,ip,ntohs(cli_addr.sin_port));

                set_nonblock(newfd);
                ev.data.fd=newfd;
                ev.events=EPOLLIN | EPOLLONESHOT | EPOLLET;
                ret=epoll_ctl(epfd, EPOLL_CTL_ADD, newfd, &ev);
                if(ret==-1){
                    perror("newfd add epoll_ctl");
                    close(newfd);
                    continue;
                }
                enqueue(newfd);     // 入队
            }
            else{
                enqueue(fd);        // 客户端可读/可写，直接丢fd给线程池
            }
        }
    }

    //退出代码
    stop=1;
    ret=pthread_cond_broadcast(&cond);      // 广播唤醒所有阻塞的worker
    err_pth(ret,"main broadcast error");
    for(int i=0;i<WORKER_NUM;i++){
        ret=pthread_join(g_workers[i],NULL);
        err_pth(ret,"main join error");
    }
    ret=pthread_mutex_destroy(&mutex);
    err_pth(ret,"main mutex destroy error");
    ret=pthread_cond_destroy(&cond);
    err_pth(ret,"main cond destroy error");

    close(epfd);
    close(lfd);
    return 0;
}
