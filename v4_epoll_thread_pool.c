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

typedef struct{
    int fd;
}task_t;

pthread_mutex_t mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond=PTHREAD_COND_INITIALIZER;
int stop=0;     // 线程池停止标志：0运行，1停止
task_t *queue[QUEUE_MAX];   // 环形任务队列：存放malloc出来的fd
int qhead=0,qtail=0;    // 队列头下标(取任务)、队列尾下标(放任务)
pthread_t g_workers[WORKER_NUM];    // 存放所有worker线程ID的数组
int epfd;       //全局epoll fd，worker需要修改epoll事件

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
        free(t);
        // fd 已 ADD 进 epoll，队列满放不进任务时也要 DEL+close 兜底，否则成孤儿 fd
        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        return ;
    }

    queue[qtail]=t;
    qtail=(qtail+1)%QUEUE_MAX;

    ret=pthread_cond_signal(&cond);
    err_pth(ret,"enqueue signal error");
    ret=pthread_mutex_unlock(&mutex);
    err_pth(ret,"enqueue unlock error");
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
            break;                       // 只有收到 stop 才真正退出线程
        }

        task_t *p=queue[qhead];
        qhead=(qhead+1)%QUEUE_MAX;
        ret=pthread_mutex_unlock(&mutex);
        err_pth(ret,"worker unlock error");

        int fd=p->fd;
        free(p);

        char buf[1024];
        memset(buf,0,sizeof(buf));
        ssize_t r;
        int closed=0;                    // 本任务是否已 DEL+close
        while((r=recv(fd,buf,sizeof(buf)-1,0))>0){
            buf[r]='\0';
            printf("服务端已收到客户端%d信息:%s\n",fd,buf);

            int total=0;
            while(total<r){
                ssize_t s=send(fd,buf+total,r-total,0);
                if(s==-1){
                    if(errno==EINTR) continue;
                    perror("send");
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                    close(fd);
                    closed=1;
                    break;
                }
                total+=s;
            }
        }
        if(closed) continue;            // send 失败已关 fd，取下一个任务

        if(r==0){                        // 对端正常断开
            printf("客户端%d退出\n",fd);
            epoll_ctl(epfd,EPOLL_CTL_DEL,fd,NULL);
            close(fd);
            continue;                    // 回顶部，worker 不退出
        }
        if(r<0 && errno!=EAGAIN && errno!=EINTR){   // 真实错误（非 EAGAIN/EINTR）
            perror("recv");
            epoll_ctl(epfd,EPOLL_CTL_DEL,fd,NULL);
            close(fd);
            continue;
        }
        // 到此：r<0 且为 EAGAIN 或 EINTR → 连接仍存活，必须重新 ARM ONESHOT
        struct epoll_event ev;
        ev.data.fd=fd;
        ev.events=EPOLLIN|EPOLLONESHOT;
        if(epoll_ctl(epfd,EPOLL_CTL_MOD,fd,&ev)<0){
            perror("epoll MOD");
            epoll_ctl(epfd,EPOLL_CTL_DEL,fd,NULL);
            close(fd);
        }
    }
    return NULL;
}

int main(){
    signal(SIGPIPE,SIG_IGN);
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

    while(1){
        int n=epoll_wait(epfd,events,EVENTS_MAX,-1);
        if(n==-1){
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
                ev.events=EPOLLIN | EPOLLONESHOT;
                ret=epoll_ctl(epfd, EPOLL_CTL_ADD, newfd, &ev);
                if(ret==-1){
                    perror("newfd add epoll_ctl");
                    close(newfd);
                    continue;
                }
                enqueue(newfd);     // 入队
            }
            else{
                enqueue(fd);        // 客户端可读，直接丢fd给线程池
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
