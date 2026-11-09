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

#define WORKER_NUM 4    // 预创建的worker工作线程数量
#define QUEUE_MAX 1024  // 环形任务队列最大容量

pthread_mutex_t mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond=PTHREAD_COND_INITIALIZER;
int stop=0;     // 线程池停止标志：0运行，1停止
int *queue[QUEUE_MAX];   // 环形任务队列：存放malloc出来的fd指针
int qhead=0,qtail=0;    // 队列头下标(取任务)、队列尾下标(放任务)
pthread_t g_workers[WORKER_NUM];    // 存放所有worker线程ID的数组

void err_pth(int ret,const char *str){
    if(ret!=0){
        fprintf(stderr,"%s : %s\n",str,strerror(ret));
        pthread_exit (NULL);
    }
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

        int *p=queue[qhead];
        qhead=(qhead+1)%QUEUE_MAX;
        ret=pthread_mutex_unlock(&mutex);
        err_pth(ret,"worker unlock error");

        int fd=*p;
        free(p);

        char buf[1024];
        memset(buf,0,sizeof(buf));
        ssize_t r=recv(fd,buf,sizeof(buf)-1,0);
        if(r<0){
            if(errno==EINTR)    continue;
            perror("recv");
            close(fd);
            break;
        }
        else if(r==0){
            printf("客户端%d退出\n",fd);
            close(fd);
            break;
        }
        buf[r]='\0';
        printf("服务端已收到客户端%d信息:%s\n",fd,buf);

        int total=0;
        while(total<r){
            ssize_t s=send(fd,buf+total,r-total,0);
            if(s==-1){
                perror("send");
                break;
            }
            total+=s;
        }
        close(fd);
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

    for(int i=0;i<WORKER_NUM;i++){
        ret=pthread_create(&g_workers[i],NULL,worker,NULL);
        err_pth(ret,"pthread create error");
        printf("worker=%d\n",i);
    }

    while(1){
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

        int *pfd=(int *)malloc(sizeof(int));
        *pfd=newfd;

        ret=pthread_mutex_lock(&mutex);
        err_pth(ret,"main lock error");
        queue[qtail]=pfd;
        qtail=(qtail+1)%QUEUE_MAX;
        ret=pthread_cond_signal(&cond);
        err_pth(ret,"main signal error");
        ret=pthread_mutex_unlock(&mutex);
        err_pth(ret,"main unlock error");
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

    close(lfd);
    return 0;
}
