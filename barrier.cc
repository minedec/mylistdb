#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#define PTHREAD_BARRIER_SIZE 4

pthread_barrier_t barrier;

void err_exit(const char *err_msg)
{
     printf("error:%s\n", err_msg);
     exit(1);
}

void *thread_fun(void *arg)
{
    
    int result;
    char *thr_name = (char *)arg;
    sleep(1);
    printf("线程%s工作完成...\n", thr_name);
    result = pthread_barrier_wait(&barrier);
    if (result == PTHREAD_BARRIER_SERIAL_THREAD)
                 printf("线程%s，wait后第一个返回\n", thr_name);
         else if (result == 0)
                 printf("线程%s，wait后返回为0\n", thr_name);
    return NULL;
}

int main(void)
{
    pthread_t tid_1, tid_2, tid_3;

    /* 初始化屏障 */
    pthread_barrier_init(&barrier, NULL, 4);
    char* a = "1";
    char* b = "2";
    char* c = "3";

    if (pthread_create(&tid_1, NULL, thread_fun, a) != 0)
        err_exit("create thread 1");

    if (pthread_create(&tid_2, NULL, thread_fun, b) != 0)
        err_exit("create thread 2");

    if (pthread_create(&tid_3, NULL, thread_fun, c) != 0)
        err_exit("create thread 3");

    /* 主线程等待工作完成 */
    
    pthread_barrier_wait(&barrier);
    printf("所有线程工作已完成...\n");
    printf("%d ...\n",_POSIX_THREADS);

    sleep(1);
    return 0;
}