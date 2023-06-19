#ifndef DELEGATE_H_
#define DELEGATE_H_
#include <iostream>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <future>
#include <numa.h>
#include <future>

#include <unistd.h>
#include <syscall.h>
#include "listdb/common.h"

static const int kDelegateQueueDepth = 4096;
static const int kDelegateNumWorkers = 40;
static std::thread main_delegate_thread;





#endif


