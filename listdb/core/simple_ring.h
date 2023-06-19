#ifndef SIMPLE_RING_H_
#define SIMPLE_RING_H_

// #include <spinglock.h>
#include "listdb/db_client.h"
#include "listdb/common.h"
#include "listdb/core/delegation.h"
#include <pthread.h>

static const int RING_BUFFER_SIZE = 1024 * 4;

struct ring_entry {
  Task* task;
  bool valid;
};

struct RingBuffer {
  struct ring_entry requests[RING_BUFFER_SIZE];
  pthread_spinlock_t spinlock;
  int tail_idx = 0;
  int head_idx = 0;
  int num_of_entry = RING_BUFFER_SIZE;
  int entry_size;
};


class RingBufferPool {
public:
  void Init();
  
  void Close();

  bool SendRequest(RingBuffer* ring, Task* task);

  Task* ReceiveRequest(RingBuffer* ring);

  RingBuffer* GetRingBuffer(int region, int index);

public:
  RingBuffer ring_buffer_pool[kNumRegions][kDelegateNumWorkers];
};

RingBuffer* RingBufferPool::GetRingBuffer(int region, int index) {
  return &ring_buffer_pool[region][index];
}

void RingBufferPool::Init() {
  for(int i = 0; i < kNumRegions; i++) {
    for(int j = 0; j < kDelegateNumWorkers; j++) {
       pthread_spin_init(&ring_buffer_pool[i][j].spinlock, PTHREAD_PROCESS_PRIVATE);
    }
  }
}

void RingBufferPool::Close() {
  for(int i = 0; i < kNumRegions; i++) {
    for(int j = 0; j < kDelegateNumWorkers; j++) {
       pthread_spin_destroy(&ring_buffer_pool[i][j].spinlock);
    }
  }
}

bool RingBufferPool::SendRequest(RingBuffer* ring, Task* t) {

  pthread_spin_lock(&ring->spinlock);

  if(ring->requests[ring->tail_idx].valid) {
    pthread_spin_unlock(&ring->spinlock);
    return false;
  }

  ring->requests[ring->tail_idx].task = t;
  ring->requests[ring->tail_idx].valid = true;

  ring->tail_idx = (ring->tail_idx + 1) % (ring->num_of_entry);

  pthread_spin_unlock(&ring->spinlock);

  return true;
}

Task* RingBufferPool::ReceiveRequest(RingBuffer* ring) {
  pthread_spin_lock(&ring->spinlock);

  if(!ring->requests[ring->head_idx].valid) {
    pthread_spin_unlock(&ring->spinlock);
    return nullptr;
  }

  Task* ret;
  ret = ring->requests[ring->head_idx].task;
  ring->requests[ring->head_idx].valid = false;

  ring->head_idx = (ring->head_idx + 1) % ring->num_of_entry;
  pthread_spin_unlock(&ring->spinlock);
  return ret;
}
#endif