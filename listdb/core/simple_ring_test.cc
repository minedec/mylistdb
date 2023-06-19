#include "listdb/core/simple_ring.h"
#include "listdb/db_client.h"

int nums = 10000;

RingBufferPool* ring_pool = new RingBufferPool();
bool stop = false;

void Producer() {
  RingBuffer* rb = ring_pool->GetRingBuffer(0,0);

  for(int i = 0; i < nums; i++) {
    Task* t = new Task();
    t->region = 1;
    t->type = 2;
    t->key = i;
    t->value = i+1;
    printf("send task %d\n", i);
    bool res = ring_pool->SendRequest(rb, t);
    if(!res) {
      printf("senf %d fail\n", i);
      continue;
    }
  }
}

void Consumer() {
  RingBuffer* rb = ring_pool->GetRingBuffer(0,0);

  int cnt = 0;
  Task* t;
  while(!stop) {
    t = ring_pool->ReceiveRequest(rb);
    if(t == nullptr) continue;
    printf("task region %d type %d key %ld value %ld\n", t->region, t->type, t->key, t->value);
    cnt++;
    delete t;
  }
  printf("finish %d task\n", cnt);
}

int main() {
  ring_pool->Init();

  RingBuffer* rb = ring_pool->GetRingBuffer(0,0);

  std::thread t1(Producer);
  std::thread t2(Consumer);
  t1.join();
  t2.join();

  // ring_pool->Close();
  return 0;
}