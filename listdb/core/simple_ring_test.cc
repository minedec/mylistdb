#include "listdb/core/simple_ring.h"
#include "listdb/db_client.h"

int threads = 20;
int nums = 10000;
int cnt = 0;

RingBufferPool* ring_pool = new RingBufferPool();
bool stop = false;

void GetClient() {
  RingBuffer* rb = ring_pool->GetRingBuffer(0,0);
  for(int i = 0; i < nums; i++) {
    Task* t = new Task();
    t->region = 1;
    t->type = 3;
    t->key = i;
    t->value = i+1;
    while(!ring_pool->SendRequest(rb, t));
  }
}

void PutClient() {
  RingBuffer* rb = ring_pool->GetRingBuffer(0,0);
  for(int i = 0; i < nums; i++) {
    Task* t = new Task();
    t->region = 1;
    t->type = 2;
    t->key = i;
    t->value = i+1;  
    while(!ring_pool->SendRequest(rb, t));
  }
}

void Consumer() {
  RingBuffer* rb = ring_pool->GetRingBuffer(0,0);
  Task* t;
  uint64_t start = Clock::NowMicros();
  while(!stop) {
    t = ring_pool->ReceiveRequest(rb);
    if(t == nullptr) continue;
    cnt++;
    delete t;
    if(cnt == nums * threads) break;
  }
  uint64_t finish = Clock::NowMicros();
  printf("consumer cost %ld\n", finish - start);
  printf("finish %d task\n", cnt);
}

int main() {
  ring_pool->Init();

  RingBuffer* rb = ring_pool->GetRingBuffer(0,0);

  
  for(int i = 0; i < threads; i++) {
    std::thread t(PutClient);
    t.detach();
  }
  std::thread t2(Consumer);
  t2.join();

  ring_pool->Close();
  return 0;
}