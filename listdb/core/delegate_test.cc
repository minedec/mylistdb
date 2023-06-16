#include <iostream>
#include "listdb/core/delegate.h"
#include "listdb/common.h"
#include "listdb/db_client.h"

int main() {
  DelegatePool* dp = new DelegatePool();
  dp->Init();

  for(int i = 0; i < 5; i++) {
    Task* t = nullptr;
    t = (Task*)malloc(sizeof(Task));
    printf("new Task %p\n", t);
    if(i%2) t->type = DelegateType::kPut;
    else t->type = DelegateType::kGet;
    if(t->type == DelegateType::kGet) {
      t->promise = new std::promise<Value*>();
      
    }
    t->region = i % 2;
    printf("add task region %d promise %p\n", t->region, t->promise);
    dp->AddTask(t);
    if(t->type == DelegateType::kGet) {
      t->promise->get_future().get();
      delete t->promise;
    }
    printf("loop %d promise %p\n", i, t->promise);
  }

  dp->Close();
  delete dp;
}