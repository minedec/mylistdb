#include <vector>
#include <string>
#include <iostream>
#include "listdb/db_client.h"
#include "listdb/listdb.h"
#include "listdb/core/delegation.h"
#include "listdb/core/simple_ring.h"

ListDB* db_;
DelegatePool* dp_;
static std::vector<long> keys;
static std::vector<long> values;

int client_num = 10;
int keynum = 200000;
int key_size = 16;
int value_size = 1024;

std::atomic<uint64_t> bytes  {0};

std::vector<std::thread*> dele_clients;
std::vector<std::thread*> local_clients;

std::string_view AllocateKey(std::unique_ptr<const char[]>* key_guard) {
  char* data = new char[key_size];
  const char* const_data = data;
  key_guard->reset(const_data);
  return std::string_view(key_guard->get(), key_size);
}

std::string_view AllocateValue() {
  char* data = new char[value_size];
  const char* const_data = data;
  return std::string_view(const_data, value_size);
}

void GenerateKeyFromInt(uint64_t v, std::string_view* key) {
    if (v == 0) v++;
    char* start = const_cast<char*>(key->data());
    char* pos = start;

    int bytes_to_fill = std::min(key_size - static_cast<int>(pos - start), 8);
    memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
    pos += bytes_to_fill;
    if (key_size > pos - start) {
      memset(pos, '0', key_size - (pos - start));
    }
}

void GenerateValFromInt(uint64_t v, std::string_view* val) {
  if (v == 0) v++;
    char* start = const_cast<char*>(val->data());
    char* pos = start;

    int bytes_to_fill = std::min(value_size - static_cast<int>(pos - start), 8);
    memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
    pos += bytes_to_fill;
    if (value_size > pos - start) {
      memset(pos, '0', value_size - (pos - start));
    }
}

void delegate_func(int keynum, DBClient* client) {
  std::unique_ptr<const char[]> key_guard;
  std::string_view key = AllocateKey(&key_guard);
  uint64_t total_byte;

  for(int i = 0; i < keynum; i++) {
    std::string_view val = AllocateValue();
    GenerateKeyFromInt(keys[i], &key);
    GenerateValFromInt(values[i], &val);
    total_byte += key_size;
    total_byte += value_size;
    client->PutStringKV(key, val);
  }
  bytes.fetch_add(total_byte);
}

void local_func(int keynum, DBClient* client) {
  std::unique_ptr<const char[]> key_guard;
  std::string_view key = AllocateKey(&key_guard);
  uint64_t total_byte;

  for(int i = 0; i < keynum; i++) {
    std::string_view val = AllocateValue();
    GenerateKeyFromInt(keys[i], &key);
    GenerateValFromInt(values[i], &val);
    total_byte += key_size;
    total_byte += value_size;
    client->PutStringKVHook(key, val);
  }
  bytes.fetch_add(total_byte);
}

void delegate_put_test(int num_threads, int keynum) {
  std::vector<DBClient*> clients(num_threads);
  uint64_t start_ = DBClient::GetPutStringKVCost();
  for(int i = 0; i < num_threads; i++) {
    clients[i] = new DBClient(db_, i, 0);
    dele_clients.push_back(new std::thread(delegate_func, keynum, clients[i]));
  }
  uint64_t start;
  uint64_t finish;
  start = Clock::NowMicros();
  for(int i = 0; i < num_threads; i++) {
    dele_clients[i]->detach();
  }
  dp_->Close();
  finish = Clock::NowMicros();
  printf("delegate_put_test detach wait time: %ld\n", finish - start);
  uint64_t finish_ = DBClient::GetPutStringKVCost();
  printf("--------------------------\n");
  printf("delegate client num: %d\n", num_threads);
  printf("delegate delegate num: %d\n", kDelegateNumWorkers);
  printf("total key num: %d\n", num_threads * keynum);
  printf("total time cost: %ldns\n", finish_ - start_);
  printf("per op time: %.3fns\n", ((double)finish_ - start_) / (num_threads * keynum));
  printf("throughput %.3f Mb/s\n", (bytes / 1048576.0) / ((finish - start) * 1e-6));
  printf("---------------------------\n");
}

void local_put_test(int num_threads, int keynum) {
  std::vector<DBClient*> clients(num_threads);
  uint64_t start_ = DBClient::GetPutStringKVCost();

  uint64_t start;
  uint64_t finish;
  start = Clock::NowMicros();
  for(int i = 0; i < num_threads; i++) {
    clients[0] = new DBClient(db_, i, 0);
    local_clients.push_back(new std::thread(local_func, keynum, clients[0]));
  }
  for(int i = 0; i < num_threads; i++) {
    if(local_clients[i]->joinable()) {
      local_clients[i]->join();
    }
  }
  finish = Clock::NowMicros();
  uint64_t finish_ = DBClient::GetPutStringKVCost();
  printf("--------------------------\n");
  printf("local client num: %d\n", num_threads);
  printf("total key num: %d\n", num_threads * keynum);
  printf("total time cost: %ldns\n", finish_ - start_);
  printf("per op time: %.3fns\n", ((double)finish_ - start_) / (num_threads * keynum));
  printf("throughput %.3f Mb/s\n", (bytes / 1048576.0) / ((finish - start) * 1e-6));
  printf("---------------------------\n");
}



int main() {
  db_ = new ListDB();
  db_->Init();

  for(int i = 0; i < keynum; i++) {
    keys.push_back(random());
    values.push_back(random());
  }

  // rbp->Close();
  // dp_->Close();
  uint64_t start;
  uint64_t finish;
  bytes = 0;

  printf("start local test\n");
  start = Clock::NowMicros();
  local_put_test(client_num, keynum);
  finish = Clock::NowMicros();
  printf("local_put_test time: %ld\n", finish - start);
  printf("end local test\n");

  bytes = 0;
  RingBufferPool* rbp = new RingBufferPool();
  rbp->Init();
  db_->ring_buffer_pool = rbp;
  dp_ = new DelegatePool();
  dp_->db_ = db_;
  dp_->Init();
  db_->delegate_pool = dp_;
  
  sleep(5);

  printf("start delegate test\n");
  start = Clock::NowMicros();
  delegate_put_test(client_num, keynum);
  finish = Clock::NowMicros();
  printf("delegate_put_test time: %ld\n", finish - start);
  printf("end delegate test\n");
}