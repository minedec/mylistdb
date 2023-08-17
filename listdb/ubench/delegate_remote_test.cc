#include <vector>
#include <string>
#include <iostream>
#include "listdb/db_client.h"
#include "listdb/listdb.h"
#include "listdb/core/delegation.h"
#include "listdb/core/simple_ring.h"
#include "listdb/util/random.h"
#include "listdb/port/port_posix.h"

class ThreadState;

ListDB* db_;
DelegatePool* dp_;
static std::vector<long> keys;
static std::vector<long> values;

int client_num = 20;
int keynum = 500000;
int key_size = 16;
int value_size = 1024;
void (*local_test)(ThreadState*);
void (*delegate_test)(ThreadState*);

std::atomic<uint64_t> bytes  {0};
std::atomic<uint64_t> elapse {0};

std::vector<std::thread*> dele_clients;
std::vector<std::thread*> local_clients;

struct SharedState {
  std::mutex mu;
  std::condition_variable cv;
  int total;
  long num_initialized;
  long num_done;
  bool start;
  int keynum;
};

class Stats {
public:
  int id_;
  uint64_t start_;
  uint64_t finish_;
  double seconds_;
  uint64_t done_;
  uint64_t bytes_;
  uint64_t elapse_;

  void start() {
    start_ = Clock::NowMicros();
  }

  void end() {
    finish_ = Clock::NowMicros();
    elapse_ = finish_ - start_;
    seconds_ = (finish_ - start_) * 1e-6;
  }
};

struct ThreadState {
  int tid;
  Random64 rand;
  Stats stats;
  SharedState* shared;
  DBClient* client;
  void (*method)(ThreadState*);
  explicit ThreadState(int index) : tid(index), rand(1000 + index) {}
};

static void CompressibleString(Random* rnd, double compressed_fraction,
                                int len, std::string* dst) {
  int raw = static_cast<int>(len * compressed_fraction);
  if (raw < 1) raw = 1;
  std::string raw_data = rnd->RandomString(raw);

  // Duplicate the random data until we have filled "len" bytes
  dst->clear();
  while (dst->size() < (unsigned int)len) {
    dst->append(raw_data);
  }
  dst->resize(len);
}

class RandomGenerator {
 private:
  std::string data_;
  unsigned int pos_;

 public:
  RandomGenerator() {
    auto max_value_size = 102400;
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < (unsigned)std::max(1048576, max_value_size)) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      CompressibleString(&rnd, 0.5, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  std::string_view Generate(unsigned int len) {
    assert(len <= data_.size());
    if (pos_ + len > data_.size()) {
      pos_ = 0;
    }
    pos_ += len;
    return std::string_view(data_.data() + pos_ - len, len);
  }
};

enum WriteMode {
    RANDOM, SEQUENTIAL, UNIQUE_RANDOM
};

class KeyGenerator {
  public:
    KeyGenerator(Random64* rand, WriteMode mode, uint64_t num,
                 uint64_t /*num_per_set*/ = 64 * 1024)
        : rand_(rand), mode_(mode), num_(num), next_(0) {}

    uint64_t Next() {
      switch (mode_) {
        case SEQUENTIAL:
          return next_++;
        case RANDOM:
          return (rand_->Next() % (num_ - 1)) + 1;
        case UNIQUE_RANDOM:
          assert(next_ < num_);
          return values_[next_++];
      }
      return std::numeric_limits<uint64_t>::max();
    }

  private:
    Random64* rand_;
    WriteMode mode_;
    const uint64_t num_;
    uint64_t next_;
    std::vector<uint64_t> values_;
};

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

int GetCh() {
  unsigned long a,d,c;
  asm volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
  int chip = (c & 0xFFF000)>>12;
  //int core = c & 0xFFF;
  return chip;
}

int64_t GetRandomKey(Random64* rand, int& keynum) {
  uint64_t rand_int = rand->Next();
  int64_t key_rand;
  key_rand = (rand_int % (keynum - 1)) + 1;
  
  return key_rand;
}

void GenerateKeyFromInt(uint64_t v, std::string_view* key) {
    if (v == 0) v++;
    char* start = const_cast<char*>(key->data());
    char* pos = start;

    int bytes_to_fill = std::min(key_size - static_cast<int>(pos - start), 8);
    if(port::kLittleEndian) {
      for (int i = 0; i < bytes_to_fill; ++i) {
        pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
      }
    } else {
      memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
    }
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

void delegate_put_func(ThreadState* thread_state) {
  int keynum = thread_state->shared->keynum;
  DBClient* client = thread_state->client;
  std::unique_ptr<const char[]> key_guard;
  std::string_view key = AllocateKey(&key_guard);
  uint64_t total_byte = 0;
  RandomGenerator gen;
  KeyGenerator keygen(&(thread_state->rand), WriteMode::RANDOM, keynum);

  for(int i = 0; i < keynum; i++) {
    std::string_view val;
    uint64_t randnum = keygen.Next();
    GenerateKeyFromInt(randnum, &key);
    val = gen.Generate(value_size);
    total_byte += (key_size + val.size());
    client->PutStringKV(key, val);
  }
  bytes.fetch_add(total_byte);
}

void delegate_get_func(ThreadState* thread_state) {
  int64_t read = 0;
  int64_t found = 0;
  int64_t key_rand = 0;

  int keynum = thread_state->shared->keynum;
  DBClient* client = thread_state->client;

  std::unique_ptr<const char[]> key_guard;
  std::string_view key = AllocateKey(&key_guard);
  std::string val;
  val.reserve(value_size);

  uint64_t total_byte = 0;
  uint64_t tstart_ = Clock::NowMicros();
  
  for(int i = 0; i < keynum; i++) {
    key_rand = GetRandomKey(&thread_state->rand, keynum);
    GenerateKeyFromInt(key_rand, &key);
    read++;
    int s;
    uint64_t value_addr = 0;
    s = client->GetStringKV(key, &value_addr);
    if(s) {
      char* p = (char*)value_addr;
      size_t val_len = *((uint64_t*)p);
      p += sizeof(size_t);
      val.assign(p, val_len);
      found++;
      total_byte += (key_size + val.size());
    }
  }

  uint64_t tfinish_ = Clock::NowMicros();
  printf("thread elapse %ld\n", tfinish_ - tstart_);
  bytes.fetch_add(total_byte);
  elapse.fetch_add(tfinish_ - tstart_);
  printf("%lu of %lu found\n", found, keynum);
}

void local_put_func(ThreadState* thread_state) {
  int keynum = thread_state->shared->keynum;
  DBClient* client = thread_state->client;
  std::unique_ptr<const char[]> key_guard;
  std::string_view key = AllocateKey(&key_guard);
  uint64_t total_byte = 0;
  RandomGenerator gen;
  KeyGenerator keygen(&(thread_state->rand), WriteMode::SEQUENTIAL, keynum);
  uint64_t tstart_ = Clock::NowMicros();
  
  for(int i = 0; i < keynum; i++) {
    std::string_view val;
    int randnum = keygen.Next();
    GenerateKeyFromInt(randnum, &key);
    val = gen.Generate(value_size);
    total_byte += (key_size + val.size());
    client->PutStringKVHook(key, val);
  }

  uint64_t tfinish_ = Clock::NowMicros();
  bytes.fetch_add(total_byte);
  elapse.fetch_add(tfinish_ - tstart_);
}



void local_get_func(ThreadState* thread_state) {
  int64_t read = 0;
  int64_t found = 0;
  int64_t key_rand = 0;

  int keynum = thread_state->shared->keynum;
  DBClient* client = thread_state->client;

  std::unique_ptr<const char[]> key_guard;
  std::string_view key = AllocateKey(&key_guard);
  std::string val;
  val.reserve(value_size);

  uint64_t total_byte = 0;
  uint64_t tstart_ = Clock::NowMicros();
  
  for(int i = 0; i < keynum; i++) {
    key_rand = GetRandomKey(&thread_state->rand, keynum);
    GenerateKeyFromInt(key_rand, &key);
    read++;
    int s;
    uint64_t value_addr = 0;
    s = client->GetStringKVHook(key, &value_addr);
    if(s) {
      char* p = (char*)value_addr;
      size_t val_len = *((uint64_t*)p);
      p += sizeof(size_t);
      val.assign(p, val_len);
      found++;
      total_byte += (key_size + val.size());
    }
  }

  uint64_t tfinish_ = Clock::NowMicros();
  printf("thread elapse %ld\n", tfinish_ - tstart_);
  bytes.fetch_add(total_byte);
  elapse.fetch_add(tfinish_ - tstart_);
  printf("%lu of %lu found\n", found, keynum);
}

void delegate_thread_body(void* ptr) {
  ThreadState* thread_state = static_cast<ThreadState*>(ptr);
  SharedState* shared = thread_state->shared;
  int index = thread_state->stats.id_;
  DBClient* client = thread_state->client;

  {
    std::unique_lock<std::mutex> lk(shared->mu);
    shared->num_initialized++;
    if(shared->num_initialized >= shared->total) {
      shared->cv.notify_all();
    }
    shared->cv.wait(lk, [&]{return shared->start;});
  }
  
  thread_state->stats.start();
  thread_state->method(thread_state);
  thread_state->stats.end();

  {
    std::unique_lock<std::mutex> lk(shared->mu);
    shared->num_done++;
    if(shared->num_done >= shared->total) {
      shared->cv.notify_all();
    }
  }
}

void delegate_put_test(int num_threads, int keynum) {
  std::vector<DBClient*> clients(num_threads);
  uint64_t start_ = DBClient::GetPutStringKVCost();

  SharedState shared;
  shared.total = num_threads;
  shared.num_initialized = 0;
  shared.num_done = 0;
  shared.start = false;
  shared.keynum = keynum;

  std::vector<ThreadState*> tstates;

  for(int i = 0; i < num_threads; i++) {
    ThreadState* s = new ThreadState(i);
    int r = GetCh();
    s->client = new DBClient(db_, i, r);
    s->shared = &shared;
    s->method = delegate_test;
    tstates.push_back(s);
  }

  for(int i = 0; i < num_threads; i++) {
    dele_clients.push_back(new std::thread(delegate_thread_body, tstates[i]));
  }

  std::unique_lock<std::mutex> lk(shared.mu);
  shared.cv.wait(lk, [&]{return shared.num_initialized == num_threads;});
  
  uint64_t start;
  uint64_t finish;
  start = Clock::NowMicros();
  shared.start = true;
  shared.cv.notify_all();
  shared.cv.wait(lk, [&]{return shared.num_done == num_threads;});
  lk.unlock();
  
  for(int i = 0; i < num_threads; i++) {
    if(dele_clients[i]->joinable()) {
      dele_clients[i]->join();
    }
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
  printf("total bytes: %ld\n", bytes.load());
  printf("func time cost: %ld\n", finish - start);
  printf("throughput %.3f Mb/s\n", (bytes / 1048576.0) / ((finish - start) * 1e-6));
  printf("---------------------------\n");
}

void local_thread_body(void* ptr) {
  ThreadState* thread_state = static_cast<ThreadState*>(ptr);
  SharedState* shared = thread_state->shared;
  int index = thread_state->stats.id_;
  DBClient* client = thread_state->client;
  {
    std::unique_lock<std::mutex> lk(shared->mu);
    shared->num_initialized++;
    if(shared->num_initialized >= shared->total) {
      shared->cv.notify_all();
    }
    shared->cv.wait(lk, [&]{return shared->start;});
  }
  
  thread_state->stats.start();
  thread_state->method(thread_state);
  thread_state->stats.end();

  {
    std::unique_lock<std::mutex> lk(shared->mu);
    shared->num_done++;
    if(shared->num_done >= shared->total) {
      shared->cv.notify_all();
    }
  }
}

void local_put_test(int num_threads, int keynum) {
  std::vector<DBClient*> clients(num_threads);
  uint64_t start_ = DBClient::GetPutStringKVCost();
  SharedState shared;
  shared.total = num_threads;
  shared.num_initialized = 0;
  shared.num_done = 0;
  shared.start = false;
  shared.keynum = keynum;

  bytes = 0;

  std::vector<ThreadState*> tstates;
  
  for(int i = 0; i < num_threads; i++) {
    ThreadState* s = new ThreadState(i);
    int r = GetCh();
    s->client = new DBClient(db_, i, r);
    s->shared = &shared;
    s->method = local_test;
    tstates.push_back(s);
  }

  for(int i = 0; i < num_threads; i++) {
    local_clients.push_back(new std::thread(local_thread_body, tstates[i]));
  }

  std::unique_lock<std::mutex> lk(shared.mu);
  shared.cv.wait(lk, [&]{return shared.num_initialized == num_threads;});

  uint64_t start;
  uint64_t finish;
  start = Clock::NowMicros();
  shared.start = true;
  shared.cv.notify_all();
  shared.cv.wait(lk, [&]{return shared.num_done == num_threads;});
  lk.unlock();

  for(int i = 0; i < num_threads; i++) {
    if(local_clients[i]->joinable()) {
      local_clients[i]->join();
    }
  }

  finish = Clock::NowMicros();
  uint64_t finish_ = DBClient::GetPutStringKVCost();

  uint64_t state_cost = 0;
  for(int i = 0; i < num_threads; i++) {
    state_cost += tstates[i]->stats.elapse_;
  }
  printf("thread state cost %ld\n", state_cost / num_threads);
  printf("--------------------------\n");
  printf("local client num: %d\n", num_threads);
  printf("total key num: %d\n", num_threads * keynum);
  printf("total time cost: %ldns\n", finish_ - start_);
  printf("per op time: %.3fns\n", ((double)finish_ - start_) / (num_threads * keynum));
  printf("total bytes: %ld\n", bytes.load());
  printf("func time cost: %ld\n", finish - start);
  printf("throughput %.3f Mb/s\n", (bytes / 1048576.0) / ((finish - start) * 1e-6));
  printf("---------------------------\n");
}



int main() {
  db_ = new ListDB();
  db_->Init();

  local_test = &local_put_func;
  delegate_test = &delegate_put_func;

  for(int i = 0; i < keynum; i++) {
    keys.push_back(random());
    values.push_back(random());
  }

  uint64_t start;
  uint64_t finish;

  RingBufferPool* rbp = new RingBufferPool();
  rbp->Init();
  db_->ring_buffer_pool = rbp;
  dp_ = new DelegatePool();
  dp_->db_ = db_;
  dp_->Init();
  db_->delegate_pool = dp_;
  sleep(1);

  printf("start delegate test\n");
  start = Clock::NowMicros();
  delegate_put_test(client_num, keynum);
  finish = Clock::NowMicros();
  printf("delegate_put_test time: %ld\n", finish - start);

  // printf("start local test\n");
  // start = Clock::NowMicros();
  // local_put_test(client_num, keynum);
  // finish = Clock::NowMicros();
  // printf("local_put_test time: %ld\n", finish - start);
  // printf("end local test\n");

  // local_test = &local_get_func;
  // start = Clock::NowMicros();
  // local_put_test(client_num, keynum);
  // finish = Clock::NowMicros();
  // printf("local_get_test time: %ld\n", finish - start);
  // printf("end local test\n");

  // rbp->Init();
  // dp_->Init();
  // sleep(1);

  // delegate_test = &delegate_get_func;
  // start = Clock::NowMicros();
  // delegate_put_test(client_num, keynum);
  // finish = Clock::NowMicros();
  // printf("delegate_get_test time: %ld\n", finish - start);
  // printf("end delegate test\n");
}
