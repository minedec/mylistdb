#ifndef LISTDB_DB_CLIENT_H_
#define LISTDB_DB_CLIENT_H_

#include <algorithm>
#include <vector>
#include <future>
#include <atomic>

#include "listdb/common.h"
#include "listdb/listdb.h"
#include "listdb/util.h"
#include "listdb/util/random.h"

#include "listdb/core/delegation.h"
#ifdef RING_DELEGATE
  #include "listdb/core/simple_ring.h"
#endif

class DBClient;

enum DelegateType {       
  kPut, 
  kGet
};

struct Task {
  Key key = 0;
#if defined(LISTDB_STRING_KEY) && defined(LISTDB_WISCKEY)
  std::string_view string_key;
  std::string_view string_value;
#endif
  Value value;
  uint64_t pvalue = 0;
  int type = -1;
  std::promise<uint64_t> *promise = nullptr;
  DBClient* client = nullptr;
  int region;
  uint64_t id;
};

struct DelegateWorkerData {
  int id;
  int region;
  int index;
  bool stop;

  uint64_t start_;
  uint64_t finish_;
  
  std::mutex mu;
  std::condition_variable cv;
  Task* current_task;
#ifdef MUTEX_DELEGATE
  std::queue<Task*> q;
#endif 
#ifdef RING_DELEGATE
  struct RingBuffer* ring = nullptr;
#endif
};

static std::thread main_delegate_thread;

class DelegatePool {
public:
  void Init();

  void Close();

  void BackgroundMainLoop();

  void BackgroundDelegateLoop(DelegateWorkerData*);

  void AddTask(Task* task);

  int RandomPick();
  
public:
  ListDB* db_;
  DelegateWorkerData worker_data_[kNumRegions][kDelegateNumWorkers];

private:
  
  std::thread worker_thread_[kNumRegions][kDelegateNumWorkers];
  int tasks_assigned_num[kNumRegions][kDelegateNumWorkers];
  std::deque<Task*> worker_requests_;
  std::mutex task_mu_;
  std::condition_variable task_cv_;
  std::atomic_int finish_cnt = {0};
  std::atomic_int available_cnt = {kNumRegions * kDelegateNumWorkers};
  bool stop_;
};


#define LEVEL_CHECK_PERIOD_FACTOR 1
static std::atomic<uint64_t> putstringkv_time {0};

//#define LOG_NTSTORE
class DBClient {
 public:
  using MemNode = ListDB::MemNode;
  using PmemNode = ListDB::PmemNode;

  DBClient(ListDB* db, int id, int region);

  void SetRegion(int region);

  int GetRegion();

  void Put(const Key& key, const Value& value);

  bool Get(const Key& key, Value* value_out);

  void PutHook(const Key& key, const Value& value);

  bool GetHook(const Key& key, Value* value_out);

#if defined(LISTDB_STRING_KEY) && defined(LISTDB_WISCKEY)
  void PutStringKV(const std::string_view& key_sv, const std::string_view& value);
  bool GetStringKV(const std::string_view& key_sv, Value* value_out);
  void PutStringKVHook(const std::string_view& key_sv, const std::string_view& value);
  bool GetStringKVHook(const std::string_view& key_sv, Value* value_out);
#endif
  
  //void ReserveLatencyHistory(size_t size);
  
  size_t pmem_get_cnt() { return pmem_get_cnt_; }
  size_t search_visit_cnt() { return search_visit_cnt_; }
  size_t height_visit_cnt(int h) { return height_visit_cnt_[h]; }
  
  int RandomPick();

  static uint64_t GetPutStringKVCost();

 private:
  int DramRandomHeight();
  int PmemRandomHeight();

  static int KeyShard(const Key& key);

#ifdef LISTDB_EXPERIMENTAL_SEARCH_LEVEL_CHECK
  PmemPtr LevelLookup(const Key& key, const int pool_id, const int level, BraidedPmemSkipList* skiplist);
#endif
  PmemPtr Lookup(const Key& key, const int pool_id, BraidedPmemSkipList* skiplist);
  PmemPtr LookupL1(const Key& key, const int pool_id, BraidedPmemSkipList* skiplist, const int shard);

  ListDB* db_;
  int id_;
  int region_;
  int l0_pool_id_;
  int l1_pool_id_;
  Random rnd_;
  PmemLog* log_[kNumShards];
#ifdef LISTDB_WISCKEY
  PmemBlob* value_blob_[kNumShards];
#endif
  //BraidedPmemSkipList* bsl_[kNumShards];
  size_t pmem_get_cnt_ = 0;
  size_t search_visit_cnt_ = 0;
  size_t height_visit_cnt_[kMaxHeight] = {};

#ifdef GROUP_LOGGING
  struct LogItem {
    Key key;
    uint64_t tag;
    Value value;
    MemNode* mem_node;
    //uint64_t offset;
  };
  std::vector<LogItem> log_group_[kNumShards];
  size_t log_group_alloc_size_[kNumShards];
#endif

  //std::vector<std::chrono::duration<double>> latencies_;
#ifdef MUTEX_DELEGATE
  DelegatePool* dp_ = nullptr;
#endif
#ifdef RING_DELEGATE
  RingBufferPool* rbp_ = nullptr;
#endif
};

DBClient::DBClient(ListDB* db, int id, int region) : db_(db), id_(id), region_(region % kNumRegions), rnd_(id) {
  for (int i = 0; i < kNumShards; i++) {
    log_[i] = db_->log(region_, i);
#ifdef LISTDB_WISCKEY
    value_blob_[i] = db_->value_blob(region_, i);
#endif
  }
  l0_pool_id_ = db_->l0_pool_id(region_);
  l1_pool_id_ = db_->l1_pool_id(region_);

  // DG
#ifdef MUTEX_DELEGATE
  dp_ = db_->delegate_pool;
#endif
#ifdef RING_DELEGATE
  rbp_ = db_->ring_buffer_pool;
#endif
}

void DBClient::SetRegion(int region) {
  region_ = region;
  for (int i = 0; i < kNumShards; i++) {
    log_[i] = db_->log(region_, i);
#ifdef LISTDB_WISCKEY
    value_blob_[i] = db_->value_blob(region_, i);
#endif
  }
}

int DBClient::GetRegion() {
  return region_;
}

inline int DBClient::RandomPick() {
  return rnd_.Next() % kDelegateNumWorkers;
}

uint64_t DBClient::GetPutStringKVCost() {
  return putstringkv_time.load(std::memory_order::memory_order_relaxed);
}

void DBClient::Put(const Key& key, const Value& value) {
  Task *t = new Task();
  t->type = 0; // kPut = 0
  t->key = key;
  t->value = value;
  t->client = this;
  t->region = region_;
  t->id = rnd_.Next64();
#ifdef MUTEX_DELEGATE
  dp_->AddTask(t);
#endif
#ifdef RING_DELEGATE
  int index = RandomPick();
  while(!rbp_->SendRequest(rbp_->GetRingBuffer(region_, index), t)) {
    index = RandomPick();
  }
#endif
}

void DBClient::PutHook(const Key& key, const Value& value) {
#ifndef GROUP_LOGGING
  int s = KeyShard(key);

  uint64_t pmem_height = PmemRandomHeight();
  size_t iul_entry_size = sizeof(PmemNode) + (pmem_height - 1) * sizeof(uint64_t);
  size_t kv_size = key.size() + sizeof(Value);

  // Determine L0 id
  auto mem = db_->GetWritableMemTable(kv_size, s);
  uint64_t l0_id = mem->l0_id();

  // Write log
  auto log_paddr = log_[s]->Allocate(iul_entry_size);
  PmemNode* iul_entry = (PmemNode*) log_paddr.get();
#ifdef LOG_NTSTORE
  _mm_stream_pi((__m64*) &iul_entry->tag, (__m64) pmem_height);
  _mm_stream_pi((__m64*) &iul_entry->value, (__m64) value);
  //_mm_sfence();
  _mm_stream_pi((__m64*) &iul_entry->key, (__m64) (uint64_t) key);
#else
  iul_entry->tag = (l0_id << 32) | pmem_height;
  iul_entry->value = value;
  clwb(&iul_entry->tag, 16);
  _mm_sfence();
  iul_entry->key = key;
  clwb(iul_entry, 8);
  //clwb(iul_entry, sizeof(PmemNode) - sizeof(uint64_t));
#endif

  // Create skiplist node
  uint64_t dram_height = DramRandomHeight();
  MemNode* node = (MemNode*) malloc(sizeof(MemNode) + (dram_height - 1) * sizeof(uint64_t));
  node->key = key;
  node->tag = (l0_id << 32) | dram_height;
  node->value = log_paddr.dump();
  memset((void*) &node->next[0], 0, dram_height * sizeof(uint64_t));

  auto skiplist = mem->skiplist();
  skiplist->Insert(node);
  mem->w_UnRef();
#else
  int s = KeyShard(key);

  uint64_t height = RandomHeight();

  size_t kv_size = key.size() + sizeof(Value);


  // Create skiplist node
  MemNode* node = (MemNode*) malloc(sizeof(MemNode) + (height - 1) * sizeof(uint64_t));
  node->key = key;
  node->tag = height;
  //node->value = value;
  node->value = 0;
  memset(&node->next[0], 0, height * sizeof(uint64_t));

  auto mem = db_->GetWritableMemTable(kv_size, s);
  auto skiplist = mem->skiplist();

  size_t iul_entry_size = sizeof(PmemNode) + (height - 1) * sizeof(uint64_t);
  log_group_[s].emplace_back(LogItem{ key, height, value, node });
  log_group_alloc_size_[s] += iul_entry_size;
  if (log_group_[s].size() > 7) {
    int group_size = log_group_[s].size();
    //size_t log_space = 0;
    //std::vector<size_t> offset;
    //for (int i = 0; i < group_size; i++) {
    //  size_t iul_entry_size = sizeof(PmemNode) + (log_group_[s][i].tag - 1) * sizeof(uint64_t);
    //  offset.push_back(log_space);
    //  log_space += iul_entry_size;
    //}
    // Write log
    auto log_paddr = log_[s]->Allocate(log_group_alloc_size_[s]);
    char* p = (char*) log_paddr.get();
    auto pool_id = log_paddr.pool_id();
    auto pool_offset = log_paddr.offset();

    for (int i = 0; i < group_size; i++) {
      PmemNode* iul_entry = (PmemNode*) (p);
      iul_entry->key = log_group_[s][i].key;
      iul_entry->tag = log_group_[s][i].tag;
      iul_entry->value = log_group_[s][i].value;
      log_group_[s][i].mem_node->value = PmemPtr(pool_id, p).dump();
      p += sizeof(PmemNode) + (log_group_[s][i].tag - 1) * 8;
    }

    clwb(log_paddr.get(), log_group_alloc_size_[s]);
    log_group_[s].clear();
    log_group_alloc_size_[s] = 0;
  }

  skiplist->Insert(node);
  mem->w_UnRef();
#endif
}

bool DBClient::Get(const Key& key, Value* value_out) {
  return GetHook(key, value_out);
//   Task* t = new Task();
//   t->type = 1; // kGet = 1
//   t->key = key;
//   t->client = this;
//   t->region = region_;
//   t->pvalue = (uint64_t)value_out;
//   t->id = rnd_.Next64();
//   t->promise = new std::promise<uint64_t>();
// #ifdef MUTEX_DELEGATE
//   dp_->AddTask(t);
// #endif
// #ifdef RING_DELEGATE
//   int index = RandomPick();
//   rbp_->SendRequest(rbp_->GetRingBuffer(region_, index), t);
// #endif
//   uint64_t res = t->promise->get_future().get();
//   *value_out = res;
//   delete t;
//   return res != 0;
}

bool DBClient::GetHook(const Key& key, Value* value_out) {
  int s = KeyShard(key);
  {
    MemTableList* tl = (MemTableList*) db_->GetTableList(0, s);

    auto table = tl->GetFront();
    while (table) {
      if (table->type() == TableType::kMemTable) {
        auto mem = (MemTable*) table;
        auto skiplist = mem->skiplist();
        auto found = skiplist->Lookup(key);
        if (found && found->key == key) {
          *value_out = found->value;
          return true;
        }
      } else if (table->type() == TableType::kPmemTable) {
        break;
      }
      table = table->Next();
    }

#ifdef LISTDB_L0_CACHE
    {
      auto ht = db_->GetHashTable(s);
#if LISTDB_L0_CACHE == L0_CACHE_T_SIMPLE
      if (ht->Get(key, value_out)) {
        return true;
      }
#elif LISTDB_L0_CACHE == L0_CACHE_T_STATIC
      ListDB::PmemNode* rv = ht->Lookup(key);
      if (rv) {
        *value_out = rv->value;
        return true;
      }
#elif LISTDB_L0_CACHE == L0_CACHE_T_DOUBLE_HASHING
      ListDB::PmemNode* rv = ht->Lookup(key);
      if (rv) {
        *value_out = rv->value;
        return true;
      }
#elif LISTDB_L0_CACHE == L0_CACHE_T_LINEAR_PROBING
      ListDB::PmemNode* rv = ht->Lookup(key);
      if (rv) {
        *value_out = rv->value;
        return true;
      }
#endif
    }
#endif
    pmem_get_cnt_++;
    while (table) {
      auto pmem = (PmemTable*) table;
      auto skiplist = pmem->skiplist();
      //auto found_paddr = skiplist->Lookup(key, region_);
      auto found_paddr = Lookup(key, l0_pool_id_, skiplist);
      ListDB::PmemNode* found = (ListDB::PmemNode*) found_paddr.get();
      if (found && found->key == key) {
        //fprintf(stdout, "found on pmem\n");
        *value_out = found->value;
        return true;
      }
      table = table->Next();
    }
  }
  {
    // Level 1 Lookup
    auto tl = (PmemTableList*) db_->GetTableList(1, s);
    auto table = tl->GetFront();
    while (table) {
      auto pmem = (PmemTable*) table;
      auto skiplist = pmem->skiplist();
      //auto found_paddr = skiplist->Lookup(key, region_);
      auto found_paddr = LookupL1(key, l1_pool_id_, skiplist, s);
      ListDB::PmemNode* found = (ListDB::PmemNode*) found_paddr.get();
      if (found && found->key == key) {
        //fprintf(stdout, "found on pmem\n");
        *value_out = found->value;
        return true;
      }
      table = table->Next();
    }
  }
  return false;
}

#if defined(LISTDB_STRING_KEY) && defined(LISTDB_WISCKEY)
void DBClient::PutStringKV(const std::string_view& key_sv, const std::string_view& value) {
  Task *t = new Task();
  t->type = 2; // kStringPut = 2
  // t->string_key = key_sv;
  std::string *skey = new std::string(key_sv.data(), key_sv.length());
  t->string_key = std::string_view(*skey);
  t->string_value = value;
  t->client = this;
  t->region = region_;
  t->id = rnd_.Next64();
#ifdef MUTEX_DELEGATE
  dp_->AddTask(t);
#endif
#ifdef RING_DELEGATE
  uint64_t start_ = Clock::NowMicros();
  int index = RandomPick();
  while(!rbp_->SendRequest(rbp_->GetRingBuffer(region_, index), t)) {
    index = RandomPick();
  }
  uint64_t finish_ = Clock::NowMicros();
  // printf("client run on cpu%d send req cost %ld us task %ld\n", sched_getcpu(), finish_ - start_, t->id);
#endif
}

void DBClient::PutStringKVHook(const std::string_view& key_sv, const std::string_view& value) {
  uint64_t start_ = Clock::NowMicros();
  Key& key = *((Key*) key_sv.data());
  //if (!key.Valid()) {
  //  fprintf(stdout, "key is not valid: %s, %zu, key_num=%zu\n", std::string(key_sv).c_str(), *((uint64_t*) key.data()), key.key_num());
  //}
  int s = KeyShard(key);

  uint64_t pmem_height = PmemRandomHeight();
  size_t iul_entry_size = sizeof(PmemNode) + (pmem_height - 1) * sizeof(uint64_t);
  //size_t kv_size = key.size() + value.size();
  // clock_t a,b,c,d,e;

  // a = clock();
  // Write value
  size_t value_alloc_size = util::AlignedSize(8, 8 + value.size());
  auto value_paddr = value_blob_[s]->Allocate(value_alloc_size);
  char* value_p = (char*) value_paddr.get();
  *((size_t*) value_p) = value.size();
  value_p += sizeof(size_t);
  memcpy(value_p, value.data(), value.size());

  uint64_t dram_height = DramRandomHeight();
  size_t mem_node_size = sizeof(MemNode) + (dram_height - 1) * sizeof(uint64_t);
  auto mem = db_->GetWritableMemTable(mem_node_size, s);
  uint64_t l0_id = mem->l0_id();

  // b = clock();
  // Write log
  auto log_paddr = log_[s]->Allocate(iul_entry_size);
  PmemNode* iul_entry = (PmemNode*) log_paddr.get();
  iul_entry->tag = (l0_id << 32) | pmem_height;
  iul_entry->value = value_paddr.dump();
  clwb(&iul_entry->tag, 16);
  _mm_sfence();
  iul_entry->key = key;
  clwb(iul_entry, key.size());
  //clwb(iul_entry, sizeof(PmemNode) - sizeof(uint64_t));

  // c = clock();
  // Create skiplist node
  MemNode* node = (MemNode*) malloc(mem_node_size);
  node->key = key;
  node->tag = (l0_id << 32) | dram_height;
  //node->value = value;
  node->value = log_paddr.dump();
  memset((void*) &node->next[0], 0, dram_height * sizeof(uint64_t));

  auto skiplist = mem->skiplist();
  skiplist->Insert(node);
  mem->w_UnRef();

  // d = clock();
  // printf("write value time: %f\n", (double)(b-a)/CLOCKS_PER_SEC);
  // printf("write log time: %f\n", (double)(c-b)/CLOCKS_PER_SEC);
  // printf("write skiplist time: %f\n", (double)(d-c)/CLOCKS_PER_SEC);
  uint64_t finish_ = Clock::NowMicros();
  // printf("run on cpu%d putstringkv %ld\n", sched_getcpu(), (finish_ - start_));
  putstringkv_time.fetch_add(finish_ - start_);
}

bool DBClient::GetStringKV(const std::string_view& key_sv, Value* value_out) {
  return GetStringKVHook(key_sv, value_out);
//   Task* t = new Task();
//   t->type = 3; // kGetStringKV = 3
//   t->string_key = key_sv;
//   t->client = this;
//   t->region = region_;
//   t->pvalue = (uint64_t)value_out;
//   t->id = rnd_.Next64();
//   t->promise = new std::promise<uint64_t>();
// #ifdef MUTEX_DELEGATE
//   dp_->AddTask(t);
// #endif
// #ifdef RING_DELEGATE
//   int index = RandomPick();
//   rbp_->SendRequest(rbp_->GetRingBuffer(region_, index), t);
// #endif
//   uint64_t res = t->promise->get_future().get();
//   *value_out = res;
//   delete t;
//   return res != 0;
}

bool DBClient::GetStringKVHook(const std::string_view& key_sv, Value* value_out) {
Key& key = *((Key*) key_sv.data());
  int s = KeyShard(key);
  {
    MemTableList* tl = (MemTableList*) db_->GetTableList(0, s);

    auto table = tl->GetFront();
    while (table) {
      if (table->type() == TableType::kMemTable) {
        auto mem = (MemTable*) table;
        auto skiplist = mem->skiplist();
        auto found = skiplist->Lookup(key);
        if (found && found->key == key) {
          PmemNode* p_node = PmemPtr::Decode<PmemNode>(found->value);
          *value_out = (uint64_t) PmemPtr::Decode<char>(p_node->value);
          return true;
        }
      } else if (table->type() == TableType::kPmemTable) {
        break;
      }
      table = table->Next();
    }
#ifdef LISTDB_L0_CACHE
    {
      auto ht = db_->GetHashTable(s);
#if LISTDB_L0_CACHE == L0_CACHE_T_SIMPLE
      if (ht->Get(key, value_out)) {
        return true;
      }
#elif LISTDB_L0_CACHE == L0_CACHE_T_STATIC
      ListDB::PmemNode* rv = ht->Lookup(key);
      if (rv) {
        *value_out = (uint64_t) PmemPtr::Decode<char>(rv->value);
        return true;
      }
#elif LISTDB_L0_CACHE == L0_CACHE_T_DOUBLE_HASHING
      ListDB::PmemNode* rv = ht->Lookup(key);
      if (rv) {
        *value_out = (uint64_t) PmemPtr::Decode<char>(rv->value);
        return true;
      }
#elif LISTDB_L0_CACHE == L0_CACHE_T_LINEAR_PROBING
      ListDB::PmemNode* rv = ht->Lookup(key);
      if (rv) {
        *value_out = (uint64_t) PmemPtr::Decode<char>(rv->value);
        return true;
      }
#endif
    }
#endif
    pmem_get_cnt_++;
    while (table) {
      auto pmem = (PmemTable*) table;
      auto skiplist = pmem->skiplist();
      //auto found_paddr = skiplist->Lookup(key, region_);
      auto found_paddr = Lookup(key, l0_pool_id_, skiplist);
      ListDB::PmemNode* found = (ListDB::PmemNode*) found_paddr.get();
      if (found && found->key == key) {
        //fprintf(stdout, "found on pmem\n");
        //PmemPtr value_paddr(found->value);
        //char* value_buf = (char*) value_paddr.get();
        //std::string_view value_sv(value_buf + 8, *((size_t*) value_buf));
        //fprintf(stdout, "key: %s, value: %s\n", found->key.data(), value_sv.data());
        //*value_out = found->value;
        *value_out = (uint64_t) PmemPtr::Decode<char>(found->value);
        return true;
      }
      table = table->Next();
    }
  }
  {
    // Level 1 Lookup
    auto tl = (PmemTableList*) db_->GetTableList(1, s);
    auto table = tl->GetFront();
    while (table) {
      auto pmem = (PmemTable*) table;
      auto skiplist = pmem->skiplist();
      //auto found_paddr = skiplist->Lookup(key, region_);
      auto found_paddr = LookupL1(key, l1_pool_id_, skiplist, s);
      ListDB::PmemNode* found = (ListDB::PmemNode*) found_paddr.get();
      if (found && found->key == key) {
        //fprintf(stdout, "found on pmem\n");
        //PmemPtr value_paddr(found->value);
        //char* value_buf = (char*) value_paddr.get();
        //std::string_view value_sv(value_buf + 8, *((size_t*) value_buf));
        //fprintf(stdout, "key: %s, value: %s\n", found->key.data(), value_sv.data());
        //*value_out = found->value;
        *value_out = (uint64_t) PmemPtr::Decode<char>(found->value);
        return true;
      }
      table = table->Next();
    }
  }
  return false;
}
#endif

inline int DBClient::PmemRandomHeight() {
#if defined(LISTDB_L1_LRU) || defined(LISTDB_SKIPLIST_CACHE)
  static const unsigned int kBranching = 2;
#else
  static const unsigned int kBranching = 4;
#endif
  int height = 1;
#if 1
  if (rnd_.Next() % std::max<int>(1, (kBranching / kNumRegions)) == 0) {
    height++;
    while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
      height++;
    }
  }
#else
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
#endif
  return height;
}

inline int DBClient::DramRandomHeight() {
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  return height;
}

inline int DBClient::KeyShard(const Key& key) {
  return key.key_num() % kNumShards;
  //return key.key_num() / kShardSize;
}

#ifdef LISTDB_EXPERIMENTAL_SEARCH_LEVEL_CHECK
PmemPtr DBClient::LevelLookup(const Key& key, const int region, const int level, BraidedPmemSkipList* skiplist) {
  using Node = PmemNode;
  Node* pred = skiplist->head(pool_id);
  uint64_t curr_paddr_dump;
  Node* curr;
  int height = pred->height();

  // NUMA-local upper layers
  for (int i = height - 1; i >= 1; i--) {
    while (true) {
      curr_paddr_dump = pred->next[i];
      curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
      if (curr) {
        if (rnd_.Next() % LEVEL_CHECK_PERIOD_FACTOR == 0) {
          int curr_level = (curr->tag & 0xf00) >> 8;
          if (curr_level > level) {
            fprintf(stdout, "Level 1 detected. Skip to L1 Search.");
            return 0;  // PmemPtr(0).get() == nullptr
          }
        }
        if (curr->key.Compare(key) < 0) {
          pred = curr;
          continue;
        }
      }
      break;
    }
  }

  // Braided bottom layer
  if (pred == skiplist->head(pool_id)) {
    pred = skiplist->head(0);
  }
  while (true) {
    curr_paddr_dump = pred->next[0];
    curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
    if (curr) { 
      if (rnd_.Next() % LEVEL_CHECK_PERIOD_FACTOR == 0) {
        int curr_level = (curr->tag & 0xf00) >> 8;
        if (curr_level > level) {
          fprintf(stdout, "Level 1 detected. Skip to L1 Search.");
          return 0;  // PmemPtr(0).get() == nullptr
        }
      }
      if (curr->key.Compare(key) < 0) {
        pred = curr;
        continue;
      }
    }
    //fprintf(stdout, "lookupkey=%zu, curr->key=%zu\n", key, curr->key);
    break;
  }
  return curr_paddr_dump;
}
#endif

PmemPtr DBClient::Lookup(const Key& key, const int pool_id, BraidedPmemSkipList* skiplist) {
  using Node = PmemNode;
  Node* pred = skiplist->head(pool_id);
  search_visit_cnt_++;
  height_visit_cnt_[kMaxHeight - 1]++;
  uint64_t curr_paddr_dump;
  Node* curr;
  int height = pred->height();

  // NUMA-local upper layers
  for (int i = height - 1; i >= 1; i--) {
    while (true) {
      curr_paddr_dump = pred->next[i];
      curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
      if (curr) {
        search_visit_cnt_++;
        height_visit_cnt_[i]++;
        if (curr->key.Compare(key) < 0) {
          pred = curr;
          continue;
        }
      }
      break;
    }
  }

  // Braided bottom layer
  if (pred == skiplist->head(pool_id)) {
    if (pool_id != skiplist->primary_pool_id()) {
      search_visit_cnt_++;
      height_visit_cnt_[kMaxHeight - 1]++;
    }
    pred = skiplist->head();
  }
  while (true) {
    curr_paddr_dump = pred->next[0];
    curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
    if (curr) {
      search_visit_cnt_++;
      height_visit_cnt_[0]++;
      if (curr->key.Compare(key) < 0) {
        pred = curr;
        continue;
      }
    }
    //fprintf(stdout, "lookupkey=%zu, curr->key=%zu\n", key, curr->key);
    break;
  }
  return curr_paddr_dump;
}

PmemPtr DBClient::LookupL1(const Key& key, const int pool_id, BraidedPmemSkipList* skiplist, const int shard) {
  using Node = PmemNode;
  Node* pred = skiplist->head(pool_id);
  uint64_t curr_paddr_dump;
  Node* curr;
  int height = pred->height();

#ifdef LISTDB_L1_LRU
  if (0) {
    using MyType1 = std::pair<Key, uint64_t>;
    MyType1 search_key(key, 0);
    auto&& sorted_arr = db_->sorted_arr(pool_id, shard);
    auto found = std::upper_bound(sorted_arr.begin(),
        sorted_arr.end(), search_key,
        [&](const MyType1 &a, const MyType1 &b) { return a.first > b.first; });
    if (found != sorted_arr.end()) {
      //fprintf(stdout, "lookup key: %zu, found dram copy: %zu\n", key, found->first);
      pred = (Node*) ((PmemPtr*) &((*found).second))->get();
      height = pred->height();
    }
  } 

  {
    auto c = db_->lru_cache(shard, pool_id);
    uint64_t lt = c->FindLessThan(key);
    if (lt != 0) {
      pred = (Node*) ((PmemPtr*) &lt)->get();
      height = pred->height();
    }
  }
#endif
#ifdef LISTDB_SKIPLIST_CACHE
  auto c = db_->skiplist_cache(shard, db_->pool_id_to_region(pool_id));
  #if 0
  PmemNode* rv = c->LookupLessThan(key);
  if (rv) {
    pred = rv;
    height = pred->height();
  }
  #else
  PmemNode* lte_pnode = nullptr;
  int rv = c->LookupLessThanOrEqualsTo(key, &lte_pnode);
  if (lte_pnode) {
    if (rv == 0) {
      return PmemPtr(pool_id, (char*) lte_pnode);
    } else {
      pred = lte_pnode;
      //height = pred->height();
      height = kSkipListCacheMinPmemHeight;
    }
  }

  #endif
#endif
  search_visit_cnt_++;
  height_visit_cnt_[height - 1]++;

  // NUMA-local upper layers
  for (int i = height - 1; i >= 1; i--) {
    while (true) {
      curr_paddr_dump = pred->next[i];
      curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
      if (curr) {
        search_visit_cnt_++;
        height_visit_cnt_[i]++;
        if (curr->key.Compare(key) < 0) {
          pred = curr;
          continue;
        }
      }
      break;
    }
  }

  // Braided bottom layer
  if (pred == skiplist->head(pool_id)) {
    if (pool_id != skiplist->primary_pool_id()) {
      search_visit_cnt_++;
      height_visit_cnt_[kMaxHeight - 1]++;
    }
    pred = skiplist->head();
  }
  while (true) {
    curr_paddr_dump = pred->next[0];
    curr = (Node*) ((PmemPtr*) &curr_paddr_dump)->get();
    if (curr) {
      search_visit_cnt_++;
      height_visit_cnt_[0]++;
      if (curr->key.Compare(key) < 0) {
        pred = curr;
        continue;
      }
    }
    //fprintf(stdout, "lookupkey=%zu, curr->key=%zu\n", key, curr->key);
    break;
  }
  return curr_paddr_dump;
}



void DelegatePool::Close() {
#ifdef MUTEX_DELEGATE
  stop_ = true;
  task_cv_.notify_one();
  if(main_delegate_thread.joinable()) {
    main_delegate_thread.join();
  }

  for(int i = 0; i < kNumRegions; i++) {
    for(int j = 0; j < kDelegateNumWorkers; j++) {
      worker_data_[i][j].stop = true;
      worker_data_[i][j].cv.notify_one();
      if(worker_thread_[i][j].joinable()) {
        worker_thread_[i][j].join();
      }
    }
  }
#endif
#ifdef RING_DELEGATE
  for(int i = 0; i < kNumRegions; i++) {
      for(int j = 0; j < kDelegateNumWorkers; j++) {
        worker_data_[i][j].stop = true;
      }
  }
  db_->ring_buffer_pool->Close();
  for(int i = 0; i < kNumRegions; i++) {
    for(int j = 0; j < kDelegateNumWorkers; j++) {
      if(worker_thread_[i][j].joinable()) {
        worker_thread_[i][j].join();
      }
    }
  }
#endif
  printf("delegate finish %d tasks\n", finish_cnt.load(std::memory_order_relaxed));
}

void DelegatePool::Init() {
#ifdef MUTEX_DELEGATE
  main_delegate_thread = std::thread(std::bind(&DelegatePool::BackgroundMainLoop, this));
#endif

  for(int i = 0; i < kNumRegions; i++) {
    for(int j = 0; j < kDelegateNumWorkers; j++) {
      worker_data_[i][j].id = i * kDelegateNumWorkers + j;
      worker_data_[i][j].region = i;
      worker_data_[i][j].index = j;
      worker_data_[i][j].stop = false;
      worker_data_[i][j].start_ = Clock::NowMicros();
#ifdef RING_DELEGATE
      worker_data_[i][j].ring = &db_->ring_buffer_pool->ring_buffer_pool[i][j];
#endif
      worker_thread_[i][j] = std::thread(std::bind(&DelegatePool::BackgroundDelegateLoop,this, &worker_data_[i][j]));
    }
  }

  printf("init delegatepool, region nums %d, per region workers %d\n", kNumRegions, kDelegateNumWorkers);
}



void DelegatePool::AddTask(Task* task) {
  std::unique_lock<std::mutex> lk(task_mu_);
  worker_requests_.push_back(task);
  task_cv_.notify_one();
  lk.unlock();
}

void DelegatePool::BackgroundMainLoop() {
#ifdef MUTEX_DELEGATE
  while(true) {

    std::deque<Task*> new_work_requests;
    std::unique_lock<std::mutex> lk(task_mu_);
    task_cv_.wait(lk, [&] {return !worker_requests_.empty() || stop_;});
    if(stop_) {
      printf("task main thread quit\n");
      break;
    }
    
    new_work_requests.swap(worker_requests_);
    for(auto& task : new_work_requests) {
      int region = task->client->GetRegion();
      DelegateWorkerData* available_worker = nullptr;
      int index = 0;
      while(available_cnt.load(std::memory_order_relaxed) == 0);
      for(index = 0; index < kDelegateNumWorkers; index++) {
        if(tasks_assigned_num[region][index] < kDelegateQueueDepth) {
          available_worker = &worker_data_[region][index];
          available_cnt--;
          break;
        }
      }
      assert(available_worker != nullptr);
      tasks_assigned_num[region][index]++;
      std::unique_lock<std::mutex> lk(available_worker->mu);
      available_worker->q.push(task);
      available_worker->cv.notify_one();
      lk.unlock();
    }

  }
#endif
}

void DelegatePool::BackgroundDelegateLoop(DelegateWorkerData* data) {
  // Bind thread to numa region
  // struct bitmask *mask = numa_bitmask_alloc(numa_num_possible_nodes());
  // numa_bitmask_setbit(mask, data->region);
  // numa_bind(mask);
  // numa_bitmask_free(mask);

  // Force bind to node0 core
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(data->id * 2, &mask);
  sched_setaffinity(0, sizeof(mask), &mask);

  while(true) {
#ifdef MUTEX_DELEGATE
    std::unique_lock<std::mutex> lk(data->mu);
    data->cv.wait(lk, [&] {return data->stop || !data->q.empty();});
    
    if(data->stop) break;

    Task* task = data->q.front();
    data->q.pop();
    data->current_task = task;
    lk.unlock();
#endif
#ifdef RING_DELEGATE
// printf("before judge worker %d stop %d sendcnt %d recvcnt %d finishcnt %d\n", data->id, data->stop, 
//                                       data->ring->sendcnt.load(std::memory_order_relaxed),
//                                       data->ring->recvcnt.load(std::memory_order_relaxed),
//                                       finish_cnt.load(std::memory_order_relaxed));
    if(data->stop && data->ring->recvcnt.load(std::memory_order_relaxed) == data->ring->sendcnt.load(std::memory_order_relaxed)) {
      // printf("worker %d run on cpu%d about to quit\n", data->id, sched_getcpu());
      data->finish_ = Clock::NowMicros();
      pthread_barrier_wait(data->ring->barrier);
      // printf("worker %d run on cpu%d quit\n", data->id, sched_getcpu());
      break;
    }
    Task* task = db_->ring_buffer_pool->ReceiveRequest(data->ring);
    //for(int i = 0; i < 5000; i++){};
    if(task == nullptr) continue;

#endif
    switch(task->type) {
      case 0:{
        task->client->PutHook(task->key, task->value);
        delete task;
        break;
      }
      case 1: {
        uint64_t val;
        bool status = task->client->GetHook(task->key, &val);
        if(status) {
          task->promise->set_value(val);
        } else {
          task->promise->set_value(0);
        }
        break;
      }
#if defined(LISTDB_STRING_KEY) && defined(LISTDB_WISCKEY)
      case 2: {
        uint64_t start_ = Clock::NowMicros();
        task->client->PutStringKVHook(task->string_key, task->string_value);
        uint64_t finish_ = Clock::NowMicros();
        // printf("worker %d run on cpu%d putstringkv %ld task %ld\n", data->id, sched_getcpu(), (finish_ - start_), task->id);
        delete task;
        break;
      }
      case 3: {
        uint64_t val;
        bool status = task->client->GetStringKVHook(task->string_key, &val);
        if(status) {
          task->promise->set_value(val);
        } else {
          task->promise->set_value(0);
        }
        break;
      }
#endif
    }
    // tasks_assigned_num[data->region][data->index]--;
#ifdef MUTEX_DELEGATE
    available_cnt++;
#endif
    finish_cnt++;
#ifdef RING_DELEGATE
    // printf("before judge worker %d stop %d sendcnt %d recvcnt %d finishcnt %d\n", data->id, data->stop, 
    //                                   data->ring->sendcnt.load(std::memory_order_relaxed),
    //                                   data->ring->recvcnt.load(std::memory_order_relaxed),
    //                                   finish_cnt.load(std::memory_order_relaxed));
    if(data->stop && data->ring->recvcnt.load(std::memory_order_relaxed) == data->ring->sendcnt.load(std::memory_order_relaxed)) {
      // printf("worker %d run on cpu%d about to quit\n", data->id, sched_getcpu());
      data->finish_ = Clock::NowMicros();
      pthread_barrier_wait(data->ring->barrier);
      // printf("worker %d run on cpu%d quit\n", data->id, sched_getcpu());
      break;
    }
#endif
  }
  // printf("worker %d run on cpu%d func finish\n", data->id, sched_getcpu());
}

#endif  // LISTDB_DB_CLIENT_H_
