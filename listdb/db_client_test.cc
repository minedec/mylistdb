#include <iostream>
#include <vector>

#include "listdb/listdb.h"
#include "listdb/db_client.h"
#include "listdb/core/delegation.h"
#include "listdb/core/simple_ring.h"

class DelegatePool;

int main() {
  ListDB* db = new ListDB();
  db->Init();
#ifdef RING_DELEGATE
  RingBufferPool* rbp = new RingBufferPool();
  rbp->Init();
  db->ring_buffer_pool = rbp;
#endif
  DelegatePool* dp = new DelegatePool();
  dp->db_ = db;
  dp->Init();
  db->delegate_pool = dp;

  DBClient* client = new DBClient(db, 0, 1);

  client->Put(10, 10);
  client->Put(1, 1);
  client->Put(5, 5);

  uint64_t val_read;
  client->Get(10, &val_read);
  std::cout << *(PmemPtr::Decode<uint64_t>(val_read)) << std::endl;
  client->Get(1, &val_read);
  std::cout << *(PmemPtr::Decode<uint64_t>(val_read)) << std::endl;
  client->Get(5, &val_read);
  std::cout << *(PmemPtr::Decode<uint64_t>(val_read)) << std::endl;

#if defined(LISTDB_STRING_KEY) && defined(LISTDB_WISCKEY)
  std::vector<std::string_view> keys;
  std::vector<std::string_view> values;
  int nums = 300;
  for(int i = 0; i < nums; i++) {
    std::string_view key = std::to_string(random());
    std::string_view value = std::to_string(random());
    keys.push_back(key);
    values.push_back(value);
    printf("put string kv seq %d\n", i);
    client->PutStringKV(key, value);
  }
  for(int i = 0; i <nums; i++) {
    uint64_t value_addr;
    printf("get string kv seq %d\n", i);
    client->GetStringKV(keys[i], &value_addr);
  }
#endif

  dp->Close();
  db->Close();
  return 0;
}
