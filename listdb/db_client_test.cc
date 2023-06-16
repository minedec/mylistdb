#include <iostream>
#include <vector>

#include "listdb/listdb.h"
#include "listdb/db_client.h"
#include "listdb/core/delegate.h"

class DelegatePool;

int main() {
  ListDB* db = new ListDB();
  db->Init();
  DelegatePool* dp = new DelegatePool();
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

  return 0;
}
