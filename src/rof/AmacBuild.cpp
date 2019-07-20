#include "rof/AmacBuild.hpp"

size_t amac_build_q11_date(size_t begin, size_t end, Database& db, runtime::Hashmap* hash_table, Allocator*allo, int entry_size, uint64_t* pos_buff) {
  size_t found = 0, cur = begin;
  auto& d = db["date"];
  auto d_datekey = d["d_datekey"].data<types::Integer>();
  int build_key_off = sizeof(runtime::Hashmap::EntryHeader);
  uint32_t key=0;
  uint8_t done = 0, k = 0;
  BuildState state[stateNum];

  for (int i = 0; i < stateNum; ++i) {
    state[i].stage = 1;
  }

  while (done < stateNum) {
    k = (k >= stateNum) ? 0 : k;
    switch (state[k].stage) {
      case 1: {
        if (cur >= end) {
          ++done;
          state[k].stage = 3;
          break;
        }
        key = d_datekey[pos_buff[cur]].value;
        state[k].ptr = (Hashmap::EntryHeader*) allo->allocate(entry_size);
        *(int*) (((char*) state[k].ptr) + build_key_off) = key;
        state[k].hash_value = hash()(key, primitives::seed);
        ++cur;
        state[k].stage = 0;
        hash_table->PrefetchEntry(state[k].hash_value);
      }
        break;
      case 0: {
        hash_table->insert_tagged(state[k].ptr, state[k].hash_value);
        ++found;
        state[k].stage = 1;
      }
        break;
    }
    ++k;
  }
  return found;
}
