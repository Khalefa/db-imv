#pragma once
#include "imv/HashProbe.hpp"
   struct AMACState {
     uint8_t stage;
     int probeKey=0;
     types::Numeric<12,2>probeValue;
     pos_t tuple_id=0;
     runtime::Hashmap::hash_t probeHash;
     runtime::Hashmap::EntryHeader* buildMatch;
   };
size_t agg_local_raw(size_t begin, size_t end, Database& db,Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>* hash_table,PartitionedDeque<1024>* partition);
size_t agg_local_amac(size_t begin, size_t end, Database& db,Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>* hash_table,PartitionedDeque<1024>* partition);
size_t agg_local_gp(size_t begin, size_t end, Database& db,Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>* hash_table,PartitionedDeque<1024>* partition);
