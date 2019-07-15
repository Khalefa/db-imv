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
 struct __attribute__((aligned(64))) AggState {
   __m512i v_probe_keys;
   __m512i  v_bucket_addrs;
   __m512i v_probe_offset,v_probe_hash,v_probe_value;
   __mmask8 m_valid_probe;
   uint8_t stage;
   AggState(): v_probe_keys(_mm512_set1_epi64(0)),
       v_bucket_addrs(_mm512_set1_epi64(0)),v_probe_offset(_mm512_set1_epi64(0)),m_valid_probe(0),stage(1){}
   void reset() {
        v_probe_keys =_mm512_set1_epi64(0);
        v_bucket_addrs = _mm512_set1_epi64(0);
        v_probe_offset = _mm512_set1_epi64(0);
        m_valid_probe = 0;
        stage=1;
      }
   void* operator new(size_t size) {
     return memalign(64, size);
   }
   void operator delete(void* mem) {
     return free(mem);
   }
   void* operator new[](size_t size) {
     return memalign(64, size);
   }
   void operator delete[](void* mem) {
     return free(mem);
   }

 };
size_t agg_local_raw(size_t begin, size_t end, Database& db,Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>* hash_table,PartitionedDeque<1024>* partition);
size_t agg_local_amac(size_t begin, size_t end, Database& db,Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>* hash_table,PartitionedDeque<1024>* partition);
size_t agg_local_gp(size_t begin, size_t end, Database& db,Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>* hash_table,PartitionedDeque<1024>* partition);
size_t agg_local_simd(size_t begin, size_t end, Database& db,Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>* hash_table,PartitionedDeque<1024>* partition);
