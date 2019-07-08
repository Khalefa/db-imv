#include "imv/HashProbe.hpp"
struct __attribute__((aligned(64))) BuildSIMDState {
  __m512i v_entry_addr;
  __m512i v_hash_value;
  uint8_t valid_size, stage;
  BuildSIMDState()
      : v_entry_addr(_mm512_set1_epi64(0)),
        v_hash_value(_mm512_set1_epi64(0)),
        valid_size(VECTORSIZE),
        stage(1) {
  }
  inline void reset() {
    v_entry_addr = _mm512_set1_epi64(0);
    v_hash_value = _mm512_set1_epi64(0);
    valid_size = VECTORSIZE;
    stage = 1;
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
struct BuildState{
  Hashmap::EntryHeader* ptr;
  hash_t hash_value;
  uint8_t stage;
};
size_t build_raw(size_t begin, size_t end, Database& db, runtime::Hashmap* hash_table, Allocator*allo, int entry_size);
size_t build_simd(size_t begin, size_t end, Database& db, runtime::Hashmap* hash_table, Allocator*allo, int entry_size);
size_t build_imv(size_t begin, size_t end, Database& db, runtime::Hashmap* hash_table, Allocator*allo, int entry_size);
size_t build_gp(size_t begin, size_t end, Database& db, runtime::Hashmap* hash_table, Allocator*allo, int entry_size);
