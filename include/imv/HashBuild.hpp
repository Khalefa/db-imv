#include "imv/HashProbe.hpp"
size_t build_raw(size_t begin, size_t end, Database& db, runtime::Hashmap* hash_table, Allocator*, int entry_size);
size_t build_simd(size_t begin, size_t end, Database& db, runtime::Hashmap* hash_table, Allocator*, int entry_size);
