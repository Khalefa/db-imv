#pragma once

#include "head.hpp"
size_t probe_row(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe, uint64_t* pos_buff=nullptr);
size_t probe_imv(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe, uint64_t* pos_buff=nullptr);
size_t probe_simd(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe, uint64_t* pos_buff=nullptr);
size_t probe_amac(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe, uint64_t* pos_buff=nullptr);
size_t probe_gp(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe, uint64_t* pos_buff=nullptr);
size_t probe_simd_amac(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe, uint64_t* pos_buff=nullptr);
