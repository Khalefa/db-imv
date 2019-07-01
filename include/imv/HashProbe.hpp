#pragma once
#include "common/runtime/Hashmap.hpp"
#include "vectorwise/Operators.hpp"
#include <assert.h>
#include "common/Compat.hpp"
#include "common/runtime/Concurrency.hpp"
#include "common/runtime/SIMD.hpp"
#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <tuple>
#include <x86intrin.h>
#include <string>
#include "vectorwise/Primitives.hpp"
#include <vector>
#include "common/runtime/Hash.hpp"
#define VECTORSIZE 8
using namespace vectorwise;
size_t probe_row(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe);
size_t probe_imv(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe);
size_t probe_simd(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe);
size_t probe_amac(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe);
size_t probe_simd_amac(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe);
