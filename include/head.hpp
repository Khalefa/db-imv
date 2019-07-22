#pragma once
#include "common/runtime/Hashmap.hpp"
#include "common/runtime/Database.hpp"
#include "vectorwise/Operators.hpp"
#include <assert.h>
#include "common/Compat.hpp"
#include "common/runtime/Concurrency.hpp"
#include "common/runtime/SIMD.hpp"
#include "hyper/ParallelHelper.hpp"
#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <tuple>
#include <x86intrin.h>
#include <string>
#include "vectorwise/Primitives.hpp"
#include <vector>
#include "common/runtime/Hash.hpp"
#include "common/defs.hpp"
#define VECTORSIZE 8
#define ROF_VECTOR_SIZE 10000
#define PDIS 2048
#define PDISD 72

#define SEQ_PREFETCH 0
#define WRITE_SEQ_PREFETCH 0
static int stateNum = vectorwise::Hashjoin::stateNum;
static int stateNumSIMD = vectorwise::Hashjoin::imvNum;

#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define DEBUG 0
using namespace vectorwise;
using namespace runtime;
using namespace std;
using hashFun = runtime::MurMurHash;
using hash_t = defs::hash_t;
static hash_t hash_seed =  primitives::seed;

inline void v_prefetch(__m512i& vec){
  uint64_t * ht_pos = (uint64_t*)&vec;
  for (int i = 0; i < VECTORSIZE; ++i) {
    _mm_prefetch((char * )(ht_pos[i]), _MM_HINT_T0);
    _mm_prefetch(((char * )(ht_pos[i]) + 64), _MM_HINT_T0);
  }
}
