#include "imv/HashProbe.hpp"
//using vectorwise;
//using runtime;
size_t probe_row(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe, uint64_t* pos_buff) {
  // std::cout<<"use join row "<<std::endl;
  size_t found = 0, pos = 0;
  uint32_t probeKey;
  int build_key_off = sizeof(runtime::Hashmap::EntryHeader);
  for (uint32_t nextProbe = 0; nextProbe < num; ++nextProbe) {
    if (pos_buff) {
      probeKey = probe_keys[pos_buff[nextProbe]].value;
    } else {
      probeKey = probe_keys[nextProbe].value;
    }
    auto probeHash = runtime::MurMurHash()(probeKey, vectorwise::primitives::seed);
    auto buildMatch = hash_table->find_chain_tagged(probeHash);

    for (auto entry = buildMatch; entry != nullptr; entry = entry->next) {
      uint32_t buildkey = *((uint32_t*) (((void*) entry) + build_key_off));
      if ((buildkey == probeKey)) {
        pos = pos < morselSize ? pos : 0;
        output_build[pos] = ((void*) entry);
        output_probe[pos] = (nextProbe);
        ++found;
        ++pos;
      }
    }
  }
  return found;
}
size_t probe_imv(types::Integer* probe_keys, uint32_t tuple_num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe, uint64_t* pos_buff) {
  size_t found = 0, pos = 0;
  int k = 0, done = 0, keyOff = sizeof(runtime::Hashmap::EntryHeader), imvNum = vectorwise::Hashjoin::imvNum, nextProbe = 0;
  IMVState* imv_state = new IMVState[vectorwise::Hashjoin::imvNum + 1];

  __attribute__((aligned(64)))      __mmask8 m_match = 0, m_new_probes = -1, mask[VECTORSIZE + 1];

  __m512i v_base_offset = _mm512_set_epi64(7, 6, 5, 4, 3, 2, 1, 0);
  __m512i v_offset = _mm512_set1_epi64(0), v_new_build_key, v_build_keys;
  __m512i v_base_offset_upper = _mm512_set1_epi64(tuple_num);
  __m512i v_seed = _mm512_set1_epi64(vectorwise::primitives::seed), v_build_key_off = _mm512_set1_epi64(keyOff);
  __m512i v_zero = _mm512_set1_epi64(0);
  __m256i v256_zero = _mm256_set1_epi32(0), v256_probe_keys, v256_build_keys;
  uint64_t * ht_pos = nullptr;
  uint8_t num, num_temp;
  for (int i = 0; i <= VECTORSIZE; ++i) {
    mask[i] = (1 << i) - 1;
  }
  while (true) {
    k = (k >= imvNum) ? 0 : k;
    if ((nextProbe >= tuple_num)) {
      if (imv_state[k].m_valid_probe == 0 && imv_state[k].stage != 3) {
        ++done;
        imv_state[k].stage = 3;
        ++k;
        continue;
      }
    }
    if (done >= imvNum) {
      if (imv_state[imvNum].m_valid_probe > 0) {
        k = imvNum;
        imv_state[imvNum].stage = 0;
      } else {
        break;
      }
    }
    switch (imv_state[k].stage) {
      case 1: {
        /// step 1: load the offsets of probing tuples
        imv_state[k].v_probe_offset = _mm512_add_epi64(_mm512_set1_epi64(nextProbe), v_base_offset);
        imv_state[k].m_valid_probe = _mm512_cmpgt_epu64_mask(v_base_offset_upper, imv_state[k].v_probe_offset);
        if (pos_buff) {
          imv_state[k].v_probe_offset = _mm512_maskz_loadu_epi64(imv_state[k].m_valid_probe, (char*) (pos_buff + nextProbe));
        }
        /// step 2: gather the probe keys // why is load() so faster than gather()?
        v256_probe_keys = _mm512_mask_i64gather_epi32(v256_zero, imv_state[k].m_valid_probe, imv_state[k].v_probe_offset, (void* )probe_keys, 4);
        // v256_probe_keys = _mm256_maskz_loadu_epi32(imv_state[k].m_valid_probe, (char*)(probe_keys+nextProbe));
        imv_state[k].v_probe_keys = _mm512_cvtepi32_epi64(v256_probe_keys);
        nextProbe += VECTORSIZE;
        /// step 3: compute the hash values of probe keys
        imv_state[k].v_probe_hash = runtime::MurMurHash()((imv_state[k].v_probe_keys), (v_seed));
        imv_state[k].stage = 2;
#if SEQ_PREFETCH
        _mm_prefetch((((char* )(probe_keys+nextProbe))+PDIS), _MM_HINT_T0);
        _mm_prefetch((((char* )(probe_keys+nextProbe))+PDIS+64), _MM_HINT_T0);
#endif
        hash_table->prefetchEntry((imv_state[k].v_probe_hash));
      }
        break;
      case 2: {
        /// step 4: find the addresses of corresponding buckets for new probes
        Vec8uM v_new_bucket_addrs = hash_table->find_chain_tagged((imv_state[k].v_probe_hash));
        imv_state[k].m_valid_probe = _mm512_kand(imv_state[k].m_valid_probe, v_new_bucket_addrs.mask);
        imv_state[k].v_bucket_addrs = v_new_bucket_addrs.vec;
        imv_state[k].stage = 0;
        ht_pos = (uint64_t *) &imv_state[k].v_bucket_addrs;
        for (int i = 0; i < VECTORSIZE; ++i) {
          _mm_prefetch((char * )(ht_pos[i]), _MM_HINT_T0);
          _mm_prefetch(((char * )(ht_pos[i]) + 64), _MM_HINT_T0);
        }
      }
        break;
      case 0: {
        /// step 5: gather the all new build keys
        v256_build_keys = _mm512_mask_i64gather_epi32(v256_zero, imv_state[k].m_valid_probe, _mm512_add_epi64(imv_state[k].v_bucket_addrs, v_build_key_off), nullptr, 1);
        v_build_keys = _mm512_cvtepi32_epi64(v256_build_keys);
        /// step 6: compare the probe keys and build keys and write points
        m_match = _mm512_cmpeq_epi64_mask(imv_state[k].v_probe_keys, v_build_keys);
        m_match = _mm512_kand(m_match, imv_state[k].m_valid_probe);
        pos = pos + VECTORSIZE < morselSize ? pos : 0;
#if WRITE_SEQ_PREFETCH
        _mm_prefetch((char *)(((char *)(output_build+pos)) + PDIS), _MM_HINT_T0);
        _mm_prefetch((char *)(((char *)(output_build+pos)) + PDIS + 64), _MM_HINT_T0);
        _mm_prefetch((char *)(((char *)(output_probe+pos)) + PDIS), _MM_HINT_T0);
        _mm_prefetch((char *)(((char *)(output_probe+pos)) + PDIS + 64), _MM_HINT_T0);
#endif
        _mm512_mask_compressstoreu_epi64((output_build + pos), m_match, imv_state[k].v_bucket_addrs);
        _mm256_mask_compressstoreu_epi32((output_probe + pos), m_match, _mm512_cvtepi64_epi32(imv_state[k].v_probe_offset));
        pos += _mm_popcnt_u32(m_match);
        found += _mm_popcnt_u32(m_match);
        /// step 7: move to the next bucket nodes
        imv_state[k].v_bucket_addrs = _mm512_mask_i64gather_epi64(v_zero, imv_state[k].m_valid_probe, imv_state[k].v_bucket_addrs, nullptr, 1);
        imv_state[k].m_valid_probe = _mm512_kand(imv_state[k].m_valid_probe, _mm512_cmpneq_epi64_mask(imv_state[k].v_bucket_addrs, v_zero));

        num = _mm_popcnt_u32(imv_state[k].m_valid_probe);
        if (num == VECTORSIZE) {
          ht_pos = (uint64_t *) &imv_state[k].v_bucket_addrs;
          for (int i = 0; i < VECTORSIZE; ++i) {
            _mm_prefetch((char * )(ht_pos[i]), _MM_HINT_T0);
            _mm_prefetch(((char * )(ht_pos[i]) + 64), _MM_HINT_T0);
          }
        } else {
          if ((done < imvNum)) {
            num_temp = _mm_popcnt_u32(imv_state[imvNum].m_valid_probe);
            if (num + num_temp < VECTORSIZE) {
              // compress imv_state[k]
              compress(&imv_state[k]);
              // expand imv_state[k] -> imv_state[imvNum]
              expand(&imv_state[k], &imv_state[imvNum]);
              imv_state[imvNum].m_valid_probe = mask[num + num_temp];
              imv_state[k].m_valid_probe = 0;
              imv_state[k].stage = 1;
            } else {
              // expand imv_state[imvNum] -> expand imv_state[k]
              expand(&imv_state[imvNum], &imv_state[k]);
              imv_state[imvNum].m_valid_probe = _mm512_kand(imv_state[imvNum].m_valid_probe, _mm512_knot(mask[VECTORSIZE - num]));
              // compress imv_state[imvNum]
              compress(&imv_state[imvNum]);
              imv_state[imvNum].m_valid_probe = imv_state[imvNum].m_valid_probe >> (VECTORSIZE - num);
              imv_state[k].m_valid_probe = mask[VECTORSIZE];
              imv_state[k].stage = 0;
              ht_pos = (uint64_t *) &imv_state[k].v_bucket_addrs;
              for (int i = 0; i < VECTORSIZE; ++i) {
                _mm_prefetch((char * )(ht_pos[i]), _MM_HINT_T0);
                _mm_prefetch(((char * )(ht_pos[i]) + 64), _MM_HINT_T0);
              }
            }
          }
        }
      }
        break;
    }
    ++k;
  }
  delete[] imv_state;
  imv_state = nullptr;
  return found;
}
size_t probe_simd(types::Integer* probe_keys, uint32_t probe_num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe, uint64_t* pos_buff) {
  size_t found = 0, pos = 0;
  int k = 0, done = 0, keyOff = sizeof(runtime::Hashmap::EntryHeader), imvNum = vectorwise::Hashjoin::imvNum, nextProbe = 0, curProbe;
  SIMDContinuation* SIMDcon = new SIMDContinuation();
  __mmask8 m_match = 0, m_new_probes = -1;
  __m512i v_base_offset = _mm512_set_epi64(7, 6, 5, 4, 3, 2, 1, 0);
  __m512i v_offset = _mm512_set1_epi64(0), v_new_build_key, v_build_keys, v_index_offset;
  __m512i v_base_offset_upper = _mm512_set1_epi64(probe_num);
  __m512i v_seed = _mm512_set1_epi64(primitives::seed), v_build_key_off = _mm512_set1_epi64(keyOff);
  __m512i v_probe_hash = _mm512_set1_epi64(0), v_zero = _mm512_set1_epi64(0);
  __m256i v256_zero = _mm256_set1_epi32(0), v256_probe_keys, v256_build_keys;

  for (; nextProbe < probe_num || SIMDcon->m_valid_probe;) {
    /// step 1: load the offsets of probing tuples
#if 1
    if (pos_buff) {
      v_offset = _mm512_add_epi64(_mm512_set1_epi64(nextProbe), v_base_offset);
      v_index_offset = _mm512_maskz_expand_epi64(_mm512_knot(SIMDcon->m_valid_probe), v_offset);
      SIMDcon->v_probe_offset = _mm512_mask_expandloadu_epi64(SIMDcon->v_probe_offset, _mm512_knot(SIMDcon->m_valid_probe), (char*) (pos_buff + nextProbe));
      m_new_probes = _mm512_knot(SIMDcon->m_valid_probe);
      nextProbe = nextProbe + _mm_popcnt_u32(m_new_probes);
      SIMDcon->m_valid_probe = _mm512_cmpgt_epu64_mask(v_base_offset_upper, v_index_offset);
      m_new_probes = _mm512_kand(m_new_probes, SIMDcon->m_valid_probe);
      v256_probe_keys = _mm512_mask_i64gather_epi32(v256_zero, m_new_probes, SIMDcon->v_probe_offset, (void* )probe_keys, 4);
    } else {
      v_offset = _mm512_add_epi64(_mm512_set1_epi64(nextProbe), v_base_offset);
      SIMDcon->v_probe_offset = _mm512_mask_expand_epi64(SIMDcon->v_probe_offset, _mm512_knot(SIMDcon->m_valid_probe), v_offset);
      // count the number of empty tuples
      m_new_probes = _mm512_knot(SIMDcon->m_valid_probe);
      nextProbe = nextProbe + _mm_popcnt_u32(m_new_probes);
      SIMDcon->m_valid_probe = _mm512_cmpgt_epu64_mask(v_base_offset_upper, SIMDcon->v_probe_offset);
      m_new_probes = _mm512_kand(m_new_probes, SIMDcon->m_valid_probe);
      /// step 2: gather the probe keys
      v256_probe_keys = _mm512_mask_i64gather_epi32(v256_zero, m_new_probes, SIMDcon->v_probe_offset, (void* )probe_keys, 4);
    }
#else
    v_offset = _mm512_add_epi64(_mm512_set1_epi64(nextProbe), v_base_offset);
    SIMDcon->v_probe_offset = _mm512_mask_expand_epi64(SIMDcon->v_probe_offset, _mm512_knot(SIMDcon->m_valid_probe), v_offset);
    // count the number of empty tuples
    m_new_probes = _mm512_knot(SIMDcon->m_valid_probe);
    curProbe = nextProbe;
    nextProbe = nextProbe + _mm_popcnt_u32(m_new_probes);
    SIMDcon->m_valid_probe = _mm512_cmpgt_epu64_mask(v_base_offset_upper, SIMDcon->v_probe_offset);
    m_new_probes = _mm512_kand(m_new_probes, SIMDcon->m_valid_probe);
    v256_probe_keys = _mm256_maskz_expandloadu_epi32(m_new_probes, (char *) (probe_keys + curProbe));
#endif
    SIMDcon->v_probe_keys = _mm512_mask_blend_epi64(m_new_probes, SIMDcon->v_probe_keys, _mm512_cvtepi32_epi64(v256_probe_keys));
    /// step 3: compute the hash values of probe keys
    v_probe_hash = runtime::MurMurHash()((SIMDcon->v_probe_keys), (v_seed));
    /// step 4: find the addresses of corresponding buckets for new probes
    Vec8uM v_new_bucket_addrs = hash_table->find_chain_tagged_sel((v_probe_hash), m_new_probes);
    // the addresses are null, then the corresponding probes are invalid
    SIMDcon->m_valid_probe = _mm512_kand(_mm512_kor(_mm512_knot(m_new_probes), v_new_bucket_addrs.mask), SIMDcon->m_valid_probe);
    SIMDcon->v_bucket_addrs = _mm512_mask_blend_epi64(v_new_bucket_addrs.mask, SIMDcon->v_bucket_addrs, v_new_bucket_addrs.vec);
    /// step 5: gather the all new build keys
    v256_build_keys = _mm512_mask_i64gather_epi32(v256_zero, SIMDcon->m_valid_probe, _mm512_add_epi64(SIMDcon->v_bucket_addrs, v_build_key_off), nullptr, 1);
    v_build_keys = _mm512_cvtepi32_epi64(v256_build_keys);
    /// step 6: compare the probe keys and build keys and write points
    m_match = _mm512_cmpeq_epi64_mask(SIMDcon->v_probe_keys, v_build_keys);
    m_match = _mm512_kand(m_match, SIMDcon->m_valid_probe);
    pos = pos + VECTORSIZE < morselSize ? pos : 0;
#if WRITE_SEQ_PREFETCH
        _mm_prefetch((char *)(((char *)(output_build+pos)) + PDIS), _MM_HINT_T0);
        _mm_prefetch((char *)(((char *)(output_build+pos)) + PDIS + 64), _MM_HINT_T0);
        _mm_prefetch((char *)(((char *)(output_probe+pos)) + PDIS), _MM_HINT_T0);
        _mm_prefetch((char *)(((char *)(output_probe+pos)) + PDIS + 64), _MM_HINT_T0);
#endif
    _mm512_mask_compressstoreu_epi64((output_build + pos), m_match, SIMDcon->v_bucket_addrs);
    _mm256_mask_compressstoreu_epi32((output_probe + pos), m_match, _mm512_cvtepi64_epi32(SIMDcon->v_probe_offset));
    pos += _mm_popcnt_u32(m_match);
    found += _mm_popcnt_u32(m_match);
    /// step 7: move to the next bucket nodes
    SIMDcon->v_bucket_addrs = _mm512_mask_i64gather_epi64(v_zero, SIMDcon->m_valid_probe, SIMDcon->v_bucket_addrs, nullptr, 1);
    SIMDcon->m_valid_probe = _mm512_kand(SIMDcon->m_valid_probe, _mm512_cmpneq_epi64_mask(SIMDcon->v_bucket_addrs, v_zero));
  }
  SIMDcon->m_valid_probe = 0;
  nextProbe = probe_num;
  delete SIMDcon;
  SIMDcon = nullptr;
  return found;
}
size_t probe_amac(types::Integer* probe_keys, uint32_t probe_num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe, uint64_t* pos_buff) {
  size_t found = 0, pos = 0;
  int k = 0, done = 0, keyOff = sizeof(runtime::Hashmap::EntryHeader), nextProbe = 0, buildkey;
  Hashjoin::AMACState amac_state[stateNum];
  int probeKey;
  hash_t probeHash;
  // initialization
  for (int i = 0; i < stateNum; ++i) {
    amac_state[i].stage = 1;
  }

  while (done < stateNum) {
    k = (k >= stateNum) ? 0 : k;
    switch (amac_state[k].stage) {
#if 0
      case 1: {
        if (nextProbe >= probe_num) {
          ++done;
          amac_state[k].stage = 3;
          //   std::cout<<"amac done one "<<probe_num<<" , "<<nextProbe<<std::endl;
          break;
        }
#if SEQ_PREFETCH
        _mm_prefetch((char*)(probe_keys+nextProbe)+PDIS,_MM_HINT_T0);
        _mm_prefetch(((char*)(probe_keys+nextProbe)+PDIS+64),_MM_HINT_T0);
#endif
        if(pos_buff) {
          probeKey = *(int*) (probe_keys + pos_buff[nextProbe]);
        } else {
          probeKey = *(int*) (probe_keys + nextProbe);
        }
        probeHash = (runtime::MurMurHash()(probeKey, primitives::seed));
        // prefetch the address of the beginning hash buckets
        //hash_table->prefetchEntry(probeHash);
        // suppose the hashEngtries reside in the cache
        amac_state[k].buildMatch = hash_table->find_chain_tagged(probeHash);
        amac_state[k].tuple_id = nextProbe;
        ++nextProbe;
        if (nullptr == amac_state[k].buildMatch) {
          --k;
          break;
        }
        amac_state[k].probeKey = probeKey;
        amac_state[k].stage = 0;
        _mm_prefetch((char * )(amac_state[k].buildMatch), _MM_HINT_T0);
        _mm_prefetch((char * )(amac_state[k].buildMatch) + 64, _MM_HINT_T0);
      }
      break;
#else
      case 1: {
        if (nextProbe >= probe_num) {
          ++done;
          amac_state[k].stage = 3;
          //   std::cout<<"amac done one "<<probe_num<<" , "<<nextProbe<<std::endl;
          break;
        }
#if SEQ_PREFETCH
        _mm_prefetch((char*)(probe_keys+nextProbe)+PDIS,_MM_HINT_T0);
        _mm_prefetch(((char*)(probe_keys+nextProbe)+PDIS+64),_MM_HINT_T0);
#endif
        if (pos_buff) {
          probeKey = *(int*) (probe_keys + pos_buff[nextProbe]);
        } else {
          probeKey = *(int*) (probe_keys + nextProbe);
        }
        probeHash = (runtime::MurMurHash()(probeKey, primitives::seed));
        amac_state[k].tuple_id = nextProbe;
        ++nextProbe;
        amac_state[k].probeKey = probeKey;
        amac_state[k].probeHash = probeHash;
        _mm_prefetch((char * )(hash_table->entries + probeHash), _MM_HINT_T0);
        amac_state[k].stage = 2;
      }
        break;
      case 2: {
        amac_state[k].buildMatch = hash_table->find_chain_tagged(amac_state[k].probeHash);
        if (nullptr == amac_state[k].buildMatch) {
          amac_state[k].stage = 1;
          --k;
          break;
        } else {
          _mm_prefetch((char * )(amac_state[k].buildMatch), _MM_HINT_T0);
          _mm_prefetch((char * )(amac_state[k].buildMatch) + 64, _MM_HINT_T0);
          amac_state[k].stage = 0;
        }
      }
        break;
#endif
      case 0: {
        auto entry = amac_state[k].buildMatch;

        buildkey = *((addBytes((reinterpret_cast<int*>(entry)), keyOff)));
        if ((buildkey == amac_state[k].probeKey)) {
          pos = pos < morselSize ? pos : 0;
#if WRITE_SEQ_PREFETCH
          _mm_prefetch((char * )(output_build+pos)+PDIS, _MM_HINT_T0);
          _mm_prefetch((char * )(output_probe+pos)+PDIS + 64, _MM_HINT_T0);
#endif
          output_build[pos] = entry;
          output_probe[pos++] = amac_state[k].tuple_id;
          ++found;
        }
        entry = entry->next;
        if (nullptr == entry) {
          amac_state[k].stage = 1;
          --k;
        } else {
          amac_state[k].buildMatch = entry;
          _mm_prefetch((char * )(entry), _MM_HINT_T0);
          _mm_prefetch((char * )(entry) + 64, _MM_HINT_T0);
        }

      }
        break;
    }
    ++k;
  }
  return found;
}
size_t probe_gp(types::Integer* probe_keys, uint32_t num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe, uint64_t* pos_buff) {
  size_t found = 0, pos = 0;
  int k = 0, done = 0, keyOff = sizeof(runtime::Hashmap::EntryHeader), nextProbe = 0, buildkey;
  Hashjoin::AMACState amac_state[stateNum];
  int probeKey, valid_size = 0;
  hash_t probeHash;
  while (nextProbe < num) {
    // step 1: get probe key, compute hashing
    for (k = 0; (k < stateNum) && (nextProbe < num); ++k, ++nextProbe) {
#if SEQ_PREFETCH
        _mm_prefetch((char*)(probe_keys+nextProbe)+PDIS,_MM_HINT_T0);
        _mm_prefetch(((char*)(probe_keys+nextProbe)+PDIS+64),_MM_HINT_T0);
#endif
      if (pos_buff) {
        probeKey = *(int*) (probe_keys + pos_buff[nextProbe]);
      } else {
        probeKey = *(int*) (probe_keys + nextProbe);
      }
      probeHash = (runtime::MurMurHash()(probeKey, primitives::seed));
      amac_state[k].tuple_id = nextProbe;
      amac_state[k].probeKey = probeKey;
      amac_state[k].probeHash = probeHash;
      _mm_prefetch((char * )(hash_table->entries + probeHash), _MM_HINT_T0);
    }
    valid_size = k;
    done = 0;
    // step 2: fetch the first node in the hash table bucket
    for (k = 0; k < valid_size; ++k) {
      amac_state[k].buildMatch = hash_table->find_chain_tagged(amac_state[k].probeHash);
      if (nullptr == amac_state[k].buildMatch) {
        ++done;
      } else {
        _mm_prefetch((char * )(amac_state[k].buildMatch), _MM_HINT_T0);
        _mm_prefetch((char * )(amac_state[k].buildMatch) + 64, _MM_HINT_T0);
      }
    }
    // step 3: repeating matching each node in the bucket
    while (done < valid_size) {
      for (k = 0; k < valid_size; ++k) {
        auto entry = amac_state[k].buildMatch;
        if(nullptr==entry) {
          continue;
        }
        buildkey = *((addBytes((reinterpret_cast<int*>(entry)), keyOff)));
        if ((buildkey == amac_state[k].probeKey)) {
          pos = pos < morselSize ? pos : 0;
#if WRITE_SEQ_PREFETCH
          _mm_prefetch((char * )(output_build+pos)+PDIS, _MM_HINT_T0);
          _mm_prefetch((char * )(output_probe+pos)+PDIS + 64, _MM_HINT_T0);
#endif
          output_build[pos] = entry;
          output_probe[pos++] = amac_state[k].tuple_id;
          ++found;
        }
        entry = entry->next;
        amac_state[k].buildMatch = entry;
        if (nullptr == entry) {
          ++done;
        } else {
          _mm_prefetch((char * )(entry), _MM_HINT_T0);
          _mm_prefetch((char * )(entry) + 64, _MM_HINT_T0);
        }

      }
    }
  }
  return found;
}
size_t probe_simd_amac(types::Integer* probe_keys, uint32_t probe_num, runtime::Hashmap* hash_table, void** output_build, uint32_t*output_probe, uint64_t* pos_buff) {
  size_t found = 0, pos = 0;
  int k = 0, done = 0, keyOff = sizeof(runtime::Hashmap::EntryHeader), imvNum = vectorwise::Hashjoin::imvNum, nextProbe = 0;
  vectorwise::IMVState* imv_state = new IMVState[VECTORSIZE + 1];
  __mmask8 m_match = 0, m_new_probes = -1;
  __m512i v_base_offset = _mm512_set_epi64(7, 6, 5, 4, 3, 2, 1, 0);
  __m512i v_offset = _mm512_set1_epi64(0), v_new_build_key, v_build_keys;
  __m512i v_base_offset_upper = _mm512_set1_epi64(probe_num), v_index_offset;
  __m512i v_seed = _mm512_set1_epi64(primitives::seed), v_build_key_off = _mm512_set1_epi64(keyOff);
  __m512i v_probe_hash = _mm512_set1_epi64(0), v_zero = _mm512_set1_epi64(0);
  __m256i v256_zero = _mm256_set1_epi32(0), v256_probe_keys, v256_build_keys;

  while (done < imvNum) {
    k = (k >= imvNum) ? 0 : k;
    if (nextProbe >= probe_num) {
      if (imv_state[k].m_valid_probe == 0 && imv_state[k].stage != 3) {
        ++done;
        imv_state[k].stage = 3;
        ++k;
        continue;
      }
    }
    switch (imv_state[k].stage) {
      case 1: {
        /// step 1: load the offsets of probing tuples
#if SEQ_PREFETCH
        _mm_prefetch((((char* )(probe_keys+nextProbe))+PDIS), _MM_HINT_T0);
        _mm_prefetch((((char* )(probe_keys+nextProbe))+PDIS+64), _MM_HINT_T0);
        _mm_prefetch((((char* )(probe_keys+nextProbe))+PDIS+128), _MM_HINT_T0);
#endif
#if 1
        if (pos_buff) {
          v_offset = _mm512_add_epi64(_mm512_set1_epi64(nextProbe), v_base_offset);
          v_index_offset = _mm512_maskz_expand_epi64(_mm512_knot(imv_state[k].m_valid_probe), v_offset);
          imv_state[k].v_probe_offset = _mm512_mask_expandloadu_epi64(imv_state[k].v_probe_offset, _mm512_knot(imv_state[k].m_valid_probe), (char*) (pos_buff + nextProbe));
          m_new_probes = _mm512_knot(imv_state[k].m_valid_probe);
          nextProbe = nextProbe + _mm_popcnt_u32(m_new_probes);
          imv_state[k].m_valid_probe = _mm512_cmpgt_epu64_mask(v_base_offset_upper, v_index_offset);
          m_new_probes = _mm512_kand(m_new_probes, imv_state[k].m_valid_probe);
        } else {
          v_offset = _mm512_add_epi64(_mm512_set1_epi64(nextProbe), v_base_offset);
          imv_state[k].v_probe_offset = _mm512_mask_expand_epi64(imv_state[k].v_probe_offset, _mm512_knot(imv_state[k].m_valid_probe), v_offset);
          // count the number of empty tuples
          m_new_probes = _mm512_knot(imv_state[k].m_valid_probe);
          nextProbe = nextProbe + _mm_popcnt_u32(m_new_probes);
          imv_state[k].m_valid_probe = _mm512_cmpgt_epu64_mask(v_base_offset_upper, imv_state[k].v_probe_offset);
          m_new_probes = _mm512_kand(m_new_probes, imv_state[k].m_valid_probe);
        }
        /// step 2: gather the probe keys
        v256_probe_keys = _mm512_mask_i64gather_epi32(v256_zero, m_new_probes, imv_state[k].v_probe_offset, (void* )probe_keys, 4);
        imv_state[k].v_probe_keys = _mm512_mask_blend_epi64(m_new_probes, imv_state[k].v_probe_keys, _mm512_cvtepi32_epi64(v256_probe_keys));
#else
        v_offset = _mm512_add_epi64(_mm512_set1_epi64(nextProbe), v_base_offset);
        imv_state[k].v_probe_offset = _mm512_mask_expand_epi64(imv_state[k].v_probe_offset, _mm512_knot(imv_state[k].m_valid_probe), v_offset);
        // count the number of empty tuples
        m_new_probes = _mm512_knot(imv_state[k].m_valid_probe);
        int cur = nextProbe;
        nextProbe = nextProbe + _mm_popcnt_u32(m_new_probes);
        imv_state[k].m_valid_probe = _mm512_cmpgt_epu64_mask(v_base_offset_upper, imv_state[k].v_probe_offset);
        m_new_probes = _mm512_kand(m_new_probes, imv_state[k].m_valid_probe);
        v256_probe_keys = _mm256_maskz_expandloadu_epi32(m_new_probes, (char *) (probe_keys + cur));
        imv_state[k].v_probe_keys = _mm512_mask_blend_epi64(m_new_probes, imv_state[k].v_probe_keys, _mm512_cvtepi32_epi64(v256_probe_keys));
#endif
        /// step 3: compute the hash values of probe keys
        v_probe_hash = runtime::MurMurHash()((imv_state[k].v_probe_keys), (v_seed));
        /// step 4: find the addresses of corresponding buckets for new probes
        Vec8uM v_new_bucket_addrs = hash_table->find_chain_tagged_sel((v_probe_hash), m_new_probes);
        // the addresses are null, then the corresponding probes are invalid
        imv_state[k].m_valid_probe = _mm512_kand(_mm512_kor(_mm512_knot(m_new_probes), v_new_bucket_addrs.mask), imv_state[k].m_valid_probe);
        imv_state[k].v_bucket_addrs = _mm512_mask_blend_epi64(v_new_bucket_addrs.mask, imv_state[k].v_bucket_addrs, v_new_bucket_addrs.vec);
        imv_state[k].stage = 0;
        uint64_t * ht_pos = (uint64_t *) &imv_state[k].v_bucket_addrs;
        for (int i = 0; i < VECTORSIZE; ++i) {
          _mm_prefetch((char * )(ht_pos[i]), _MM_HINT_T0);
          _mm_prefetch(((char * )(ht_pos[i]) + 64), _MM_HINT_T0);
        }
      }
        break;
      case 0: {
        /// step 5: gather the all new build keys
        v256_build_keys = _mm512_mask_i64gather_epi32(v256_zero, imv_state[k].m_valid_probe, _mm512_add_epi64(imv_state[k].v_bucket_addrs, v_build_key_off), nullptr, 1);
        v_build_keys = _mm512_cvtepi32_epi64(v256_build_keys);
        /// step 6: compare the probe keys and build keys and write points
        m_match = _mm512_cmpeq_epi64_mask(imv_state[k].v_probe_keys, v_build_keys);
        m_match = _mm512_kand(m_match, imv_state[k].m_valid_probe);
        pos = pos + VECTORSIZE < morselSize ? pos : 0;
#if WRITE_SEQ_PREFETCH
        _mm_prefetch((char *)(((char *)(output_build+pos)) + PDIS), _MM_HINT_T0);
        _mm_prefetch((char *)(((char *)(output_build+pos)) + PDIS + 64), _MM_HINT_T0);
        _mm_prefetch((char *)(((char *)(output_probe+pos)) + PDIS), _MM_HINT_T0);
        _mm_prefetch((char *)(((char *)(output_probe+pos)) + PDIS + 64), _MM_HINT_T0);
#endif
        _mm512_mask_compressstoreu_epi64((output_build + pos), m_match, imv_state[k].v_bucket_addrs);
        _mm256_mask_compressstoreu_epi32((output_probe + pos), m_match, _mm512_cvtepi64_epi32(imv_state[k].v_probe_offset));
        pos += _mm_popcnt_u32(m_match);
        found += _mm_popcnt_u32(m_match);
        /// step 7: move to the next bucket nodes
        imv_state[k].v_bucket_addrs = _mm512_mask_i64gather_epi64(v_zero, imv_state[k].m_valid_probe, imv_state[k].v_bucket_addrs, nullptr, 1);
        imv_state[k].m_valid_probe = _mm512_kand(imv_state[k].m_valid_probe, _mm512_cmpneq_epi64_mask(imv_state[k].v_bucket_addrs, v_zero));
        imv_state[k].stage = 1;
      }
        break;
    }
    ++k;
  }
  k = 100;
  return found;
}
