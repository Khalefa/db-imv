#include "imv/HashAgg.hpp"
int agg_constrant = 50;

size_t agg_local_raw(size_t begin, size_t end, Database& db, Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>* hash_table,
                     PartitionedDeque<1024>*partition) {
  size_t found = 0;
  auto& li = db["lineitem"];
  // auto l_returnflag = li["l_returnflag"].data<types::Char<1>>();
  auto l_orderkey = li["l_orderkey"].data<types::Integer>();

  auto l_discount = li["l_discount"].data<types::Numeric<12, 2>>();
  using group_t = Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>::Entry;

  for (size_t i = begin; i < end; ++i) {
    if (l_orderkey[i].value > agg_constrant)
      continue;
    hash_t hash_value = hash()(l_orderkey[i], primitives::seed);
    auto entry = hash_table->findOneEntry(l_orderkey[i], hash_value);
    if (!entry) {
      entry = (group_t*) partition->partition_allocate(hash_value);
      entry->h.hash = hash_value;
      entry->h.next = nullptr;
      entry->k = l_orderkey[i];
      entry->v = types::Numeric<12, 2>();
      hash_table->insert<false>(*entry);
    }
    entry->v = entry->v + l_discount[i];
    ++found;
  }
  return found;
}

size_t agg_local_amac(size_t begin, size_t end, Database& db, Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>* hash_table,
                      PartitionedDeque<1024>* partition) {
  size_t found = 0, pos = 0, cur = begin;
  int k = 0, done = 0, keyOff = sizeof(runtime::Hashmap::EntryHeader), buildkey, probeKey;
  AMACState amac_state[stateNum];
  hash_t probeHash;
  auto& li = db["lineitem"];
  // auto l_returnflag = li["l_returnflag"].data<types::Char<1>>();
  auto l_orderkey = li["l_orderkey"].data<types::Integer>();

  auto l_discount = li["l_discount"].data<types::Numeric<12, 2>>();
  using group_t = Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>::Entry;

  // initialization
  for (int i = 0; i < stateNum; ++i) {
    amac_state[i].stage = 1;
  }

  while (done < stateNum) {
    k = (k >= stateNum) ? 0 : k;
    switch (amac_state[k].stage) {
      case 1: {
        /*        while ((l_orderkey[cur].value > agg_constrant) && (cur<end)) {
         ++cur;
         }*/
        if (cur >= end) {
          ++done;
          amac_state[k].stage = 3;
          break;
        }
        probeKey = l_orderkey[cur].value;
        probeHash = (runtime::MurMurHash()(probeKey, primitives::seed));
        amac_state[k].probeValue = l_discount[cur];
        amac_state[k].tuple_id = cur;
        ++cur;
        amac_state[k].probeKey = probeKey;
        amac_state[k].probeHash = probeHash;
        hash_table->PrefetchEntry(probeHash);
        amac_state[k].stage = 2;
      }
        break;
      case 2: {
        amac_state[k].buildMatch = hash_table->find_chain(amac_state[k].probeHash);
        if (nullptr == amac_state[k].buildMatch) {
          amac_state[k].stage = 4;
          --k;  // must immediately shift to case 4
        } else {
          _mm_prefetch((char * )(amac_state[k].buildMatch), _MM_HINT_T0);
          _mm_prefetch((char * )(amac_state[k].buildMatch) + 64, _MM_HINT_T0);
          amac_state[k].stage = 0;
        }
      }
        break;
      case 0: {
        auto entry = (group_t*) amac_state[k].buildMatch;
        buildkey = entry->k.value;
        if ((buildkey == amac_state[k].probeKey)) {
          entry->v += amac_state[k].probeValue;
          ++found;
          amac_state[k].stage = 1;
          --k;
          break;
        }
        auto entryHeader = entry->h.next;
        if (nullptr == entryHeader) {
          amac_state[k].stage = 4;
          --k;  // must immediately shift to case 4
        } else {
          amac_state[k].buildMatch = entryHeader;
          _mm_prefetch((char * )(entryHeader), _MM_HINT_T0);
          _mm_prefetch((char * )(entryHeader) + 64, _MM_HINT_T0);
        }
      }
        break;
      case 4: {
        auto entry = (group_t*) partition->partition_allocate(amac_state[k].probeHash);
        entry->h.hash = amac_state[k].probeHash;
        entry->h.next = nullptr;
        entry->k = types::Integer(amac_state[k].probeKey);
        entry->v = amac_state[k].probeValue;

        auto lastEntry = (group_t*) amac_state[k].buildMatch;
        if (lastEntry == nullptr) { /* the bucket is empty*/
          hash_table->insert<false>(*entry);
        } else {
          lastEntry->h.next = (decltype(lastEntry->h.next)) entry;
        }

        amac_state[k].stage = 1;
        ++found;
        --k;
      }
        break;
    }
    ++k;
  }

  return found;
}
size_t agg_local_gp(size_t begin, size_t end, Database& db, Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>* hash_table,
                    PartitionedDeque<1024>* partition) {
  size_t found = 0, pos = 0, cur = begin;
  int k = 0, done = 0, keyOff = sizeof(runtime::Hashmap::EntryHeader), buildkey, probeKey, valid_size;
  AMACState amac_state[stateNum];
  hash_t probeHash;
  auto& li = db["lineitem"];
  auto l_orderkey = li["l_orderkey"].data<types::Integer>();
  auto l_discount = li["l_discount"].data<types::Numeric<12, 2>>();
  using group_t = Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>::Entry;
  auto insetNewEntry = [&](AMACState& state) {
    auto entry = (group_t*) partition->partition_allocate(state.probeHash);
    entry->h.hash = state.probeHash;
    entry->h.next = nullptr;
    entry->k = types::Integer(state.probeKey);
    entry->v = state.probeValue;

    auto lastEntry = (group_t*) state.buildMatch;
    if(lastEntry == nullptr) { /* the bucket is empty*/
      hash_table->insert<false>(*entry);
    } else {
      lastEntry->h.next = (decltype(lastEntry->h.next))entry;
    }

    ++found;
  };
  while (cur < end) {
    /// step 1: get the hash key and compute hash value
    for (k = 0; (k < stateNum) && (cur < end); ++k, ++cur) {
      probeKey = l_orderkey[cur].value;
      probeHash = (runtime::MurMurHash()(probeKey, primitives::seed));
      amac_state[k].probeValue = l_discount[cur];
      amac_state[k].tuple_id = cur;
      amac_state[k].probeKey = probeKey;
      amac_state[k].probeHash = probeHash;
      amac_state[k].stage = 0;
      hash_table->PrefetchEntry(probeHash);
    }
    valid_size = k;
    done = 0;
    /// step 2: fetch the first node in the hash table bucket
    for (k = 0; k < valid_size; ++k) {
      amac_state[k].buildMatch = hash_table->find_chain(amac_state[k].probeHash);
      if (nullptr == amac_state[k].buildMatch) {
        //// must immediately write a new entry
        insetNewEntry(amac_state[k]);
        amac_state[k].stage = 4;
        ++done;
      } else {
        _mm_prefetch((char * )(amac_state[k].buildMatch), _MM_HINT_T0);
        _mm_prefetch((char * )(amac_state[k].buildMatch) + 64, _MM_HINT_T0);
      }
    }
    /// step 3: repeating probing the hash buckets
    while (done < valid_size) {
      for (k = 0; k < valid_size; ++k) {
        // done or need to insert
        if (amac_state[k].stage >= 3) {
          continue;
        }
        auto entry = (group_t*) amac_state[k].buildMatch;
        buildkey = entry->k.value;
        // found, then update the aggregators
        if ((buildkey == amac_state[k].probeKey)) {
          entry->v += amac_state[k].probeValue;
          ++found;
          amac_state[k].stage = 3;
          ++done;
          continue;
        }
        auto entryHeader = entry->h.next;
        // not found, to insert
        if (nullptr == entryHeader) {
          //// must immediately write a new entry
          insetNewEntry(amac_state[k]);
          amac_state[k].stage = 4;
          ++done;
          continue;
        } else {
          // not found, then continue
          amac_state[k].buildMatch = entryHeader;
          _mm_prefetch((char * )(entryHeader), _MM_HINT_T0);
          _mm_prefetch((char * )(entryHeader) + 64, _MM_HINT_T0);
        }
      }
    }
  }
  return found;
}
size_t agg_local_simd(size_t begin, size_t end, Database& db, Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>* hash_table,
                      PartitionedDeque<1024>* partition) {
  size_t found = 0, pos = 0, cur = begin;
  int k = 0, done = 0, buildkey, probeKey, valid_size;
  AggState state;
  hash_t probeHash;
  auto& li = db["lineitem"];
  auto l_orderkey = li["l_orderkey"].data<types::Integer>();
  auto l_discount = li["l_discount"].data<types::Numeric<12, 2>>();
  using group_t = Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>::Entry;
  __m512i v_base_offset = _mm512_set_epi64(7, 6, 5, 4, 3, 2, 1, 0), v_zero = _mm512_set1_epi64(0);
  __m512i v_offset = _mm512_set1_epi64(0), v_base_offset_upper = _mm512_set1_epi64(end - begin), v_seed = _mm512_set1_epi64(vectorwise::primitives::seed),
      v_all_ones = _mm512_set1_epi64(-1), v_conflict, v_ht_keys, v_hash_mask, v_ht_value, v_next;
  Vec8u u_new_addrs(uint64_t(0)), u_offset_hash(offsetof(group_t, h.hash)), u_offset_k(offsetof(group_t, k)), u_offset_v(offsetof(group_t, v));
  __mmask8 m_no_conflict, m_rest, m_match, m_to_insert;
  state.m_valid_probe = -1;
  void* probe_keys = (void*) l_orderkey, *probe_value = (void*) l_discount;
  __m256i v256_zero = _mm256_set1_epi32(0), v256_probe_keys, v256_probe_value, v256_ht_keys;

  auto insertNewEntry = [&]() {
    Vec8u u_probe_hash(state.v_probe_hash);
    for(int i=0;i<VECTORSIZE;++i) {
      u_new_addrs.entry[i] =0;
      if(m_no_conflict & (1<<i)) {
        u_new_addrs.entry[i] = (uint64_t)partition->partition_allocate(u_probe_hash.entry[i]);
      }
    }
    // write entry->next
      _mm512_mask_i64scatter_epi64(0,m_no_conflict,u_new_addrs.reg,v_zero,1);
      // write entry->hash
      _mm512_mask_i64scatter_epi64(0,m_no_conflict,u_new_addrs + u_offset_hash,state.v_probe_hash,1);
      // write entry->k , NOTE it is 32 bits
      _mm512_mask_i64scatter_epi32(0,m_no_conflict,u_new_addrs + u_offset_k,_mm512_cvtepi64_epi32(state.v_probe_keys),1);
      // write entry->v
      _mm512_mask_i64scatter_epi64(0,m_no_conflict,u_new_addrs + u_offset_v,state.v_probe_value,1);
    };
  for (cur = begin; cur < end;) {
    /// step 1: get offsets
    state.v_probe_offset = _mm512_add_epi64(_mm512_set1_epi64(cur), v_base_offset);
    cur += VECTORSIZE;
    state.m_valid_probe = -1;
    if (cur >= end) {
      state.m_valid_probe = (state.m_valid_probe >> (cur - end));
    }
    /// step 2: gather probe keys and values
    v256_probe_keys = _mm512_mask_i64gather_epi32(v256_zero, state.m_valid_probe, state.v_probe_offset, (void* )probe_keys, 4);
    state.v_probe_keys = _mm512_cvtepi32_epi64(v256_probe_keys);
    state.v_probe_value = _mm512_mask_i64gather_epi64(v_zero, state.m_valid_probe, state.v_probe_offset, (void* )probe_value, 8);
    /// step 3: compute hash values
    state.v_probe_hash = runtime::MurMurHash()((state.v_probe_keys), (v_seed));
    /// step 4: find the addresses of corresponding buckets for new probes
    Vec8uM v_new_bucket_addrs = hash_table->find_chain(state.v_probe_hash);

    /// insert new nodes in the corresponding hash buckets
    m_to_insert = _mm512_kandn(v_new_bucket_addrs.mask, state.m_valid_probe);
    v_hash_mask = ((Vec8u(state.v_probe_hash) & Vec8u(hash_table->mask)));
    v_conflict = _mm512_conflict_epi64(v_hash_mask);
    m_no_conflict = _mm512_testn_epi64_mask(v_conflict, v_all_ones);
    m_no_conflict = _mm512_kand(m_no_conflict, m_to_insert);

    insertNewEntry();
    // insert the new addresses to the hash table
    _mm512_mask_i64scatter_epi64(hash_table->entries, m_no_conflict, v_hash_mask, u_new_addrs.reg, 8);

    // get rid of no-conflict elements
    state.m_valid_probe = _mm512_kandn(m_no_conflict, state.m_valid_probe);
    state.v_bucket_addrs = _mm512_mask_i64gather_epi64(v_all_ones,state.m_valid_probe,v_hash_mask,hash_table->entries,8);

    while (state.m_valid_probe != 0) {
      /// step 5: gather the all new build keys
      v256_ht_keys = _mm512_mask_i64gather_epi32(v256_zero, state.m_valid_probe, _mm512_add_epi64(state.v_bucket_addrs , u_offset_k.reg), nullptr, 1);
      v_ht_keys = _mm512_cvtepi32_epi64(v256_ht_keys);
      /// step 6: compare the probe keys and build keys and write points
      m_match = _mm512_cmpeq_epi64_mask(state.v_probe_keys, v_ht_keys);
      m_match = _mm512_kand(m_match, state.m_valid_probe);
      /// update the aggregators
      v_conflict = _mm512_conflict_epi64(state.v_bucket_addrs);
      m_no_conflict = _mm512_testn_epi64_mask(v_conflict, v_all_ones);
      m_no_conflict = _mm512_kand(m_no_conflict, m_match);

      v_ht_value = _mm512_mask_i64gather_epi64(v_zero, m_no_conflict,  _mm512_add_epi64(state.v_bucket_addrs , u_offset_v.reg), nullptr, 1);
      _mm512_mask_i64scatter_epi64(0, m_no_conflict, _mm512_add_epi64(state.v_bucket_addrs,u_offset_v.reg), _mm512_add_epi64(state.v_probe_value, v_ht_value), 1);

      state.m_valid_probe = _mm512_kandn(m_no_conflict, state.m_valid_probe);
      // the remaining matches, DO NOT get next
      m_match = _mm512_kandn(m_no_conflict, m_match);

      /// step 7: NOT found, then insert
      v_next = _mm512_mask_i64gather_epi64(v_all_ones, _mm512_kandn(m_match, state.m_valid_probe), state.v_bucket_addrs, nullptr, 1);
      m_to_insert = _mm512_kand(_mm512_kandn(m_match, state.m_valid_probe), _mm512_cmpeq_epi64_mask(v_next, v_zero));
      // get rid of bucket address of matched probes
      v_next= _mm512_mask_blend_epi64(_mm512_kandn(m_match, state.m_valid_probe),v_all_ones,state.v_bucket_addrs);
      v_conflict = _mm512_conflict_epi64(v_next);
      m_no_conflict = _mm512_testn_epi64_mask(v_conflict, v_all_ones);
      m_no_conflict = _mm512_kand(m_no_conflict, m_to_insert);
      insertNewEntry();
      // insert the new addresses to the hash table
      _mm512_mask_i64scatter_epi64(0, m_no_conflict, state.v_bucket_addrs, u_new_addrs.reg, 1);

      state.m_valid_probe = _mm512_kandn(m_no_conflict, state.m_valid_probe);
      v_next = _mm512_mask_i64gather_epi64(v_all_ones, state.m_valid_probe, state.v_bucket_addrs, nullptr, 1);
      // the remaining matches, DO NOT get next
      state.v_bucket_addrs = _mm512_mask_blend_epi64(m_match,v_next,state.v_bucket_addrs);
    }
  }

  return found;
}
