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

}
