#include "imv/PipelineTPCH.hpp"
using namespace std;

size_t agg_raw_q1(size_t begin, size_t end, Database& db,
                  Hashmapx<types::Integer, tuple<Numeric<12, 2>, Numeric<12, 2>, Numeric<12, 4>, Numeric<12, 6>, int64_t>, hashFun, false>* hash_table,
                  PartitionedDeque<PARTITION_SIZE>* partition, void** entry_addrs, void** results_entry) {
  size_t found = 0, pos = 0, cur = begin;
  types::Date c1 = types::Date::castString("1998-09-02");
  types::Numeric<12, 2> one = types::Numeric<12, 2>::castString("1.00");
  auto& li = db["lineitem"];
  auto l_returnflag = li["l_returnflag"].data<types::Char<1>>();
  auto l_linestatus = li["l_linestatus"].data<types::Char<1>>();
  auto l_extendedprice = li["l_extendedprice"].data<types::Numeric<12, 2>>();
  auto l_discount = li["l_discount"].data<types::Numeric<12, 2>>();
  auto l_tax = li["l_tax"].data<types::Numeric<12, 2>>();
  auto l_quantity = li["l_quantity"].data<types::Numeric<12, 2>>();
  auto l_shipdate = li["l_shipdate"].data<types::Date>();
  using group_t = Hashmapx<types::Integer, tuple<Numeric<12, 2>, Numeric<12, 2>,
  Numeric<12, 4>, Numeric<12, 6>, int64_t>, hashFun, false>::Entry;
  hash_t hash_value;
  uint32_t key;
  group_t* entry = nullptr, *old_entry = nullptr;
  uint64_t* values,*values_old;
  for (size_t cur = begin; cur < end; ++cur) {
    if (nullptr == entry_addrs) {
      *(char*) (((char*) (&key)) + 0) = l_returnflag[cur].value;
      *(char*) (((char*) (&key)) + 1) = l_linestatus[cur].value;
      hash_value = hashFun()(key, primitives::seed);
      entry = hash_table->findOneEntry(key, hash_value);
      values = (uint64_t*) (((char*) entry) + offsetof(group_t, v));
      if (!entry) {
        entry = (group_t*) partition->partition_allocate(hash_value);
        entry->h.hash = hash_value;
        entry->h.next = nullptr;
        entry->k = key;
        // initialize values
        *values = types::Numeric<12, 2>().value;
        *(values + 1) = types::Numeric<12, 2>().value;
        *(values + 2) = types::Numeric<12, 4>().value;
        *(values + 3) = types::Numeric<12, 6>().value;
        *(values + 4) = 0;
        hash_table->insert<false>(*entry);
        ++found;
      }

      // update aggregators
      *values += l_quantity[cur].value;
      *(values + 1) += l_extendedprice[cur].value;
      auto disc_price = l_extendedprice[cur] * (one - l_discount[cur]);
      *(values + 2) += disc_price.value;
      auto charge = disc_price * (one + l_tax[cur]);
      *(values + 3) += charge.value;
      *(values + 4) += 1;
    } else {
      old_entry = (group_t*) entry_addrs[cur];
      entry = hash_table->findOneEntry(old_entry->k, old_entry->h.hash);
      if (!entry) {
        old_entry->h.next = nullptr;
        hash_table->insert<false>(*old_entry);
        results_entry[found++] = entry_addrs[cur];
      } else {
        // update aggregators with old's
        values = (uint64_t*) (((char*) entry) + offsetof(group_t, v));
        values_old = (uint64_t*) (((char*) old_entry) + offsetof(group_t, v));
        *values += *values_old;
        *(values + 1) += *(values_old + 1);
        *(values + 2) += *(values_old + 2);
        *(values + 3) += *(values_old + 3);
        *(values + 4) += *(values_old + 4);
      }
    }
  }

  return found;
}
