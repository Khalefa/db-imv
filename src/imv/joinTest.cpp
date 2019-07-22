#pragma once
#include "benchmarks/tpch/Queries.hpp"
#include "common/runtime/Hash.hpp"
#include "common/runtime/Types.hpp"
#include "hyper/GroupBy.hpp"
#include "hyper/ParallelHelper.hpp"
#include "tbb/tbb.h"
#include "vectorwise/Operations.hpp"
#include "vectorwise/Operators.hpp"
#include "vectorwise/Primitives.hpp"
#include "vectorwise/QueryBuilder.hpp"
#include "vectorwise/VectorAllocator.hpp"
#include <iostream>
#include "profile.hpp"
#include "common/runtime/Import.hpp"
#include <unordered_set>
#include "imv/HashProbe.hpp"
#include <vector>
#include "imv/Pipeline.hpp"
#include "imv/HashBuild.hpp"
#include "imv/HashAgg.hpp"
using namespace types;
using namespace runtime;
using namespace std;
using vectorwise::primitives::Char_10;
using vectorwise::primitives::hash_t;

#define RESULTS 1
//static  const size_t morselSize = 100000;
using hash = runtime::MurMurHash;
using range = tbb::blocked_range<size_t>;
int repetitions = 0;
// select count(*) from lineitem, orders where  l_orderkey = o_orderkey;
auto vectorJoinFun = &vectorwise::Hashjoin::joinAMAC;
auto compilerjoinFun = &probe_amac;
auto pipelineFun = &filter_probe_simd_imv;
auto aggFun = &agg_raw;
auto buildFun = &build_raw;

bool agg(Database& db, size_t nrThreads) {
  /*
   * select sum(l_discount) from lineitem group by l_orderkey;
   */
  auto resources = initQuery(nrThreads);
  auto& li = db["lineitem"];
  auto l_returnflag = li["l_returnflag"].data<types::Char<1>>();
  auto l_discount = li["l_discount"].data<types::Numeric<12, 2>>();
  using hash = runtime::MurMurHash;
  using range = tbb::blocked_range<size_t>;
  const auto add = [](const size_t& a, const size_t& b) {return a + b;};

  tbb::enumerable_thread_specific<Hashmapx<types::Char<1>, types::Numeric<12, 2>, hash, false>> hash_table;

  using group_t = typename decltype(hash_table)::value_type::Entry;

  /// Memory for materialized entries in hashmap
  tbb::enumerable_thread_specific<runtime::Stack<group_t>> entries;
  /// Memory for spilling hastable entries
  tbb::enumerable_thread_specific<runtime::PartitionedDeque<1024>> partitionedDeques;
  // local aggregation
  int ptimes = 0;
  auto found2 = tbb::parallel_reduce(range(0, li.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
    auto found = f;
    bool exist=false;
    auto& ht = hash_table.local(exist);
    auto& localEntries = entries.local();

    if(!exist) {
      ht.setSize(1024);
    }
    auto& partition = partitionedDeques.local(exist);
    if(!exist) {
      partition.postConstruct(nrThreads * 4, sizeof(group_t));
    }

    for (size_t i = r.begin(), end = r.end(); i < end; ++i) {
      hash_t hash_value = hash()(l_returnflag[i],primitives::seed);
      auto entry = ht.findOneEntry(l_returnflag[i],hash_value);
      if(!entry) {
        entry = (group_t*)partition.partition_allocate(hash_value);
        entry->h.hash=hash_value;
        entry->h.next = nullptr;
        entry->k = l_returnflag[i];
        entry->v = types::Numeric<12, 2>();
        ht.insert<false>(*entry);
      }
      entry->v=entry->v +l_discount[i];
      ++found;
    }
    return found;
  },
                                     add);

  auto printResult = [&](BlockRelation* result) {
    size_t found = 0;
    auto nameAttr = result->getAttribute("l_returnflag");
    auto sumAttr = result->getAttribute("sum_discount");
    for (auto& block : *result) {
      auto elementsInBlock = block.size();
      found += elementsInBlock;
      auto name = reinterpret_cast<types::Char<1>*>(block.data(nameAttr));
      auto sum = reinterpret_cast<types::Numeric<12, 2>*>(block.data(sumAttr));
      for (size_t i = 0; i < elementsInBlock; ++i) {
        cout << name[i] << "\t" << sum[i] << endl;
      }
    }
    cout << found << endl;
  };
  cout << "local agg num = " << found2 << endl;

  // global aggregation: each thread process some partitions
  // aggregate from spill partitions
  auto nrPartitions = partitionedDeques.begin()->getPartitions().size();
  /* push aggregated groups into following pipeline*/
  auto& result = resources.query->result;
  auto retAttr = result->addAttribute("l_returnflag", sizeof(types::Char<1>));
  auto discountAttr = result->addAttribute("sum_discount", sizeof(types::Numeric<12, 2>));

  tbb::parallel_for(0ul, nrPartitions, [&](auto partitionNr) {
    auto& ht = hash_table.local();
    auto& localEntries = entries.local();
    ht.clear();
    localEntries.clear();
    /* aggregate values from all deques for partitionNr
     */

    for (auto& deque : partitionedDeques) {
      auto& partition = deque.getPartitions()[partitionNr];
      for (auto chunk = partition.first; chunk; chunk = chunk->next) {
        for (auto value = chunk->template data<group_t>(),
            end = value + partition.size(chunk, sizeof(group_t));
            value < end; value++) {
          value->h.next = nullptr;/*NOTE: get rid of searching old next in the new hash table*/
          auto entry = ht.findOneEntry(value->k,value->h.hash);
          if(!entry) {
            localEntries.emplace_back(value->h.hash,value->k,types::Numeric<12, 2>());
            auto& g = localEntries.back();
            ht.insert<false>(g);
            entry = &g;
          }
          entry->v = entry->v + value->v;
        }
      }
    }

    auto consume = [&](runtime::Stack<group_t>& /*auto&*/entries) {
      auto n = entries.size();
      auto block = result->createBlock(n);
      auto ret = reinterpret_cast<types::Char<1>*>(block.data(retAttr));
      auto disc = reinterpret_cast<types::Numeric<12, 2>*>(block.data(discountAttr));
      auto ret_=ret;
      auto disc_ =disc;
      for (auto blocks : entries) {
        for (auto& entry : blocks) {
          *ret++ = entry.k;
          *disc++ = entry.v;
        }
      }
      block.addedElements(n);
    };
    if (!localEntries.empty()) {
      consume(localEntries);
    }
  });

  printResult(resources.query->result.get());
  leaveQuery(nrThreads);
  return true;
}

bool agg_intkey(Database& db, size_t nrThreads) {
  /*
   * select sum(l_discount) from lineitem group by l_returnflag;
   */
  auto resources = initQuery(nrThreads);
  auto& li = db["lineitem"];
  // auto l_returnflag = li["l_returnflag"].data<types::Char<1>>();
  auto l_orderkey = li["l_orderkey"].data<types::Integer>();

  auto l_discount = li["l_discount"].data<types::Numeric<12, 2>>();
  using hash = runtime::MurMurHash;
  using range = tbb::blocked_range<size_t>;
  const auto add = [](const size_t& a, const size_t& b) {return a + b;};

  tbb::enumerable_thread_specific<Hashmapx<types::Integer, types::Numeric<12, 2>, hash, false>> hash_table;

  using group_t = typename decltype(hash_table)::value_type::Entry;
  int agg_constrant = 2000;
  auto printResult = [&](BlockRelation* result) {
    size_t found = 0;
    auto nameAttr = result->getAttribute("l_orderkey");
    auto sumAttr = result->getAttribute("sum_discount");
    for (auto& block : *result) {
      auto elementsInBlock = block.size();
      found += elementsInBlock;
      auto name = reinterpret_cast<types::Integer*>(block.data(nameAttr));
      auto sum = reinterpret_cast<types::Numeric<12, 2>*>(block.data(sumAttr));
      for (size_t i = 0; i < elementsInBlock; ++i) {
        if(name[i].value>agg_constrant || name[i].value< agg_constrant-10) continue;
        cout << name[i] << "\t" << sum[i] << endl;
      }
    }
    cout << found << endl;
  };
  /* push aggregated groups into following pipeline*/
  auto& result = resources.query->result;
  auto retAttr = result->addAttribute("l_orderkey", sizeof(types::Integer));
  auto discountAttr = result->addAttribute("sum_discount", sizeof(types::Numeric<12, 2>));

  /// Memory for materialized entries in hashmap
  tbb::enumerable_thread_specific<runtime::Stack<group_t>> entries;
  /// Memory for spilling hastable entries
  tbb::enumerable_thread_specific<runtime::PartitionedDeque<1024>> partitionedDeques;
  /// globally collect entry addresses
  tbb::enumerable_thread_specific<vector<group_t*>> entry_addrs;
  tbb::enumerable_thread_specific<vector<group_t*>> results_addrs;

  // local aggregation
  auto found2 = tbb::parallel_reduce(range(0, li.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
    auto found = f;
    bool exist=false;
    auto& ht = hash_table.local(exist);
    //   auto& localEntries = entries.local();

                                     if(!exist) {
#if ORDERKEY
                                     ht.setSize(1500000);
#else
                                     ht.setSize(100000);

#endif
                                   }
                                   auto& partition = partitionedDeques.local(exist);
                                   if(!exist) {
                                     partition.postConstruct(nrThreads * 4, sizeof(group_t));
                                   }
                                   found += aggFun(r.begin(),r.end(),db,&ht,&partition,nullptr,nullptr);
                                   return found;
                                 },
                                     add);
  cout << "local agg num = " << found2 << endl;

// global aggregation: each thread process some partitions
// aggregate from spill partitions
  auto nrPartitions = partitionedDeques.begin()->getPartitions().size();

  tbb::parallel_for(0ul, nrPartitions, [&](auto partitionNr) {
    bool exist=false;
    auto& ht = hash_table.local(exist);
    //   auto& localEntries = entries.local();

                    if(!exist) {
#if ORDERKEY
                    ht.setSize(1500000);
#else
                    ht.setSize(100000);

#endif
                  }
                  ht.clear();
                  auto& partition = partitionedDeques.local(exist);
                  if(!exist) {
                    partition.postConstruct(nrThreads * 4, sizeof(group_t));
                  }
                  /* aggregate values from all deques for partitionNr     */
#if 0
                    auto& localEntries = entries.local();
                    localEntries.clear();
                    for (auto& deque : partitionedDeques) {
                      auto& partition = deque.getPartitions()[partitionNr];
                      for (auto chunk = partition.first; chunk; chunk = chunk->next) {
                        for (auto value = chunk->template data<group_t>(),
                            end = value + partition.size(chunk, sizeof(group_t));
                            value < end; value++) {
                          value->h.next = nullptr;/*NOTE: get rid of searching old next in the new hash table*/
                          if(value->k>agg_constrant) continue;
                          auto entry = ht.findOneEntry(value->k,value->h.hash);
                          if(!entry) {
                            localEntries.emplace_back(value->h.hash,value->k,types::Numeric<12, 2>());
                            auto& g = localEntries.back();
#if TEST_LOCAL
#else
                    ht.insert<false>(g);
#endif
                    entry = &g;
                  }
                  entry->v = entry->v + value->v;
                }
              }
            }

            auto consume = [&](runtime::Stack<group_t>& /*auto&*/entries) {
              auto n = entries.size();
              auto block = result->createBlock(n);
              auto ret = reinterpret_cast<types::Integer*>(block.data(retAttr));
              auto disc = reinterpret_cast<types::Numeric<12, 2>*>(block.data(discountAttr));
              auto ret_=ret;
              auto disc_ =disc;
              for (auto blocks : entries) {
                for (auto& entry : blocks) {
                  *ret++ = entry.k;
                  *disc++ = entry.v;
                }
              }
              block.addedElements(n);
            };
            if (!localEntries.empty()) {
              consume(localEntries);
            }
#else

                    // collect entry addresses from a partition
                    auto& entry_addrs_ = entry_addrs.local();
                    entry_addrs_.clear();
                    auto& results_addrs_ = results_addrs.local();
                    results_addrs_.clear();
                    for (auto& deque : partitionedDeques) {
                      auto& partition = deque.getPartitions()[partitionNr];
                      for (auto chunk = partition.first; chunk; chunk = chunk->next) {
                        for (auto value = chunk->template data<group_t>(), end = value + partition.size(chunk, sizeof(group_t)); value < end; value++) {
                          //    if(value->k>agg_constrant) continue;

      entry_addrs_.push_back(value);
    }
  }
}
results_addrs_.resize(entry_addrs_.size());
auto found = aggFun(0,entry_addrs_.size(),db,&ht,nullptr,(void**)&entry_addrs_[0],(void**)&results_addrs_[0]);
if(found>0) {
  auto block = result->createBlock(found);
  auto ret = reinterpret_cast<types::Integer*>(block.data(retAttr));
  auto disc = reinterpret_cast<types::Numeric<12, 2>*>(block.data(discountAttr));
#if 0
      write_results((int*)ret,(uint64_t*)disc,(void**)&results_addrs_[0],found);
#else
      for(int i=0;i<found;++i) {
        *ret++ = results_addrs_[i]->k;
        *disc++ = results_addrs_[i]->v;
      }
#endif
      block.addedElements(found);
    }

#endif
    });
  printResult(resources.query->result.get());

  leaveQuery(nrThreads);
  return true;
}
void test_agg(Database& db, size_t nrThreads) {
  vector<pair<string, decltype(aggFun)> > agg_name2fun;
  // agg_name2fun.push_back(make_pair("agg_simd", agg_simd));
  agg_name2fun.push_back(make_pair("agg_imv_merged", agg_imv_merged));
  agg_name2fun.push_back(make_pair("agg_imv", agg_imv));
  agg_name2fun.push_back(make_pair("agg_imv_serial", agg_imv_serial));
  agg_name2fun.push_back(make_pair("agg_raw", agg_raw));
  agg_name2fun.push_back(make_pair("agg_gp", agg_gp));
  agg_name2fun.push_back(make_pair("agg_amac", agg_amac));

  PerfEvents event;
  uint64_t found2 = 0;
  for (auto name2fun : agg_name2fun) {
    cout << name2fun.first << "  results :---------------" << endl;
    aggFun = name2fun.second;
    event.timeAndProfile(name2fun.first, 10000, [&]() {agg_intkey(db,nrThreads);}, repetitions);

  }
}
bool join_hyper(Database& db, size_t nrThreads) {

  auto resources = initQuery(nrThreads);
  auto c1 = types::Date::castString("1994-01-01");

  auto& ord = db["orders"];
  auto& li = db["lineitem"];

  auto o_orderkey = ord["o_orderkey"].data<types::Integer>();
  auto l_orderkey = li["l_orderkey"].data<types::Integer>();
  auto o_orderdate = ord["o_orderdate"].data<types::Date>();

//  using hash = runtime::CRC32Hash;
  using hash = runtime::MurMurHash;
  using range = tbb::blocked_range<size_t>;
  const auto add = [](const size_t& a, const size_t& b) {return a + b;};
#if 0
  // build a hash table from [orders]
  Hashset<types::Integer, hash> ht1;
  tbb::enumerable_thread_specific<runtime::Stack<decltype(ht1)::Entry>> entries1;
  auto found1 = tbb::parallel_reduce(range(0, ord.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
        auto found = f;
        auto& entries = entries1.local();
        for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
#if 1
          if (true) {
#else
            if(o_orderdate[i] >= c1) {
#endif
              entries.emplace_back(ht1.hash(o_orderkey[i]), o_orderkey[i]);
              found++;
            }
          }
          return found;
        },
        add);
    ht1.setSize(found1);
    parallel_insert(entries1, ht1);
#elif 0

    auto buildHT = [&](string funName,decltype(buildFun) build_fun) {
      Hashset<types::Integer, hash> ht1;
      ht1.setSize(ord.nrTuples);
      auto entry_size = sizeof(decltype(ht1)::Entry);
      auto found1 = tbb::parallel_reduce(range(0, ord.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
            auto found = f;

            found +=build_fun(r.begin(),r.end(),db,&ht1,&this_worker->allocator,entry_size);

            return found;
          },
          add);
      cout<<funName<<"  hash table tuples num = "<< found1<<endl;
      //   ht1.printSta();
      ht1.clear();
    };
    vector<pair<string, decltype(buildFun)> > buildName2fun;
    buildName2fun.push_back(make_pair("build_raw", build_raw));
    buildName2fun.push_back(make_pair("build_simd", build_simd));
    buildName2fun.push_back(make_pair("build_gp", build_gp));
    buildName2fun.push_back(make_pair("build_imv", build_imv));
    PerfEvents event;

    for (auto name2fun : buildName2fun) {
      event.timeAndProfile(name2fun.first, 10000, [&]() {
            buildHT(name2fun.first, name2fun.second);},
          repetitions);
    }

#else
  Hashset<types::Integer, hash> ht1;
  ht1.setSize(ord.nrTuples);
  auto entry_size = sizeof(decltype(ht1)::Entry);
  auto found1 = tbb::parallel_reduce(range(0, ord.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
    auto found = f;
#if 0
                                     for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
                                       decltype(ht1)::Entry* ptr =(decltype(ht1)::Entry*)runtime::this_worker->allocator.allocate(entry_size);

                                       ptr->k = o_orderkey[i];
                                       ptr->h.hash = ht1.hash(ptr->k);
                                       //auto head= ht1.entries+ptr->h.hash;
                                     ht1.insert_tagged(&(ptr->h),ptr->h.hash);
                                     ++found;
                                   }
#else
                                     found +=build_gp(r.begin(),r.end(),db,&ht1,&this_worker->allocator,entry_size);
#endif
                                     return found;
                                   },
                                     add);
  cout << "Build hash table tuples num = " << found1 << endl;
#endif
  uint32_t probe_off[morselSize];
  void* build_add[morselSize];
#if 0
  // look up the hash table 1
  auto found2 = tbb::parallel_reduce(range(0, li.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
        auto found = f;
#if 0
        for (size_t i = r.begin(), end = r.end(); i != end; ++i)
        if ( ht1.contains(l_orderkey[i])) {
          found++;
        }

#else
        found+=compilerjoinFun(l_orderkey+r.begin(),r.size(),&ht1,build_add,probe_off);
#endif
        return found;
      },
      add);
#if RESULTS
  cout << "hyper join results :" << found2 << endl;
#endif
#elif 1
  vector<pair<string, decltype(compilerjoinFun)> > compilerName2fun;
  compilerName2fun.push_back(make_pair("probe_row", probe_row));
  compilerName2fun.push_back(make_pair("probe_simd", probe_simd));
  compilerName2fun.push_back(make_pair("probe_amac", probe_amac));
  compilerName2fun.push_back(make_pair("probe_gp", probe_gp));
  compilerName2fun.push_back(make_pair("probe_simd_amac", probe_simd_amac));
  compilerName2fun.push_back(make_pair("probe_imv", probe_imv));
  PerfEvents event;
  uint64_t found2 = 0;
  for (auto name2fun : compilerName2fun) {
    compilerjoinFun = name2fun.second;

    event.timeAndProfile(name2fun.first, 10000, [&]() {
      found2 = tbb::parallel_reduce(range(0, li.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
            auto found = f;
#if 0
                         for (size_t i = r.begin(), end = r.end(); i != end; ++i)
                         if ( ht1.contains(l_orderkey[i])) {
                           found++;
                         }

#else
                         found+=compilerjoinFun(l_orderkey+r.begin(),r.size(),&ht1,build_add,probe_off, nullptr);
#endif
                         return found;
                       },
                       add);
                 },
                         repetitions);
#if RESULTS
    cout << "hyper join results :" << found2 << endl;
#endif
  }
#endif
  leaveQuery(nrThreads);

  return true;
}

bool pipeline(Database& db, size_t nrThreads) {

  auto resources = initQuery(nrThreads);
  auto c1 = types::Date::castString("1996-01-01");
  auto c2 = types::Numeric<12, 2>::castString("0.07");
  auto c3 = types::Integer(24);

  auto& ord = db["orders"];
  auto& li = db["lineitem"];

  auto o_orderkey = ord["o_orderkey"].data<types::Integer>();
  auto l_orderkey = li["l_orderkey"].data<types::Integer>();
  auto o_orderdate = ord["o_orderdate"].data<types::Date>();
  auto l_quantity_col = li["l_quantity"].data<types::Numeric<12, 2>>();

//  using hash = runtime::CRC32Hash;
  using hash = runtime::MurMurHash;
  using range = tbb::blocked_range<size_t>;
  const auto add = [](const size_t& a, const size_t& b) {return a + b;};

  // build a hash table from [orders]
  Hashset<types::Integer, hash> ht1;
  tbb::enumerable_thread_specific<runtime::Stack<decltype(ht1)::Entry>> entries1;
  auto found1 = tbb::parallel_reduce(range(0, ord.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
    auto found = f;
    auto& entries = entries1.local();
    for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
      if(o_orderdate[i] >= c1) {
        entries.emplace_back(ht1.hash(o_orderkey[i]), o_orderkey[i]);
        found++;
      }
    }
    return found;
  },
                                     add);
//
  ht1.setSize(found1);
  parallel_insert(entries1, ht1);
  uint32_t probe_off[morselSize];
  void* build_add[morselSize];
#if 0
  // look up the hash table 1
  auto found2 = tbb::parallel_reduce(range(0, li.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
        auto found = f;
#if 0
        for (size_t i = r.begin(), end = r.end(); i != end; ++i)
        if ( ht1.contains(l_orderkey[i])) {
          found++;
        }

#else
        found+=compilerjoinFun(l_orderkey+r.begin(),r.size(),&ht1,build_add,probe_off);
#endif
        return found;
      },
      add);
#if RESULTS
  cout << "hyper join results :" << found2 << endl;
#endif
#else
  vector<pair<string, decltype(pipelineFun)> > compilerName2fun;

  compilerName2fun.push_back(make_pair("filter_probe_imv1", filter_probe_imv1));
  compilerName2fun.push_back(make_pair("filter_probe_imv", filter_probe_imv));
  compilerName2fun.push_back(make_pair("filter_probe_simd_imv", filter_probe_simd_imv));
  compilerName2fun.push_back(make_pair("filter_probe_simd_gp", filter_probe_simd_gp));
  compilerName2fun.push_back(make_pair("filter_probe_simd_amac", filter_probe_simd_amac));
  compilerName2fun.push_back(make_pair("filter_probe_scalar", filter_probe_scalar));

  PerfEvents event;
  uint64_t found2 = 0;
  for (auto name2fun : compilerName2fun) {
    auto pipelineFun = name2fun.second;
    event.timeAndProfile(name2fun.first, 10000, [&]() {
      found2 = tbb::parallel_reduce(range(0, li.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
            auto found = f;
            uint64_t rof_buff[ROF_VECTOR_SIZE];
#if 1
                         found+=pipelineFun(r.begin(),r.end(),db,&ht1,build_add,probe_off,rof_buff);
#else
                         found+=compilerjoinFun(l_orderkey+r.begin(),r.size(),&ht1,build_add,probe_off,nullptr);
#endif
                         return found;
                       },
                       add);
                 },
                         repetitions);
#if RESULTS
    cout << "pipeline results :" << found2 << endl;
#endif
  }
#endif
  leaveQuery(nrThreads);

  return true;
}

std::unique_ptr<Q3Builder::Q3> Q3Builder::getQuery() {
  using namespace vectorwise;
  auto result = Result();
  previous = result.resultWriter.shared.result->participate();
  auto r = make_unique<Q3>();

  auto order = Scan("orders");
  auto lineitem = Scan("lineitem");

  HashJoin(Buffer(cust_ord, sizeof(pos_t)), vectorJoinFun).  //
      addBuildKey(Column(order, "o_orderkey"), conf.hash_int32_t_col(), primitives::scatter_int32_t_col).  //
      addProbeKey(Column(lineitem, "l_orderkey"), conf.hash_int32_t_col(), primitives::keys_equal_int32_t_col);

// count(*)
  FixedAggregation(Expression().addOp(primitives::aggr_static_count_star, Value(&r->count)));

//   result.addValue("count", Buffer(cust_ord))
//       .finalize();

  r->rootOp = popOperator();
  return r;
}
bool join_vectorwise(Database& db, size_t nrThreads, size_t vectorSize) {
  using namespace vectorwise;
  WorkerGroup workers(nrThreads);
  vectorwise::SharedStateManager shared;
  std::unique_ptr<runtime::Query> result;
  std::atomic<int64_t> aggr;
  aggr = 0;
  workers.run([&]() {
    Q3Builder builder(db, shared, vectorSize);
    auto query = builder.getQuery();
    /* auto found = */
    auto n_ = query->rootOp->next();
    if (n_) {
      aggr.fetch_add(query->count);
    }
#if RESULTS
              auto leader = barrier();
              if (leader) {
                cout<<"vectorwise join result: "<< aggr.load()<<endl;
              }
#endif
            });
  return true;
}

size_t nrTuples(Database& db, std::vector<std::string> tables) {
  size_t sum = 0;
  for (auto& table : tables)
    sum += db[table].nrTuples;
  return sum;
}
void test_vectorwise_probe(Database& db, size_t nrThreads) {
  PerfEvents e;
  size_t vectorSize = 1024;
  vector<pair<string, decltype(vectorJoinFun)> > vectorName2fun;
  vectorName2fun.push_back(make_pair("joinAllSIMD", &vectorwise::Hashjoin::joinAllSIMD));
  vectorName2fun.push_back(make_pair("joinAllParallel", &vectorwise::Hashjoin::joinAllParallel));
  vectorName2fun.push_back(make_pair("joinBoncz", &vectorwise::Hashjoin::joinBoncz));
  vectorName2fun.push_back(make_pair("joinRow", &vectorwise::Hashjoin::joinRow));
  vectorName2fun.push_back(make_pair("joinAMAC", &vectorwise::Hashjoin::joinAMAC));
  vectorName2fun.push_back(make_pair("joinFullSIMD", &vectorwise::Hashjoin::joinFullSIMD));
  vectorName2fun.push_back(make_pair("joinSIMDAMAC", &vectorwise::Hashjoin::joinSIMDAMAC));
  vectorName2fun.push_back(make_pair("joinIMV", &vectorwise::Hashjoin::joinIMV));

  for (auto name2fun : vectorName2fun) {
    vectorJoinFun = name2fun.second;
    e.timeAndProfile(name2fun.first, nrTuples(db, { "orders", "lineitem" }), [&]() {
      join_vectorwise(db,nrThreads,vectorSize);
    },
                     repetitions);
  }
}
int main(int argc, char* argv[]) {
  if (argc <= 2) {
    std::cerr << "Usage: ./" << argv[0] << "<number of repetitions> <path to tpch dir> [nrThreads = all] \n "
              " EnvVars: [vectorSize = 1024] [SIMDhash = 0] [SIMDjoin = 0] "
              "[SIMDsel = 0]";
    exit(1);
  }

  Database tpch;
// load tpch data
  importTPCH(argv[2], tpch);

// run queries
  repetitions = atoi(argv[1]);
  size_t nrThreads = std::thread::hardware_concurrency();
  size_t vectorSize = 1024;
  bool clearCaches = false;
  if (argc > 3)
    nrThreads = atoi(argv[3]);

  tbb::task_scheduler_init scheduler(nrThreads);

// pipeline(tpch, nrThreads);
//   join_hyper(tpch, nrThreads);
//  join_vectorwise(tpch,nrThreads,1000);
  //test_agg(tpch, nrThreads);

  test_vectorwise_probe(tpch, nrThreads);
  scheduler.terminate();
  return 0;
}
