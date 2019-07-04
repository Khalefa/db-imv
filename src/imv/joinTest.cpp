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

using namespace runtime;
using namespace std;
using vectorwise::primitives::Char_10;
using vectorwise::primitives::hash_t;
#define RESULTS 1
//static  const size_t morselSize = 100000;

int repetitions=0;
// select count(*) from lineitem, orders where  l_orderkey = o_orderkey;
auto vectorJoinFun = &vectorwise::Hashjoin::joinAMAC;
auto compilerjoinFun =&probe_amac;
auto pipelineFun = &filter_probe_simd_imv;
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

  // build a hash table from [orders]
  Hashset<types::Integer, hash> ht1;
  tbb::enumerable_thread_specific<runtime::Stack<decltype(ht1)::Entry>> entries1;
  auto found1 = tbb::parallel_reduce(range(0, ord.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
    auto found = f;
    auto& entries = entries1.local();
    for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
#if 0
      if (true) {
#else
        if(o_orderdate[i] >= c1){
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
  vector<pair<string, decltype(compilerjoinFun)> >compilerName2fun;
  compilerName2fun.push_back(make_pair("probe_row",probe_row));
  compilerName2fun.push_back(make_pair("probe_simd",probe_simd));
    compilerName2fun.push_back(make_pair("probe_amac",probe_amac));
    compilerName2fun.push_back(make_pair("probe_gp",probe_gp));
  compilerName2fun.push_back(make_pair("probe_simd_amac",probe_simd_amac));
  compilerName2fun.push_back(make_pair("probe_imv",probe_imv));
     PerfEvents event;
     uint64_t found2=0;
  for(auto name2fun: compilerName2fun) {
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
#if 0
      if (true) {
#else
        if(o_orderdate[i] >= c1){
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
       vector<pair<string, decltype(pipelineFun)> >compilerName2fun;

       compilerName2fun.push_back(make_pair("filter_probe_imv1",filter_probe_imv1));
       compilerName2fun.push_back(make_pair("filter_probe_imv",filter_probe_imv));
       compilerName2fun.push_back(make_pair("filter_probe_simd_imv",filter_probe_simd_imv));
       compilerName2fun.push_back(make_pair("filter_probe_simd_gp",filter_probe_simd_gp));
       compilerName2fun.push_back(make_pair("filter_probe_simd_amac",filter_probe_simd_amac));
       compilerName2fun.push_back(make_pair("filter_probe_scalar",filter_probe_scalar));

       PerfEvents event;
       uint64_t found2=0;
       for(auto name2fun: compilerName2fun) {
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

   HashJoin(Buffer(cust_ord, sizeof(pos_t)), vectorJoinFun)
       .addBuildKey(Column(order,"o_orderkey"),conf.hash_int32_t_col(),primitives::scatter_int32_t_col)
       .addProbeKey(Column(lineitem, "l_orderkey"),          //
                    conf.hash_int32_t_col(),         //
                    primitives::keys_equal_int32_t_col);

   // count(*)
   FixedAggregation(Expression().addOp(primitives::aggr_static_count_star,
                                       Value(&r->count)));

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
      if (leader){
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
#if 0
#if 0
 PerfEvents e;
  vector<pair<string, decltype(vectorJoinFun)> >vectorName2fun;
  vectorName2fun.push_back(make_pair("joinAllSIMD",&vectorwise::Hashjoin::joinAllSIMD));
  vectorName2fun.push_back(make_pair("joinAllParallel",&vectorwise::Hashjoin::joinAllParallel));
  vectorName2fun.push_back(make_pair("joinRow",&vectorwise::Hashjoin::joinRow));
  vectorName2fun.push_back(make_pair("joinAMAC",&vectorwise::Hashjoin::joinAMAC));
  vectorName2fun.push_back(make_pair("joinFullSIMD",&vectorwise::Hashjoin::joinFullSIMD));
  vectorName2fun.push_back(make_pair("joinSIMDAMAC",&vectorwise::Hashjoin::joinSIMDAMAC));
  vectorName2fun.push_back(make_pair("joinIMV",&vectorwise::Hashjoin::joinIMV));

  for(auto name2fun : vectorName2fun) {
    vectorJoinFun = name2fun.second;
    e.timeAndProfile(name2fun.first,
        nrTuples(tpch, {"orders", "lineitem"}),
        [&]() {
          join_vectorwise(tpch,nrThreads,vectorSize);
        },
        repetitions);
  }
#endif
  vector<pair<string, decltype(compilerjoinFun)> >compilerName2fun;
  compilerName2fun.push_back(make_pair("probe_row",probe_row));
  compilerName2fun.push_back(make_pair("probe_simd",probe_simd));
  compilerName2fun.push_back(make_pair("probe_amac",probe_amac));
  compilerName2fun.push_back(make_pair("probe_simd_amac",probe_simd_amac));
  compilerName2fun.push_back(make_pair("probe_imv",probe_imv));

  for(auto name2fun: compilerName2fun) {
    compilerjoinFun=name2fun.second;
    e.timeAndProfile(name2fun.first,
        nrTuples(tpch, {"orders", "lineitem"}),
        [&]() {
          join_hyper(tpch,nrThreads);
        },
        repetitions);
  }
#else
  pipeline(tpch, nrThreads);
 //  join_hyper(tpch, nrThreads);
//  join_vectorwise(tpch,nrThreads,1000);
#endif
  scheduler.terminate();
  return 0;
}
