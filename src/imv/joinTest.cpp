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

using namespace runtime;
using namespace std;
using vectorwise::primitives::Char_10;
using vectorwise::primitives::hash_t;

// select count(*) from lineitem, orders where  l_orderkey = o_orderkey;

bool join_hyper(Database& db, size_t nrThreads) {

  auto resources = initQuery(nrThreads);

  auto& ord = db["orders"];
  auto& li = db["lineitem"];

  auto o_orderkey = ord["o_orderkey"].data<types::Integer>();
  auto l_orderkey = li["l_orderkey"].data<types::Integer>();
  using hash = runtime::CRC32Hash;
  using range = tbb::blocked_range<size_t>;
  const auto add = [](const size_t& a, const size_t& b) {return a + b;};

  const size_t morselSize = 100000;

  // build a hash table from [orders]
  Hashset<types::Integer, hash> ht1;
  tbb::enumerable_thread_specific<runtime::Stack<decltype(ht1)::Entry>> entries1;
  auto found1 = tbb::parallel_reduce(range(0, ord.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
    auto found = f;
    auto& entries = entries1.local();
    for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
      if (true) {
        entries.emplace_back(ht1.hash(o_orderkey[i]), o_orderkey[i]);
        found++;
      }
    }
    return found;
  },
                                     add);
  ht1.setSize(found1);
  parallel_insert(entries1, ht1);

  // look up the hash table 1
  auto found2 = tbb::parallel_reduce(range(0, li.nrTuples, morselSize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
    auto found = f;
    for (size_t i = r.begin(), end = r.end(); i != end; ++i)
    if ( ht1.contains(l_orderkey[i])) {
      found++;
    }
    return found;
  },
                                     add);
  cout << "hyper join results :" << found2 << endl;
  leaveQuery(nrThreads);

  return true;
}
auto joinFun = &vectorwise::Hashjoin::joinAMAC;
std::unique_ptr<Q3Builder::Q3> Q3Builder::getQuery() {
   using namespace vectorwise;
   auto result = Result();
   previous = result.resultWriter.shared.result->participate();
   auto r = make_unique<Q3>();

   auto order = Scan("orders");
   auto lineitem = Scan("lineitem");

   HashJoin(Buffer(cust_ord, sizeof(pos_t)), joinFun)
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
      auto leader = barrier();
      if (leader){
        cout<<"vectorwise join result: "<< aggr.load()<<endl;
      }
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

  PerfEvents e;
  Database tpch;
  // load tpch data
  importTPCH(argv[2], tpch);

  // run queries
  auto repetitions = atoi(argv[1]);
  size_t nrThreads = std::thread::hardware_concurrency();
  size_t vectorSize = 1024;
  bool clearCaches = false;
  if (argc > 3)
    nrThreads = atoi(argv[3]);

  tbb::task_scheduler_init scheduler(nrThreads);
#if 1
#if 0
  joinFun = &vectorwise::Hashjoin::joinAllSIMD;
  e.timeAndProfile("joinAllSIMD    ",
      nrTuples(tpch, {"orders", "lineitem"}),
      [&]() {
        join_vectorwise(tpch,nrThreads,vectorSize);
      },
      repetitions);
  joinFun = &vectorwise::Hashjoin::joinAllParallel;

  e.timeAndProfile("joinAllParallel",
      nrTuples(tpch, {"orders", "lineitem"}),
      [&]() {
        join_vectorwise(tpch,nrThreads,vectorSize);
      },
      repetitions);
  joinFun = &vectorwise::Hashjoin::joinRow;
  e.timeAndProfile("joinRow       ",
      nrTuples(tpch, {"orders", "lineitem"}),
      [&]() {
        join_vectorwise(tpch,nrThreads,vectorSize);
      },
      repetitions);
  joinFun = &vectorwise::Hashjoin::joinAMAC;
  e.timeAndProfile("joinAMAC      ",
      nrTuples(tpch, {"orders", "lineitem"}),
      [&]() {
        join_vectorwise(tpch,nrThreads,vectorSize);
      },
      repetitions);
        joinFun = &vectorwise::Hashjoin::joinFullSIMD;
  e.timeAndProfile("joinFullSIMD  ",
      nrTuples(tpch, {"orders", "lineitem"}),
      [&]() {
        join_vectorwise(tpch,nrThreads,vectorSize);
      },
      repetitions);

  joinFun = &vectorwise::Hashjoin::joinSIMDAMAC;
  e.timeAndProfile("joinSIMDAMAC  ",
      nrTuples(tpch, {"orders", "lineitem"}),
      [&]() {
        join_vectorwise(tpch,nrThreads,vectorSize);
      },
      repetitions);
#endif
  joinFun = &vectorwise::Hashjoin::joinIMV;
  e.timeAndProfile("joinIMV       ",
      nrTuples(tpch, {"orders", "lineitem"}),
      [&]() {
        join_vectorwise(tpch,nrThreads,vectorSize);
      },
      repetitions);
  e.timeAndProfile("join hyper     ",
      nrTuples(tpch, {"orders", "lineitem"}),
      [&]() {
        join_hyper(tpch,nrThreads);
      },
      repetitions);
#else
  join_hyper(tpch, nrThreads);
  join_vectorwise(tpch,nrThreads,1000);
#endif
  scheduler.terminate();
  return 0;
}
