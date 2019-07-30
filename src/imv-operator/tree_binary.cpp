#include <string>
#include <utility>
#include <vector>
#include<iostream>
#include "imv-operator/no_partitioning_join.h"
#include "imv-operator/tree_node.h"
#include "tbb/tbb.h"

using std::__cxx11::string;
using std::make_pair;
using std::pair;
using std::vector;
void build_tree_st(tree_t *tree, relation_t *rel) {
  chainedtnodebuffer_t *cb = chainedtnodebuffer_init();
  tree->first_node = nb_next_writepos(cb);
  tree->buffer = cb;
  tuple_t *first_tuple = rel->tuples;
  tree->first_node->key = first_tuple->key;
  tree->first_node->payload = first_tuple->payload;
  tuple_t *tp = NULL;
  tnode_t *node, *new_node;
  tree->num = 1;
  for (uint64_t i = 1; i < rel->num_tuples; ++i) {
    tp = rel->tuples + i;
    node = tree->first_node;
    while (NULL != node) {
      if (tp->key < node->key) {
        if (NULL == node->lnext) {
          new_node = nb_next_writepos(cb);
          new_node->key = tp->key;
          new_node->payload = tp->payload;
          ++tree->num;
          node->lnext = new_node;
          break;
        } else {
          node = node->lnext;
        }
      } else if (tp->key > node->key) {
        if (NULL == node->rnext) {
          new_node = nb_next_writepos(cb);
          new_node->key = tp->key;
          new_node->payload = tp->payload;
          ++tree->num;
          node->rnext = new_node;
          break;
        } else {
          node = node->rnext;
        }
      } else {
        break;
      }
    }
  }
}
int64_t search_tree_raw(tree_t *tree, relation_t *rel, void *output) {
  int64_t matches = 0;
  tuple_t *tp = NULL;
  tnode_t *node = tree->first_node;
  chainedtuplebuffer_t *chainedbuf = (chainedtuplebuffer_t *)output;
  for (uint64_t i = 0; i < rel->num_tuples; i++) {
    tp = rel->tuples + i;
    node = tree->first_node;
    while (NULL != node) {
      if (tp->key < node->key) {
        node = node->lnext;
      } else if (tp->key > node->key) {
        node = node->rnext;
      } else {
        ++matches;
#ifdef JOIN_RESULT_MATERIALIZE
        /* copy to the result buffer */
        tuple_t *joinres = cb_next_writepos(chainedbuf);
        joinres->key = node->payload;   /* R-rid */
        joinres->payload = tp->payload; /* S-rid */
#endif
        break;
      }
    }
  }
  return matches;
}
int64_t search_tree_AMAC(tree_t *tree, relation_t *rel, void *output) {
  int64_t matches = 0;
  int16_t k = 0, done = 0, j = 0;
  tuple_t *tp = NULL;
  tree_state_t state[ScalarStateSize];
  tnode_t *node = NULL;
  chainedtuplebuffer_t *chainedbuf = (chainedtuplebuffer_t *)output;
  // init # of the state
  for (int i = 0; i < ScalarStateSize; ++i) {
    state[i].stage = 1;
  }

  for (uint64_t cur = 0; done < ScalarStateSize;) {
    k = (k >= ScalarStateSize) ? 0 : k;
    switch (state[k].stage) {
      case 1: {
        if (cur >= rel->num_tuples) {
          ++done;
          state[k].stage = 3;
          break;
        }
#if SEQPREFETCH
        _mm_prefetch((char *)(rel->tuples + cur) + PDIS, _MM_HINT_T0);
#endif
        state[k].b = tree->first_node;
        state[k].tuple_id = cur;
        state[k].stage = 0;
        ++cur;
        _mm_prefetch((char *)(tree->first_node), _MM_HINT_T0);
      } break;
      case 0: {
        tp = rel->tuples + state[k].tuple_id;
        node = state[k].b;
        if (tp->key == node->key) {
          ++matches;
#ifdef JOIN_RESULT_MATERIALIZE
          /* copy to the result buffer */
          tuple_t *joinres = cb_next_writepos(chainedbuf);
          joinres->key = node->payload;   /* R-rid */
          joinres->payload = tp->payload; /* S-rid */
#endif
          state[k].stage = 1;
          --k;
        } else {
          if (tp->key < node->key) {
            node = node->lnext;
          } else if (tp->key > node->key) {
            node = node->rnext;
          }
          if (node) {
            _mm_prefetch((char *)(node), _MM_HINT_T0);
            state[k].b = node;
          } else {
            state[k].stage = 1;
            --k;
          }
        }
      } break;
    }
    ++k;
  }
  return matches;
}
volatile char g_lock;
volatile uint64_t total_num = 0;
void *bts_thread(void *param) {
  int rv;
  total_num = 0;
  tree_arg_t *args = (tree_arg_t *)param;
  struct timeval t1, t2;
  int deltaT = 0;

#ifdef PERF_COUNTERS
  if (args->tid == 0) {
    PCM_initPerformanceMonitor(NULL, NULL);
    PCM_start();
  }
#endif

  /* wait at a barrier until each thread starts and start timer */
  BARRIER_ARRIVE(args->barrier, rv);

#ifndef NO_TIMING
  /* the first thread checkpoints the start time */
  if (args->tid == 0) {
    gettimeofday(&args->start, NULL);
    startTimer(&args->timer1);
    startTimer(&args->timer2);
    args->timer3 = 0; /* no partitionig phase */
  }
#endif
  if (args->tid == 0) {
    gettimeofday(&t1, NULL);
    /* insert tuples from the assigned part of relR to the ht */
    build_tree_st(args->tree, &args->relR);

    /* wait at a barrier until each thread completes build phase */
    gettimeofday(&t2, NULL);
    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
    printf("--------build tree costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
    // print_hashtable(args->ht);
    printf("size of tnode_t = %d, total num = %lld\n", sizeof(tnode_t),
           args->tree->num);
  }
  BARRIER_ARRIVE(args->barrier, rv);
#ifdef PERF_COUNTERS
  if (args->tid == 0) {
    PCM_stop();
    PCM_log("========== Build phase profiling results ==========\n");
    PCM_printResults();
    PCM_start();
  }
  /* Just to make sure we get consistent performance numbers */
  BARRIER_ARRIVE(args->barrier, rv);
#endif

#ifndef NO_TIMING
  /* build phase finished, thread-0 checkpoints the time */
  if (args->tid == 0) {
    stopTimer(&args->timer2);
  }
#endif
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }

  ////////// compact, do two branches in the integration

  /*chainedtuplebuffer_t *chainedbuf_compact = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results =
        probe_simd_amac_compact2(args->ht, &args->relS, chainedbuf_compact);
    lock(&g_lock);
    #if DIVIDE
    total_num += args->num_results;
#else
    total_num = args->num_results;
#endif
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("---- COMPACT2 probe costs time (ms) = %lf\n",
             deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_compact);
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }
    //
*/
  chainedtuplebuffer_t *chainedbuf_compact1 = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results = bts_smv(args->tree, &args->relS, chainedbuf_compact1);
    lock(&g_lock);
#if DIVIDE
    total_num += args->num_results;
#else
    total_num = args->num_results;
#endif
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("------ SMV bts costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_compact1);
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }

  ////////////////raw GP probe
  /*  chainedtuplebuffer_t *chainedbuf_rp = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results =
        probe_hashtable_raw_prefetch(args->ht, &args->relS, chainedbuf_rp);
    lock(&g_lock);
#if DIVIDE
    total_num += args->num_results;
#else
    total_num = args->num_results;
#endif
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("------RAW GP probe costs time (ms) = %lf\n", deltaT * 1.0 /
  1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_rp);
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }
  */
  ////////////////GP probe
  /*chainedtuplebuffer_t *chainedbuf_gp = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results = probe_gp(args->ht, &args->relS, chainedbuf_gp);
    lock(&g_lock);
    #if DIVIDE
    total_num += args->num_results;
#else
    total_num = args->num_results;
#endif
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("-------- GP  probe costs time (ms) = %lf\n", deltaT * 1.0 /
  1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_gp);
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }
*/
  ////////////////AMAC probe
  chainedtuplebuffer_t *chainedbuf_amac = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results =
        search_tree_AMAC(args->tree, &args->relS, chainedbuf_amac);
    lock(&g_lock);
#if DIVIDE
    total_num += args->num_results;
#else
    total_num = args->num_results;
#endif
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("--------AMAC tree costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_amac);
  //////////////////
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }

  ////////////////SIMD GP probe
  /*
  chainedtuplebuffer_t *chainedbuf_simd_gp = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results =
        probe_simd_gp(args->ht, &args->relS, chainedbuf_simd_gp);
    lock(&g_lock);
    #if DIVIDE
    total_num += args->num_results;
#else
    total_num = args->num_results;
#endif
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("-----SIMD GP probe costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_simd_gp);
  //////////////////
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }

  */  ////////////////SIMD AMAC probe
  chainedtuplebuffer_t *chainedbuf_simd_amac = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results =
        bts_simd_amac(args->tree, &args->relS, chainedbuf_simd_amac);
    lock(&g_lock);
#if DIVIDE
    total_num += args->num_results;
#else
    total_num = args->num_results;
#endif
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("---SIMD AMAC bts costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_simd_amac);
  //////////////////
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }

  ////////////////SIMD AMAC probe raw
  chainedtuplebuffer_t *chainedbuf_simd_amac_raw = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results =
        bts_simd_amac_raw(args->tree, &args->relS, chainedbuf_simd_amac_raw);
    lock(&g_lock);
#if DIVIDE
    total_num += args->num_results;
#else
    total_num = args->num_results;
#endif
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("---  SIMD AMAC RAW costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_simd_amac_raw);
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }
  ////////////////SIMD probe full vectorization
  chainedtuplebuffer_t *chainedbuf_simd = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results = bts_simd(args->tree, &args->relS, chainedbuf_simd);
    lock(&g_lock);
#if DIVIDE
    total_num += args->num_results;
#else
    total_num = args->num_results;
#endif
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("--------SIMD bts costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_simd);
  //////////////////
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }

  //////// raw probe in scalar
  chainedtuplebuffer_t *chainedbuf = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results = search_tree_raw(args->tree, &args->relS, chainedbuf);
    lock(&g_lock);
#if DIVIDE
    total_num += args->num_results;
#else
    total_num = args->num_results;
#endif
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("-------- RAW bts costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf);

//------------------------------------
#ifdef JOIN_RESULT_MATERIALIZE
  args->threadresult->nresults = args->num_results;
  args->threadresult->threadid = args->tid;
// args->threadresult->results = (void *)chainedbuf;
#endif

#ifndef NO_TIMING

  /* for a reliable timing we have to wait until all finishes */
  BARRIER_ARRIVE(args->barrier, rv);

  /* probe phase finished, thread-0 checkpoints the time */
  if (args->tid == 0) {
    stopTimer(&args->timer1);
    gettimeofday(&args->end, NULL);
  }
#endif

#ifdef PERF_COUNTERS
  if (args->tid == 0) {
    PCM_stop();
    PCM_log("========== Probe phase profiling results ==========\n");
    PCM_printResults();
    PCM_log("===================================================\n");
    PCM_cleanup();
  }
  /* Just to make sure we get consistent performance numbers */
  BARRIER_ARRIVE(args->barrier, rv);
#endif
  return 0;
}
typedef int64_t (*BTSFun)(tree_t *tree, relation_t *rel, void *output);

static void tbb_run(relation_t *relR, relation_t *relS, int nthreads) {
  int nrThreads = nthreads;

  tbb::task_scheduler_init scheduler(nrThreads);
//  auto resources = initQuery(nrThreads);
  using range = tbb::blocked_range<size_t>;
  const auto add = [](const size_t& a, const size_t& b) {return a + b;};
  bucket_buffer_t *overflowbuf;
  tree_t *tree = (tree_t *)malloc(sizeof(tree_t));
  tree->buffer = NULL;

  build_tree_st(tree, relR);
  uint64_t found2=0;
  int morsesize = 10000;
  if(nrThreads==1){
    morsesize = relS->num_tuples;
  }
  auto probe=[&](BTSFun fun){
   found2 = tbb::parallel_reduce(range(0, relS->num_tuples, morsesize), 0, [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
    auto found = f;
    relation_t rel;
    chainedtuplebuffer_t *chainedbuf = chainedtuplebuffer_init();

    rel.num_tuples=r.end()-r.begin();
    rel.tuples= relS->tuples+r.begin();
    found+=fun(tree,&rel,chainedbuf);
    chainedtuplebuffer_free(chainedbuf);

    return found;
  },
                                     add);
  };
  PerfEvents event;
  vector<pair<string, BTSFun> > agg_name2fun;
  int repetitions=5;
  agg_name2fun.push_back(make_pair("AMAC", search_tree_AMAC));
  agg_name2fun.push_back(make_pair("FVA", bts_simd_amac));
  agg_name2fun.push_back(make_pair("DVA", bts_simd_amac_raw));
  agg_name2fun.push_back(make_pair("SIMD", bts_simd));
  agg_name2fun.push_back(make_pair("IMV", bts_smv));
//  agg_name2fun.push_back(make_pair("Naive", probe_hashtable));

  for (auto name2fun : agg_name2fun) {
    event.timeAndProfile(name2fun.first, 10000,[&](){ probe(name2fun.second);}, repetitions);

  }
//  event.timeAndProfile("imv_probe",10000,probe,2,0);

  std::cout<<"num of results = "<<found2<<" threads = "<<nrThreads<<std::endl;
//  leaveQuery(nrThreads);
  chainedtnodebuffer_free(tree->buffer);
  free(tree);
  scheduler.terminate();
}


result_t *BTS(relation_t *relR, relation_t *relS, int nthreads) {
  int64_t result = 0;
  int32_t numR, numS, numRthr, numSthr; /* total and per thread num */
  int i, rv;
  cpu_set_t set;
  tree_arg_t args[nthreads];
  pthread_t tid[nthreads];
  pthread_attr_t attr;
  pthread_barrier_t barrier;

  result_t *joinresult = 0;
  joinresult = (result_t *)malloc(sizeof(result_t));

#ifdef JOIN_RESULT_MATERIALIZE
  joinresult->resultlist =
      (threadresult_t *)alloc_aligned(sizeof(threadresult_t) * nthreads);
#endif

#if !USE_TBB
  pthread_attr_init(&attr);
  for (i = 0; i < nthreads; i++) {
    int cpu_idx = get_cpu_id(i);

    DEBUGMSG(1, "Assigning thread-%d to CPU-%d\n", i, cpu_idx);
#if AFFINITY
    CPU_ZERO(&set);
    CPU_SET(cpu_idx, &set);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);
#endif
  }

  tbb_run(relR,relS,nthreads);
  joinresult->totalresults = result;
  joinresult->nthreads = nthreads;
  return joinresult;
#endif

  uint32_t nbuckets = (relR->num_tuples / BUCKET_SIZE);
  tree_t *tree = (tree_t *)malloc(sizeof(tree_t));
  tree->buffer = NULL;
  numR = relR->num_tuples;
  numS = relS->num_tuples;
  numRthr = numR / nthreads;
  numSthr = numS / nthreads;

  rv = pthread_barrier_init(&barrier, NULL, nthreads);
  if (rv != 0) {
    printf("Couldn't create the barrier\n");
    exit(EXIT_FAILURE);
  }

  pthread_attr_init(&attr);
  for (i = 0; i < nthreads; i++) {
    int cpu_idx = get_cpu_id(i);

    DEBUGMSG(1, "Assigning thread-%d to CPU-%d\n", i, cpu_idx);

#if AFFINITY
    CPU_ZERO(&set);
    CPU_SET(cpu_idx, &set);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);
#endif
    args[i].tid = i;
    args[i].tree = tree;
    args[i].barrier = &barrier;

    /* assing part of the relR for next thread */
    args[i].relR.num_tuples = relR->num_tuples;
    args[i].relR.tuples = relR->tuples;

#if DIVIDE
    /* assing part of the relS for next thread */
    args[i].relS.num_tuples = (i == (nthreads - 1)) ? numS : numSthr;
    args[i].relS.tuples = relS->tuples + numSthr * i;
    numS -= numSthr;
#else
    args[i].relS.num_tuples = relS->num_tuples;
    args[i].relS.tuples = relS->tuples;
#endif
    args[i].threadresult = &(joinresult->resultlist[i]);

    rv = pthread_create(&tid[i], &attr, bts_thread, (void *)&args[i]);
    if (rv) {
      printf("ERROR; return code from pthread_create() is %d\n", rv);
      exit(-1);
    }
  }

  for (i = 0; i < nthreads; i++) {
    pthread_join(tid[i], NULL);
/* sum up results */
#if DIVIDE
    result += args[i].num_results;
#else
    result = args[i].num_results;
#endif
  }
  joinresult->totalresults = result;
  joinresult->nthreads = nthreads;

  chainedtnodebuffer_free(tree->buffer);
  free(tree);

  return joinresult;
}
