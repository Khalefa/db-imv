#include "imv-operator/aggregation.hpp"
using namespace std;

size_t agg_raw(hashtable_t *ht, relation_t *rel, bucket_buffer_t **overflowbuf) {
  uint64_t key, hash_value, new_add = 0;
  tuple_t *dest;
  bucket_t *cur_node, *next_node, *bucket;
  for (size_t cur = 0; cur < rel->num_tuples; ++cur) {
    // step 1: get key
    key = rel->tuples[cur].key;
    // step 2: hash
    hash_value = HASH(key, ht->hash_mask, ht->skip_bits);
    // step 3: search hash bucket
    cur_node = ht->buckets + hash_value;
    next_node = cur_node->next;
    // step 4.1: insert new bucket at the first
    while (true) {
      if (cur_node->count == 0) {
        cur_node->count = 1;
        cur_node->tuples->key = key;
        cur_node->tuples->payload = 1;
        cur_node->next = NULL;
        ++new_add;
        break;
      } else {
        //  step 4.2: update agg
        if (cur_node->tuples->key == key) {
          cur_node->tuples->payload++;
          break;
        } else {
          next_node = cur_node->next;
          if (next_node == NULL) {
            // step 4.3: insert new bucket at the tail
            get_new_bucket(&bucket, overflowbuf);
            cur_node->next = bucket;
            bucket->next = NULL;
            bucket->tuples->key = key;
            bucket->tuples->payload = 1;
            bucket->count = 1;
            ++new_add;
            break;
          } else {
            cur_node = next_node;
          }
        }
      }
    }
  }
  return new_add;
}
size_t agg_amac(hashtable_t *ht, relation_t *rel, bucket_buffer_t **overflowbuf) {
  uint64_t key, hash_value, new_add = 0, cur = 0, end = rel->num_tuples;
  tuple_t *dest;
  bucket_t *cur_node, *next_node, *bucket;
  scalar_state_t state[ScalarStateSize];
  for (int i = 0; i < ScalarStateSize; ++i) {
    state[i].stage = 1;
  }
  int done = 0, k = 0;

  while (done < ScalarStateSize) {
    k = (k >= ScalarStateSize) ? 0 : k;
    switch (state[k].stage) {
      case 1: {
        if (cur >= end) {
          ++done;
          state[k].stage = 3;
          break;
        }
#if SEQPREFETCH
        _mm_prefetch(((char *)(rel->tuples + cur) + PDIS), _MM_HINT_T0);
#endif
        // step 1: get key
        key = rel->tuples[cur].key;
        // step 2: hash
        hash_value = HASH(key, ht->hash_mask, ht->skip_bits);
        state[k].b = ht->buckets + hash_value;
        state[k].stage = 0;
        state[k].key = key;
        ++cur;
        _mm_prefetch((char * )(state[k].b), _MM_HINT_T0);

      }
        break;
      case 0: {
        cur_node = state[k].b;
        if (cur_node->count == 0) {
          cur_node->count = 1;
          cur_node->tuples->key = state[k].key;
          cur_node->tuples->payload = 1;
          cur_node->next = NULL;
          ++new_add;
          state[k].stage = 1;
          --k;
          break;
        } else {
          //  step 4.2: update agg
          if (cur_node->tuples->key == state[k].key) {
            cur_node->tuples->payload++;
            state[k].stage = 1;
            --k;
            break;
          } else {
            next_node = cur_node->next;
            if (next_node == NULL) {
              // step 4.3: insert new bucket at the tail
              get_new_bucket(&bucket, overflowbuf);
              cur_node->next = bucket;
              bucket->next = NULL;
              bucket->tuples->key = state[k].key;
              bucket->tuples->payload = 1;
              bucket->count = 1;
              ++new_add;
              state[k].stage = 1;
              --k;
              break;
            } else {
              state[k].b = next_node;
            }
          }
        }
      }
    }
    ++k;
  }

  return new_add;
}

volatile static char g_lock = 0, g_lock_morse = 0;
volatile static uint64_t total_num = 0, global_curse = 0, global_upper;
typedef int64_t (*AGGFun)(hashtable_t *ht, relation_t *rel, bucket_buffer_t **overflowbuf);
volatile static struct Fun {
  AGGFun fun_ptr;
  char fun_name[8];
} pfun[10];
volatile static int pf_num = 0;
static void morse_driven(void*param, AGGFun fun, bucket_buffer_t **overflowbuf) {
  arg_t *args = (arg_t *) param;
  uint64_t base = 0, num = 0;
  args->num_results = 0;
  relation_t relS;
  relS.tuples = args->relS.tuples;
  relS.num_tuples = 0;
  while (1) {
    lock(&g_lock_morse);
    base = global_curse;
    global_curse += MORSE_SIZE;
    unlock(&g_lock_morse);
    if (base >= global_upper) {
      break;
    }
    num = (global_upper - base) < MORSE_SIZE ? (global_upper - base) : MORSE_SIZE;
    relS.tuples = args->relS.tuples + base;
    relS.num_tuples = num;
    args->num_results += fun(args->ht, &relS, overflowbuf);
  }
}

void *agg_thread(void *param) {
  int rv;
  arg_t *args = (arg_t *) param;
  struct timeval t1, t2;
  int deltaT = 0, thread_num = 0;
  bucket_buffer_t *overflowbuf;
  init_bucket_buffer(&overflowbuf);
  hashtable_t *ht;
  uint32_t nbuckets = (args->relR.num_tuples / BUCKET_SIZE);
  if (args->tid == 0) {
    strcpy(pfun[0].fun_name, "IMV");
    strcpy(pfun[1].fun_name, "AMAC");
    strcpy(pfun[2].fun_name, "FVA");
    strcpy(pfun[3].fun_name, "DVA");
    strcpy(pfun[4].fun_name, "SIMD");
    strcpy(pfun[5].fun_name, "Naive");

    pfun[0].fun_ptr = agg_raw;
    pfun[1].fun_ptr = agg_amac;
    pfun[2].fun_ptr = agg_raw;
    pfun[3].fun_ptr = agg_raw;
    pfun[4].fun_ptr = agg_raw;
    pfun[5].fun_ptr = agg_raw;

    pf_num = 2;
  }
  BARRIER_ARRIVE(args->barrier, rv);

  for (int fid = 0; fid < pf_num; ++fid) {
    for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
      allocate_hashtable(&ht, nbuckets);
      BARRIER_ARRIVE(args->barrier, rv);
      gettimeofday(&t1, NULL);

#if MORSE_SIZE
      args->ht = ht;
      morse_driven(param, pfun[fid].fun_ptr, &overflowbuf);
#else
      args->num_results = pfun[fid].fun_ptr(ht, &args->relS, &overflowbuf);
#endif
      lock(&g_lock);
#if DIVIDE
      total_num += args->num_results;
#elif MORSE_SIZE
      total_num += args->num_results;
#else
      total_num = args->num_results;
#endif
      unlock(&g_lock);
      BARRIER_ARRIVE(args->barrier, rv);
      if (args->tid == 0) {
        gettimeofday(&t2, NULL);
        printf("total result num = %lld\t", total_num);
        deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
        printf("---- %5s AGG costs time (ms) = %10.4lf , tid = %3d\n", pfun[fid].fun_name, deltaT * 1.0 / 1000, args->tid);
        total_num = 0;
        global_curse = 0;

      }
      destroy_hashtable(ht);
    }
  }
  free_bucket_buffer(overflowbuf);
  return nullptr;
}

result_t *AGG(relation_t *relR, relation_t *relS, int nthreads) {
  hashtable_t *ht;
  int64_t result = 0;
  int32_t numR, numS, numRthr, numSthr; /* total and per thread num */
  int i, rv;
  cpu_set_t set;
  arg_t args[nthreads];
  pthread_t tid[nthreads];
  pthread_attr_t attr;
  pthread_barrier_t barrier;

  result_t *joinresult = 0;
  joinresult = (result_t *) malloc(sizeof(result_t));

#ifdef JOIN_RESULT_MATERIALIZE
  joinresult->resultlist = (threadresult_t *) alloc_aligned(sizeof(threadresult_t) * nthreads);
#endif

#if USE_TBB
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

  tbb_run(relR, relS, nthreads);
  joinresult->totalresults = result;
  joinresult->nthreads = nthreads;
  return joinresult;
#endif
  uint32_t nbuckets = (relR->num_tuples / BUCKET_SIZE);
  allocate_hashtable(&ht, nbuckets);

  numR = relR->num_tuples;
  numS = relS->num_tuples;
  numRthr = numR / nthreads;
  numSthr = numS / nthreads;

  rv = pthread_barrier_init(&barrier, NULL, nthreads);
  if (rv != 0) {
    printf("Couldn't create the barrier\n");
    exit(EXIT_FAILURE);
  }
  global_curse = 0;
  global_upper = relS->num_tuples;

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
    args[i].ht = ht;
    args[i].barrier = &barrier;
#if TEST_NUMA
    args[i].relR.num_tuples = relR->num_tuples;
    args[i].relR.tuples = relR->tuples;
#else
    /* assing part of the relR for next thread */
    args[i].relR.num_tuples = (i == (nthreads - 1)) ? numR : numRthr;
    args[i].relR.tuples = relR->tuples + numRthr * i;
    numR -= numRthr;
#endif
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

    rv = pthread_create(&tid[i], &attr, agg_thread, (void *) &args[i]);
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

  destroy_hashtable(ht);

  return joinresult;
}
