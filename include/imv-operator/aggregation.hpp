#ifndef AGGREGATION_HEAD_
#define AGGREGATION_HEAD_
#include "prefetch.hpp"
#include <string.h>
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include "tbb/tbb.h"
#include "profile.hpp"
#include "no_partitioning_join.h"
using std::__cxx11::string;
using std::make_pair;
using std::pair;
using std::vector;
result_t *AGG(relation_t *relR, relation_t *relS, int nthreads);
#endif
