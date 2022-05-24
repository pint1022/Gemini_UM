/**
 * Copyright 2020 Hung-Hsin Chen, LSA Lab, National Tsing Hua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <list>
#include <map>
#include <string>
#include <chrono>

#include "comm.h"

struct History {
  std::string name;
  double start;
  double end;
};

// some bias used for self-adaptive quota calculation
const double EXTRA_QUOTA = 10.0;
const double SCHD_OVERHEAD = 2.0;


class ClientInfo {
 public:
  ClientInfo(double baseq, double minq, double maxq, double minf, double maxf);
  ~ClientInfo();
  void set_burst(double burst);
  void set_h2dsize(int h2dsize);
  void set_d2hsize(int);
  void update_return_time(double overuse);
  void Record(double quota);
  double get_min_fraction();
  double get_max_fraction();
  double get_quota();
  double get_overuse();
  double get_burst();
  int get_h2dsize();
  int get_d2hsize();
  double get_memsize();
  void set_memsize(double memsize);

  std::map<unsigned long long, size_t> memory_map;
  std::string name;
  size_t gpu_mem_limit;

 private:
  const double MIN_FRAC;    // min percentage of GPU compute resource usage
  const double MAX_FRAC;    // max percentage of GPU compute resource usage
  const double BASE_QUOTA;  // from command line argument
  const double MIN_QUOTA;   // from command line argument
  const double MAX_QUOTA;   // calculated from time window and max fraction
  double quota_;
  double latest_overuse_;
  double latest_actual_usage_;  // client may return eariler (before quota expire)
  double burst_;                // duration of kernel burst
  int h2dsize_;                // h2d mem size
  int d2hsize_;                // d2h mem size
  double h2d_;                // duration of h2d memcpy
  double d2h_;                // duration of d2h memcpy
  double memsize_;            // memory used 
};

// the connection to specific container
struct candidate_t {
  int socket;
  std::string name;  // container name
  reqid_t req_id;
  double arrived_time;
};

struct valid_candidate_t {
  double missing;    // requirement - usage
  double remaining;  // limit - usage
  double usage;
  double arrived_time;
  std::list<candidate_t>::iterator iter;
};

bool schd_priority(const valid_candidate_t &a, const valid_candidate_t &b);


#endif
