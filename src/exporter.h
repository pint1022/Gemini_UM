/**
 * Copyright 2022 Steven Wang, Futurewei Inc
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

//
// PRF: profiling 
//
struct Sample {
  uint64_t  ts;
  std::string name;
  double start;  //kernel start time
  double end;   //kernel end time
  double burst;   // real burst time 
  int quota;   //time slice quota
  int overuse; //overuse time slice
  double h2d;   //duration of h2d memcpy
  double d2h;   //duration of d2h memcpy
  int h2dsize;   //h2d memcpy size
  int d2hsize;   //d2h memcpy size
  double memsize; 
};

#endif
