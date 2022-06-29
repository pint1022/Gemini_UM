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

#ifndef _CUHOOK_COMM_H_
#define _CUHOOK_COMM_H_

#include <unistd.h>

#include <climits>
#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <functional>

typedef int32_t reqid_t;
enum comm_request_t { REQ_QUOTA=0, REQ_MEM_LIMIT, REQ_MEM_UPDATE, REQ_SAMPLE, REQ_MEM_D2H, REQ_MEM_H2D, REQ_REC, REQ_QRY, REQ_HD };
enum comm_h2d_t { H2D_START=0, H2D_END, D2H_START, D2H_END};
const size_t REQ_MSG_LEN = 128;
const size_t RSP_MSG_LEN = 40;
const size_t NAME_LEN = 20;
const size_t POD_NAME_LEN = 4;
const size_t UUID_LEN = 4;
const size_t SAMPLE_LEN = 120;
const size_t SAMPLE_MSG_LEN = 148;
const size_t UUID_FULL_LEN = strlen("GPU-177586d7-7962-c76c-9be7-fdbfc63c030e") + 1;

reqid_t prepare_request(char *buf, comm_request_t type, ...);

char *parse_request(char *buf, char **name, size_t *name_len, reqid_t *id, comm_request_t *type);

size_t prepare_response(char *buf, comm_request_t type, reqid_t id, ...);
char *parse_response(char *buf, reqid_t *id);
size_t prepare_sample(char *buf,  reqid_t id, char *sample, char * , char*);


// helper function for parsing message
template <typename T>
T get_msg_data(char *buf, size_t &pos) {
  T data;
  memcpy(&data, buf + pos, sizeof(T));
  pos += sizeof(T);
  return data;
}

// helper function for creating message
template <typename T>
size_t append_msg_data(char *buf, size_t &pos, T data) {
  memcpy(buf + pos, &data, sizeof(T));
  return (pos = pos + sizeof(T));
}

// Attempt a function several times. Non-zero return of func is treated as an error
int multiple_attempt(std::function<int()> func, int max_attempt, int interval = 0);

//
// PRF: profiling 
//
// int SAMPLING_RATE = 1000;
const int STORE_FACT = 5;
// bool InSampling = true;
const int SAMPLE_COUNT = 3;

// char *parse_export_request(char *buf, char **name, size_t *name_len, reqid_t *id, comm_request_t *type, char** uuid, size_t * uuid_len) ;
char *parse_export_request(char *buf, char **name, size_t *name_len, reqid_t *id, comm_request_t *type) ;
reqid_t prepare_export_request(char *buf, char *sample, char *podname, char *uuid) ;

struct Sample {
  uint64_t  ts;
  std::string name;
  double start;  //kernel start time
  double end;   //kernel end time
  int burst;   // real burst time 
  int quota;   //time slice quota
  int overuse; //overuse time slice
  int h2d;   //duration of h2d memcpy
  int d2h;   //duration of d2h memcpy
  int h2dsize;   //h2d memcpy size
  int d2hsize;   //d2h memcpy size
  int memsize;   // memory limit of the pod?
  int remain;   // time quota remained for the pod
  int used;    // memory used by the pod
  int elapse;  // kernel elapse time
};
void Sampling();

struct Record {
  char name[POD_NAME_LEN + 1];
  char uuid[UUID_LEN + 1];
  char jsonstr[SAMPLE_LEN];
};
#endif