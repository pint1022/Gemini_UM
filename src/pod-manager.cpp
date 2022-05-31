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

/**
 * From Kubernetes concepts: A Pod is the basic execution unit of a Kubernetes application–the
 * smallest and simplest unit in the Kubernetes object model that you create or deploy. A Pod
 * represents processes running on your cluster.
 *
 * This manager will run like a daemon in each Pod. User program will interact with this manager
 * when they call certain CUDA-related functions.
 */

#include <arpa/inet.h>
#include <execinfo.h>
#include <pthread.h>
#include <thread>

#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>
#include  <netdb.h>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <list>
#include <map>
#include <queue>

#include "comm.h"
#include "debug.h"
#include "util.h"

using std::chrono::duration_cast;
using std::chrono::microseconds;
using std::chrono::steady_clock;
using std::chrono::time_point;

// connection information, below are default values
// can be changed by environment vairables
char SCHEDULER_IP[20] = "127.0.0.1";
uint16_t SCHEDULER_PORT = 50051;
uint16_t POD_SERVER_PORT = 50052;

void sig_handler(int);

int SAMPLING_RATE = 1000;
// int STORE_FACT = 5;
bool InSampling = true;
std::list<Sample> sample_list;
char EXPORTER_NAME[] = "alnr-exporter.kube-system.svc";
uint16_t EXPORTER_PORT = 60018;
Sample a_sample;
char UUID[UUID_FULL_LEN];

// thread interact with scheduler
void *scheduler_thread_send_func(void *sockfd);
void *scheduler_thread_recv_func(void *sockfd);
// service thread for each hook library
void *hook_thread_func(void *sockfd);

/* communication between scheduler thread and hook threads */
enum actions {
  KERNEL_LAUNCH,
};
struct request {
  reqid_t req_id;
  char *data;
};
std::queue<request> request_queue;
uint32_t req_cnt = 0;
pthread_mutex_t req_queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t req_queue_cond = PTHREAD_COND_INITIALIZER;

struct response {
  void *data;
};
std::map<reqid_t, response> response_map;
pthread_mutex_t rsp_map_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t rsp_map_cond = PTHREAD_COND_INITIALIZER;

/* global variables to store memory limit */
size_t gpu_mem_limit = 0, gpu_mem_used = 0;
std::map<int, size_t> allocation_map;  // memory usage of each connection
pthread_mutex_t mem_info_mutex = PTHREAD_MUTEX_INITIALIZER;

/* computation utilization */
typedef time_point<steady_clock> quota_tp;
double pod_overuse_ms = 0.0;
std::map<int, double> client_burst_map;
pthread_mutex_t client_stat_mutex = PTHREAD_MUTEX_INITIALIZER;
double pod_quota = 0.0;
quota_tp quota_updated_tp;
int quota_state = 0;  // 0 means usual state, 1 means someone is updating quota
pthread_mutex_t quota_state_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t quota_state_cond = PTHREAD_COND_INITIALIZER;

/* communication with scheduler */
size_t pod_name_len;
char pod_name[HOST_NAME_MAX];

// retrieve memory limit information from scheduler
int retrieve_mem_info(int sockfd, const int MAX_RETRY, const long RETRY_TIMEOUT) {
  int rc;
  char sbuf[REQ_MSG_LEN], rbuf[RSP_MSG_LEN], *attached;
  size_t pos = 0;

  // set socket timeout option
  struct timeval tv = {RETRY_TIMEOUT, 0};
  setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  bzero(sbuf, REQ_MSG_LEN);
  prepare_request(sbuf, REQ_MEM_LIMIT);

  rc = multiple_attempt(
      [&]() -> int {
        if (send(sockfd, &sbuf, REQ_MSG_LEN, 0) == -1) return -1;
        if (recv(sockfd, rbuf, RSP_MSG_LEN, 0) == -1) return -1;
        return 0;
      },
      MAX_RETRY, 0);

  // disable timeout option
  tv.tv_sec = 0;
  setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  if (rc != 0) return rc;  // failed to get memory info from scheduler

  // parse received data and get memory limit
  attached = parse_response(rbuf, nullptr);
  gpu_mem_used = get_msg_data<size_t>(attached, pos);  // should be 0
  gpu_mem_limit = get_msg_data<size_t>(attached, pos);
  a_sample.memsize = gpu_mem_used;

  assert(gpu_mem_used == (size_t)0);
  INFO("GPU memory limit: %lu bytes.", gpu_mem_limit);
  return 0;
}

void pack_sample(char* buf, Sample &_sample) {
// struct Sample {
//   uint64_t  ts;
//   std::string name;
//   double start;  //kernel start time
//   double end;   //kernel end time
//   double burst;   // real burst time 
//   int quota;   //time slice quota
//   int overuse; //overuse time slice
//   double h2d;   //duration of h2d memcpy
//   double d2h;   //duration of d2h memcpy
//   int h2dsize;   //h2d memcpy size
//   int d2hsize;   //d2h memcpy size
//   uint64_t memsize;  //memory used
//   double remain;   //remain time in quota
// };  

//
// TBF: need check the length 
//
  sprintf(buf, "{\"Ts\": %ld, \"Bs\": %d, \"Ou\": %d, \"Rm\": %d, \"Mm\": %d}",
     _sample.ts,
     _sample.burst,
     _sample.overuse,
     _sample.remain,
     _sample.memsize
  );

}
//
// A background thread to record the profiling data
//
void *sampling_thread(void * args) {
  int export_sock = *((int *)args);  
  // const int kHeartbeatIntv = 500;
  const int kSampleIntv = SAMPLING_RATE;
  int save_data = STORE_FACT;
  reqid_t req_id = 0;  // simply pass this req_id back to Pod manager
  char sbuf[SAMPLE_MSG_LEN], rbuf[RSP_MSG_LEN], *attached;
  int rc;

  DEBUG("start sampling in podmanager: %d", kSampleIntv);
  while (true) {
    if (!InSampling)
       break;
    std::this_thread::sleep_for(std::chrono::milliseconds(kSampleIntv));       

    a_sample.ts = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    a_sample.memsize = gpu_mem_used;
    char sample[SAMPLE_LEN];

    pack_sample(sample, a_sample);
    bzero(sbuf, SAMPLE_MSG_LEN);

    DEBUG("Podmanager Sample: %s, pod_name %s, uuid %s", sample, pod_name, UUID);
    prepare_export_request(sbuf, sample, pod_name, UUID);
    DEBUG("Podmanager msg: %x", sbuf);

    send(export_sock, sbuf, SAMPLE_MSG_LEN, 0); 

  }
  DEBUG("Stopped sampling in podmanager...");
  pthread_exit(nullptr);
}

int main(int argc, char *argv[]) {
  const int NET_OP_MAX_ATTEMPT = 5;  // maximum time retrying failed network operations
  const int NET_OP_RETRY_INTV = 10;  // seconds between two retries
  int rc;

  // for debugging
  signal(SIGSEGV, sig_handler);

  // use host name as Pod name
  char *name = getenv("POD_NAME");
  if (name != NULL) {
    strcpy(pod_name, name);
  } else {
    gethostname(pod_name, HOST_NAME_MAX);
  }
  pod_name_len = strlen(pod_name);

  //
  // UDE: profiling
  //
  // strcpy(a_sample.name,pod_name);

  /* get connection information from environment variable */
  // Pod server
  char *pod_server_port_str = getenv("POD_MANAGER_PORT");
  if (pod_server_port_str != NULL) {
    POD_SERVER_PORT = atoi(pod_server_port_str);
  }
  INFO("Pod server port = %u.", POD_SERVER_PORT);

  // scheduler
  char *scheduler_ip_envstr = getenv("SCHEDULER_IP");
  if (scheduler_ip_envstr != NULL) {
    strcpy(SCHEDULER_IP, scheduler_ip_envstr);
  }
  char *scheduler_port_envstr = getenv("SCHEDULER_PORT");
  if (scheduler_port_envstr != NULL) {
    SCHEDULER_PORT = atoi(scheduler_port_envstr);
  }
  INFO("scheduler %s:%u", SCHEDULER_IP, SCHEDULER_PORT);

  char *uuid = getenv("UUID");
  if (uuid != NULL) {
    strcpy(UUID, uuid);
  } else {
    ERROR("retrieve UUID of GPU..");
    exit(-1);
  }
  INFO("Pod  UUID = %s.", uuid);
  /* establish connection with scheduler */
  // create socket
  int schd_sockfd = socket(PF_INET, SOCK_STREAM, 0);
  if (schd_sockfd == -1) {
    int err = errno;
    ERROR("failed to create socket: %s", strerror(err));
    exit(err);
  }

  // setup socket info
  struct sockaddr_in schd_info;
  bzero(&schd_info, sizeof(sockaddr_in));
  schd_info.sin_family = AF_INET;
  schd_info.sin_addr.s_addr = inet_addr(SCHEDULER_IP);
  schd_info.sin_port = htons(SCHEDULER_PORT);

  // connect to scheduler
  rc = multiple_attempt(
      [&]() -> int {
        return connect(schd_sockfd, (struct sockaddr *)&schd_info, sizeof(schd_info));
      },
      NET_OP_MAX_ATTEMPT, NET_OP_RETRY_INTV);
  if (rc != 0) exit(rc);

  /* get memory limit for this pod */
  rc = retrieve_mem_info(schd_sockfd, NET_OP_MAX_ATTEMPT, NET_OP_RETRY_INTV);
  if (rc != 0) exit(rc);

  // initialize quota receiving time
  quota_updated_tp = steady_clock::now();

  /* accept connections from hook libraries */
  // create accept socket
  int accept_sockfd = socket(PF_INET, SOCK_STREAM, 0);
  if (accept_sockfd == -1) {
    ERROR("accept_socket == -1");
    exit(-1);
  }

  // setup accept socket info
  struct sockaddr_in server_info;
  bzero(&server_info, sizeof(server_info));
  server_info.sin_family = AF_INET;
  server_info.sin_addr.s_addr = INADDR_ANY;
  server_info.sin_port = htons(POD_SERVER_PORT);

  rc = multiple_attempt(
      [&]() -> int {
        return bind(accept_sockfd, (struct sockaddr *)&server_info, sizeof(server_info));
      },
      NET_OP_MAX_ATTEMPT, NET_OP_RETRY_INTV);
  if (rc != 0) exit(rc);
  listen(accept_sockfd, SOMAXCONN);

  // start scheduler threads
  pthread_t schd_send_tid, schd_recv_tid;
  pthread_create(&schd_send_tid, NULL, scheduler_thread_send_func, &schd_sockfd);
  pthread_create(&schd_recv_tid, NULL, scheduler_thread_recv_func, &schd_sockfd);
  pthread_detach(schd_send_tid);
  pthread_detach(schd_recv_tid);

  // start sampling thread
  pthread_t tid;
  int alnr_sockfd = socket(PF_INET, SOCK_STREAM, 0);
  if (alnr_sockfd == -1) {
    int err = errno;
    ERROR("failed to create socket: %s", strerror(err));
    exit(err);
  }
   // setup socket info
  struct sockaddr_in exporter_info;
  struct hostent *he;


  // get the addresses of www.yahoo.com:

  he = gethostbyname(EXPORTER_NAME);

  bzero(&exporter_info, sizeof(exporter_info));
  exporter_info.sin_family = AF_INET;
  exporter_info.sin_addr.s_addr = inet_addr(inet_ntoa(*(struct in_addr*)he->h_addr));
  exporter_info.sin_port = htons(EXPORTER_PORT);

  // connect to collector server
  rc = multiple_attempt(
      [&]() -> int {
        return connect(alnr_sockfd, (struct sockaddr *)&exporter_info, sizeof(exporter_info));
      },
      NET_OP_MAX_ATTEMPT, NET_OP_RETRY_INTV);
  if (rc != 0) {
    ERROR("collector server is not reachable...");
    exit(rc);
  }

  rc = pthread_create(&tid, nullptr, sampling_thread, &alnr_sockfd);
   if (rc != 0) {
    ERROR("Return code from pthread_create() - sampling: %d", rc);
    exit(rc);
  }
  INFO("%d: Pod manager sampling", __LINE__);

  pthread_detach(tid);

  int client_sockfd = 0;
  struct sockaddr_in client_info;
  int addr_len = sizeof(client_info);

  // wait for incoming connections
  while ((client_sockfd =
              accept(accept_sockfd, (struct sockaddr *)&client_info, (socklen_t *)&addr_len))) {
    if (client_sockfd == -1) {
      ERROR("accept() return -1");
      break;
    }

    // create allocation accounting entry
    allocation_map.insert(std::make_pair(client_sockfd, 0));

    // create client statistics entries
    pthread_mutex_lock(&client_stat_mutex);
    client_burst_map.insert(std::make_pair(client_sockfd, 0.0));
    pthread_mutex_unlock(&client_stat_mutex);

    // create a thread for each client
    pthread_t tid;
    int *sockfd = new int;
    *sockfd = client_sockfd;
    pthread_create(&tid, NULL, hook_thread_func, (void *)sockfd);
    pthread_detach(tid);
  }

  return 0;
}

void sig_handler(int sig) {
  void *arr[10];
  size_t s = backtrace(arr, 10);
  ERROR("Received signal %d", sig);
  backtrace_symbols_fd(arr, s, STDERR_FILENO);
  exit(sig);
}

// update GPU memory usage information
int hook_update_memory_usage(size_t mem_size, int allocate, int sockfd) {
  int ok = 1;  // meets memory limit
  pthread_mutex_lock(&mem_info_mutex);
  if (allocate) {
    if (gpu_mem_used + mem_size > gpu_mem_limit) {
      ok = 0;
    } else {
      gpu_mem_used += mem_size;
      allocation_map[sockfd] += mem_size;
    }
  } else {
    gpu_mem_used -= mem_size;
    allocation_map[sockfd] -= mem_size;
  }
  // DEBUG("GPU memory usage = %ld bytes.", gpu_mem_used);
  pthread_mutex_unlock(&mem_info_mutex);
  return ok;
}

// handle kernel launch request, return remaining quota time (ms)
double hook_kernel_launch(int sockfd, double overuse_ms, double burst) {
  // wait if someone else is working with quota
  while (true) {
    pthread_mutex_lock(&quota_state_mutex);
    if (quota_state == 0) {
      pthread_mutex_unlock(&quota_state_mutex);
      break;
    } else {
      DEBUG("wait for quota operation complete.");
      pthread_cond_wait(&quota_state_cond, &quota_state_mutex);
      pthread_mutex_unlock(&quota_state_mutex);
    }
  }

  // update Pod overuse time
  pod_overuse_ms = std::max(overuse_ms, pod_overuse_ms);

  // update statistics for this client
  pthread_mutex_lock(&client_stat_mutex);
  client_burst_map[sockfd] = burst;
  pthread_mutex_unlock(&client_stat_mutex);

  quota_tp now_tp = steady_clock::now();
  double elapsed_time = duration_cast<microseconds>(now_tp - quota_updated_tp).count() / 1e3;
  // ask scheduler for quota if we are expected to go over quota
  if (elapsed_time + burst > pod_quota) {
    /* expired, request quota from scheduler */
    char *sbuf;
    reqid_t req_id;
    bool complete = false;
    size_t rpos = 0;
    double max_burst = 0.0;

    // update quota state: updating quota
    pthread_mutex_lock(&quota_state_mutex);
    quota_state = 1;
    pthread_mutex_unlock(&quota_state_mutex);

    // calculate estimation values
    pthread_mutex_lock(&client_stat_mutex);
    for (auto x : client_burst_map) max_burst = std::max(x.second, max_burst);
    pthread_mutex_unlock(&client_stat_mutex);

    // place request into request queue
    pthread_mutex_lock(&req_queue_mutex);
    sbuf = new char[REQ_MSG_LEN];
    bzero(sbuf, REQ_MSG_LEN);
    req_id = prepare_request(sbuf, REQ_QUOTA, pod_overuse_ms, max_burst);
    request_queue.push({req_id, sbuf});
    // wake scheduler thread up
    pthread_cond_signal(&req_queue_cond);
    pthread_mutex_unlock(&req_queue_mutex);

    // wait for response
    while (!complete) {
      pthread_mutex_lock(&rsp_map_mutex);
      pthread_cond_wait(&rsp_map_cond, &rsp_map_mutex);
      if (response_map.find(req_id) != response_map.end()) {
        // request completed
        complete = true;  // exit while loop

        // update quota information
        pod_quota = get_msg_data<double>((char *)response_map[req_id].data, rpos);
        quota_updated_tp = steady_clock::now();
        elapsed_time = 0.0;
        pod_overuse_ms = 0.0;

        delete (double *)response_map[req_id].data;
        response_map.erase(req_id);
      }
      pthread_mutex_unlock(&rsp_map_mutex);
    }

    delete[] sbuf;
  }

  // update quota state and notify threads waiting on quota state
  pthread_mutex_lock(&quota_state_mutex);
  quota_state = 0;  // usual state
  pthread_cond_broadcast(&quota_state_cond);
  pthread_mutex_unlock(&quota_state_mutex);

  return pod_quota - elapsed_time;
}

// a thread interact with a hook library
void *hook_thread_func(void *args) {
  DEBUG("hook thread started.");
  int sockfd = *((int *)args);
  char rbuf[REQ_MSG_LEN], sbuf[RSP_MSG_LEN];
  ssize_t rc;
  while ((rc = recv(sockfd, rbuf, REQ_MSG_LEN, 0)) > 0) {
    comm_request_t req;
    reqid_t rid;
    size_t pos = 0;  // attached data reading position
    size_t len = 0;  // length of sending data
    char *attached = parse_request(rbuf, nullptr, nullptr, &rid, &req);

    bzero(sbuf, RSP_MSG_LEN);
    if (req == REQ_MEM_LIMIT) {
      // send gpu_mem_used and gpu_mem_limit to hook library
      len = prepare_response(sbuf, REQ_MEM_LIMIT, rid, gpu_mem_used, gpu_mem_limit);
    } else if (req == REQ_MEM_UPDATE) {
      // update memory usage
      size_t mem_size = get_msg_data<size_t>(attached, pos);
      int allocate = get_msg_data<int>(attached, pos);
      int ok = hook_update_memory_usage(mem_size, allocate, sockfd);
      len = prepare_response(sbuf, REQ_MEM_UPDATE, rid, ok);
    } else if (req == REQ_QUOTA) {
      // check if there is available quota
      double overuse_ms = get_msg_data<double>(attached, pos);
      double burst = get_msg_data<double>(attached, pos);
      double quota_remain = hook_kernel_launch(sockfd, overuse_ms, burst);

      // return remaining quota time
      len = prepare_response(sbuf, REQ_QUOTA, rid, quota_remain);

      // UDE: Profiling
      a_sample.burst = burst;
      a_sample.overuse = overuse_ms;
      a_sample.remain = quota_remain;
     
    }

    if (len > 0) {
      // have message to send
      if (send(sockfd, sbuf, RSP_MSG_LEN, 0) == -1) {
        ERROR("failed to send message to hook library!");
      }
    }
  }

  INFO("connetion closed by peer. recv() returns %ld.", rc);
  // since hook library close socket only when process terminated, we can use this as an indicator
  // of process termination, and recover memory usage
  pthread_mutex_lock(&mem_info_mutex);
  gpu_mem_used -= allocation_map[sockfd];
  allocation_map.erase(sockfd);
  // DEBUG("GPU memory usage = %ld bytes.", gpu_mem_used);
  pthread_mutex_unlock(&mem_info_mutex);

  pthread_mutex_lock(&client_stat_mutex);
  client_burst_map.erase(sockfd);
  pthread_mutex_unlock(&client_stat_mutex);

  close(sockfd);
  delete (int *)args;
  pthread_exit(NULL);
}

// forward requests to scheduler
void *scheduler_thread_send_func(void *args) {
  int sockfd = *((int *)args);
  ssize_t send_rc;
  /* waiting for request from hook threads */
  while (true) {
    pthread_mutex_lock(&req_queue_mutex);
    pthread_cond_wait(&req_queue_cond, &req_queue_mutex);
    if (!request_queue.empty()) {
      // process request
      request req = request_queue.front();
      request_queue.pop();

      if ((send_rc = send(sockfd, req.data, REQ_MSG_LEN, 0)) <= 0) {
        ERROR("failed to send request to scheduler! return code %ld.", send_rc);
      } else {
        DEBUG("send a kernel launch request, req_id: %d", req.req_id);
      }
    }
    pthread_mutex_unlock(&req_queue_mutex);
  }
  pthread_exit(NULL);
}

// receive response from scheduler and place responded data into response_map
void *scheduler_thread_recv_func(void *args) {
  int sockfd = *((int *)args);

  char buf[RSP_MSG_LEN], *attached;
  ssize_t rc;
  while ((rc = recv(sockfd, buf, RSP_MSG_LEN, 0)) > 0) {
    // process response
    // read response data into another buffer
    reqid_t req_id;
    response rsp;

    attached = parse_response(buf, &req_id);
    rsp.data = new char[RSP_MSG_LEN - sizeof(reqid_t)];
    memcpy(rsp.data, attached, RSP_MSG_LEN - sizeof(reqid_t));
    // DEBUG("req_id %d complete.", req_id);

    // put response data into response_map and notify hook threads
    pthread_mutex_lock(&rsp_map_mutex);
    response_map.insert(std::make_pair(req_id, rsp));
    pthread_cond_broadcast(&rsp_map_cond);
    pthread_mutex_unlock(&rsp_map_mutex);
  }

  WARNING("connection closed by scheduler. recv() returns %ld.", rc);
  close(sockfd);
  pthread_exit(NULL);
}