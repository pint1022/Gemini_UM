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
 * This is a per-GPU manager/scheduler.
 * Based on the information provided by clients, it decide which client to run
 * and give token to that client. This scheduler act as a daemon, accepting
 * connection and requests from pod manager or hook library directly.
 */

#include "scheduler.h"

#include <arpa/inet.h>
#include <errno.h>
#include <execinfo.h>
#include <getopt.h>
#include <linux/limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <sys/inotify.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <string>
#include <thread>
#include <typeinfo>
#include <vector>
#include <glib.h>

#include <gio/gio.h>
#include "debug.h"
#include "util.h"
#ifdef RANDOM_QUOTA
#include <random>
#endif

using std::string;
using std::chrono::duration_cast;
using std::chrono::microseconds;
using std::chrono::steady_clock;

// signal handler
void sig_handler(int);
#ifdef _DEBUG
void dump_history(int);
#endif

// helper function for getting timespec
struct timespec get_timespec_after(double ms) {
  struct timespec ts;
  // now
  clock_gettime(CLOCK_MONOTONIC, &ts);

  double sec = ms / 1e3;
  ts.tv_sec += floor(sec);
  ts.tv_nsec += (sec - floor(sec)) * 1e9;
  ts.tv_sec += ts.tv_nsec / 1000000000;
  ts.tv_nsec %= 1000000000;
  return ts;
}

// all in milliseconds
double QUOTA = 250.0;
double MIN_QUOTA = 100.0;
double WINDOW_SIZE = 10000.0;
bool isSampling = true;
int SAMPLE_COUNT = 3;

int verbosity = 0;

//
// PRF: profiling 
//
int SAMPLING_RATE = 1000;
int STORE_FACT = 5;
bool InSampling = true;
std::list<Sample> sample_list;
void upload_sampling();

#define EVENT_SIZE sizeof(struct inotify_event)
#define BUF_LEN (1024 * (EVENT_SIZE + 16))
auto PROGRESS_START = steady_clock::now();
char limit_file_name[PATH_MAX] = "resource-config.txt";
char limit_file_dir[PATH_MAX] = ".";

std::list<History> history_list;
#ifdef _DEBUG
std::list<History> full_history;
#endif
GMainLoop *main_loop = nullptr;

// milliseconds since scheduler process started
inline double ms_since_start() {
  return duration_cast<microseconds>(steady_clock::now() - PROGRESS_START).count() / 1e3;
}

ClientInfo::ClientInfo(double baseq, double minq, double maxq, double minf, double maxf)
    : BASE_QUOTA(baseq), MIN_QUOTA(minq), MAX_QUOTA(maxq), MIN_FRAC(minf), MAX_FRAC(maxf) {
  quota_ = BASE_QUOTA;
  latest_overuse_ = 0.0;
  latest_actual_usage_ = 0.0;
  burst_ = 0.0;
};

ClientInfo::~ClientInfo(){};

void ClientInfo::set_burst(double estimated_burst) { burst_ = estimated_burst; }

void ClientInfo::update_return_time(double overuse) {
  double now = ms_since_start();
  for (auto it = history_list.rbegin(); it != history_list.rend(); it++) {
    if (it->name == this->name) {
      // client may not use up all of the allocated time
      it->end = std::min(now, it->end + overuse);
      latest_actual_usage_ = it->end - it->start;
      break;
    }
  }
  latest_overuse_ = overuse;
#ifdef _DEBUG
  for (auto it = full_history.rbegin(); it != full_history.rend(); it++) {
    if (it->name == this->name) {
      it->end = std::min(now, it->end + overuse);
      break;
    }
  }
#endif
}
void ClientInfo::set_h2dsize(int h2dsize) { h2dsize_ = h2dsize; }
void ClientInfo::set_d2hsize(int d2hsize) { d2hsize_ = d2hsize; }
int ClientInfo::get_h2dsize() { return  h2dsize_;  }
int ClientInfo::get_d2hsize() { return  d2hsize_;  }

void ClientInfo::set_memsize(double memsize) { memsize_ = memsize; }
double ClientInfo::get_memsize() { return memsize_; }

void ClientInfo::Record(double quota) {
  History hist;
  hist.name = this->name;
  hist.start = ms_since_start();
  hist.end = hist.start + quota;
  history_list.push_back(hist);
#ifdef _DEBUG
  full_history.push_back(hist);
#endif
}
double ClientInfo::get_min_fraction() { return MIN_FRAC; }

double ClientInfo::get_max_fraction() { return MAX_FRAC; }

// self-adaptive quota algorithm
double ClientInfo::get_quota() {
  
  quota_ = BASE_QUOTA;
  // printf("%s:static quota, assign quota: %.3fms\n", name.c_str(), quota_);
  return quota_;
}

double ClientInfo::get_overuse() {  
  return latest_overuse_;
}

double ClientInfo::get_burst() {  
  return burst_;
}

// map container name to object
std::map<string, ClientInfo *> client_info_map;

std::list<candidate_t> candidates;
pthread_mutex_t candidate_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t candidate_cond;  // initialized with CLOCK_MONOTONIC in main()

void read_resource_config(char* full_path) {
  std::fstream fin;
  ClientInfo *client_inf;
  char client_name[HOST_NAME_MAX];
  //, full_path[PATH_MAX];
  size_t gpu_memory_size;
  double gpu_min_fraction, gpu_max_fraction;
  int container_num;

  // bzero(full_path, PATH_MAX);
  // strncpy(full_path, limit_file_dir, PATH_MAX);
  // if (limit_file_dir[strlen(limit_file_dir) - 1] != '/') full_path[strlen(limit_file_dir)] = '/';
  // strncat(full_path, limit_file_name, PATH_MAX - strlen(full_path));

  // Read GPU limit usage
  fin.open(full_path, std::ios::in);
  if (!fin.is_open()) {
    ERROR("failed to open file %s: %s", full_path, strerror(errno));
    exit(1);
  }
  fin >> container_num;
  INFO("There are %d clients in the system...", container_num);
  for (int i = 0; i < container_num; i++) {
    fin >> client_name >> gpu_min_fraction >> gpu_max_fraction >> gpu_memory_size;
    // INFO("%d: %s, %.2f", __LINE__, client_name, gpu_min_fraction);
    client_inf = new ClientInfo(QUOTA, MIN_QUOTA, gpu_max_fraction * WINDOW_SIZE, gpu_min_fraction,
                                gpu_max_fraction);
    client_inf->name = client_name;
    client_inf->gpu_mem_limit = gpu_memory_size;
    if (client_info_map.find(client_name) != client_info_map.end())
      delete client_info_map[client_name];
    client_info_map[client_name] = client_inf;
    INFO("%s request: %.2f, limit: %.2f, memory limit: %lu bytes", client_name, gpu_min_fraction,
         gpu_max_fraction, gpu_memory_size);
  }
  fin.close();
}

// Callback function when resource config file is changed.
// Monitor the change to resource config file, and spawn new client group management threads if
// needed.
void onResourceConfigFileUpdate(GFileMonitor *monitor, GFile *file, GFile *other_file,
                                GFileMonitorEvent event_type, gpointer user_data) {
  if (event_type == G_FILE_MONITOR_EVENT_CHANGED || event_type == G_FILE_MONITOR_EVENT_CREATED) {
    INFO("Update resource configurations...");

    char *path = g_file_get_path(file);
    read_resource_config(path);
    g_free(path);
  }
}


/**
 * Select a candidate whose current usage is less than its limit.
 * If no such candidates, calculate the time until time window content changes and sleep until then,
 * or until another candidate comes. If more than one candidate meets the requirement, select one
 * according to scheduling policy.
 * @return selected candidate
 */
candidate_t select_candidate() {
  while (true) {
    /* update history list and get usage in a time interval */
    double window_size = WINDOW_SIZE;
    int overlap_cnt = 0, i, j, k;
    std::map<string, double> usage;
    struct container_timestamp {
      string name;
      double time;
    };

    // start/end timestamp of history in this window
    std::vector<container_timestamp> timestamp_vector;
    // instances to check for overlap
    std::vector<string> overlap_check_vector;

    double now = ms_since_start();
    double window_start = now - WINDOW_SIZE;
    double current_time;
    if (window_start < 0) {
      // elapsed time less than a window size
      window_size = now;
    }

    auto beyond_window = [=](const History &h) -> bool { return h.end < window_start; };
    history_list.remove_if(beyond_window);
    for (auto h : history_list) {
      timestamp_vector.push_back({h.name, -h.start});
      timestamp_vector.push_back({h.name, h.end});
      usage[h.name] = 0;
      if (verbosity > 1) {
        printf("{'container': '%s', 'start': %.3f, 'end': %.3f},\n", h.name.c_str(), h.start / 1e3,
               h.end / 1e3);
      }
    }

    /* select the candidate to give token */

    // quick exit if the first one in candidates does not use GPU recently
    auto check_appearance = [=](const History &h) -> bool {
      return h.name == candidates.front().name;
    };
    if (std::find_if(history_list.begin(), history_list.end(), check_appearance) ==
        history_list.end()) {
      candidate_t selected = candidates.front();
      candidates.pop_front();
      return selected;
    }

    // sort by time
    auto ts_comp = [=](container_timestamp a, container_timestamp b) -> bool {
      return std::abs(a.time) < std::abs(b.time);
    };
    std::sort(timestamp_vector.begin(), timestamp_vector.end(), ts_comp);

    /* calculate overall utilization */
    current_time = window_start;
    for (k = 0; k < timestamp_vector.size(); k++) {
      if (std::abs(timestamp_vector[k].time) <= window_start) {
        // start before this time window
        overlap_cnt++;
        overlap_check_vector.push_back(timestamp_vector[k].name);
      } else {
        break;
      }
    }

    // i >= k: events in this time window
    for (i = k; i < timestamp_vector.size(); ++i) {
      // instances in overlap_check_vector overlaps,
      // overlapped interval = current_time ~ i-th timestamp
      for (j = 0; j < overlap_check_vector.size(); ++j)
        usage[overlap_check_vector[j]] +=
            (std::abs(timestamp_vector[i].time) - current_time) / overlap_cnt;

      if (timestamp_vector[i].time < 0) {
        // is a "start" timestamp
        overlap_check_vector.push_back(timestamp_vector[i].name);
        overlap_cnt++;
      } else {
        // is a "end" timestamp
        // remove instance from overlap_check_vector
        for (j = 0; j < overlap_check_vector.size(); ++j) {
          if (overlap_check_vector[j] == timestamp_vector[i].name) {
            overlap_check_vector.erase(overlap_check_vector.begin() + j);
            break;
          }
        }
        overlap_cnt--;
      }

      // advance current_time
      current_time = std::abs(timestamp_vector[i].time);
    }

    /* select the one to execute */
    std::vector<valid_candidate_t> vaild_candidates;

    for (auto it = candidates.begin(); it != candidates.end(); it++) {
      string name = it->name;
      double limit, require, missing, remaining;
      limit = client_info_map[name]->get_max_fraction() * window_size;
      require = client_info_map[name]->get_min_fraction() * window_size;
      missing = require - usage[name];
      remaining = limit - usage[name];
      if (remaining > 0)
        vaild_candidates.push_back({missing, remaining, usage[it->name], it->arrived_time, it});
    }

    if (vaild_candidates.size() == 0) {
      // all candidates reach usage limit
      auto ts = get_timespec_after(history_list.begin()->end - window_start);
      DEBUG("sleep until %ld.%03ld", ts.tv_sec, ts.tv_nsec / 1000000);
      // also wakes up if new requests come in
      pthread_cond_timedwait(&candidate_cond, &candidate_mutex, &ts);
      continue;  // go to begin of loop
    }

    std::sort(vaild_candidates.begin(), vaild_candidates.end(), schd_priority);

    auto selected_iter = vaild_candidates.begin()->iter;
    candidate_t result = *selected_iter;
    candidates.erase(selected_iter);
    return result;
  }
}

// Get the information from message
void handle_message(int client_sock, char *message) {
  reqid_t req_id;  // simply pass this req_id back to Pod manager
  comm_request_t req;
  size_t hostname_len, offset = 0;
  char sbuf[RSP_MSG_LEN];
  char *attached, *client_name;
  ClientInfo *client_inf;
  attached = parse_request(message, &client_name, &hostname_len, &req_id, &req);

  if (client_info_map.find(string(client_name)) == client_info_map.end()) {
    WARNING("%d: Unknown client \"%s\". Ignore this request.", __LINE__, client_name);
    return;
  }
  client_inf = client_info_map[string(client_name)];
  bzero(sbuf, RSP_MSG_LEN);

  if (req == REQ_QUOTA) {
    double overuse, burst, window;
    overuse = get_msg_data<double>(attached, offset);
    burst = get_msg_data<double>(attached, offset);

    client_inf->update_return_time(overuse);
    client_inf->set_burst(burst);
    // client_inf->Sample( burst, overuse);
    pthread_mutex_lock(&candidate_mutex);
    candidates.push_back({client_sock, string(client_name), req_id, ms_since_start()});
    pthread_cond_signal(&candidate_cond);  // wake schedule_daemon_func up
    pthread_mutex_unlock(&candidate_mutex);
    // select_candidate() will give quota later

  } else if (req == REQ_MEM_LIMIT) {
    prepare_response(sbuf, REQ_MEM_LIMIT, req_id, (size_t)0, client_inf->gpu_mem_limit);
    send(client_sock, sbuf, RSP_MSG_LEN, 0);
  } 
  // else if (req == REQ_MEM_UPDATE) {
  //   // ***for communication interface compatibility only***
  //   // memory usage is only tracked on hook library side
  //   int bytes, is_allocated;
  //   bytes = get_msg_data<int>(attached, offset);
  //   is_allocated = get_msg_data<int>(attached, offset);
  //   INFO(" memory usage update %d, inc: %d\n", bytes, is_allocated);
  //   if (is_allocated == 1) {
  //     client_inf->set_memsize(client_inf->get_memsize() + bytes /1000.0);
  //   } else {
  //     client_inf->set_memsize(client_inf->get_memsize() - bytes /1000.0);
  //   }
  //   prepare_response(sbuf, REQ_MEM_UPDATE, req_id, 1);
  //   send(client_sock, sbuf, RSP_MSG_LEN, 0);
  //  }else if (req == REQ_MEM_H2D) {
  //   int h2dsize;
  //   h2dsize = get_msg_data<int>(attached, offset);
  //   client_inf->set_h2dsize(h2dsize);
  //   prepare_response(sbuf, REQ_MEM_H2D, req_id, 1);
  //   send(client_sock, sbuf, RSP_MSG_LEN, 0);
  // } else if (req == REQ_MEM_D2H) {
  //   int d2hsize;
  //   d2hsize = get_msg_data<int>(attached, offset);
  //   client_inf->set_d2hsize(d2hsize);
  //   prepare_response(sbuf, REQ_MEM_D2H, req_id, 1);
  //   send(client_sock, sbuf, RSP_MSG_LEN, 0);
  // } 
  else if (req == REQ_SAMPLE) {
    auto  it = sample_list.begin();
    if (it !=sample_list.end()) {
      // a_sample.ts = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
      // a_sample.name = it->name;
      // a_sample.quota = client_info_map[it->name]->get_quota();
      // a_sample.start = it->start;
      // a_sample.end = it->end;
      // a_sample.burst = client_info_map[it->name]->get_burst();
      // a_sample.overuse = client_info_map[it->name]->get_overuse();
      sprintf(sbuf, "\t{\"ts\": \"%jd\",\"cn\": \"%s\",\"me\" : %.3lf (K),}",it->ts, it->name.c_str(),
               it->memsize);    

      // sprintf(sbuf, "\t{\"tms\": \"%jd\",\"ctn\": \"%s\", \"bst\": %.3lf, \"ovs\" : %.3lf, \"h2d\" : %d(K), \"d2h\" : %d (K),\"mem\" : %.3lf (K),}",it->ts, it->name.c_str(),\
              it->burst / 1000.0, it->overuse / 1000.0, it->h2dsize / 1000, it->d2hsize / 1000, it->memsize);    
      sample_list.pop_front();
      send(client_sock, sbuf, RSP_MSG_LEN, 0);
    }
  } else {
    WARNING("\"%s\" send an unknown request.", client_name);
  }
}

void *schedule_daemon_func(void *) {
#ifdef RANDOM_QUOTA
  std::random_device rd;
  std::default_random_engine gen(rd());
  std::uniform_real_distribution<double> dis(0.4, 1.0);
#endif
  double quota;

  while (1) {
    pthread_mutex_lock(&candidate_mutex);
    if (candidates.size() != 0) {
      // remove an entry from candidates
      candidate_t selected = select_candidate();
      DEBUG("select %s, waiting time: %.3f ms", selected.name.c_str(),
            ms_since_start() - selected.arrived_time);

      quota = client_info_map[selected.name]->get_quota();
#ifdef RANDOM_QUOTA
      quota *= dis(gen);
#endif
      client_info_map[selected.name]->Record(quota);

      pthread_mutex_unlock(&candidate_mutex);

      // send quota to selected instance
      char sbuf[RSP_MSG_LEN];
      bzero(sbuf, RSP_MSG_LEN);
      prepare_response(sbuf, REQ_QUOTA, selected.req_id, quota);
      send(selected.socket, sbuf, RSP_MSG_LEN, 0);

      struct timespec ts = get_timespec_after(quota);

      // wait until the selected one's quota time out
      bool should_wait = true;
      pthread_mutex_lock(&candidate_mutex);
      while (should_wait) {
        int rc = pthread_cond_timedwait(&candidate_cond, &candidate_mutex, &ts);
        if (rc == ETIMEDOUT) {
          DEBUG("%s didn't return on time", selected.name.c_str());
          should_wait = false;
        } else {
          // check if the selected one comes back
          for (auto conn : candidates) {
            if (conn.name == selected.name) {
              should_wait = false;
              break;
            }
          }
        }
      }
      pthread_mutex_unlock(&candidate_mutex);
    } else {
      // wait for incoming connections
      DEBUG("no candidates");
      pthread_cond_wait(&candidate_cond, &candidate_mutex);
      pthread_mutex_unlock(&candidate_mutex);
    }
  }
}

void Sampling() {
  Sample a_sample;
  int count = SAMPLE_COUNT;
  
  // if (history_list.back() != NULL) {
  //   History t_hist = history_list.back();
  //   a_sample.ts = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  //   a_sample.name = t_hist.name;
  //   a_sample.quota = client_info_map[t_hist.name]->get_quota();
  //   a_sample.start = t_hist.start;
  //   a_sample.end = t_hist.end;
  //   a_sample.burst = client_info_map[t_hist.name]->get_burst();
  //   a_sample.memsize = client_info_map[t_hist.name]->get_memsize();
  //   a_sample.overuse = client_info_map[t_hist.name]->get_overuse();
  //   a_sample.h2dsize = client_info_map[t_hist.name]->get_h2dsize();
  //   a_sample.d2hsize = client_info_map[t_hist.name]->get_d2hsize();
  //   sample_list.push_back(a_sample);
  // }
  for (auto it = history_list.rbegin(); it != history_list.rend() && count-- > 0; it++) {
      // client may not use up all of the allocated time
      a_sample.ts = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
      a_sample.name = it->name;
      a_sample.quota = client_info_map[it->name]->get_quota();
      a_sample.start = it->start;
      a_sample.end = it->end;
      a_sample.burst = client_info_map[it->name]->get_burst();
      a_sample.memsize = client_info_map[it->name]->get_memsize();
      a_sample.overuse = client_info_map[it->name]->get_overuse();
      a_sample.h2dsize = client_info_map[it->name]->get_h2dsize();
      a_sample.d2hsize = client_info_map[it->name]->get_d2hsize();
      sample_list.push_back(a_sample);
    }
}

//
// A background thread to record the profiling data
//
void *sampling_thread(void *) {
  // const int kHeartbeatIntv = 500;
  const int kHeartbeatIntv = SAMPLING_RATE;
  int save_data = STORE_FACT;
  DEBUG("start sampling: %d", kHeartbeatIntv);
  while (true) {
    if (!InSampling)
       break;
    Sampling();
    std::this_thread::sleep_for(std::chrono::milliseconds(kHeartbeatIntv));     
    if (sample_list.size() > 0) {
      if (save_data-- == 0) {
        upload_sampling();
        save_data = STORE_FACT;
        // break;
      }
    } 
  }
  DEBUG("Stopped sampling...");
  pthread_exit(nullptr);
}

// daemon function for Pod manager: waiting for incoming request
void *pod_client_func(void *args) {
  int pod_sockfd = *((int *)args);
  char *rbuf = new char[REQ_MSG_LEN];
  ssize_t recv_rc;
  while ((recv_rc = recv(pod_sockfd, rbuf, REQ_MSG_LEN, 0)) > 0) {
    handle_message(pod_sockfd, rbuf);
  }
  DEBUG("Connection closed by Pod manager. recv() returns %ld.", recv_rc);
  close(pod_sockfd);
  InSampling = false;
  delete (int *)args;
  delete[] rbuf;
  pthread_exit(NULL);
}

void *podGroupMgmt(void * sockfd) {
  struct sockaddr_in clientInfo;
  int addrlen = sizeof(clientInfo);
  int forClientSockfd = 0;

  INFO("Waiting for incoming connection: %d", *(int*) sockfd);

  while (
      (forClientSockfd = accept( *(int*) sockfd, (struct sockaddr *)&clientInfo, (socklen_t *)&addrlen))) {
    pthread_t tid;
    int *pod_sockfd = new int;
    *pod_sockfd = forClientSockfd;
    // create a thread to service this Pod manager
    pthread_create(&tid, NULL, pod_client_func, pod_sockfd);
    pthread_detach(tid);
  }
  if (forClientSockfd < 0) {
    ERROR("Accept failed");
  }

  pthread_exit(nullptr);
}


int main(int argc, char *argv[]) {
  uint16_t schd_port = 50051;
  // parse command line options
  const char *optstring = "P:q:m:w:f:p:s:v:h";
  struct option opts[] = {{"port", required_argument, nullptr, 'P'},
                          {"quota", required_argument, nullptr, 'q'},
                          {"min_quota", required_argument, nullptr, 'm'},
                          {"window", required_argument, nullptr, 'w'},
                          {"limit_file", required_argument, nullptr, 'f'},
                          {"limit_file_dir", required_argument, nullptr, 'p'},
                          {"sampling_rate", required_argument, nullptr, 's'},
                          {"verbose", required_argument, nullptr, 'v'},
                          {"help", no_argument, nullptr, 'h'},
                          {nullptr, 0, nullptr, 0}};
  int opt;
  while ((opt = getopt_long(argc, argv, optstring, opts, NULL)) != -1) {
    switch (opt) {
      case 'P':
        schd_port = strtoul(optarg, nullptr, 10);
        break;
      case 'q':
        QUOTA = atof(optarg);
        break;
      case 'm':
        MIN_QUOTA = atof(optarg);
        break;
      case 'w':
        WINDOW_SIZE = atof(optarg);
        break;
      case 's':
        SAMPLING_RATE = atof(optarg);
        break;
      case 'f':
        strncpy(limit_file_name, optarg, PATH_MAX - 1);
        break;
      case 'p':
        strncpy(limit_file_dir, optarg, PATH_MAX - 1);
        break;
      case 'v':
        verbosity = atoi(optarg);
        break;
      case 'h':
        printf("usage: %s [options]\n", argv[0]);
        puts("Options:");
        puts("    -P [PORT], --port [PORT]");
        puts("    -q [QUOTA], --quota [QUOTA]");
        puts("    -m [MIN_QUOTA], --min_quota [MIN_QUOTA]");
        puts("    -w [WINDOW_SIZE], --window [WINDOW_SIZE]");
        puts("    -f [LIMIT_FILE], --limit_file [LIMIT_FILE]");
        puts("    -p [LIMIT_FILE_DIR], --limit_file_dir [LIMIT_FILE_DIR]");
        puts("    -s [SAMPLING_RATE], --sampling_rate [SAMPLING_RATE]");
        puts("    -v [LEVEL], --verbose [LEVEL]");
        puts("    -h, --help");
        return 0;
      default:
        break;
    }
  }

  if (verbosity > 0) {
    printf("Scheduler settings:\n");
    printf("    %-20s %.3f ms\n", "default quota:", QUOTA);
    printf("    %-20s %.3f ms\n", "minimum quota:", MIN_QUOTA);
    printf("    %-20s %.3f ms\n", "time window:", WINDOW_SIZE);
    printf("    %-20s %d ms\n", "sampling rate:", SAMPLING_RATE);
  }

  // register signal handler for debugging
  signal(SIGSEGV, sig_handler);
#ifdef _DEBUG
  if (verbosity > 0) signal(SIGINT, dump_history);
#endif

  int rc;
  int sockfd = 0;
  // int forClientSockfd = 0;
  // struct sockaddr_in clientInfo;
  // int addrlen = sizeof(clientInfo);

  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd == -1) {
    ERROR("Fail to create a socket!");
    exit(-1);
  }

  struct sockaddr_in serverInfo;
  bzero(&serverInfo, sizeof(serverInfo));

  serverInfo.sin_family = PF_INET;
  serverInfo.sin_addr.s_addr = INADDR_ANY;
  serverInfo.sin_port = htons(schd_port);
  if (bind(sockfd, (struct sockaddr *)&serverInfo, sizeof(serverInfo)) < 0) {
    ERROR("cannot bind port");
    exit(-1);
  }
  INFO("%d: scheduler port: %ld\n", __LINE__, schd_port);
  listen(sockfd, SOMAXCONN);
  INFO("%d: Received sockfd. %d\n", __LINE__, sockfd);

  pthread_t tid;


  // initialize candidate_cond with CLOCK_MONOTONIC
  pthread_condattr_t attr_monotonic_clock;
  pthread_condattr_init(&attr_monotonic_clock);
  pthread_condattr_setclock(&attr_monotonic_clock, CLOCK_MONOTONIC);
  pthread_cond_init(&candidate_cond, &attr_monotonic_clock);

  rc = pthread_create(&tid, NULL, schedule_daemon_func, NULL);
  if (rc != 0) {
    ERROR("Return code from pthread_create() - schedule daemon: %d", rc);
    exit(rc);
  }
  pthread_detach(tid);


  // start sampling thread
  rc = pthread_create(&tid, nullptr, sampling_thread, nullptr);
   if (rc != 0) {
    ERROR("Return code from pthread_create() - sampling: %d", rc);
    exit(rc);
  }
  INFO("%d: sampling", __LINE__);

  pthread_detach(tid);

  // read configuration file to setup pods
  // Watch for newcomers (new ClientGroup).
  char fullpath[PATH_MAX];
  snprintf(fullpath, PATH_MAX, "%s/%s", limit_file_dir, limit_file_name);
  read_resource_config(fullpath);

  rc = pthread_create(&tid, nullptr, podGroupMgmt, (void*) &sockfd);
  if (rc != 0) {
    ERROR("Failed to create pod group management thread: %s", strerror(rc));
    exit(rc);
  }
  pthread_detach(tid);
  

 // Wait for file change events
  main_loop = g_main_loop_new(nullptr, false);
  g_assert(main_loop);

  GError *err = nullptr;
  GFile *file = g_file_new_for_path(fullpath);
  if (file == nullptr) {
    ERROR("Failed to construct GFile for %s", fullpath);
    exit(EXIT_FAILURE);
  }
  GFileMonitor *monitor = g_file_monitor(file, G_FILE_MONITOR_WATCH_MOVES, nullptr, &err);
  if (monitor == nullptr || err != nullptr) {
    ERROR("Failed to create monitor for %s: %s", fullpath, err->message);
    exit(EXIT_FAILURE);
  }

  g_signal_connect(monitor, "changed", G_CALLBACK(onResourceConfigFileUpdate), nullptr);
  char *fpath = g_file_get_path(file);
  // g_print("monitoring %s\n", fpath);
  INFO("New Monitor thread created on %s.\n", fullpath);
  g_free(fpath);
 

  g_main_loop_run(main_loop);

  g_object_unref(monitor);
  return 0;
}

void sig_handler(int sig) {
  void *arr[10];
  size_t s;
  s = backtrace(arr, 10);
  ERROR("Received signal %d", sig);
  backtrace_symbols_fd(arr, s, STDERR_FILENO);
  exit(sig);
}

void upload_sampling() {
  char fullpath[PATH_MAX];
  char filename[20];  
  sprintf(filename, "sampling.json");
  snprintf(fullpath, PATH_MAX, "%s/d_%s/%s", limit_file_dir, limit_file_name, filename);
  INFO("dump log: %s", fullpath);

//
// overwrite the previous data, assume it is picked up already
//
  FILE *f = fopen(fullpath, "w");
  fputs("{\n", f);

  //output some  data
  int count = SAMPLE_COUNT;

  for (auto it = sample_list.begin(); it != sample_list.end() && count-- > 0; it++) {
    fprintf(f, "\t{\"tms\": \"%jd\",\"ctn\": \"%s\", \"bst\": %.3lf, \"ovs\" : %.3lf, \"h2d\" : %d(K), \"d2h\" : %d(K), \"mem\" : %.3lf (K)}",it->ts, it->name.c_str(),
              it->burst / 1000.0, it->overuse / 1000.0, it->h2dsize / 1000,  it->d2hsize / 1000, it->memsize);    
 

    if (std::next(it) == sample_list.end())
      fprintf(f, "\n");
    else
      fprintf(f, ",\n");
    // sample_list.pop_front();
  }
  fputs("}\n", f);
  // sample_list.clear();
  fclose(f);

  // INFO("sample appended to %s", fullpath);
}

#ifdef _DEBUG
// write full history to a json file
void dump_history(int sig) {
  char filename[20];
  sprintf(filename, "%ld.json", time(NULL));
  FILE *f = fopen(filename, "w");
  fputs("[\n", f);
  for (auto it = full_history.begin(); it != full_history.end(); it++) {
    fprintf(f, "\t{\"container\": \"%s\", \"start\": %.3lf, \"end\" : %.3lf}", it->name.c_str(),
            it->start / 1000.0, it->end / 1000.0);
    if (std::next(it) == full_history.end())
      fprintf(f, "\n");
    else
      fprintf(f, ",\n");
  }
  fputs("]\n", f);
  fclose(f);

  INFO("history dumped to %s", filename);
}
#endif
