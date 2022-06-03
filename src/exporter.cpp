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

/**
 * This is a per-GPU manager/scheduler.
 * Based on the information provided by clients, it decide which client to run
 * and give token to that client. This scheduler act as a daemon, accepting
 * connection and requests from pod manager or hook library directly.
 */

#include "exporter.h"

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

#define GPU_ID_LEN   100
#define GPU_ON_NODE  20
#define GPU_LIST_MAX (GPU_ID_LEN * GPU_ON_NODE)
char sample_file_dir[PATH_MAX] = ".";
char gpu_list[GPU_LIST_MAX] = ".";
std::list<Record> sample_list ;
pthread_mutex_t sample_mutex = PTHREAD_MUTEX_INITIALIZER;

//
// PRF: profiling 
//
int SAMPLING_RATE = 1000;
auto PROGRESS_START = steady_clock::now();

// signal handler
void sig_handler(int);

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

int verbosity = 0;
GMainLoop *main_loop = nullptr;

// milliseconds since scheduler process started
inline double ms_since_start() {
  return duration_cast<microseconds>(steady_clock::now() - PROGRESS_START).count() / 1e3;
}

// Get the information from message
void handle_message(int client_sock, char *message) {
  reqid_t req_id;  // simply pass this req_id back to Pod manager
  comm_request_t req;
  size_t clientname_len, offset = 0, uuid_len;
  char sbuf[SAMPLE_MSG_LEN];
  char *attached, *client_name, *uuid;
  Record _record;

  // DEBUG("received msg:[%x]", message);
// char *parse_export_request(char *buf, char **name, size_t *name_len, reqid_t *id, comm_request_t *type, char** uuid, size_t * uuid_len) ;

  // attached = parse_export_request(message, &client_name, &clientname_len, &req_id, &req, &uuid, &uuid_len);
  attached = parse_export_request(message, &client_name, &clientname_len, &req_id, &req);  
  // DEBUG("name: %s, name_len_:%d, req %d\n", client_name, clientname_len, req);

  bzero(sbuf, RSP_MSG_LEN);

  if (req == REQ_QRY) {
    DEBUG("Req (%s): query.", client_name);

  } else if (req == REQ_REC) {
    INFO("Recording Req (%s): record.", client_name);
    // send(client_sock, sbuf, RSP_MSG_LEN, 0);
      // _record.ts = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
      strncpy(_record.name, client_name, clientname_len);
      offset = 0;
      int msg_len = get_msg_data<int32_t>(attached, offset);
      char * tmp;
      if (msg_len > UUID_LEN) {
         tmp = attached + offset + msg_len - UUID_LEN;
         msg_len = UUID_LEN;
      } else {
        tmp = attached + offset;
      }
      strncpy(_record.uuid, tmp, msg_len);
      // INFO("UUID (%s): msg_len: %d", _record.uuid, msg_len);
      offset += msg_len + 1;
      msg_len = get_msg_data<int32_t>(attached, offset);
      if (msg_len > SAMPLE_LEN) {
         tmp = attached + offset + msg_len - SAMPLE_LEN;
         msg_len = SAMPLE_LEN;
      } else {
        tmp = attached + offset;
      }
      strncpy(_record.jsonstr, tmp, msg_len);
      INFO("str (%s): msg_len: %d", _record.jsonstr, msg_len);
      pthread_mutex_lock(&sample_mutex);
      sample_list.push_back(_record);    
      pthread_mutex_unlock(&sample_mutex);

  } 
  else if (req == REQ_SAMPLE) {
    DEBUG("Req: sampling. There are %d.", sample_list.size());

    if (sample_list.size() > 0) {
      // INFO("Sample count %d", sample_list.size());
      bool isSent = false;
      pthread_mutex_lock(&sample_mutex);

      for (auto it = sample_list.begin(); it != sample_list.end() ; it++) {
        offset = prepare_sample(sbuf, req_id, it->jsonstr, it->name, it->uuid);
        int _len = strlen(it->jsonstr);
        // INFO("Resp: %d, length %d, sample %s", offset, strlen(it->jsonstr), it->jsonstr);
        it = sample_list.erase(it);
        if (( _len > 0 ) && ( !isSent ) ){
             send(client_sock, sbuf, SAMPLE_MSG_LEN, 0);    
             isSent = true;
        } else {
          break;
        }
      }
      pthread_mutex_unlock(&sample_mutex);

    } else {
      // _record.ts = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

      char buf[SAMPLE_LEN];
        sprintf(buf, "{\"Ts\": %ld, \"Bs\": 0, \"Ou\": 0, \"Dh\": 0, \"Hd\": 0}",duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
      char *podname = "test";
      char *uuid = "NONE";      
      offset = prepare_sample(sbuf, req_id, buf, podname, uuid);
      send(client_sock, sbuf, SAMPLE_MSG_LEN, 0);    
    }

  } else {
    WARNING("\"%s\" receive an unknown request.", client_name);
  }
}

//
// req handling function for a sampler: waiting for incoming request
// 
void *sampler_service_func(void *args) {
  int server_sockfd = *((int *)args);
  char *rbuf = new char[SAMPLE_MSG_LEN];
  ssize_t recv_rc;
  while ((recv_rc = recv(server_sockfd, rbuf, REQ_MSG_LEN, 0)) > 0) {
    // DEBUG("recv: %d\n", recv_rc);
    handle_message(server_sockfd, rbuf);
  }
  DEBUG("Alnair Exporter Server: Connection closed. recv() returns %ld.", recv_rc);
  close(server_sockfd);
  delete (int *)args;
  delete[] rbuf;
  pthread_exit(NULL);
}


//
// server waits for the request, and spin off a handler
//
void *sampling_daemon_func(void * sockfd) {
  struct sockaddr_in clientInfo;
  int addrlen = sizeof(clientInfo);
  int forClientSockfd = 0;

  // INFO("Waiting for sampling req: %d", *(int*) sockfd);

  while (
      (forClientSockfd = accept( *(int*) sockfd, (struct sockaddr *)&clientInfo, (socklen_t *)&addrlen))) {
    pthread_t tid;
    int *server_sockfd = new int;
    *server_sockfd = forClientSockfd;
    // create a thread to service this Pod manager
    pthread_create(&tid, NULL, sampler_service_func, server_sockfd);
    pthread_detach(tid);
  }
  if (forClientSockfd < 0) {
    ERROR("Accept failed");
  }

  pthread_exit(nullptr);
}

int main(int argc, char *argv[]) {
  uint16_t alnr_port = 60018;
  // parse command line options
  const char *optstring = "d:P:G:s:v:h";
  struct option opts[] = {
                          {"sample_file_dir", required_argument, nullptr, 'd'},                          
                          {"port", required_argument, nullptr, 'P'},
                          {"gpu_list", required_argument, nullptr, 'G'},
                          {"sampling_rate", required_argument, nullptr, 's'},
                          {"verbose", required_argument, nullptr, 'v'},
                          {"help", no_argument, nullptr, 'h'},
                          {nullptr, 0, nullptr, 0}
                        };
  int opt;
  while ((opt = getopt_long(argc, argv, optstring, opts, NULL)) != -1) {
    switch (opt) {
      case 'd':
        strncpy(sample_file_dir, optarg, PATH_MAX - 1);
        break;        
      case 'P':
        alnr_port = strtoul(optarg, nullptr, 10);
        break;      
      case 'G':
        // strncpy(gpu_list, optarg, GPU_LIST_MAX - 1);
        INFO("gpu list %s\n", optarg);

        break;         
      case 's':
        SAMPLING_RATE = atof(optarg);
        break;
      case 'v':
        verbosity = atoi(optarg);
        break;
      case 'h':
        printf("usage: %s [options]\n", argv[0]);
        puts("Options:");
        puts("    -d [SAMPLE_DIR], --sample_file_dir [SAMPLE_DIR]");
        puts("    -P [PORT], --port [PORT]");
        puts("    -G [GPU_LIST], --gpu_list [GPU_LIST]");
        puts("    -s [SAMPLING_RATE], --sampling_rate [SAMPLING_RATE]");
        puts("    -v [LEVEL], --verbose [LEVEL]");
        puts("    -h, --help");
        return 0;
     
      default:
        break;
    }
  }
  INFO("Alnair server starting ... v %d\n", verbosity);

  if (verbosity > 0) {
    INFO("Sampling settings:");
    INFO("    %-20s %s", "GPUs:", gpu_list);
    INFO("    %-20s %d", "server port:", alnr_port);
    INFO("    %-20s %d", "sampling rate:", SAMPLING_RATE);
  }

  // register signal handler for debugging
  signal(SIGSEGV, sig_handler);
#ifdef _DEBUG
  if (verbosity > 0) signal(SIGINT, dump_history);
#endif

  int rc;
  int sockfd = 0;

  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd == -1) {
    ERROR("Fail to create a socket for sampler server!");
    exit(-1);
  }

  struct sockaddr_in serverInfo;
  bzero(&serverInfo, sizeof(serverInfo));

  serverInfo.sin_family = PF_INET;
  serverInfo.sin_addr.s_addr = INADDR_ANY;
  serverInfo.sin_port = htons(alnr_port);
  if (bind(sockfd, (struct sockaddr *)&serverInfo, sizeof(serverInfo)) < 0) {
    ERROR("cannot bind sampler port");
    exit(-1);
  }
  listen(sockfd, SOMAXCONN);
  // INFO("%s,%d: Received sampling sockfd. %d\n",__FILE__, __LINE__, sockfd);

  pthread_t tid;

  rc = pthread_create(&tid, nullptr, sampling_daemon_func, (void*) &sockfd);
   if (rc != 0) {
    ERROR("Return code from pthread_create() - sampling_daemon_func: %d", rc);
    exit(rc);
  }

  pthread_detach(tid);
  main_loop = g_main_loop_new(nullptr, false);
  g_assert(main_loop);
  g_main_loop_run(main_loop);
  return 0;
}

void sig_handler(int sig) {
  void *arr[10];
  size_t s;
  s = backtrace(arr, 10);
  ERROR("Received signal %d in sampler server", sig);
  backtrace_symbols_fd(arr, s, STDERR_FILENO);
  exit(sig);
}

void upload_sampling(char * g_list) {
  char fullpath[PATH_MAX];
  char filename[20];  
  sprintf(filename, "sampling.json");
  char *ptr; // declare a ptr pointer  
  ptr = strtok(g_list, "\n"); // use strtok() function to separate string using comma (,) delimiter.

  while (ptr != NULL) {
    snprintf(fullpath, PATH_MAX, "%s/d_%s/%s", sample_file_dir, ptr, filename);
    INFO("dump log: %s", fullpath);
    ptr = strtok(NULL, "\n");
  }

//
// overwrite the previous data, assume it is picked up already
//
  // FILE *f = fopen(fullpath, "r");
  // fputs("{\n", f);

  // //output some  data
  // int count = SAMPLE_COUNT;

  // for (auto it = sample_list.begin(); it != sample_list.end() && count-- > 0; it++) {
  //   fprintf(f, "\t{\"tms\": \"%jd\",\"ctn\": \"%s\", \"bst\": %.3lf, \"ovs\" : %.3lf, \"h2d\" : %d(K), \"d2h\" : %d(K), \"mem\" : %.3lf (K)}",it->ts, it->name.c_str(),
  //             it->burst / 1000.0, it->overuse / 1000.0, it->h2dsize / 1000,  it->d2hsize / 1000, it->memsize);    
 

  //   if (std::next(it) == sample_list.end())
  //     fprintf(f, "\n");
  //   else
  //     fprintf(f, ",\n");
  // }
  // fputs("}\n", f);
  // fclose(f);

  INFO("sample appended to %s", fullpath);
}
