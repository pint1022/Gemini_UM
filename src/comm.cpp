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
 * Unified communication interface and network-related helper functions.
 * This should be the only place for message/request creation.
 */

#include "comm.h"

#include "debug.h"

reqid_t prepare_request(char *buf, comm_request_t type, ...) {
  static char *client_name = nullptr;
  static size_t client_name_len = 0;
  static reqid_t id = 0;
  size_t pos = 0;
  va_list vl;

  if (client_name == nullptr) {
    client_name = getenv("POD_NAME");
    if (client_name == nullptr) {
      client_name = (char *)malloc(HOST_NAME_MAX);
      gethostname(client_name, HOST_NAME_MAX);
    }
    client_name_len = strlen(client_name);
  }

  append_msg_data(buf, pos, client_name_len);
  strncpy(buf + pos, client_name, client_name_len);
  pos += client_name_len;
  append_msg_data(buf, pos, '\0');  // append a terminator
  append_msg_data(buf, pos, id);
  append_msg_data(buf, pos, type);

  // extra information for specific types
  if (type == REQ_QUOTA) {
    va_start(vl, 3);
    append_msg_data(buf, pos, va_arg(vl, double));  // overuse
    append_msg_data(buf, pos, va_arg(vl, double));  // burst duration
    va_end(vl);
  } else if (type == REQ_MEM_UPDATE) {
    va_start(vl, 2);
    append_msg_data(buf, pos, va_arg(vl, size_t));  // bytes
    append_msg_data(buf, pos, va_arg(vl, int));     // is_allocate
    va_end(vl);
  } else if (type == REQ_REC) {
    va_start(vl, 2);
    append_msg_data(buf, pos, va_arg(vl, size_t));  // bytes
    append_msg_data(buf, pos, va_arg(vl, int));     // is_allocate
    va_end(vl);
  } else if (type == REQ_HD) {
    va_start(vl, 2);
    append_msg_data(buf, pos, va_arg(vl, size_t));  // bytes
    append_msg_data(buf, pos, va_arg(vl, int));     // is_allocate
    va_end(vl);
  }



  return id++;
}

// fill corresponding data into passed arguments

char *parse_request(char *buf, char **name, size_t *name_len, reqid_t *id, comm_request_t *type) {
  size_t pos = 0;
  size_t name_len_;
  char *name_;
  reqid_t id_;
  comm_request_t type_;


  name_len_ = get_msg_data<size_t>(buf, pos);;
  name_ = buf +  sizeof(size_t);
  pos += name_len_ + 1;  // 1 for the terminator
  id_ = get_msg_data<reqid_t>(buf, pos);
  type_ = get_msg_data<comm_request_t>(buf, pos);
  // DEBUG("name: %s, name_len_:%ld, reqtype size %d, id_ %d, type %d\n", name_, name_len_, sizeof(comm_request_t), id_, type_);

  if (name != nullptr) *name = name_;
  if (name_len != nullptr) *name_len = name_len_;
  if (id != nullptr) *id = id_;
  if (type != nullptr) *type = type_;
  return buf + pos;
}



size_t prepare_sample(char *buf, reqid_t id, char *sample, char *podname, char *uuid) {
  size_t pos = 0;
  int32_t _len = strlen(sample);

  if ((sample == nullptr) || (_len <= 0)) {
    ERROR("Sample is empty");
    return 0;
  }
  if ((podname == nullptr) || (strlen(podname) <= 0)) {
    ERROR("Pod name is empty");
    return 0;
  }
  if ((uuid == nullptr) || (strlen(uuid) <= 0)) {
    ERROR("GPU uuid is empty");
    return 0;
  }
  char *tmp;

  append_msg_data(buf, pos, id);
  //
  //pod name section
  //
   _len = strlen(podname);
  if (_len > POD_NAME_LEN) {
     tmp = podname + _len - POD_NAME_LEN;
     _len = POD_NAME_LEN;
  }
  else
     tmp = podname;
  append_msg_data(buf, pos, _len);
  strncpy(buf + pos, tmp, _len);
  pos += _len;
  //
  //uuid name section
  //
   _len = strlen(uuid);
  if (_len > UUID_LEN) {
     tmp = uuid + _len - UUID_LEN;
     _len = UUID_LEN;
  }
  else
     tmp = uuid;
  append_msg_data(buf, pos, _len);
  strncpy(buf + pos, tmp, _len);
  pos += _len;

  _len = strlen(sample);
  append_msg_data(buf, pos, _len );
  strncpy(buf + pos, sample, _len);
  pos += _len;

  return pos;
}

size_t prepare_response(char *buf, comm_request_t type, reqid_t id, ...) {
  size_t pos = 0;
  va_list vl;

  append_msg_data(buf, pos, id);

  // extra information for specific types
  if (type == REQ_QUOTA) {
    va_start(vl, 1);
    append_msg_data(buf, pos, va_arg(vl, double));  // quota
    va_end(vl);
  } else if (type == REQ_MEM_UPDATE) {
    va_start(vl, 1);
    append_msg_data(buf, pos, va_arg(vl, int));  // verdict
    va_end(vl);
  } else if (type == REQ_MEM_LIMIT) {
    va_start(vl, 2);
    append_msg_data(buf, pos, va_arg(vl, size_t));  // used memory
    append_msg_data(buf, pos, va_arg(vl, size_t));  // total memory
    va_end(vl);
  }

  return pos;
}

char *parse_response(char *buf, reqid_t *id) {
  size_t pos = 0;
  reqid_t id_;

  id_ = get_msg_data<reqid_t>(buf, pos);

  if (id != nullptr) *id = id_;

  return buf + pos;
}

// Attempt a function several times. Non-zero return of func is treated as an error. If func return
// -1, errno will be returned.
int multiple_attempt(std::function<int()> func, int max_attempt, int interval) {
  int rc;
  for (int attempt = 1; attempt <= max_attempt; attempt++) {
    rc = func();
    if (rc == 0) break;
    if (rc == -1) rc = errno;
    ERROR("attempt %d: %s", attempt, strerror(rc));
    if (interval > 0) sleep(interval);
  }
  return rc;
}

reqid_t prepare_export_request(char *buf, char *sample, char *podname, char *uuid) {
  size_t pos = 0;
  int32_t _len = strlen(sample);
  static reqid_t sample_id = 0;

  if ((sample == nullptr) || (_len <= 0)) {
    ERROR("Sample is empty");
    return 0;
  }
  if ((podname == nullptr) || (strlen(podname) <= 0)) {
    ERROR("Pod name is empty");
    return 0;
  }
  if ((uuid == nullptr) || (strlen(uuid) <= 0)) {
    ERROR("GPU uuid is empty");
    return 0;
  }
  char *tmp;

  //
  //pod name section
  //
   _len = strlen(podname);
  if (_len > POD_NAME_LEN) {
     tmp = podname + _len - POD_NAME_LEN;
     _len = POD_NAME_LEN;
  }
  else
     tmp = podname;
  append_msg_data<size_t>(buf, pos, _len);
  strncpy(buf + pos, tmp, _len);
  pos += _len;
  append_msg_data(buf, pos, '\0');  // append a terminator
  append_msg_data(buf, pos, sample_id);
  append_msg_data(buf, pos, REQ_REC);  // request type

  //
  //uuid name section
  //
   _len = strlen(uuid);
  if (_len > UUID_LEN) {
     tmp = uuid + _len - UUID_LEN;
     _len = UUID_LEN;
  }
  else
     tmp = uuid;
  append_msg_data<int32_t>(buf, pos, _len);
  strncpy(buf + pos, tmp, _len);
  pos += _len;
  append_msg_data(buf, pos, '\0');  // append a terminator

  _len = strlen(sample);
  append_msg_data<int32_t>(buf, pos, _len );
  strncpy(buf + pos, sample, _len);
  pos += _len;


  return sample_id++;
}


char *parse_export_request(char *buf, char **name, size_t *name_len, reqid_t *id, comm_request_t *type) {
  size_t pos = 0;
  int32_t name_len_, uuid_len_;
  char *name_, *uuid_, *msg_;
  reqid_t id_;
  comm_request_t type_;


  name_len_ = get_msg_data<size_t>(buf, pos);;
  name_ = buf +  sizeof(size_t);
  pos += name_len_ + 1;  // 1 for the terminator
  DEBUG("Request from %s ...", name_);
  id_ = get_msg_data<reqid_t>(buf, pos);
  type_ = get_msg_data<comm_request_t>(buf, pos);

  if (name != nullptr) *name = name_;
  if (name_len != nullptr) *name_len = name_len_;
  if (id != nullptr) *id = id_;
  if (type != nullptr) *type = type_;

  return buf + pos;
}