/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <thread>

#include "client.h"
#include "cmd_thread_pool.h"
#include "net/event_loop.h"
#include "net/http_client.h"
#include "net/http_server.h"

namespace pikiwidb {

class IOThreadPool {
 public:
  IOThreadPool() = default;
  ~IOThreadPool() = default;

  static size_t GetMaxWorkerNum() { return kMaxWorkers; }

  bool Init(const char* ip, int port, const NewTcpConnectionCallback& ccb);
  void Run(int argc, char* argv[]);
  virtual void Exit();
  bool IsExit() const;
  EventLoop* BaseLoop();

  // choose a loop
  EventLoop* ChooseNextWorkerEventLoop();

  // set worker threads, each thread has a EventLoop object
  bool SetWorkerNum(size_t n);

  // app name, for top command
  void SetName(const std::string& name);

  // TCP server
  bool Listen(const char* ip, int port, const NewTcpConnectionCallback& ccb);

  // TCP client
  void Connect(const char* ip, int port, const NewTcpConnectionCallback& ccb,
               const TcpConnectionFailCallback& fcb = TcpConnectionFailCallback(), EventLoop* loop = nullptr);

  // HTTP server
  std::shared_ptr<HttpServer> ListenHTTP(const char* ip, int port,
                                         HttpServer::OnNewClient cb = HttpServer::OnNewClient());

  // HTTP client
  std::shared_ptr<HttpClient> ConnectHTTP(const char* ip, int port, EventLoop* loop = nullptr);

  virtual void PushWriteTask(std::shared_ptr<PClient> /*unused*/){};

  // for unittest only
  void Reset();

 protected:
  virtual void StartWorkers();

  static const size_t kMaxWorkers;

  std::string name_;
  std::string listen_ip_;
  int listen_port_{0};
  NewTcpConnectionCallback new_conn_cb_;

  EventLoop base_;

  std::atomic<size_t> worker_num_{0};
  std::vector<std::thread> worker_threads_;
  std::vector<std::unique_ptr<EventLoop>> worker_loops_;
  mutable std::atomic<size_t> current_worker_loop_{0};

  enum class State {
    kNone,
    kStarted,
    kStopped,
  };
  std::atomic<State> state_{State::kNone};
};

class WorkIOThreadPool : public IOThreadPool {
 public:
  WorkIOThreadPool() = default;
  ~WorkIOThreadPool() = default;

  void Exit() override;
  void PushWriteTask(std::shared_ptr<PClient> client) override;

 private:
  void StartWorkers() override;

 private:
  std::vector<std::thread> writeThreads_;
  std::vector<std::unique_ptr<std::mutex>> writeMutex_;
  std::vector<std::unique_ptr<std::condition_variable>> writeCond_;
  std::vector<std::deque<std::shared_ptr<PClient>>> writeQueue_;
  std::atomic<uint64_t> counter_ = 0;
  bool writeRunning_ = true;
};

}  // namespace pikiwidb
