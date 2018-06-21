/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef _THRIFT_SERVER_TZMQSERVER_H_
#define _THRIFT_SERVER_TZMQSERVER_H_ 1

#include <thrift/transport/TZmqTransport.h>
#include <thrift/TProcessor.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/Mutex.h>

#include <zmq.hpp>

namespace apache { namespace thrift { namespace server {

/** A simple ZeroMQ server based on @ref TZmqTransport.
 *
 * @internally Internally does not derive from TServer, because it does not
 * implement most of its features, including:
 * - No support for per-client processors.
 * - No differentiation between the server's transport and the support to
 *   process client requests.
 * - No support for server event handlers (@ref TServerEventHandler).
 */
class TZmqServer : public concurrency::Runnable {
 public:
  TZmqServer(zmq::context_t & context, stdcxx::shared_ptr<TProcessor> processor,
             stdcxx::shared_ptr<transport::TZmqTransport> transport,
             stdcxx::shared_ptr<protocol::TProtocolFactory> protocolFactory);

  void serve();

  void stop();

  // Allows running the server as a @ref Runnable thread
  virtual void run();

 private:
  stdcxx::shared_ptr<TProcessor> processor_;
  stdcxx::shared_ptr<protocol::TProtocol> protocol_;

  // Used to internally control the server
  concurrency::Mutex rwMutex_;
  zmq::socket_t interruptSend_;
  stdcxx::shared_ptr<zmq::socket_t> interruptRecv_;
};

}}} // apache::thrift::server

#endif // #ifndef _THRIFT_SERVER_TZMQSERVER_H_
