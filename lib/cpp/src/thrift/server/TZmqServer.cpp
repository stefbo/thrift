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

#include "TZmqServer.h"
#include <thrift/transport/TBufferTransports.h>

#include <iostream> // \todo rmeove
using namespace std;

using apache::thrift::transport::TTransportException;

namespace apache { namespace thrift { namespace server {

TZmqServer::TZmqServer(
    zmq::context_t & context,
    stdcxx::shared_ptr<TProcessor> processor,
    stdcxx::shared_ptr<transport::TZmqTransport> transport,
    stdcxx::shared_ptr<protocol::TProtocolFactory> protocolFactory)
    : processor_(processor),
      protocol_(protocolFactory->getProtocol(transport)),
      interruptSend_(context, ZMQ_PAIR),
      interruptRecv_(new zmq::socket_t(context, ZMQ_PAIR)) {
  const char interruptEndpoint[] = "inproc://TZmqServer_interrupt";
  interruptRecv_->bind(interruptEndpoint);
  interruptSend_.connect(interruptEndpoint);
  transport->setInterruptSocket(interruptRecv_);
}

void TZmqServer::serve() {
  for(bool done = false; !done; ) {
    try {
      if (!processor_->process(protocol_, NULL)) {
        done = true;
      }
    } catch (TTransportException & e) {
      if (e.getType() == TTransportException::TIMED_OUT) {
        // Accept timeout - continue processing.
      } else if (e.getType() == TTransportException::INTERRUPTED) {
        // Server was interrupted.  This only happens when stopping.
        done = true;
      } else {
        throw;
      }
    }
  }  // for-loop
}

void TZmqServer::stop() {
  concurrency::Guard g(rwMutex_);
  (void) interruptSend_.send("", 0);
}

void TZmqServer::run() {
  serve();
}

}}} // apache::thrift::server
