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

#include <algorithm>
#include <cassert>
#include <sstream>

#include "TZmqTransport.h"

namespace apache { namespace thrift { namespace transport {

TZmqTransport::TZmqTransport(stdcxx::shared_ptr<zmq::socket_t>& sock)
    : sock_(sock) {
  if (!sock_) {
    throw TTransportException(TTransportException::BAD_ARGS,
                              "ZeroMQ socket is invalid..");
  }
}

void TZmqTransport::open() {
  // Intentionally left empty.
}

bool TZmqTransport::isOpen() {
  return true;
}

void TZmqTransport::close() {
  sock_->close();
}

uint32_t TZmqTransport::read(uint8_t* buf, uint32_t len) {
  // \todo Does not work with e.g. DEALER or multi-part PUB.

  zmq_pollitem_t items[2] = { { *sock_, 0, ZMQ_POLLIN, 0 },
      {NULL, 0, ZMQ_POLLIN, 0} };
  int itemsUsed = 1;

  if (interruptListener_) {
    items[1].socket = *interruptListener_;
    ++itemsUsed;
  }

  if (rbuf_.available_read() == 0) {
    try {
      int ret = zmq::poll(items, itemsUsed);
      if (ret > 0) {
        if (itemsUsed >= 2 && (items[1].revents & ZMQ_POLLIN) != 0) {
          // Read the message used to interrupt, so the transport can be
          // used again later.
          zmq::message_t msg;
          (void) interruptListener_->recv(&msg);
          throw TTransportException(TTransportException::INTERRUPTED);
        } else if ((items[0].revents & ZMQ_POLLIN) != 0)
          if (!sock_->recv(&inmsg_)) {
            throw TTransportException(TTransportException::TIMED_OUT);
          }

        rbuf_.resetBuffer((uint8_t*) inmsg_.data(), inmsg_.size());
      }

    } catch (zmq::error_t& e) {
      throw TTransportException(
          TTransportException::UNKNOWN,
          std::string("Receiving ZeroMQ message failed. ") + e.what());
    }
  }
  return rbuf_.read(buf, len);
}

void TZmqTransport::write(const uint8_t* buf, uint32_t len) {
  wbuf_.write(buf, len);
}

void TZmqTransport::flush() {
  try {
    uint8_t* buf = NULL;
    uint32_t size = 0;
    wbuf_.getBuffer(&buf, &size);
    zmq::message_t msg(buf, size);

    // Make sure the data is flushed internally, even in case of an error.
    wbuf_.resetBuffer(true);

    if (!sock_->send(msg)) {
      throw TTransportException(TTransportException::TIMED_OUT);
    }
  } catch (zmq::error_t& e) {
    throw TTransportException(TTransportException::UNKNOWN,
        std::string("Sending ZeroMQ message failed. ") + e.what());
  }
}

stdcxx::shared_ptr<zmq::socket_t> TZmqTransport::getSocket() {
  return sock_;
}

void TZmqTransport::setInterruptSocket(
    stdcxx::shared_ptr<zmq::socket_t> interruptListener) {
  interruptListener_ = interruptListener;
}

}
}
}  // apache::thrift::transport
