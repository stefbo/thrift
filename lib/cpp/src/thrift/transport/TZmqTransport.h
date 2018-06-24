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

#ifndef H4687D222_9464_4DB6_BF87_AC5840A8137E
#define H4687D222_9464_4DB6_BF87_AC5840A8137E

#include <thrift/stdcxx.h>
#include <thrift/transport/TBufferTransports.h> // TMemoryBuffer
#include <thrift/transport/TVirtualTransport.h>

#include <zmq.hpp>

namespace apache {
namespace thrift {
namespace transport {

/** ZeroMQ-based transport.
 *
 * Takes a so-called ZeroMQ socket and uses it to send and receive messages.
 *
 * ZeroMQ brings its own communication styles depending on the socket and hence,
 * using `oneway` messages and regular messages is restricted to specific socket
 * types. Regular request/reply style communication is allowed only with a pair
 * or REQ/REP sockets, where `oneway` messages are allowed with PUSH/PULL
 * sockets and SUB/PUB sockets only. Mixing `oneway` and regular messages in
 * Thrift is not easily possible.
 *
 * The concept of sockets is very different compared to BSD sockets. This has
 * the following consequences:
 * - There is no distinction between a server socket and sockets used to
 *   communicate to connected clients. Accepting client connections is is
 *   transparent to the application.
 * - When using BSD sockets, servers **bind** the socket and accept clients
 *   while clients **connect** to the server. In ZeroMQ, there is a similar
 *   concept of bind and connect, but it does not imply any role of the socket.
 *   When using a PUB/SUB socket pair, you can decide whether to bind the PUB
 *   or the SUB sockets and both alternatives make perfect sense. Therefore,
 *   implementing a useful @ref open method does not make sense, because it
 *   would have to know whether to bind or connect.
 * - In ZeroMQ a message may contain multiple so-called **frames**. When using
 *   Thrift on top of ZeroMQ only a single frame is sent, except for one case.
 *   When using a PUB/SUB socket pair, the publisher can precede the message
 *   with a message containing data that is binary matched against the
 *   subscription key. A static subscription key for the transport can be
 *   set by @ref setSubscriptionKey.
 *
 * See the constructor for a simple example how to use the socket.
 */
class TZmqTransport : public TVirtualTransport<TZmqTransport> {
public:
  /** Constructor.
   *
   * It is assumed that the socket is already connected or bound. Because ZeroMQ
   * allows to connect or bind to multiple endpoints, constructors which
   * accept one or multiple endpoints hardly make sense. Instead do the wiring
   * before or after the transport is created. Here is an example:
   *     zmq::context_t context;
   *     auto sock = std::make_shared<zmq::socket_t>(context, ZMQ_REQ);
   *     sock->bind("tcp://*:6000");
   *     sock->bind("...");
   *     ...
   *     auto transport = std::make_shared<TZmqTransport>(sock);
   *
   * In case the `stdcxx::shared_ptr` seems a bad idea in your case, consider
   * putting your existing socket into a shared pointer with a no-op deleter.
   * Most implementations support a custom deleter.
   *
   * @param sock The ZeroMQ socket. Must not be `NULL`.
   */
  explicit TZmqTransport(stdcxx::shared_ptr<zmq::socket_t>& sock);

  /** Returns the underlying ZeroMQ socket.
   */
  stdcxx::shared_ptr<zmq::socket_t> getSocket();

  /** Sets the static subscription for the socket.
   *
   * Only allowed for sockets of type `PUB`. By default no subscription key is
   * set, that is, no additional frame is sent in front of the data.
   *
   * The key can be changed at any time.
   *
   * @param key The new subscription key.
   */
  void setSubscriptionKey(const std::string& key);

  /** @name Methods to implement @ref TTransport.
   * @{
   */

  /** Has no effect.
   */
  virtual void open();

  /** Does always return `true`.
   */
  virtual bool isOpen();

  /** Has no effect.
   */
  virtual void close();

  uint32_t read(uint8_t* buf, uint32_t len);

  void write(const uint8_t* buf, uint32_t len);

  virtual void flush();

  /** @} */

  /** Sets the socket used to interrupt pending reads on the ZeroMQ socket.
   *
   * Becomes effective the next time @read is called. Does not affect
   * @ref write.
   */
  void setInterruptSocket(stdcxx::shared_ptr<zmq::socket_t> interruptListener);

private:
  void readFromSocket();

  stdcxx::shared_ptr<zmq::socket_t> sock_;
  stdcxx::shared_ptr<zmq::socket_t> interruptListener_;

  TMemoryBuffer wbuf_;
  TMemoryBuffer rbuf_;
  zmq::message_t subscriptionKeyMsg_;
};

} // namespace transport
} // namespace thrift
} // namespace apache

#endif /* H4687D222_9464_4DB6_BF87_AC5840A8137E */
