/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_ZMQSERVER_HPP
#define NES_ZMQSERVER_HPP

#include <Network/ExchangeProtocol.hpp>
#include <Network/NesPartition.hpp>
#include <Network/PartitionManager.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <atomic>
#include <boost/core/noncopyable.hpp>
#include <future>
#include <memory>
#include <thread>
#include <zmq.hpp>

namespace NES {
class ThreadBarrier;
namespace Network {

class ZmqServer : public boost::noncopyable {
  private:
    static constexpr const char* dispatcherPipe = "inproc://dispatcher";

  public:
    /**
     * Create a ZMQ server on hostname:port with numNetworkThreads i/o threads and a set of callbacks in
     * exchangeProtocol
     * @param hostname
     * @param port
     * @param numNetworkThreads
     * @param exchangeProtocol
     */
    explicit ZmqServer(const std::string& hostname, uint16_t port, uint16_t numNetworkThreads, ExchangeProtocol& exchangeProtocol,
                       BufferManagerPtr bufferManager);

    ~ZmqServer();

    /**
     * Start the server. It throws exceptions if the starting fails.
     */
    bool start();

    /**
     * Get the global zmq context
     * @return
     */
    std::shared_ptr<zmq::context_t> getContext() { return zmqContext; }

    /**
     * Checks if the server is running
     * @return
     */
    bool getIsRunning() { return isRunning; }

  private:
    /**
     * @brief the receiving thread where clients send their messages to the server, here messages are forwarded to the
     * handlerEventLoop by the proxy
     * @param numHandlerThreads number of HandlerThreads
     * @param startPromise the promise that is passed to the thread
     */
    void routerLoop(uint16_t numHandlerThreads, std::promise<bool>& startPromise);

    /**
     * @brief handler thread where threads are passed from the frontend loop
     * @param the threadBarrier to enable synchronization
     */
    void messageHandlerEventLoop(std::shared_ptr<ThreadBarrier> barrier, int index);

  private:
    const std::string hostname;
    const uint16_t port;
    const uint16_t numNetworkThreads;

    std::shared_ptr<zmq::context_t> zmqContext;
    std::unique_ptr<std::thread> routerThread;
    std::vector<std::unique_ptr<std::thread>> handlerThreads;

    std::atomic_bool isRunning;
    std::atomic_bool keepRunning;

    ExchangeProtocol& exchangeProtocol;
    BufferManagerPtr bufferManager;

    // error management
    std::promise<bool> errorPromise;
};

}// namespace Network
}// namespace NES
#endif//NES_ZMQSERVER_HPP
