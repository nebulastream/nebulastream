/*
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

#include <Network/EventBufferMessage.hpp>
#include <Network/ExchangeProtocol.hpp>
#include <Network/NetworkMessage.hpp>
#include <Network/ZmqServer.hpp>
#include <Network/ZmqUtils.hpp>
#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateAndDrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateQueryMetadata.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
#include <Util/ThreadNaming.hpp>
#include <arpa/inet.h>
#include <netdb.h>
#include <utility>

namespace NES::Network {

/// max number of tcp sockets
static constexpr auto MAX_ZMQ_SOCKET = 2 * 65536;

namespace detail {

class UlimitNumFdChanger {
  public:
    /// change ulimit to allow number of higher zmq socket
    explicit UlimitNumFdChanger() {
        struct rlimit limit;
        NES_ASSERT(getrlimit(RLIMIT_NOFILE, &limit) == 0, "Cannot retrieve ulimit");
        oldSoftNumFileLimit = limit.rlim_cur;
        oldHardNumFileLimit = limit.rlim_max;
        limit.rlim_cur = MAX_ZMQ_SOCKET;
        limit.rlim_max = MAX_ZMQ_SOCKET;
        NES_ASSERT(setrlimit(RLIMIT_NOFILE, &limit) == 0, "Cannot set ulimit");
    }

    /// restore ulimit to previous values
    ~UlimitNumFdChanger() {
        struct rlimit limit;
        limit.rlim_cur = oldSoftNumFileLimit;
        limit.rlim_max = oldHardNumFileLimit;
        setrlimit(RLIMIT_NOFILE, &limit);
    }

  private:
    int oldSoftNumFileLimit;
    int oldHardNumFileLimit;
};
//static UlimitNumFdChanger ulimitChanger;
}// namespace detail

ZmqServer::ZmqServer(std::string hostname,
                     uint16_t requestedPort,
                     uint16_t numNetworkThreads,
                     ExchangeProtocol& exchangeProtocol,
                     Runtime::BufferManagerPtr bufferManager)
    : hostname(std::move(hostname)), requestedPort(requestedPort), currentPort(requestedPort),
      numNetworkThreads(std::max(DEFAULT_NUM_SERVER_THREADS, numNetworkThreads)), isRunning(false), keepRunning(true),
      exchangeProtocol(exchangeProtocol), bufferManager(std::move(bufferManager)) {
    NES_DEBUG("ZmqServer({}:{}) Creating ZmqServer()", this->hostname, this->currentPort);
    if (numNetworkThreads < DEFAULT_NUM_SERVER_THREADS) {
        NES_WARNING("ZmqServer({}:{}) numNetworkThreads is smaller than DEFAULT_NUM_SERVER_THREADS",
                    this->hostname,
                    this->currentPort);
    }
}

bool ZmqServer::start() {
    NES_DEBUG("ZmqServer({}:{}): Starting server..", this->hostname, this->currentPort);
    std::shared_ptr<std::promise<bool>> startPromise = std::make_shared<std::promise<bool>>();
    uint16_t numZmqThreads = (numNetworkThreads - 1) / 2;
    uint16_t numHandlerThreads = 1;
    zmqContext = std::make_shared<zmq::context_t>(numZmqThreads);
    // NES_ASSERT(MAX_ZMQ_SOCKET == zmqContext->get(zmq::ctxopt::max_sockets), "Cannot set max num of sockets");
    routerThread = std::make_unique<std::thread>([this, numHandlerThreads, startPromise]() {
        setThreadName("zmq-router");
        routerLoop(numHandlerThreads, startPromise);
    });
    return startPromise->get_future().get();
}

ZmqServer::~ZmqServer() { stop(); }

bool ZmqServer::stop() {
    // Do not change the shutdown sequence!
    auto expected = true;
    if (!isRunning.compare_exchange_strong(expected, false)) {
        return false;
    }
    NES_INFO("ZmqServer({}:{}): Initiating shutdown", this->hostname, this->currentPort);
    if (!zmqContext) {
        return false;// start() not called
    }
    keepRunning = false;
    /// plz do not change the above shutdown sequence
    /// following zmq's guidelines, the correct way to terminate it is to:
    /// 1. call shutdown on the zmq context (this tells ZMQ to prepare for termination)
    /// 2. wait for all zmq sockets and threads to be closed (including the WorkerPool as it holds DataChannels)
    /// 3. call close on the zmq context
    /// 4. deallocate the zmq contect
    zmqContext->shutdown();
    auto future = errorPromise.get_future();
    routerThread->join();
    if (future.valid()) {
        // we have an error
        try {
            bool gracefullyClosed = future.get();
            if (gracefullyClosed) {
                NES_DEBUG("ZmqServer({}:{}): gracefully closed on: {}",
                          this->hostname,
                          this->currentPort,
                          hostname,
                          this->currentPort);
            } else {
                NES_WARNING("ZmqServer({}:{}): non gracefully closed on: {}",
                            this->hostname,
                            this->currentPort,
                            hostname,
                            this->currentPort);
            }
        } catch (std::exception& e) {
            NES_ERROR("ZmqServer({}:{}):  Server failed to start due to {}", this->hostname, this->currentPort, e.what());
            zmqContext->close();
            zmqContext.reset();
            throw e;
        }
    }
    NES_INFO("ZmqServer({}:{}): Going to close zmq context...", this->hostname, this->currentPort);
    zmqContext->close();
    NES_INFO("ZmqServer({}:{}): Zmq context is now closed", this->hostname, this->currentPort);
    zmqContext.reset();
    return true;
}

void ZmqServer::getServerSocketInfo(std::string& hostname, uint16_t& port) {
    hostname = this->hostname;
    port = this->currentPort.load();
}

static std::optional<std::string> getIPFromHostname(const std::string& hostname) {
    // check if hostname already is an ip
    if (sockaddr_in sa; inet_pton(AF_INET, hostname.c_str(), &(sa.sin_addr)) != 0) {
        NES_DEBUG("{} is already an IP address", hostname);
        return hostname;
    }

    // dns lookup
    addrinfo hints = {}, *addrs;
    std::array<char, INET_ADDRSTRLEN> ipStr = {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    if (int err = getaddrinfo(hostname.c_str(), nullptr, &hints, &addrs); err != 0) {
        NES_ERROR("getaddrinfo failed when looking up hostname \"{}\". Error: {}", hostname, strerror(errno));
        return std::nullopt;
    }

    // convert ip to string
    auto* psai = reinterpret_cast<struct sockaddr_in*>(addrs->ai_addr);
    if (inet_ntop(addrs->ai_family, &(psai->sin_addr), ipStr.data(), INET_ADDRSTRLEN) == nullptr) {
        NES_ERROR("inet_ntop failed when converting ip to text \"{}\". Error: {}", hostname, strerror(errno));
        free(addrs);
        return std::nullopt;
    }

    free(addrs);
    return std::string(ipStr.data(), strnlen(ipStr.data(), INET_ADDRSTRLEN));
}

void ZmqServer::routerLoop(uint16_t numHandlerThreads, const std::shared_ptr<std::promise<bool>>& startPromise) {
    zmq::socket_t frontendSocket(*zmqContext, zmq::socket_type::router);
    zmq::socket_t dispatcherSocket(*zmqContext, zmq::socket_type::dealer);

    try {
        auto bindingIPAddress = getIPFromHostname(hostname);
        if (!bindingIPAddress) {
            NES_THROW_RUNTIME_ERROR("Could not determine binding address");
        }

        auto address = "tcp://" + *bindingIPAddress + ":" + std::to_string(requestedPort);
        NES_DEBUG("ZmqServer({}:{}):  Trying to bind on {}", this->hostname, requestedPort, address);
        //< option of linger time until port is closed
        frontendSocket.set(zmq::sockopt::linger, -1);
        frontendSocket.bind(address);
        dispatcherSocket.bind(dispatcherPipe);
        NES_DEBUG("ZmqServer({}:{}):  Created socket on {}: {}", this->hostname, this->currentPort, hostname, currentPort);
    } catch (std::exception& ex) {
        NES_ERROR("ZmqServer({}:{}):   Error in routerLoop() {}", this->hostname, this->currentPort, ex.what());
        startPromise->set_value(false);
        errorPromise.set_exception(std::make_exception_ptr(ex));
        return;
    }

    const auto currentSocketAddress = frontendSocket.get(zmq::sockopt::last_endpoint);
    uint8_t host[4];
    int actualPort = -1;
    NES_ASSERT2_FMT(currentSocketAddress.size(), "cannot read zmq identity on " << hostname);
    sscanf(currentSocketAddress.c_str(), "tcp://%hhu.%hhu.%hhu.%hhu:%d", &host[0], &host[1], &host[2], &host[3], &actualPort);
    NES_ASSERT2_FMT(actualPort > 0, "Wrong port on zmq server " << this->hostname);
    currentPort = actualPort;
    NES_DEBUG("ZmqServer: Created socket on {}: {}", hostname, actualPort);

    auto barrier = std::make_shared<ThreadBarrier>(1 + numHandlerThreads);
    for (int i = 0; i < numHandlerThreads; ++i) {
        handlerThreads.emplace_back(std::make_unique<std::thread>([this, &barrier, i]() {
            setThreadName("zmq-evt-%d", i);
            messageHandlerEventLoop(barrier, i);
        }));
    }

    isRunning = true;
    // wait for the handlers to start
    barrier->wait();
    // unblock the thread that started the server
    startPromise->set_value(true);
    bool shutdownComplete = false;

    try {
        zmq::proxy(frontendSocket, dispatcherSocket);
    } catch (...) {
        // we write the following code to propagate the exception in the
        // caller thread, e.g., the owner of the nes engine
        auto eptr = std::current_exception();
        try {
            if (eptr) {
                std::rethrow_exception(eptr);
            }
        } catch (zmq::error_t& zmqError) {
            // handle
            if (zmqError.num() == ETERM) {
                shutdownComplete = true;
                //                dispatcherSocket.close();
                //                frontendSocket.close();
                NES_INFO("ZmqServer({}:{}): Frontend: Shutdown completed! address: tcp://{}:{}",
                         this->hostname,
                         this->currentPort,
                         hostname,
                         std::to_string(actualPort));

            } else {
                NES_ERROR("ZmqServer({}:{}):{}", this->hostname, this->currentPort, zmqError.what());
                errorPromise.set_exception(eptr);
            }
        } catch (std::exception& error) {
            NES_ERROR("ZmqServer({}:{}):{}", this->hostname, this->currentPort, error.what());
            errorPromise.set_exception(eptr);
        }
    }

    // At this point, we need to shut down the handlers
    if (!keepRunning) {
        for (auto& t : handlerThreads) {
            t->join();
        }
        handlerThreads.clear();
    }

    if (shutdownComplete) {
        errorPromise.set_value(true);
    }
}

namespace detail {
// helper type for the visitor #4
template<class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
// explicit deduction guide (not needed as of C++20)
template<class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;
}// namespace detail

void ZmqServer::messageHandlerEventLoop(const std::shared_ptr<ThreadBarrier>& barrier, int threadId) {
    using namespace Messages;
    zmq::socket_t dispatcherSocket(*zmqContext, zmq::socket_type::dealer);
    try {
        dispatcherSocket.connect(dispatcherPipe);
        barrier->wait();
        NES_DEBUG("Created Zmq Handler Thread #{} on {}: {}", threadId, hostname, this->currentPort);
        while (keepRunning) {
            zmq::message_t identityEnvelope;
            zmq::message_t headerEnvelope;
            auto const identityEnvelopeReceived = dispatcherSocket.recv(identityEnvelope);
            auto const headerEnvelopeReceived = dispatcherSocket.recv(headerEnvelope);
            auto* msgHeader = headerEnvelope.data<Messages::MessageHeader>();

            if (msgHeader->getMagicNumber() != Messages::NES_NETWORK_MAGIC_NUMBER || !identityEnvelopeReceived.has_value()
                || !headerEnvelopeReceived.has_value()) {
                // TODO handle error -- need to discuss how we handle errors on the node engine
                NES_THROW_RUNTIME_ERROR("ZmqServer(" << this->hostname << ":" << this->currentPort << "):  Source is corrupted");
            }

            switch (msgHeader->getMsgType()) {
                case MessageType::ClientAnnouncement: {
                    // if server receives announcement, that a client wants to send buffers
                    zmq::message_t outIdentityEnvelope;
                    zmq::message_t clientAnnouncementEnvelope;
                    auto optRecvStatus = dispatcherSocket.recv(clientAnnouncementEnvelope, kZmqRecvDefault);
                    NES_ASSERT2_FMT(optRecvStatus.has_value(), "invalid recv");
                    outIdentityEnvelope.copy(identityEnvelope);
                    auto receivedMsg = *clientAnnouncementEnvelope.data<Messages::ClientAnnounceMessage>();
                    NES_DEBUG("ZmqServer({}:{}): ClientAnnouncement received for channel {}",
                              this->hostname,
                              this->currentPort,
                              receivedMsg.getChannelId());

                    // react after announcement is received
                    auto returnMessage = exchangeProtocol.onClientAnnouncement(receivedMsg);
                    std::visit(detail::overloaded{
                                   [&dispatcherSocket, &outIdentityEnvelope](Messages::ServerReadyMessage serverReadyMessage) {
                                       sendMessageWithIdentity<Messages::ServerReadyMessage>(dispatcherSocket,
                                                                                             outIdentityEnvelope,
                                                                                             serverReadyMessage);
                                   },
                                   [this, &dispatcherSocket, &outIdentityEnvelope](Messages::ErrorMessage errorMessage) {
                                       sendMessageWithIdentity<Messages::ErrorMessage>(dispatcherSocket,
                                                                                       outIdentityEnvelope,
                                                                                       errorMessage);
                                       exchangeProtocol.onServerError(errorMessage);
                                   }},
                               returnMessage);
                    break;
                }
                case MessageType::DataBuffer: {
                    // if server receives a tuple buffer
                    zmq::message_t bufferHeaderMsg;
                    auto optRecvStatus = dispatcherSocket.recv(bufferHeaderMsg, kZmqRecvDefault);
                    NES_ASSERT2_FMT(optRecvStatus.has_value(), "invalid recv");
                    // parse buffer header
                    auto* bufferHeader = bufferHeaderMsg.data<Messages::DataBufferMessage>();
                    auto nesPartition = identityEnvelope.data<NesPartition>();

                    NES_TRACE("ZmqServer({}:{}):  DataBuffer received from origin={} and seqNumber={} and NesPartition={} "
                              "with payload size {} with num children buffer {}",
                              this->hostname,
                              this->currentPort,
                              bufferHeader->originId,
                              bufferHeader->sequenceData.toString(),
                              nesPartition->toString(),
                              bufferHeader->payloadSize,
                              bufferHeader->numOfChildren);
                    // get children if necessary
                    std::vector<Runtime::TupleBuffer> children;
                    for (uint32_t i = 0u, numOfChildren = bufferHeader->numOfChildren; i < numOfChildren; ++i) {
                        zmq::message_t childBufferHeaderMsg;
                        auto optRecvStatus = dispatcherSocket.recv(childBufferHeaderMsg, kZmqRecvDefault);
                        NES_ASSERT2_FMT(optRecvStatus.has_value(), "invalid recv");
                        auto* childBufferHeader = childBufferHeaderMsg.data<Messages::DataBufferMessage>();

                        auto childBuffer = bufferManager->getUnpooledBuffer(childBufferHeader->payloadSize);
                        auto optRetSize =
                            dispatcherSocket.recv(zmq::mutable_buffer(childBuffer->getBuffer(), childBufferHeader->payloadSize),
                                                  kZmqRecvDefault);
                        NES_ASSERT2_FMT(optRetSize.has_value(), "Invalid recv size");
                        NES_ASSERT2_FMT(optRetSize.value().size == childBufferHeader->payloadSize,
                                        "Recv not matching sizes " << optRetSize.value().size
                                                                   << "!=" << childBufferHeader->payloadSize);
                        children.emplace_back(std::move(*childBuffer));
                    }

                    // receive buffer content
                    NES_ERROR("trying to get buffer from buffer manager partition: {}, watermark {}", nesPartition->toString(), bufferHeader->watermark);
                    auto buffer = bufferManager->getBufferBlocking();
                    NES_ERROR("acquired buffer partition: {}, watermark {}", nesPartition->toString(), bufferHeader->watermark);
                    NES_ASSERT2_FMT(bufferHeader->payloadSize <= buffer.getBufferSize(),
                                    "Buffer size [" << buffer.getBufferSize() << " is smaller than payload "
                                                    << bufferHeader->payloadSize);
                    auto optRetSize = dispatcherSocket.recv(zmq::mutable_buffer(buffer.getBuffer(), bufferHeader->payloadSize),
                                                            kZmqRecvDefault);
                    NES_ASSERT2_FMT(optRetSize.has_value(), "Invalid recv size");
                    NES_ASSERT2_FMT(optRetSize.value().size == bufferHeader->payloadSize,
                                    "Recv not matching sizes " << optRetSize.value().size << "!=" << bufferHeader->payloadSize);
                    buffer.setNumberOfTuples(bufferHeader->numOfRecords);
                    buffer.setOriginId(bufferHeader->originId);
                    buffer.setWatermark(bufferHeader->watermark);
                    buffer.setCreationTimestampInMS(bufferHeader->creationTimestamp);
                    buffer.setSequenceData(bufferHeader->sequenceData);

                    for (auto&& childBuffer : children) {
                        auto idx = buffer.storeChildBuffer(childBuffer);
                        NES_ASSERT2_FMT(idx >= 0, "Invalid child index: " << idx);
                    }

                    NES_TRACE("ZmqSink: Data received with sequence number {} successful",
                              bufferHeader->sequenceData.sequenceNumber);
                    constexpr auto defaultChunkNumber = 1;
                    constexpr auto isLastChunk = true;
                    exchangeProtocol.onBuffer(*nesPartition,
                                              buffer,
                                              {bufferHeader->messageSequenceNumber, defaultChunkNumber, isLastChunk});
                    break;
                }
                case MessageType::EventBuffer: {
                    zmq::message_t bufferHeaderMsg;
                    auto optRecvStatus = dispatcherSocket.recv(bufferHeaderMsg, kZmqRecvDefault);
                    NES_ASSERT2_FMT(optRecvStatus.has_value(), "invalid recv");
                    // parse buffer header
                    auto* bufferHeader = bufferHeaderMsg.data<Messages::EventBufferMessage>();
                    auto nesPartition = *identityEnvelope.data<NesPartition>();
                    auto optBuffer = bufferManager->getUnpooledBuffer(bufferHeader->payloadSize);
                    auto buffer = *optBuffer;
                    auto optRetSize = dispatcherSocket.recv(zmq::mutable_buffer(buffer.getBuffer(), bufferHeader->payloadSize),
                                                            kZmqRecvDefault);
                    NES_ASSERT2_FMT(optRetSize.has_value(), "Invalid recv size");
                    NES_ASSERT2_FMT(optRetSize.value().size == bufferHeader->payloadSize,
                                    "Recv not matching sizes " << optRetSize.value().size << "!=" << bufferHeader->payloadSize);
                    switch (bufferHeader->eventType) {
                        case Runtime::EventType::kCustomEvent: {
                            auto event = Runtime::CustomEventWrapper(std::move(buffer));
                            exchangeProtocol.onEvent(nesPartition, event);
                            break;
                        }
                        case Runtime::EventType::kStartSourceEvent: {
                            auto event = Runtime::StartSourceEvent();
                            exchangeProtocol.onEvent(nesPartition,
                                                     event);// todo jm finish up the funtion calls folowing from here
                            break;
                        }
                        default: {
                            NES_ASSERT2_FMT(false, "Invalid event");
                        }
                    }
                    break;
                }
                case MessageType::ErrorMessage: {
                    // if server receives a message that an error occurred
                    NES_FATAL_ERROR("ZmqServer({}:{}):  ErrorMessage not supported yet", this->hostname, this->currentPort);
                    break;
                }
                case MessageType::EndOfStream: {
                    // if server receives a message that the source did terminate
                    zmq::message_t eosEnvelope;
                    auto optRetSize = dispatcherSocket.recv(eosEnvelope, kZmqRecvDefault);
                    NES_ASSERT2_FMT(optRetSize.has_value(), "Invalid recv size");
                    auto eosMsg = *eosEnvelope.data<Messages::EndOfStreamMessage>();
                    auto reconfigurationEvents = eosMsg.getReconfigurationEventCount();
                    std::optional<ReconfigurationMarkerPtr> marker = std::nullopt;
                    if (reconfigurationEvents > 0) {
                        marker = ReconfigurationMarker::create();
                        for (auto i = 0; i < reconfigurationEvents; ++i) {
                            zmq::message_t reconfigEnvelope;
                            optRetSize = dispatcherSocket.recv(reconfigEnvelope, kZmqRecvDefault);
                            NES_ASSERT2_FMT(optRetSize.has_value(), "Invalid recv size");
                            auto reconfigMsg = *reconfigEnvelope.data<Messages::ReconfigurationEventMessage>();

                            switch (reconfigMsg.metadataType) {
                                case ReconfigurationMetadataType::DrainQuery:
                                    marker.value()->addReconfigurationEvent(
                                        reconfigMsg.decomposedQueryIdWithVersion.id,
                                        reconfigMsg.decomposedQueryIdWithVersion.version,
                                        ReconfigurationMarkerEvent::create(
                                            reconfigMsg.queryState,
                                            std::make_shared<DrainQueryMetadata>(reconfigMsg.numberOfSources)));
                                    break;
                                case ReconfigurationMetadataType::UpdateAndDrainQuery:
                                    marker.value()->addReconfigurationEvent(
                                        reconfigMsg.decomposedQueryIdWithVersion.id,
                                        reconfigMsg.decomposedQueryIdWithVersion.version,
                                        ReconfigurationMarkerEvent::create(
                                            reconfigMsg.queryState,
                                            std::make_shared<UpdateAndDrainQueryMetadata>(reconfigMsg.workerId,
                                                                                          reconfigMsg.sharedQueryId,
                                                                                          reconfigMsg.decomposedQueryId,
                                                                                          reconfigMsg.decomposedQueryPlanVersion,
                                                                                          reconfigMsg.numberOfSources)));
                                    break;
                                case ReconfigurationMetadataType::UpdateQuery:
                                    auto sinkUpdateCount = reconfigMsg.sinkReconfigurationCount;
                                    std::vector<NetworkSinkUpdateInfo> updateInfos;
//                                    for (auto j = 0; j < sinkUpdateCount; ++j) {
//                                        NES_ERROR("processing sink update {}", j);
//                                        zmq::message_t reconfigEnvelope;
//                                        optRetSize = dispatcherSocket.recv(reconfigEnvelope, kZmqRecvDefault);
//                                        NES_ASSERT2_FMT(optRetSize.has_value(), "Invalid recv size");
//                                        NES_ERROR("received sink update message");
//                                        auto sinkUpdate = *reconfigEnvelope.data<NetworkSinkUpdateInfo>();
//                                        NES_ERROR("retrieve update from message");
//                                        updateInfos.push_back(sinkUpdate);
//                                        NES_ERROR("inserted into vector of update infos");
//                                    }

                                    marker.value()->addReconfigurationEvent(
                                        reconfigMsg.decomposedQueryIdWithVersion.id,
                                        reconfigMsg.decomposedQueryIdWithVersion.version,
                                        ReconfigurationMarkerEvent::create(
                                            reconfigMsg.queryState,
                                            std::make_shared<UpdateQueryMetadata>(reconfigMsg.workerId,
                                                                                  reconfigMsg.sharedQueryId,
                                                                                  reconfigMsg.decomposedQueryId,
                                                                                  reconfigMsg.decomposedQueryPlanVersion,
                                                                                  updateInfos)));
                                    break;
                            }
                        }
                    }
                    NES_WARNING("ZmqServer({}:{}): EndOfStream received for channel ",
                                this->hostname,
                                this->currentPort,
                                eosMsg.getChannelId());
                    exchangeProtocol.onEndOfStream(eosMsg, marker);
                    break;
                }
                default: {
                    NES_ERROR("ZmqServer({}:{}): received unknown message type", this->hostname, this->currentPort);
                    break;
                }
            }
        }
    } catch (zmq::error_t& err) {
        if (err.num() == ETERM) {
            //            dispatcherSocket.close();
            NES_DEBUG("ZmqServer({}:{}):  Handler #{}  closed on server{}: {}",
                      this->hostname,
                      this->currentPort,
                      threadId,
                      hostname,
                      this->currentPort);
        } else {
            errorPromise.set_exception(std::current_exception());
            NES_ERROR("ZmqServer({}:{}): event loop {} got {}", this->hostname, this->currentPort, threadId, err.what());
            std::rethrow_exception(std::current_exception());
        }
    }
}

}// namespace NES::Network
