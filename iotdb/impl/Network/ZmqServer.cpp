#include <Network/NetworkMessage.hpp>
#include <Network/ZmqServer.hpp>
#include <Network/ZmqUtils.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadBarrier.hpp>

#define TO_RAW_ZMQ_SOCKET static_cast<void*>

namespace NES {
namespace Network {

ZmqServer::ZmqServer(const std::string& hostname, uint16_t port, uint16_t numNetworkThreads,
                     ExchangeProtocol& exchangeProtocol, BufferManagerPtr bufferManager)
    : hostname(hostname), port(port), numNetworkThreads(numNetworkThreads), isRunning(false), keepRunning(true),
      exchangeProtocol(exchangeProtocol), bufferManager(bufferManager) {
    if (numNetworkThreads < DEFAULT_NUM_SERVER_THREADS) {
        NES_THROW_RUNTIME_ERROR("ZmqServer: numNetworkThreads is greater than DEFAULT_NUM_SERVER_THREADS");
    }
}

bool ZmqServer::start() {
    std::promise<bool> start_promise;
    uint16_t numZmqThreads = (numNetworkThreads - 1)/2;
    uint16_t numHandlerThreads = numNetworkThreads/2;
    zmqContext = std::make_shared<zmq::context_t>(numZmqThreads);
    frontendThread = std::make_unique<std::thread>([this, numHandlerThreads, &start_promise]() {
      routerLoop(numHandlerThreads, start_promise);
    });
    return start_promise.get_future().get();
}

ZmqServer::~ZmqServer() {
    // Do not change the shutdown sequence!
    if (!zmqContext) {
        return; // start() not called
    }
    keepRunning = false;
    zmqContext.reset();
    frontendThread->join();
    auto future = errorPromise.get_future();
    if (future.valid()) {
        // we have an error
        try {
            future.get();
        } catch (std::exception& e) {
            NES_ERROR("ZmqServer: Destructor " << e.what());
            throw e;
        }
    }
}

void ZmqServer::routerLoop(uint16_t numHandlerThreads, std::promise<bool>& startPromise) {
    // option of linger time until port is closed
    int linger = 0;
    zmq::socket_t frontendSocket(*zmqContext, zmq::socket_type::router);
    zmq::socket_t dispatcherSocket(*zmqContext, zmq::socket_type::dealer);
    auto barrier = std::make_shared<ThreadBarrier>(1 + numHandlerThreads);

    try {
        frontendSocket.setsockopt(ZMQ_LINGER, &linger, sizeof(int));
        frontendSocket.bind("tcp://" + hostname + ":" + std::to_string(port));
        dispatcherSocket.bind(dispatcherPipe);
        NES_DEBUG("Created Zmq Server socket on " << hostname << ":" << port);
        for (int i = 0; i < numHandlerThreads; ++i) {
            handlerThreads.emplace_back(std::make_unique<std::thread>([this, &barrier, i]() {
              handlerEventLoop(barrier, i);
            }));
        }
    }
    catch (...) {
        startPromise.set_exception(std::current_exception());
    }
    isRunning = true;
    // wait for the handlers to start
    barrier->wait();
    // unblock the thread that started the server
    startPromise.set_value(true);
    bool shutdownComplete = false;

    try {
        zmq::proxy(TO_RAW_ZMQ_SOCKET(frontendSocket), TO_RAW_ZMQ_SOCKET(dispatcherSocket), nullptr);
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
                NES_INFO("ZmqServer: Shutdown completed!")
            } else {
                NES_ERROR("ZmqServer: " << zmqError.what());
                errorPromise.set_exception(eptr);
            }
        } catch (std::exception& error) {
            NES_ERROR("ZmqServer: " << error.what());
            errorPromise.set_exception(eptr);
        }
    }

    // At this point, we need to shutdown the handlers
    if (!keepRunning) {
        for (auto& t : handlerThreads) {
            t->join();
        }
        handlerThreads.clear();
    }

    if (shutdownComplete) {
        errorPromise.set_value(false);
    }
}

void ZmqServer::handlerEventLoop(std::shared_ptr<ThreadBarrier> barrier, int index) {
    zmq::socket_t dispatcherSocket(*zmqContext, zmq::socket_type::dealer);
    try {
        dispatcherSocket.connect(dispatcherPipe);
        barrier->wait();
        NES_DEBUG("Created Zmq Handler Thread #" << index << " on " << hostname << ":" << port);
        while (keepRunning) {
            zmq::message_t identityEnvelope;
            zmq::message_t headerEnvelope;
            dispatcherSocket.recv(&identityEnvelope);
            dispatcherSocket.recv(&headerEnvelope);
            auto msgHeader = headerEnvelope.data<Messages::MessageHeader>();
            if (msgHeader->getMagicNumber() != Messages::NES_NETWORK_MAGIC_NUMBER) {
                // TODO handle error -- need to discuss how we handle errors on the node engine
                NES_ERROR("ZmqServer: stream is corrupted");
            }
            switch (msgHeader->getMsgType()) {
                case Messages::ClientAnnouncement: {
                    // if server receives announcement, that a client wants to send buffers
                    zmq::message_t outIdentityEnvelope;
                    zmq::message_t clientAnnouncementEnvelope;
                    dispatcherSocket.recv(&clientAnnouncementEnvelope);

                    // react after announcement is received
                    auto serverReadyMsg = exchangeProtocol.onClientAnnouncement(
                        clientAnnouncementEnvelope.data<Messages::ClientAnnounceMessage>());
                    NES_INFO("ZmqServer: ClientAnnouncement received for "
                                 << serverReadyMsg.getOperatorId() << "::" << serverReadyMsg.getPartitionId()
                                 << "::" << serverReadyMsg.getSubpartitionId() << "::" << serverReadyMsg.getQueryId());

                    // send response back to the client based on the identity
                    outIdentityEnvelope.copy(&identityEnvelope);
                    sendMessageWithIdentity<Messages::ServerReadyMessage>(dispatcherSocket,
                                                                          outIdentityEnvelope,
                                                                          serverReadyMsg);
                    break;
                }
                case Messages::DataBuffer: {
                    // if server receives a tuple buffer
                    zmq::message_t bufferHeaderMsg;
                    dispatcherSocket.recv(&bufferHeaderMsg);
                    // parse buffer header
                    auto bufferHeader = bufferHeaderMsg.data<Messages::DataBufferMessage>();

                    // parse identity
                    uint64_t* id;
                    id = (uint64_t*) identityEnvelope.data();


                    // receive buffer content
                    TupleBuffer buffer = bufferManager->getBufferBlocking();
                    dispatcherSocket.recv(buffer.getBuffer(), bufferHeader->getPayloadSize(), 0);
                    buffer.setNumberOfTuples(bufferHeader->getNumOfRecords());

                    // create a string for logging of the identity which corresponds to the
                    // queryId::operatorId::partitionId::subpartitionId
                    NES_INFO(
                        "ZmqServer: DataBuffer received from "
                            << std::to_string(id[0]) + "::" + std::to_string(id[1]) + "::" + std::to_string(id[2]) +
                                "::" + std::to_string(id[3]) << " with " << bufferHeader->getNumOfRecords()
                            << "/" << bufferHeader->getPayloadSize());
                    exchangeProtocol.onBuffer(id, buffer);
                    break;
                }
                case Messages::ErrorMessage: {
                    // if server receives a message that an error occured
                    NES_FATAL_ERROR("ZmqServer: ErrorMessage not supported yet");
                    break;
                }
                case Messages::EndOfStream: {
                    // if server receives a message that the stream did terminate
                    NES_INFO("ZmqServer: EndOfStream message received")
                    exchangeProtocol.onEndOfStream();
                    break;
                }
                default: {
                    NES_ERROR("ZmqServer: received unknown message type");
                }
            }
        }
    } catch (zmq::error_t& err) {
        if (err.num() == ETERM) {
            NES_DEBUG("Zmq Handler #" << index << " closed on server " << hostname << ":" << port);
        } else {
            errorPromise.set_exception(std::current_exception());
            NES_ERROR("ZmqServer: event loop " << index << " got " << err.what());
            throw err;
        }
    }
}

} // namespace Network
} // namespace NES
