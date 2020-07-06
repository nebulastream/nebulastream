#include <Network/NetworkMessage.hpp>
#include <Network/ZmqServer.hpp>
#include <Network/ZmqUtils.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
#include <Exceptions/NesExceptions.hpp>

#define TO_RAW_ZMQ_SOCKET static_cast<void*>

namespace NES {
namespace Network {

ZmqServer::ZmqServer(const std::string& hostname, uint16_t port, uint16_t numNetworkThreads,
                     ExchangeProtocolPtr exchangeProtocol)
    : hostname(hostname), port(port), numNetworkThreads(numNetworkThreads), isRunning(false), keepRunning(true),
      exchangeProtocol(exchangeProtocol) {
    NES_DEBUG("ZmqServer: Creating ZmqServer()");
    if (numNetworkThreads < DEFAULT_NUM_SERVER_THREADS) {
        NES_THROW_RUNTIME_ERROR("ZmqServer: numNetworkThreads is greater than DEFAULT_NUM_SERVER_THREADS");
    }
}

bool ZmqServer::start() {
    NES_DEBUG("ZmqServer: Starting server..");
    std::promise<bool> startPromise;
    uint16_t numZmqThreads = (numNetworkThreads - 1) / 2;
    uint16_t numHandlerThreads = numNetworkThreads / 2;
    zmqContext = std::make_shared<zmq::context_t>(numZmqThreads);
    routerThread = std::make_unique<std::thread>([this, numHandlerThreads, &startPromise]() {
        routerLoop(numHandlerThreads, startPromise);
    });
    return startPromise.get_future().get();
}

ZmqServer::~ZmqServer() {
    // Do not change the shutdown sequence!
    NES_INFO("ZmqServer: Initiating shutdown");
    if (!zmqContext) {
        return;// start() not called
    }
    keepRunning = false;
    zmqContext.reset();
    auto future = errorPromise.get_future();
    routerThread->join();
    if (future.valid()) {
        // we have an error
        try {
            future.get();
        } catch (std::exception& e) {
            NES_ERROR("ZmqServer: Server failed to start due to " << e.what());
            throw e;
        }
    }
}

void ZmqServer::routerLoop(uint16_t numHandlerThreads, std::promise<bool>& startPromise) {
    // option of linger time until port is closed
    int linger = -1;
    zmq::socket_t frontendSocket(*zmqContext, zmq::socket_type::router);
    zmq::socket_t dispatcherSocket(*zmqContext, zmq::socket_type::dealer);
    auto barrier = std::make_shared<ThreadBarrier>(1 + numHandlerThreads);

    try {
        NES_DEBUG("ZmqServer: Trying to bind on "
                  << "tcp://" + hostname + ":" + std::to_string(port));
        frontendSocket.setsockopt(ZMQ_LINGER, &linger, sizeof(int));
        frontendSocket.bind("tcp://" + hostname + ":" + std::to_string(port));
        dispatcherSocket.bind(dispatcherPipe);
        NES_DEBUG("ZmqServer: Created socket on " << hostname << ":" << port);
    } catch (std::exception& ex) {
        NES_ERROR("ZmqServer: Error in routerLoop() " << ex.what());
        startPromise.set_value(false);
        errorPromise.set_exception(std::make_exception_ptr(ex));
        return;
    }

    for (int i = 0; i < numHandlerThreads; ++i) {
        handlerThreads.emplace_back(std::make_unique<std::thread>([this, &barrier, i]() {
            messageHandlerEventLoop(barrier, i);
        }));
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
                NES_INFO("ZmqServer: Shutdown completed!");
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

void ZmqServer::messageHandlerEventLoop(std::shared_ptr<ThreadBarrier> barrier, int index) {
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
                NES_THROW_RUNTIME_ERROR("ZmqServer: Stream is corrupted");
            }
            switch (msgHeader->getMsgType()) {
                case Messages::ClientAnnouncement: {
                    // if server receives announcement, that a client wants to send buffers
                    zmq::message_t outIdentityEnvelope;
                    zmq::message_t clientAnnouncementEnvelope;
                    dispatcherSocket.recv(&clientAnnouncementEnvelope);
                    outIdentityEnvelope.copy(&identityEnvelope);
                    auto receivedMsg = *clientAnnouncementEnvelope.data<Messages::ClientAnnounceMessage>();

                    // react after announcement is received
                    try {
                        auto returnMessage = exchangeProtocol->onClientAnnouncement(receivedMsg);
                        sendMessageWithIdentity<Messages::ServerReadyMessage>(dispatcherSocket, outIdentityEnvelope, returnMessage);
                    } catch (NesNetworkError& ex) {
                        auto returnMessage = exchangeProtocol->onError(ex.getErrorMessage());
                        sendMessageWithIdentity<Messages::ErrMessage>(dispatcherSocket, outIdentityEnvelope, returnMessage);
                    }
                    break;
                }
                case Messages::DataBuffer: {
                    // if server receives a tuple buffer
                    zmq::message_t bufferHeaderMsg;
                    dispatcherSocket.recv(&bufferHeaderMsg);
                    // parse buffer header
                    auto bufferHeader = bufferHeaderMsg.data<Messages::DataBufferMessage>();
                    auto nesPartition = *identityEnvelope.data<NesPartition>();

                    // receive buffer content
                    TupleBuffer buffer = exchangeProtocol->getBufferManager()->getBufferBlocking();
                    dispatcherSocket.recv(buffer.getBuffer(), bufferHeader->getPayloadSize());
                    buffer.setNumberOfTuples(bufferHeader->getNumOfRecords());

                    exchangeProtocol->onBuffer(nesPartition, buffer);
                    break;
                }
                case Messages::ErrorMessage: {
                    // if server receives a message that an error occured
                    NES_FATAL_ERROR("ZmqServer: ErrorMessage not supported yet");
                    break;
                }
                case Messages::EndOfStream: {
                    // if server receives a message that the stream did terminate
                    zmq::message_t eosEnvelope;
                    dispatcherSocket.recv(&eosEnvelope);
                    auto eosMsg = *eosEnvelope.data<Messages::EndOfStreamMessage>();
                    exchangeProtocol->onEndOfStream(eosMsg);
                    break;
                }
                default: {
                    NES_ERROR("ZmqServer: received unknown message type");
                }
            }
        }
    } catch (zmq::error_t& err) {
        if (err.num() == ETERM) {
            NES_DEBUG("ZmqServer: Handler #" << index << " closed on server " << hostname << ":" << port);
        } else {
            errorPromise.set_exception(std::current_exception());
            NES_ERROR("ZmqServer: event loop " << index << " got " << err.what());
            throw err;
        }
    }
}

}// namespace Network
}// namespace NES
