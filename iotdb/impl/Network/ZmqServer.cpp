#include <Network/ZmqServer.hpp>
#include <Network/NetworkMessage.hpp>
#include <Util/Logger.hpp>
#include <Network/ZmqUtils.hpp>
#include <Util/ThreadBarrier.hpp>

#define TO_RAW_ZMQ_SOCKET static_cast<void*>

namespace NES {
namespace Network {

ZmqServer::ZmqServer(const std::string& hostname,
                     uint16_t port,
                     uint16_t numNetworkThreads,
                     ExchangeProtocol& exchangeProtocol) : hostname(hostname),
                                                           port(port),
                                                           numNetworkThreads(numNetworkThreads),
                                                           _isRunning(false),
                                                           keepRunning(true),
                                                           exchangeProtocol(exchangeProtocol) {
    assert(numNetworkThreads >= DEFAULT_NUM_SERVER_THREADS);
}

void ZmqServer::start() {
    std::promise<bool> start_promise;
    uint16_t numZmqThreads = (numNetworkThreads - 1)/2;
    uint16_t numHandlerThreads = numNetworkThreads/2;
    zmqContext = std::make_shared<zmq::context_t>(numZmqThreads);
    frontendThread = std::make_unique<std::thread>([this, numHandlerThreads, &start_promise]() {
      frontendLoop(numHandlerThreads, start_promise);
    });
    start_promise.get_future().get();
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
            throw e;
        }
    }
}

void ZmqServer::frontendLoop(uint16_t numHandlerThreads, std::promise<bool>& start_promise) {
    int linger = 100;
    zmq::socket_t frontendSocket(*zmqContext, zmq::socket_type::router);
    zmq::socket_t dispatcherSocket(*zmqContext, zmq::socket_type::dealer);
    auto barrier = std::make_shared<ThreadBarrier>(1 + numHandlerThreads);

    try {
        frontendSocket.setsockopt(ZMQ_LINGER, &linger, sizeof(int));
        frontendSocket.bind("tcp://" + hostname + ":" + std::to_string(port));
        dispatcherSocket.bind(dispatcherPipe);

        for (uint16_t i = 0; i < numHandlerThreads; ++i) {
            handlerThreads.emplace_back(std::make_unique<std::thread>([this, &barrier]() {
              handlerEventLoop(barrier);
            }));
        }
    } catch (...) {
        start_promise.set_exception(std::current_exception());
    }
    barrier->wait(); // wait for the handlers to start
    start_promise.set_value(true); // unblock the thread that started the server
    _isRunning = true;
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
            } else {
                errorPromise.set_exception(eptr);
            }
        } catch (std::exception& error) {
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

void ZmqServer::handlerEventLoop(std::shared_ptr<ThreadBarrier> barrier) {
    zmq::socket_t dispatcherSocket(*zmqContext, zmq::socket_type::dealer);
    try {
        dispatcherSocket.connect(dispatcherPipe);
        barrier->wait();
        while (keepRunning) {
            zmq::message_t identityEnvelope;
            zmq::message_t headerEnvelope;
            dispatcherSocket.recv(identityEnvelope);
            dispatcherSocket.recv(headerEnvelope);
            auto msgHeader = headerEnvelope.data<Messages::MessageHeader>();
            if (msgHeader->getMagicNumber() != Messages::NES_NETWORK_MAGIC_NUMBER) {
                // TODO handle error -- need to discuss how we handle errors on the node engine
                NES_ERROR("[ZmqServer] stream is corrupted");
            }
            switch (msgHeader->getMsgType()) {
                case Messages::kClientAnnouncement: {
                    zmq::message_t outIdentityEnvelope;
                    zmq::message_t clientAnnouncementEnvelope;
                    dispatcherSocket.recv(clientAnnouncementEnvelope);
                    auto serverReadyMsg =
                        exchangeProtocol.onClientAnnoucement(clientAnnouncementEnvelope.data<Messages::ClientAnnounceMessage>());
                    outIdentityEnvelope.copy(identityEnvelope);
                    sendMessageWithIdentity<Messages::ServerReadyMessage>(dispatcherSocket,
                                                                          outIdentityEnvelope,
                                                                          serverReadyMsg);
                    break;
                }
                case Messages::kDataBuffer : {
                    exchangeProtocol.onBuffer();
                    NES_ERROR("[ZmqServer] not supported yet");
                    assert(false);
                    break;
                }
                case Messages::kErrorMessage: {
                    NES_ERROR("[ZmqServer] not supported yet");
                    assert(false);
                    break;
                }
                case Messages::kEndOfStream: {
                    exchangeProtocol.onEndOfStream();
                    break;
                }
                default: {
                    NES_ERROR("[ZmqServer] received unknown message type");
                    // TODO propagate error maybe?
                    assert(false);
                }
            }
        }
    } catch (zmq::error_t& err) {
        if (err.num() == ETERM) {
            NES_DEBUG("Context closed on server " << hostname << ":" << port);
        } else {
            NES_ERROR("[ZmqServer] event loop got " << err.what());
            errorPromise.set_exception(std::current_exception());
            NES_ERROR("[ZmqServer] not supported yet");
            assert(false);
        }
    }
}

}
}

