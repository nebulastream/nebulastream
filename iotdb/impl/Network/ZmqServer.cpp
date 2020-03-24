#include <Network/ZmqServer.hpp>
#include <Network/NetworkMessage.hpp>
#include <Util/Logger.hpp>

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
      frontend_loop(numHandlerThreads, start_promise);
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

void ZmqServer::frontend_loop(uint16_t numHandlerThreads, std::promise<bool>& start_promise) {
    int linger = 100;
    zmq::socket_t frontendSocket(*zmqContext, zmq::socket_type::router);
    zmq::socket_t dispatcherSocket(*zmqContext, zmq::socket_type::dealer);

    try {
        frontendSocket.setsockopt(ZMQ_LINGER, &linger, sizeof(int));
        frontendSocket.bind("tcp://" + hostname + ":" + std::to_string(port));
        dispatcherSocket.bind(dispatcherPipe);

        for (uint16_t i = 0; i < numHandlerThreads; ++i) {
            handlerThreads.emplace_back(std::make_unique<std::thread>([this]() {
              handler_event_loop();
            }));
        }
    } catch (...) {
        start_promise.set_exception(std::current_exception());
    }
    start_promise.set_value(true);
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

void ZmqServer::handler_event_loop() {
    zmq::socket_t dispatcher_socket(*zmqContext, zmq::socket_type::dealer);
    try {
        dispatcher_socket.connect(dispatcherPipe);
        while (keepRunning) {
            zmq::message_t identityEnvelope;
            zmq::message_t headerEnvelope;
            dispatcher_socket.recv(identityEnvelope);
            dispatcher_socket.recv(headerEnvelope);
            auto msgHeader = headerEnvelope.data<Messages::MessageHeader>();
            if (msgHeader->getMagicNumber() != Messages::NES_NETWORK_MAGIC_NUMBER) {
                // TODO handle error -- need to discuss how we handle errors on the node engine
                NES_ERROR("[ZmqServer] stream is corrupted");
            }
            switch (msgHeader->getMsgType()) {
                case Messages::kClientAnnouncement: {
                    zmq::message_t outIdentityEnvelope;
                    zmq::message_t clientAnnouncementEnvelope;
                    dispatcher_socket.recv(clientAnnouncementEnvelope);
                    auto serverReadyMsg =
                        exchangeProtocol.onClientAnnoucement(clientAnnouncementEnvelope.data<Messages::ClientAnnounceMessage>());
                    Messages::MessageHeader sendHeader(Messages::kServerReady, sizeof(Messages::ServerReadyMessage));
                    zmq::message_t msgHeaderEnvelope(&sendHeader, sizeof(Messages::MessageHeader));
                    zmq::message_t msgServerReadyEnvelope(&serverReadyMsg, sizeof(Messages::ServerReadyMessage));
                    outIdentityEnvelope.copy(identityEnvelope);
                    dispatcher_socket.send(outIdentityEnvelope, zmq::send_flags::sndmore);
                    dispatcher_socket.send(msgHeaderEnvelope, zmq::send_flags::sndmore);
                    dispatcher_socket.send(msgServerReadyEnvelope);
                    break;
                }
                case Messages::kDataBuffer : {
                    exchangeProtocol.onBuffer();
                    break;
                }
                case Messages::kErrorMessage: {
                    break;
                }
                case Messages::kEndOfStream: {
                    exchangeProtocol.onEndOfStream();
                    break;
                }
                default: {
                    NES_ERROR("[ZmqServer] received unknown message type");
                    // TODO propagate error maybe?
                }
            }
        }
    } catch (zmq::error_t& err) {
        if (err.num() == ETERM) {
            NES_DEBUG("Context closed");
        } else {
            NES_ERROR("[ZmqServer] event loop got " << err.what());
            errorPromise.set_exception(std::current_exception());
        }
    }
}

}
}

