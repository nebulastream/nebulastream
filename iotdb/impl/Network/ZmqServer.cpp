#include <Network/ZmqServer.hpp>

#define TO_RAW_ZMQ_SOCKET static_cast<void*>

namespace NES {
namespace Network {

ZmqServer::ZmqServer(const std::string& hostname, uint16_t port, uint16_t numNetworkThreads)
        : hostname(hostname), port(port), numNetworkThreads(numNetworkThreads), _isRunning(false), keepRunning(true) {
    assert(numNetworkThreads >= DEFAULT_NUM_SERVER_THREADS);
}

void ZmqServer::start() {
    std::promise<bool> start_promise;
    uint16_t numZmqThreads = (numNetworkThreads - 1) / 2;
    uint16_t numHandlerThreads = numNetworkThreads / 2;
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

        }
    } catch (...) {
        // TODO do something here?
        auto eptr = std::current_exception();
        errorPromise.set_exception(eptr);
    }
}

}
}

