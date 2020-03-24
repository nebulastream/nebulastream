#ifndef NES_ZMQSERVER_HPP
#define NES_ZMQSERVER_HPP

#include "NetworkCommon.hpp"
#include "ExchangeProtocol.hpp"
#include <zmq.hpp>
#include <thread>
#include <memory>
#include <atomic>
#include <future>
#include <boost/core/noncopyable.hpp>

namespace NES {
namespace Network {


class ZmqServer : public boost::noncopyable {
private:
    static constexpr const char* dispatcherPipe = "inproc://dispatcher";
public:
    explicit ZmqServer(const std::string& hostname, uint16_t port, uint16_t numNetworkThreads);

    ~ZmqServer();

    void start();

    std::shared_ptr<zmq::context_t> getContext() {
        return zmqContext;
    }

    bool isRunning() const {
        return _isRunning;
    }

private:

    void frontend_loop(uint16_t numHandlerThreads, std::promise<bool>& promise);
    void handler_event_loop();

private:

    const std::string hostname;
    const uint16_t port;
    const uint16_t numNetworkThreads;

    std::shared_ptr<zmq::context_t> zmqContext;
    std::unique_ptr<std::thread> frontendThread;
    std::vector<std::unique_ptr<std::thread>> handlerThreads;

    std::atomic_bool _isRunning;
    std::atomic_bool keepRunning;

    ExchangeProtocol protocol;

    // error management
    std::promise<bool> errorPromise;
};

}
}
#endif //NES_ZMQSERVER_HPP
