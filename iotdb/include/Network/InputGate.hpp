#ifndef NES_INCLUDE_NETWORK_INPUTGATE_HPP_
#define NES_INCLUDE_NETWORK_INPUTGATE_HPP_

#include <string>
#include <zmq.hpp>
#include <NodeEngine/TupleBuffer.hpp>

namespace NES {

/**
 * @brief this class provide a zmq as data source
 */
class InputGate {
  public:
    /**
     * @brief constructor for the zmq source
     * @param host name of the source queue
     * @param port of the source queue
     */
    InputGate(std::string  host, uint16_t port);

    /**
     * @brief blocking method to receive a buffer from the zmq source
     */
    std::tuple<std::string, TupleBufferPtr> receiveData();

    /**
     * @brief method to connect zmq using the host and port specified before
     * check if already connected, if not connect try to connect, if already connected return
     * @return bool indicating if connection could be established
     */
    bool setup();

    /**
     * @brief method to disconnect zmq
     * check if already disconnected, if not disconnected try to disconnect, if already disconnected return
     * @return bool indicating if connection could be closed
     */
    bool close();

    /**
     * @brief returns the status of the InputGate
     * @return True, if ready to receive tuples. False otherwise.
     */
    bool isReady();
    std::string getHost() const;
    uint16_t getPort() const;

  private:
    std::string host;
    uint16_t port;
    bool connected;
    zmq::context_t context;
    zmq::socket_t socket;
};
}  // namespace NES

#endif //NES_INCLUDE_NETWORK_INPUTGATE_HPP_
