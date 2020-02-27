#ifndef NES_INCLUDE_NETWORK_INPUTGATE_HPP_
#define NES_INCLUDE_NETWORK_INPUTGATE_HPP_

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include <SourceSink/DataSource.hpp>
#include <NodeEngine/TupleBuffer.hpp>

namespace NES {

/**
 * @brief this class provide a zmq as data source
 */
class InputGate {

  public:
    /**
     * @brief constructor for the zmq source
     * @param schema of the input buffer
     * @param host name of the source queue
     * @param port of the source queue
     */
    InputGate(const Schema& schema, const std::string& host, const uint16_t port);

    /**
     * @brief destructor of zmq sink that disconnects the queue before deconstruction
     * @note if queue cannot be disconnected, an assertion is raised
     */
    ~InputGate();

    /**
     * @brief blocking method to receive a buffer from the zmq source
     * @return TupleBufferPtr containing thre received buffer
     */
    TupleBufferPtr receiveData();

  private:
    /**
     * @brief default constructor required for boost serialization
     */
    InputGate();

    /**
     * @brief method to connect zmq using the host and port specified before
     * check if already connected, if not connect try to connect, if already connected return
     * @return bool indicating if connection could be established
     */
    bool connect();

    /**
     * @brief method to disconnect zmq
     * check if already disconnected, if not disconnected try to disconnect, if already disconnected return
     * @return bool indicating if connection could be established
     */
    bool disconnect();

    /**
     * @brief method for serialization, all listed variable below are added to the
     * serialization/deserialization process
     */
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar,
                   const unsigned int version) {
        ar & boost::serialization::base_object<DataSource>(*this);
        ar & host;
        ar & port;
    }
    std::string host;
    uint16_t port;
    bool connected;
    zmq::context_t context;
    zmq::socket_t socket;

};
}  // namespace NES

#endif //NES_INCLUDE_NETWORK_INPUTGATE_HPP_
