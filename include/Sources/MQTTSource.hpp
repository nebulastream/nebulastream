//
// Created by eleicha on 01.02.21.
//

#ifndef NES_MQTTSOURCE_HPP
#define NES_MQTTSOURCE_HPP
#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Sources/DataSource.hpp>

namespace NES {
class TupleBuffer;
/**
 * @brief this class provide a zmq as data source
 */
class MQTTSource : public DataSource {

  public:
    /**
     * @brief constructor for the zmq source
     * @param schema of the input buffer
     * @param host name of the source queue
     * @param port of the source queue
     */
    explicit MQTTSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                       const std::string serverAddress, const std::string clientId, OperatorId operatorId);

    /**
     * @brief destructor of zmq sink that disconnects the queue before deconstruction
     * @note if queue cannot be disconnected, an assertion is raised
     */
    ~MQTTSource();

    /**
     * @brief blocking method to receive a buffer from the zmq source
     * @return TupleBufferPtr containing thre received buffer
     */
    std::optional<NodeEngine::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the zmq source
     * @return returns string describing the zmq source
     */
    const std::string toString() const override;

  private:
    /**
     * @brief default constructor required for boost serialization
     */
    MQTTSource();

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
    friend class DataSource;
    bool connected;
    std::string serverAddress;
    std::string clientId;
};

typedef std::shared_ptr<MQTTSource> MQTTSourcePtr;
}// namespace NES
#endif//NES_MQTTSOURCE_HPP
