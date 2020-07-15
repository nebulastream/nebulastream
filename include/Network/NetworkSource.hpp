#ifndef NES_INCLUDE_NETWORK_NETWORKSOURCE_HPP_
#define NES_INCLUDE_NETWORK_NETWORKSOURCE_HPP_

#include <Network/NesPartition.hpp>
#include <Network/NetworkManager.hpp>
#include <Sources/DataSource.hpp>

namespace NES {
namespace Network {

/**
 * @brief this class provide a zmq as data source
 */
class NetworkSource : public DataSource {

  public:
    NetworkSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                  NetworkManagerPtr networkManager, NesPartition nesPartition);

    ~NetworkSource();

    /**
     * @brief this method is just dummy and is replaced by the ZmqServer in the NetworkStack. Do not use!
     * @return TupleBufferPtr containing the received buffer
     */
    std::optional<TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method
     * @return returns string describing the network source
     */
    const std::string toString() const override;

    /**
     * @brief Get source type
     */
    SourceType getType() const override;

    /**
     * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
     * It registers the source on the NetworkManager
     * @return true if registration on the network stack is successful
     */
    bool start();

    /**
     * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
     * It de-registers the source on the NetworkManager
     * @return true if deregistration on the network stack is successful
     */
    bool stop();

    /**
     * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
     * @param bufferManager
     * @param queryManager
     */
    void runningRoutine(BufferManagerPtr, QueryManagerPtr);

  private:
    NetworkManagerPtr networkManager;
    NesPartition nesPartition;
};

}// namespace Network
}// namespace NES

#endif//NES_INCLUDE_NETWORK_NETWORKSOURCE_HPP_
