#ifndef NES_INCLUDE_NETWORK_NETWORKSOURCE_HPP_
#define NES_INCLUDE_NETWORK_NETWORKSOURCE_HPP_

#include <Network/NetworkCommon.hpp>
#include <Network/NetworkManager.hpp>
#include <SourceSink/DataSource.hpp>

namespace NES {
namespace Network {

/**
 * @brief this class provide a zmq as data source
 */
class NetworkSource : public DataSource {

  public:
    NetworkSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                  NetworkManager& networkManager, NesPartition nesPartition);

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

  private:
    NetworkManager& networkManager;
    NesPartition nesPartition;
};

}// namespace Network
}// namespace NES

#endif//NES_INCLUDE_NETWORK_NETWORKSOURCE_HPP_
