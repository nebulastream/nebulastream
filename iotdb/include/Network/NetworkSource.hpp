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
    NetworkSource(SchemaPtr schema,
                  BufferManagerPtr bufferManager,
                  QueryManagerPtr queryManager,
                  NetworkManager& networkManager, const NodeLocation& nodeLocation,
                  NesPartition nesPartition);

    ~NetworkSource();

    /**
     * @brief blocking method to receive a buffer
     * @return TupleBufferPtr containing thre received buffer
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
    const NodeLocation& nodeLocation;
    NesPartition nesPartition;
};

}// namespace Network
}// namespace NES

#endif//NES_INCLUDE_NETWORK_NETWORKSOURCE_HPP_
