#ifndef NES_NETWORKSINK_HPP
#define NES_NETWORKSINK_HPP

#include <Network/NesPartition.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>

#include <boost/thread/tss.hpp>
#include <string>

namespace NES {
class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

namespace Network {

class NetworkSink : public SinkMedium {
  public:
    /**
     * @brief constructor for the network sink
     * @param schema
     * @param networkManager
     * @param nodeLocation
     * @param nesPartition
     */
    explicit NetworkSink(SchemaPtr schema, NetworkManagerPtr networkManager, const NodeLocation nodeLocation,
                         NesPartition nesPartition, BufferManagerPtr bufferManager, std::chrono::seconds waitTime = std::chrono::seconds(2), uint8_t retryTimes = 5);

    ~NetworkSink();

    bool writeData(TupleBuffer& inputBuffer) override;

    const std::string toString() const override;

    void setup() override;
    void shutdown() override;

    std::string toString() override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType();

  private:
    boost::thread_specific_ptr<OutputChannel> outputChannel;

    NetworkManagerPtr networkManager;
    const NodeLocation nodeLocation;
    NesPartition nesPartition;

    const std::chrono::seconds waitTime;
    const uint8_t retryTimes;
};

}// namespace Network
}// namespace NES

#endif// NES_NETWORKSINK_HPP
