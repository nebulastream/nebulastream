#ifndef NES_NETWORKSINK_HPP
#define NES_NETWORKSINK_HPP

#include <Network/NesPartition.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Sinks/DataSink.hpp>

#include <boost/thread/tss.hpp>
#include <string>

namespace NES {
namespace Network {

class NetworkSink : public DataSink {
  public:
    /**
     * @brief constructor for the network sink
     * @param schema
     * @param networkManager
     * @param nodeLocation
     * @param nesPartition
     */
    explicit NetworkSink(SchemaPtr schema, NetworkManagerPtr networkManager, const NodeLocation nodeLocation,
                         NesPartition nesPartition, std::chrono::seconds waitTime = std::chrono::seconds(2), uint8_t retryTimes = 5);

    ~NetworkSink();

    bool writeData(TupleBuffer& inputBuffer) override;

    SinkType getType() const override;

    const std::string toString() const override;

    void setup() override;   // not needed
    void shutdown() override;// not needed
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
