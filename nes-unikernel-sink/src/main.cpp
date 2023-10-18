#include <API/Schema.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSource.hpp>
#include <Network/PartitionManager.hpp>
#include <Options.h>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Mediums/PrintSink.hpp>
#include <argumentum/argparse.h>
#include <memory>
#include <string>

using namespace std::literals::chrono_literals;

class DummyExchangeProtocolListener : public NES::Network::ExchangeProtocolListener {
  public:
    ~DummyExchangeProtocolListener() override = default;

    void onDataBuffer(NES::Network::NesPartition, NES::Runtime::TupleBuffer&) override {}

    void onEndOfStream(NES::Network::Messages::EndOfStreamMessage) override { exit(0); }

    void onServerError(NES::Network::Messages::ErrorMessage) override {}

    void onEvent(NES::Network::NesPartition, NES::Runtime::BaseEvent&) override {}

    void onChannelError(NES::Network::Messages::ErrorMessage) override {}
};

const char* data = "Hello World!";

int main(int argc, char* argv[]) {
    using namespace NES::Network;
    using namespace NES::Runtime;
    NES::Logger::setupLogging("unikernel_sink.log", NES::LogLevel::LOG_INFO);
    auto optionsResult = Options::fromCLI(argc, argv);
    if (optionsResult.has_error()) {
        NES_FATAL_ERROR("Failed to Parse Configuration: {}", optionsResult.error());
        return 1;
    }
    auto options = optionsResult.assume_value();

    auto partition_manager = std::make_shared<PartitionManager>();
    auto exchange_listener = std::make_shared<DummyExchangeProtocolListener>();
    auto buffer_manager = std::make_shared<BufferManager>();
    buffer_manager->createFixedSizeBufferPool(128);

    ExchangeProtocol exchange_protocol(partition_manager, exchange_listener);
    auto manager =
        NetworkManager::create(options.nodeId, options.hostIp, options.port, std::move(exchange_protocol), buffer_manager);

    NodeLocation location(options.upstreamId, options.upstreamIp, options.upstreamPort);
    NesPartition partition(options.queryId, options.operatorId, options.partitionId, options.subPartitionId);

    auto wc = std::make_shared<WorkerContext>(options.queryId, buffer_manager, 1);

    NES::DataSinkPtr print_sink =
        std::make_shared<NES::PrintSink>(std::make_shared<NES::CsvFormat>(options.outputSchema, buffer_manager),
                                         1,
                                         options.subQueryId,
                                         options.queryId);
    std::vector<NES::DataSinkPtr> pipelines{print_sink};
    auto source = std::make_shared<
        NetworkSource>(options.outputSchema, buffer_manager, wc, manager, partition, location, 1000, 200ms, 20, pipelines);

    NES_ASSERT(source->bind(), "Bind Failed");
    source->start();
    sleep(500);
    source->stop(NES::Runtime::QueryTerminationType::Graceful);
}