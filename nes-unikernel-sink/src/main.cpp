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
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sinks/Mediums/PrintSink.hpp>
#include <Sinks/Mediums/StatisticsMedium.hpp>
#include <argumentum/argparse.h>
#include <boost/iostreams/stream.hpp>
#include <memory>
#include <string>

using namespace std::literals::chrono_literals;

class DummyExchangeProtocolListener : public NES::Network::ExchangeProtocolListener {
    NES::DataSinkPtr sink;
    std::atomic_bool running = false;

  public:
    explicit DummyExchangeProtocolListener(const NES::DataSinkPtr& sink) : sink(sink) {}

    ~DummyExchangeProtocolListener() override = default;

    void onDataBuffer(NES::Network::NesPartition, NES::Runtime::TupleBuffer&) override {
        if (!running.exchange(true)) {
            sink->setup();
        }
    }

    void onEndOfStream(NES::Network::Messages::EndOfStreamMessage) override {
        sink->shutdown();
        running.store(false);
    }

    void onServerError(NES::Network::Messages::ErrorMessage) override {}

    void onEvent(NES::Network::NesPartition, NES::Runtime::EventPtr) override { NES_INFO("Received EVENT"); }

    void onChannelError(NES::Network::Messages::ErrorMessage) override {}
};

int main(int argc, char* argv[]) {
    using namespace NES::Network;
    using namespace NES::Runtime;
    using namespace std::chrono_literals;
    NES::Logger::setupLogging("unikernel_sink.log", NES::LogLevel::LOG_INFO);
    auto optionsResult = Options::fromCLI(argc, argv);
    if (optionsResult.has_error()) {
        NES_FATAL_ERROR("Failed to Parse Configuration: {}", optionsResult.error());
        return 1;
    }
    auto options = optionsResult.assume_value();

    auto partition_manager = std::make_shared<PartitionManager>();
    auto buffer_manager = std::make_shared<BufferManager>(8192, std::max(options.upstreams.size() * 10, 1024ul));

    boost::iostreams::stream os(NES_LOG_OS(NES::LogLevel::LOG_DEBUG));

    NES::DataSinkPtr statisticSink;
    if (!options.print && !options.latency) {
        NES_INFO("Using Throughput Sink");
        statisticSink =
            std::make_shared<NES::StatisticsMedium>(std::make_shared<NES::CsvFormat>(options.outputSchema, buffer_manager),
                                                    options.upstreams.size(),
                                                    options.subQueryId,
                                                    options.queryId,
                                                    2s);
    } else if (options.latency) {
        NES_INFO("Latency sink");
        statisticSink = std::make_shared<NES::LatencySink>(std::make_shared<NES::CsvFormat>(options.outputSchema, buffer_manager),
                                                           options.upstreams.size(),
                                                           options.subQueryId,
                                                           options.queryId,
                                                           std::chrono::milliseconds(*options.latency));
    } else {
        NES_INFO("Using Printing Sink");
        statisticSink = std::make_shared<NES::PrintSink>(std::make_shared<NES::CsvFormat>(options.outputSchema, buffer_manager),
                                                         options.upstreams.size(),
                                                         options.subQueryId,
                                                         options.queryId,
                                                         os);
    }

    auto exchange_listener = std::make_shared<DummyExchangeProtocolListener>(statisticSink);
    ExchangeProtocol exchange_protocol(partition_manager, exchange_listener);
    auto manager =
        NetworkManager::create(options.nodeId, options.hostIp, options.port, std::move(exchange_protocol), buffer_manager);

    for (const auto& upstream : options.upstreams) {
        NodeLocation location(upstream.second.nodeId, upstream.second.ip, upstream.second.port);
        NesPartition partition(options.queryId, upstream.second.operatorId, upstream.second.partitionId, upstream.second.subpartitionId);

        auto wc = std::make_shared<WorkerContext>(options.queryId, buffer_manager, 1);
        std::vector pipelines{statisticSink};
        auto source = std::make_shared<NES::Network::NetworkSource>(options.outputSchema,
                                                                    buffer_manager,
                                                                    wc,
                                                                    manager,
                                                                    partition,
                                                                    location,
                                                                    10,
                                                                    200ms,
                                                                    20,
                                                                    pipelines);

        NES_ASSERT(source->bind(), "Bind Failed");
        source->start();
    }

    NES_INFO("Setup Done");

    sleep(200000);
}