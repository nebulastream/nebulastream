#include <AutomaticDataGenerator.h>
#include <CSVDataGenerator.h>
#include <DataGeneration/Nextmark/NEAuctionDataGenerator.hpp>
#include <DataGeneration/Nextmark/NEBitDataGenerator.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/PartitionManager.hpp>
#include <Options.h>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sources/DataSource.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <YAMLModel.h>
#include <boost/asio.hpp>
#include <execution>
#include <algorithm>
#include <memory>
#include <span>
#include <string>

using namespace std::literals::chrono_literals;
class APrioriDataGenerator {
    std::vector<std::vector<uint8_t>> data_chunks;
    NES::Runtime::BufferManagerPtr bufferManager;
    NES::Benchmark::DataGeneration::DataGeneratorPtr generatorImpl;
    NES::SinkFormatPtr format;

  public:
    APrioriDataGenerator(const Options& options) {
        bufferManager = std::make_shared<NES::Runtime::BufferManager>(options.bufferSize);
        bufferManager->createFixedSizeBufferPool(32);
        switch (options.generator) {
            case NEXMARK_BID: generatorImpl = std::make_unique<NES::Benchmark::DataGeneration::NEBitDataGenerator>(); break;
            case NEXMARK_AUCTION:
                generatorImpl = std::make_unique<NES::Benchmark::DataGeneration::NEAuctionDataGenerator>();
                break;
            case NEXMARK_PERSON: NES_NOT_IMPLEMENTED(); break;
            case MANUAL: generatorImpl = AutomaticDataGenerator::create(options.schema);
            default: NES_NOT_IMPLEMENTED();
        }

        generatorImpl->setBufferManager(bufferManager);

        switch (options.format) {
            case NES::FormatTypes::CSV_FORMAT:
                format = std::make_unique<NES::CsvFormat>(generatorImpl->getSchema(), bufferManager);
                break;
            case NES::FormatTypes::JSON_FORMAT:
                format = std::make_unique<NES::CsvFormat>(generatorImpl->getSchema(), bufferManager);
                break;
            default: NES_NOT_IMPLEMENTED();
        }
    }

    void generate(size_t numberOfBuffers) {
        data_chunks.resize(numberOfBuffers);
        std::for_each(std::execution::par_unseq, data_chunks.begin(), data_chunks.end(), [this](auto& v) {
            auto buffer = generatorImpl->createData(1, bufferManager->getBufferSize());
            auto tupleData = format->getFormattedBuffer(buffer[0]);
            std::copy(tupleData.begin(), tupleData.end(), std::back_inserter(v));
        });
        NES_DEBUG("Generated {} buffers", numberOfBuffers);
    }

    std::span<const uint8_t> get_chunk(size_t chunk_index) {
        if (chunk_index >= data_chunks.size())
            return {};
        return {data_chunks[chunk_index].data(), data_chunks[chunk_index].size()};
    }
};

class TcpServer {
  public:
    TcpServer(boost::asio::io_service& ioService, const Options& option, std::unique_ptr<APrioriDataGenerator> generator)
        : acceptor(ioService, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), option.port)), delay(option.delayInMS),
          dataGenerator(std::move(generator)) {
        startAccept();
    }

  private:
    void startAccept() {
        auto newConnection = std::make_shared<boost::asio::ip::tcp::socket>(acceptor.get_executor());
        acceptor.async_accept(*newConnection, [this, newConnection](const boost::system::error_code& error) {
            if (!error) {
                std::stringstream ss;
                ss << newConnection->remote_endpoint();
                NES_INFO("Accepted connection from {}", ss.str());
                startSend(newConnection, 0);
                startAccept();// Accept the next connection
            } else {
                NES_ERROR("Error Accepting connection: {}", error.message());
            }
        });
    }

    void startSend(std::shared_ptr<boost::asio::ip::tcp::socket> socket, size_t chunk_index) {
        const auto chunk = dataGenerator->get_chunk(chunk_index);
        if (chunk.empty()) {
            return;
        }
        boost::asio::async_write(
            *socket,
            boost::asio::buffer(chunk.data(), chunk.size()),
            [this, socket, chunk_index](const boost::system::error_code& error, std::size_t /*bytes_transferred*/) {
                if (!error) {
                    if (delay > 0ms) {
                        std::this_thread::sleep_for(delay);
                    }
                    startSend(socket, chunk_index + 1);// Continue sending random tuples
                } else {
                    std::cerr << "Error sending data: " << error.message() << std::endl;
                    socket->close();
                }
            });
    }

    boost::asio::ip::tcp::acceptor acceptor;
    std::chrono::milliseconds delay;
    std::unique_ptr<APrioriDataGenerator> dataGenerator;
};

class DummyExchangeProtocolListener : public NES::Network::ExchangeProtocolListener {
  public:
    ~DummyExchangeProtocolListener() override = default;

    void onDataBuffer(NES::Network::NesPartition, NES::Runtime::TupleBuffer&) override { NES_DEBUG("On Data Buffer"); }

    void onEndOfStream(NES::Network::Messages::EndOfStreamMessage) override { NES_DEBUG("On End Stream"); }

    void onServerError(NES::Network::Messages::ErrorMessage msg) override {
        NES_DEBUG("On Server Error: {}", msg.getErrorTypeAsString());
    }

    void onEvent(NES::Network::NesPartition, NES::Runtime::BaseEvent& event) override {
        NES_DEBUG("On Event: {}", magic_enum::enum_name(event.getEventType()));
    }

    void onChannelError(NES::Network::Messages::ErrorMessage msg) override {
        NES_DEBUG("On Channel Error: {}", msg.getErrorTypeAsString());
    }
};

const char* data = "Hello World!";

int runNetworkSource(Options options) {
    using namespace NES::Network;
    using namespace NES::Runtime;

    auto partition_manager = std::make_shared<PartitionManager>();
    auto exchange_listener = std::make_shared<DummyExchangeProtocolListener>();
    auto buffer_manager = std::make_shared<BufferManager>(8192);
    buffer_manager->createFixedSizeBufferPool(128);

    ExchangeProtocol exchange_protocol(partition_manager, exchange_listener);
    auto manager =
        NetworkManager::create(options.nodeId, options.hostIp, options.port, std::move(exchange_protocol), buffer_manager);

    NodeLocation upstream_location(options.downstreamId, options.downstreamIp, options.downstreamPort);
    NesPartition partition(options.queryId, options.operatorId, options.partitionId, options.subPartitionId);

    auto wc = std::make_shared<WorkerContext>(options.workerId, buffer_manager, 100);

    //NES::Benchmark::DataGeneration::NEBitDataGenerator generator;
    auto generator = AutomaticDataGenerator::create(options.schema);
    NES_ASSERT(generator, "CSV Generator Could not be created");
    generator->setBufferManager(buffer_manager);

    auto sink = std::make_shared<NetworkSink>(generator->getSchema(),
                                              0,
                                              options.queryId,
                                              options.subQueryId,
                                              upstream_location,
                                              partition,
                                              buffer_manager,
                                              wc,
                                              manager,
                                              1,
                                              200ms,
                                              20);

    sink->setup();
    for (int i = 0; i < 1000; ++i) {
        auto buffers = generator->createData(1, 8192);

        if (buffers[0].getNumberOfTuples() == 0) {
            NES_INFO("End of Source");
            break;
        }
        buffers[0].setSequenceNumber(i + 1);
        buffers[0].setOriginId(options.originId);
        sink->writeData(buffers[0], *wc);
        sleep(1);
    }

    sink->shutdown();
    wc->releaseNetworkChannel(options.operatorId, QueryTerminationType::Graceful);

    return 0;
}

int runTcpSource(const Options& options, std::unique_ptr<APrioriDataGenerator> generator) {
    using namespace NES::Runtime;
    boost::asio::io_service ioService;
    TcpServer server(ioService, options, std::move(generator));
    ioService.run();
    return 0;
}

int main(int argc, char* argv[]) {
    NES::Logger::setupLogging("unikernel_source.log", NES::LogLevel::LOG_DEBUG);
    auto optionsResult = Options::fromCLI(argc, argv);
    if (optionsResult.has_error()) {
        NES_FATAL_ERROR("Failed to Parse Configuration: {}", optionsResult.error());
        return 1;
    }
    auto options = optionsResult.assume_value();
    auto dataGenerator = std::make_unique<APrioriDataGenerator>(options);
    dataGenerator->generate(options.numberOfBuffers);

    if (options.type == NetworkSource) {
        NES_INFO("Running Network Source");
        return runNetworkSource(options);
    } else if (options.type == TcpSource) {
        NES_INFO("Running TCP Source");
        return runTcpSource(options, std::move(dataGenerator));
    }
}