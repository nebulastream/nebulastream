#include <AutomaticDataGenerator.h>
#include <CSVDataGenerator.h>
#include <DataGeneration/Nextmark/NEAuctionDataGenerator.hpp>
#include <DataGeneration/Nextmark/NEBitDataGenerator.hpp>
#include <FileDataGenerator.h>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/PartitionManager.hpp>
#include <Options.h>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sources/DataSource.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <YAMLModel.h>
#include <algorithm>
#include <boost/asio.hpp>
#include <execution>
#include <memory>
#include <span>
#include <string>

using namespace std::literals::chrono_literals;
class APrioriDataGenerator {
  public:
    virtual ~APrioriDataGenerator() = default;

  public:
    virtual void startGenerator(size_t numberOfBuffers) = 0;
    virtual void wait() = 0;
    virtual std::span<const char> get_chunk(size_t chunkIndex) = 0;
};

class DirectFileDataGenerator : public APrioriDataGenerator {
    boost::filesystem::path filename;
    std::vector<std::unique_ptr<char[]>> data_chunks;
    std::jthread generatorThread;
    std::atomic<bool> done = false;
    size_t file_size = 0;
    constexpr static size_t chunk_size = 8192 + 8;

  public:
    explicit DirectFileDataGenerator(boost::filesystem::path filename) : filename(std::move(filename)) {}

    void startGenerator(size_t /*numberOfBuffers*/) override {
        NES_INFO("Starting DirectFileDataGenerator ({})", filename.string());
        generatorThread = std::jthread([this]() {
            boost::filesystem::ifstream file(filename);
            NES_ASSERT(file.good(), "Could not open file");
            file_size = boost::filesystem::file_size(filename);
            auto chunk = std::make_unique<char[]>(file_size);
            file.read(chunk.get(), file_size);
            data_chunks.emplace_back(std::move(chunk));
            done.store(true);
            NES_INFO("Loaded {} bytes", file_size);
        });
    }

    std::span<const char> get_chunk(size_t chunk_index) override {
        if (chunk_index * chunk_size >= file_size)
            return {};
        return {data_chunks[0].get() + (chunk_index * chunk_size),
                data_chunks[0].get() + std::min((chunk_index + 1) * chunk_size, file_size)};
    }

    void wait() override {
        if (!done)
            done.wait(false);
    }
};

class NESAPrioriDataGenerator : public APrioriDataGenerator {
    std::vector<std::vector<char>> data_chunks;
    NES::Runtime::BufferManagerPtr bufferManager;
    NES::Benchmark::DataGeneration::DataGeneratorPtr generatorImpl;
    NES::SinkFormatPtr format;
    std::jthread generatorThread;
    std::atomic<bool> done = false;

    std::unique_ptr<NES::Benchmark::DataGeneration::DataGenerator> createGenerator(const Options& options) {
        switch (options.dataSource) {
            case DATA_FILE: return FileDataGenerator::create(options.getSchema(), options.path);
            case ADHOC_GENERATOR:
                switch (options.schemaType) {
                    case NEXMARK_BID: return std::make_unique<NES::Benchmark::DataGeneration::NEBitDataGenerator>();
                    case NEXMARK_PERSON: NES_NOT_IMPLEMENTED();
                    case NEXMARK_AUCTION: return std::make_unique<NES::Benchmark::DataGeneration::NEAuctionDataGenerator>();
                    case MANUAL: return AutomaticDataGenerator::create(options.getSchema());
                }
        }
        NES_NOT_IMPLEMENTED();
    }

  public:
    NESAPrioriDataGenerator(const Options& options) {
        bufferManager = std::make_shared<NES::Runtime::BufferManager>(options.bufferSize);
        bufferManager->createFixedSizeBufferPool(32);
        generatorImpl = createGenerator(options);
        generatorImpl->setBufferManager(bufferManager);

        switch (options.format) {
            case NES::FormatTypes::NES_FORMAT:
                format = std::make_unique<NES::NesFormat>(generatorImpl->getSchema(), bufferManager);
                break;
            case NES::FormatTypes::CSV_FORMAT:
                format = std::make_unique<NES::CsvFormat>(generatorImpl->getSchema(), bufferManager);
                break;
            case NES::FormatTypes::JSON_FORMAT:
                format = std::make_unique<NES::CsvFormat>(generatorImpl->getSchema(), bufferManager);
                break;
            default: NES_NOT_IMPLEMENTED();
        }
    }

    void startGenerator(size_t numberOfBuffers) override {
        NES_INFO("Starting {}", generatorImpl->toString());
        generatorThread = std::jthread([this, numberOfBuffers]() {
            auto start = std::chrono::high_resolution_clock::now();
            for (size_t i = 0; i < numberOfBuffers; i++) {
                std::vector<char> v;
                auto buffer = generatorImpl->createData(1, bufferManager->getBufferSize());
                if (buffer.empty())
                    break;

                auto tupleData = format->getFormattedBuffer(buffer[0]);
                if (dynamic_cast<NES::NesFormat*>(this->format.get())) {
                    auto bufferSize = buffer[0].getNumberOfTuples() * generatorImpl->getSchema()->getSchemaSizeInBytes();
                    std::copy(std::bit_cast<uint8_t*>(&bufferSize),
                              std::bit_cast<uint8_t*>(&bufferSize) + sizeof(bufferSize),
                              std::back_inserter(v));
                }
                std::copy(tupleData.begin(), tupleData.end(), std::back_inserter(v));
                data_chunks.emplace_back(std::move(v));
            }

            if (dynamic_cast<NES::NesFormat*>(this->format.get())) {
                boost::filesystem::ofstream output("data.bin");
                for (const auto& chunk : data_chunks) {
                    output.write(chunk.data(), chunk.size());
                }
            } else if (dynamic_cast<NES::CsvFormat*>(this->format.get())) {
                boost::filesystem::ofstream output("data.csv");
                for (const auto& chunk : data_chunks) {
                    output.write(chunk.data(), chunk.size());
                }
            }

            NES_INFO(
                "Generated {} buffers in {}ms",
                data_chunks.size(),
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start).count());
            done.store(true);
        });
    }

    std::span<const char> get_chunk(size_t chunk_index) override {
        if (chunk_index >= data_chunks.size())
            return {};
        return {data_chunks[chunk_index].data(), data_chunks[chunk_index].size()};
    }

    void wait() override {
        if (!done)
            done.wait(false);
    }
};

class TcpServer {
  public:
    TcpServer(boost::asio::io_service& ioService, const Options& option, std::unique_ptr<APrioriDataGenerator> generator)
        : acceptor(ioService,
                   boost::asio::ip::tcp::endpoint(boost::asio::ip::address_v4::from_string(option.hostIp), option.port)),
          delay(option.delayInMS), dataGenerator(std::move(generator)), print(option.print) {
        NES_INFO("TCP Server listening on {}:{}", option.hostIp, option.port)
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
                dataGenerator->wait();
                NES_INFO("Start sending");
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
            std::stringstream ss;
            ss << socket->remote_endpoint();
            NES_INFO("{} done", ss.str());
            return;
        }

        if (print) {
            NES_INFO("Chunk {}\n{}", chunk_index, std::string_view{chunk.data(), chunk.size()});
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
                    socket->close();
                }
            });
    }

    boost::asio::ip::tcp::acceptor acceptor;
    std::chrono::milliseconds delay;
    std::unique_ptr<APrioriDataGenerator> dataGenerator;
    bool print = false;
};

class DummyExchangeProtocolListener : public NES::Network::ExchangeProtocolListener {
  public:
    ~DummyExchangeProtocolListener() override = default;

    void onDataBuffer(NES::Network::NesPartition, NES::Runtime::TupleBuffer&) override { NES_DEBUG("On Data Buffer"); }

    void onEndOfStream(NES::Network::Messages::EndOfStreamMessage) override { NES_DEBUG("On End Stream"); }

    void onServerError(NES::Network::Messages::ErrorMessage msg) override {
        NES_DEBUG("On Server Error: {}", msg.getErrorTypeAsString());
    }

    void onEvent(NES::Network::NesPartition, NES::Runtime::EventPtr event) override {
        NES_DEBUG("On Event: {}", magic_enum::enum_name(event->getEventType()));
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

bool hasFormatMismatch(Options options) {
    if (options.path.extension() == ".csv" && options.format == NES::FormatTypes::CSV_FORMAT) {
        return false;
    }
    if (options.path.extension() == ".bin" && options.format == NES::FormatTypes::NES_FORMAT) {
        return false;
    }
    if (options.path.extension() == ".json" && options.format == NES::FormatTypes::JSON_FORMAT) {
        return false;
    }

    return true;
}

int main(int argc, char* argv[]) {
    NES::Logger::setupLogging("unikernel_source.log", NES::LogLevel::LOG_DEBUG);
    auto optionsResult = Options::fromCLI(argc, argv);
    if (optionsResult.has_error()) {
        NES_FATAL_ERROR("Failed to Parse Configuration: {}", optionsResult.error());
        return 1;
    }
    auto options = optionsResult.assume_value();
    std::unique_ptr<APrioriDataGenerator> dataGenerator;

    if (options.dataSource == DATA_FILE && !hasFormatMismatch(options)) {
        dataGenerator = std::make_unique<DirectFileDataGenerator>(options.path);
    } else {
        dataGenerator = std::make_unique<NESAPrioriDataGenerator>(options);
    }

    dataGenerator->startGenerator(options.numberOfBuffers);

    if (options.type == NetworkSource) {
        NES_INFO("Running Network Source");
        return runNetworkSource(options);
    } else if (options.type == TcpSource) {
        NES_INFO("Running TCP Source");
        return runTcpSource(options, std::move(dataGenerator));
    }
}