#include <AutomaticDataGenerator.h>
#include <CSVDataGenerator.h>
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
#include <memory>
#include <string>

using namespace std::literals::chrono_literals;

class TcpServer {
  public:
    TcpServer(boost::asio::io_service& ioService, Options option)
        : acceptor(ioService, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), option.port)) {
        bufferManager = std::make_shared<NES::Runtime::BufferManager>();
        bufferManager->createFixedSizeBufferPool(128);
        generator = AutomaticDataGenerator::create(option.schema);
        generator->setBufferManager(bufferManager);
        format = std::make_unique<NES::CsvFormat>(option.schema, bufferManager);
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
                startSend(newConnection);
                startAccept();// Accept the next connection
            } else {
                NES_ERROR("Error Accepting connection: {}", error.message());
            }
        });
    }

    void startSend(std::shared_ptr<boost::asio::ip::tcp::socket> socket) {
        auto buffer = generator->createData(1, 8192);
        std::string randomTuple = format->getFormattedBuffer(buffer[0]);
        boost::asio::async_write(*socket,
                                 boost::asio::buffer(randomTuple),
                                 [this, socket](const boost::system::error_code& error, std::size_t /*bytes_transferred*/) {
                                     if (!error) {
                                         std::this_thread::sleep_for(std::chrono::seconds(10));
                                         startSend(socket);// Continue sending random tuples
                                     } else {
                                         std::cerr << "Error sending data: " << error.message() << std::endl;
                                         socket->close();
                                     }
                                 });
    }

    NES::Runtime::BufferManagerPtr bufferManager;
    std::unique_ptr<AutomaticDataGenerator> generator;
    std::unique_ptr<NES::CsvFormat> format;
    boost::asio::ip::tcp::acceptor acceptor;
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
    auto buffer_manager = std::make_shared<BufferManager>();
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

int runTcpSource(Options options) {
    using namespace NES::Runtime;
    boost::asio::io_service ioService;
    TcpServer server(ioService, options);
    ioService.run();
    return 0;
}

int main(int argc, char* argv[]) {
    NES::Logger::setupLogging("unikernel_source.log", NES::LogLevel::LOG_INFO);
    auto optionsResult = Options::fromCLI(argc, argv);
    if (optionsResult.has_error()) {
        NES_FATAL_ERROR("Failed to Parse Configuration: {}", optionsResult.error());
        return 1;
    }
    auto options = optionsResult.assume_value();

    if (options.type == NetworkSource) {
        NES_INFO("Running Network Source");
        return runNetworkSource(options);
    } else if (options.type == TcpSource) {
        NES_INFO("Running TCP Source");
        return runTcpSource(options);
    }
}