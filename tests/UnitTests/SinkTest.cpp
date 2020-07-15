#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Sinks/BinarySink.hpp>
#include <Sinks/CSVSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>

#include <gtest/gtest.h>

using namespace std;

namespace NES {

const size_t buffersManaged = 1024;
const size_t bufferSize = 4*1024;

class SinkTest : public testing::Test {
  public:
    NodeEnginePtr nodeEngine;
    BufferManagerPtr bufferManager;
    size_t tupleCnt;
    SchemaPtr test_schema;
    size_t test_data_size;
    std::array<uint32_t, 8> test_data;
    bool write_result;
    std::string path_to_csv_file;
    std::string path_to_bin_file;

    static void SetUpTestCase() {
        NES::setupLogging("SinkTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SinkTest class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down SinkTest class." << std::endl;
    }

    /* Called before a single test. */
    void SetUp() override {
        std::cout << "Setup SinkTest test case." << std::endl;
        nodeEngine = std::make_shared<NodeEngine>();
        nodeEngine->createBufferManager(bufferSize, buffersManaged);
        bufferManager = nodeEngine->getBufferManager();
        test_schema = Schema::create()->addField("KEY", DataTypeFactory::createInt32())->addField("VALUE",
                                                                                                  DataTypeFactory::createUInt32());
        write_result = false;
        path_to_csv_file = "../tests/test_data/sink.csv";
        path_to_bin_file = "../tests/test_data/sink.bin";
    }

    /* Called after a single test. */
    void TearDown() override {
        std::cout << "Tear down SinkTest test case." << std::endl;
        std::remove(path_to_csv_file.c_str());
        std::remove(path_to_bin_file.c_str());
    }
};

TEST_F(SinkTest, testCSVSink) {
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    auto write_thread = std::thread([&]() {
      const DataSinkPtr csvSink = createCSVFileSinkWithSchemaOverwrite(test_schema, path_to_csv_file);
      auto buffer_manager = nodeEngine->getBufferManager();
      auto buffer = bufferManager->getBufferBlocking();
      for (size_t i = 0; i < 10; ++i) {
          for (size_t j = 0; j < bufferSize/sizeof(uint64_t); ++j) {
              buffer.getBuffer<uint64_t>()[j] = j;
          }
          buffer.setNumberOfTuples(bufferSize/sizeof(uint64_t));
      }
      write_result = csvSink->writeData(buffer);
    });
    write_thread.join();

    EXPECT_TRUE(write_result);
}

TEST_F(SinkTest, testBinSink) {
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    auto write_thread = std::thread([&]() {
      const DataSinkPtr binSink = createBinaryFileSinkWithSchema(test_schema, path_to_csv_file);
      auto buffer_manager = nodeEngine->getBufferManager();
      auto buffer = bufferManager->getBufferBlocking();
      for (size_t i = 0; i < 10; ++i) {
          for (size_t j = 0; j < bufferSize/sizeof(uint64_t); ++j) {
              buffer.getBuffer<uint64_t>()[j] = j;
          }
          buffer.setNumberOfTuples(bufferSize/sizeof(uint64_t));
      }
      write_result = binSink->writeData(buffer);
    });
    write_thread.join();

    EXPECT_TRUE(write_result);
}

}// namespace NES
