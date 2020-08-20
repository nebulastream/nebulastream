#include <gtest/gtest.h>
//#ifdef ENABLE_OPC_BUILD
#include <string>
#include <cstring>
#include <thread>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/BufferManager.hpp>

#include <Util/Logger.hpp>
#include <Util/TimeMeasurement.hpp>
#include <Sources/OPCSource.hpp>
#include <Sources/SourceCreator.hpp>

const std::string url = "opc.tcp://eleicha-ThinkPad-T480:53530/OPCUA/SimulationServer";

namespace NES {

class OPCSourceTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;
    QueryManagerPtr queryManager;
    void SetUp() {
        NES::setupLogging("OPCTest.log", NES::LOG_DEBUG);

        schema = Schema::create()
            ->addField("nsId", 16)
            ->addField("ndIndex", 16)
            ->addField("value", 16)

        uint64_t tuple_size = schema->getSchemaSizeInBytes();
        buffer_size = num_tuples_to_process*tuple_size/num_of_buffers;

        bufferManager = std::make_shared<BufferManager>();
        queryManager = std::make_shared<QueryManager>();

        ASSERT_GT(buffer_size, 0);

        NES_DEBUG("Setup OPCTest");
    }
    void TearDown() {
        NES_DEBUG("Tear down OPCTest");
    }

  protected:
    const std::string url = std::string(url);
    UA_UInt16 nsIndex = 3;
    char *nsId = "h1";

    const size_t num_of_buffers = 5;
    const uint64_t num_tuples_to_process = 100;
    size_t buffer_size;
    SchemaPtr schema;

};

TEST_F(OPCTest, OPCSourceInit) {

    const DataSourcePtr opcSource = std::make_shared<OPCSource>(schema, bufferManager, queryManager, url, nsIndex, nsId);
    SUCCEED();
}

TEST_F(OPCTest, OPCSourceInit) {

    const DataSourcePtr opcSource = std::make_shared<OPCSource>(schema, bufferManager, queryManager, url, nsIndex, nsId, user, password);
    SUCCEED();
}


}
//#endif
