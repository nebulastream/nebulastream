#include <gtest/gtest.h>
//#ifdef ENABLE_OPC_BUILD
#include <string>
#include <cstring>
#include <thread>
#include <memory>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/BufferManager.hpp>

#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/plugin/log_stdout.h>

#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <Sources/SourceCreator.hpp>

#include <NodeEngine/NodeEngine.hpp>


const std::string url = "opc.tcp://localhost:53530/OPCUA/SimulationServer";

namespace NES {

class OPCSourceTest : public testing::Test {
  public:


    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES_DEBUG("Setup OPCSourceTest test class.");
    }

    void SetUp() {
        NES_DEBUG("Setup OPCSourceTest cases.");
        NES::setupLogging("OPCTest.log", NES::LOG_DEBUG);

        test_schema =
                Schema::create()
                        ->addField("nsId", DataTypeFactory::createFixedChar(16))
                        ->addField("nsIndex", UINT16)
                        ->addField("var", UINT64);

        tuple_size = test_schema->getSchemaSizeInBytes();
        buffer_size = num_tuples_to_process*tuple_size/num_of_buffers;

        bufferManager = std::make_shared<BufferManager>();
        queryManager = std::make_shared<QueryManager>();

        ASSERT_GT(buffer_size, 0);

    }

    /* Will be called after a test is executed. */
    void TearDown() {
        NES_DEBUG("Tear down OPCSourceTest");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("Tear down OPCSourceTest test class." );
    }

    BufferManagerPtr bufferManager;
    QueryManagerPtr queryManager;
    size_t tupleCnt;
    SchemaPtr test_schema;
    size_t tuple_size;
    size_t buffer_size;

  protected:
    UA_NodeId nodeId = UA_NODEID_STRING(3, "h1");
    const std::string& user = "";
    const std::string& password = "";


    const size_t num_of_buffers = 1;
    const uint64_t num_tuples_to_process = 1;

};

TEST_F(OPCSourceTest, OPCSourceInit) {

    auto opcSource = createOPCSource(test_schema, bufferManager, queryManager, url, &nodeId, user, password);

    SUCCEED();
}

TEST_F(OPCSourceTest, OPCSourcePrint) {

    auto opcSource = createOPCSource(test_schema, bufferManager, queryManager, url, &nodeId, user, password);

    std::string expected = "OPC_SOURCE(SCHEMA(nsId:Char nsIndex:INTEGER var:INTEGER ), URL= opc.tcp://localhost:53530/OPCUA/SimulationServer, NODE_INDEX= 3. ";

    EXPECT_EQ(opcSource->toString(), expected);

    std::cout << opcSource->toString() << std::endl;

    SUCCEED();
}


TEST_F(OPCSourceTest, OPCSourceValue) {

    auto opcSource = createOPCSource(test_schema, bufferManager, queryManager, url, &nodeId, user, password);

    std::string expected = "OPC_SOURCE(SCHEMA(nsId:Char nsIndex:INTEGER var:INTEGER ), URL= opc.tcp://localhost:53530/OPCUA/SimulationServer, NODE_INDEX= 3. ";

    auto tuple_buffer = opcSource->receiveData();

    //uint32_t *tuple = (uint32_t*) tuple_buffer->getBuffer();

    std::cout << opcSource->toString() << std::endl;

    SUCCEED();
}


}
//#endif
