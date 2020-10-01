#include <gtest/gtest.h>
#ifdef ENABLE_OPC_BUILD
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

/**
 * The tests require an OPC server running at opc.tcp://localhost:4840 with a node storing a uint32_t
 * and the node id: ns=1;s=the.answer
 */


const std::string& url = "opc.tcp://localhost:4840";

namespace NES {

class OPCSourceTest : public testing::Test {
  public:


    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES_DEBUG("OPCSOURCETEST::SetUpTestCase()");
    }

    void SetUp() {
        NES_DEBUG("OPCSOURCETEST::SetUp() OPCSourceTest cases set up.");
        NES::setupLogging("OPCSourceTest.log", NES::LOG_DEBUG);

        test_schema =
                Schema::create()
                        ->addField("var", UINT32);
        
        auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, sizeof(uint32_t), 1);

        bufferManager = nodeEngine->getBufferManager();
        queryManager = nodeEngine->getQueryManager();

        buffer_size = bufferManager->getBufferSize();

        ASSERT_GT(buffer_size, 0);

    }

    /* Will be called after a test is executed. */
    void TearDown() {
        NES_DEBUG("OPCSOURCETEST::TearDown() Tear down OPCSourceTest");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("OPCSOURCETEST::TearDownTestCases() Tear down OPCSourceTest test class." );
    }

    BufferManagerPtr bufferManager;
    QueryManagerPtr queryManager;
    SchemaPtr test_schema;
    size_t buffer_size;

  protected:
    UA_NodeId nodeId = UA_NODEID_STRING(1, "the.answer");
    const std::string user = "";
    const std::string password = "";

};

/**
 * Tests basic set up of OPC source
 */
TEST_F(OPCSourceTest, OPCSourceInit) {

    auto opcSource = createOPCSource(test_schema, bufferManager, queryManager, url, &nodeId, user, password);

    SUCCEED();
}

/**
 * Test if schema, OPC server url, and node index are the same
 */
TEST_F(OPCSourceTest, OPCSourcePrint) {

    auto opcSource = createOPCSource(test_schema, bufferManager, queryManager, url, &nodeId, user, password);

    std::string expected = "OPC_SOURCE(SCHEMA(var:INTEGER ), URL= opc.tcp://localhost:4840, NODE_INDEX= 1, NODE_IDENTIFIER= the.answer. ";

    EXPECT_EQ(opcSource->toString(), expected);

    std::cout << opcSource->toString() << std::endl;

    SUCCEED();
}

/**
 * Tests if obtained value is valid.
 * Requires an OPC test server without any security policy,
 * running at opc.tcp://localhost:4840
 * and a node with node id "ns=1;s="the.answer"
 */
TEST_F(OPCSourceTest, OPCSourceValue) {

    auto test_schema = Schema::create()
                ->addField("var", UINT32);

    auto opcSource = createOPCSource(test_schema, bufferManager, queryManager, url, &nodeId, user, password);

    auto tuple_buffer = opcSource->receiveData();

    size_t value = 0;
    auto *tuple = (uint32_t *) tuple_buffer->getBuffer();

    value = *tuple;
    size_t expected = 43;

    NES_DEBUG("OPCSOURCETEST::TEST_F(OPCSourceTest, OPCSourceValue) expected value is: " << expected << ". Received value is: " << value);

    EXPECT_EQ(value, expected);

}


}
#endif
