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
#include <Sinks/SinkCreator.hpp>

#include <NodeEngine/NodeEngine.hpp>

/**
 * The tests require an OPC server running at opc.tcp://localhost:4840
 */


const std::string url = "opc.tcp://localhost:4840";

namespace NES {

class OPCSinkTest : public testing::Test {
  public:


    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES_DEBUG("OPCSINKTEST::SetUpTestCase()");
    }

    void SetUp() {
        NES_DEBUG("OPCSINKTEST::SetUp() OPCSinkTest cases set up.");
        NES::setupLogging("OPCSinkTest.log", NES::LOG_DEBUG);

        test_schema =
                Schema::create()
                        ->addField("var", UINT32);
        bufferManager = std::make_shared<BufferManager>();

        bufferManager->configure(sizeof(uint32_t),1);

        ASSERT_GT(buffer_size, 0);

    }

    /* Will be called after a test is executed. */
    void TearDown() {
        NES_DEBUG("OPCSINKTEST::TearDown() Tear down OPCSourceTest");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("OPCSINKTEST::TearDownTestCases() Tear down OPCSourceTest test class." );
    }

    BufferManagerPtr bufferManager;
    SchemaPtr test_schema;
    size_t buffer_size;

  protected:
    UA_NodeId nodeId = UA_NODEID_STRING(1, "the.answer");
    const std::string user = "";
    const std::string password = "";

};

/**
 * Tests basic set up of OPC sink
 */
TEST_F(OPCSinkTest, OPCSourceInit) {
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337);
    auto opcSink = createOPCSink(test_schema, 0, nodeEngine, url, &nodeId, user, password);

    SUCCEED();
}

/**
 * Test if schema, OPC server url, and node index are the same
 */
TEST_F(OPCSinkTest, OPCSourcePrint) {
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337);
    auto opcSink = createOPCSink(test_schema, 0, nodeEngine, url, &nodeId, user, password);

    std::string expected = "OPC_SOURCE(SCHEMA(var:INTEGER ), URL= opc.tcp://localhost:4840, NODE_INDEX= 1, NODE_IDENTIFIER= the.answer. ";

    EXPECT_EQ(opcSink->toString(), expected);

    std::cout << opcSink->toString() << std::endl;

    SUCCEED();
}

/**
 * Tests if obtained value is valid.
 * Requires an OPC test server without any security policy,
 * running at opc.tcp://localhost:4840
 * and a node with node id "ns=1;s="the.answer"
 */

TEST_F(OPCSinkTest, OPCSourceValue) {

    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337);
    auto opcSink = createOPCSink(test_schema, 0, nodeEngine, url, &nodeId, user, password);

    EXPECT_EQ(opcSink, true);

}

}
#endif
