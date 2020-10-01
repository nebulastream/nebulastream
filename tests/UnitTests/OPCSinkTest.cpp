#include <gtest/gtest.h>
#ifdef ENABLE_OPC_BUILD
#include <string>
#include <cstring>
#include <NodeEngine/QueryManager.hpp>

#include <open62541/client_config_default.h>

#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/UtilityFunctions.hpp>

#include <NodeEngine/NodeEngine.hpp>

/**
 * The tests require an OPC server running at opc.tcp://localhost:4840
 */


const std::string& url = "opc.tcp://localhost:4840";

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

        nodeEngine = NodeEngine::create("127.0.0.1", 31337, sizeof(uint32_t), 1);
        test_schema =
                Schema::create()
                        ->addField("var", UINT32);
        bufferManager = nodeEngine->getBufferManager();
        queryManager = nodeEngine->getQueryManager();

        ASSERT_GT(bufferManager->getBufferSize(), 0);

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
    NodeEnginePtr nodeEngine;
    QueryManagerPtr queryManager;

  protected:
    UA_NodeId nodeId = UA_NODEID_STRING(1, "the.answer");
    const std::string user = "";
    const std::string password = "";

};

/**
 * Tests basic set up of OPC sink
 */
TEST_F(OPCSinkTest, OPCSourceInit) {
    auto opcSink = createOPCSink(test_schema, 0, nodeEngine, url, &nodeId, user, password);

    SUCCEED();
}

/**
 * Test if schema, OPC server url, and node index are the same
 */
TEST_F(OPCSinkTest, OPCSourcePrint) {
    auto opcSink = createOPCSink(test_schema, 0, nodeEngine, url, &nodeId, user, password);

    std::string expected = "OPC_SINK";

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

    auto test_schema = Schema::create()->addField("var", UINT32);
    WorkerContext wctx(NesThread::getId());
    TupleBuffer write_buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    write_buffer.getBuffer<uint32_t>()[0] = 45;
    write_buffer.setNumberOfTuples(1);
    auto opcSink = createOPCSink(test_schema, 0, nodeEngine, url, &nodeId, user, password);

    NES_DEBUG("OPCSINKTEST::TEST_F(OPCSinkTest, OPCSinkValue) buffer before write: " << UtilityFunctions::prettyPrintTupleBuffer(write_buffer, test_schema));

    opcSink->writeData(write_buffer, wctx);

    NES_DEBUG("OPCSINKTEST::TEST_F(OPCSinkTest, OPCSinkValue) data was written");

    write_buffer.release();

    nodeEngine->stop();
    NodeEnginePtr nodeEngine2 = NodeEngine::create("127.0.0.1", 31338, sizeof(uint32_t), 1);

    auto opcSource = createOPCSource(test_schema, nodeEngine2->getBufferManager(), queryManager, url, &nodeId, user, password);
    auto tuple_buffer = opcSource->receiveData();
    size_t value = 0;
    auto *tuple = (uint32_t *) tuple_buffer->getBuffer();
    NES_DEBUG("OPCSINKTEST::TEST_F(OPCSinkTest, OPCSinkValue) Received value is: " << *(uint32_t *) tuple_buffer->getBuffer());
    value = *tuple;
    size_t expected = 45;

    NES_DEBUG("OPCSINKTEST::TEST_F(OPCSinkTest, OPCSinkValue) expected value is: " << expected << ". Received value is: " << value);

    EXPECT_EQ(value, expected);

}

}
#endif
