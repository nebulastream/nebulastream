#include <gtest/gtest.h>
#include <Catalogs/PhysicalStreamConfig.hpp>
#ifdef ENABLE_OPC_BUILD
#include <string>
#include <cstring>
#include <iostream>

#include <open62541/server.h>
#include <open62541/server_config_default.h>
#include <open62541/plugin/pki_default.h>

#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <Sources/SourceCreator.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <thread>

const std::string& url = "opc.tcp://localhost:4840";
static const UA_NodeId baseDataVariableType = {0, UA_NODEIDTYPE_NUMERIC, {UA_NS0ID_BASEDATAVARIABLETYPE}};
static volatile UA_Boolean running = true;
static UA_Server *server = UA_Server_new();

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

        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
        auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, conf);

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

    static void
    addVariable(UA_Server *server) {
        /* Define the attribute of the myInteger variable node */
        UA_VariableAttributes attr = UA_VariableAttributes_default;
        UA_Int32 myInteger = 42;
        UA_Variant_setScalar(&attr.value, &myInteger, &UA_TYPES[UA_TYPES_INT32]);
        attr.description = UA_LOCALIZEDTEXT("en-US","the answer");
        attr.displayName = UA_LOCALIZEDTEXT("en-US","the answer");
        attr.dataType = UA_TYPES[UA_TYPES_INT32].typeId;
        attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;

        /* Add the variable node to the information model */
        UA_NodeId myIntegerNodeId = UA_NODEID_STRING(1, "the.answer");
        UA_QualifiedName myIntegerName = UA_QUALIFIEDNAME(1, "the answer");
        UA_NodeId parentNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER);
        UA_NodeId parentReferenceNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES);
        UA_Server_addVariableNode(server, myIntegerNodeId, parentNodeId,
                                  parentReferenceNodeId, myIntegerName,
                                  UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE), attr, NULL, NULL);
    }

    static void
    writeVariable(UA_Server *server) {
        UA_NodeId myIntegerNodeId = UA_NODEID_STRING(1, "the.answer");

        /* Write a different integer value */
        UA_Int32 myInteger = 43;
        UA_Variant myVar;
        UA_Variant_init(&myVar);
        UA_Variant_setScalar(&myVar, &myInteger, &UA_TYPES[UA_TYPES_INT32]);
        UA_Server_writeValue(server, myIntegerNodeId, myVar);

        /* Set the status code of the value to an error code. The function
         * UA_Server_write provides access to the raw service. The above
         * UA_Server_writeValue is syntactic sugar for writing a specific node
         * attribute with the write service. */
        UA_WriteValue wv;
        UA_WriteValue_init(&wv);
        wv.nodeId = myIntegerNodeId;
        wv.attributeId = UA_ATTRIBUTEID_VALUE;
        wv.value.status = UA_STATUSCODE_BADNOTCONNECTED;
        wv.value.hasStatus = true;
        UA_Server_write(server, &wv);

        /* Reset the variable to a good statuscode with a value */
        wv.value.hasStatus = false;
        wv.value.value = myVar;
        wv.value.hasValue = true;
        UA_Server_write(server, &wv);
    }

    static void startServer(){
        UA_ServerConfig_setDefault(UA_Server_getConfig(server));
        addVariable(server);
        writeVariable(server);
        UA_StatusCode retval = UA_Server_run(server, &running);
    }

    static void stopServer(){
        running = false;
        UA_Server_delete(server);
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

    auto opcSource = createOPCSource(test_schema, bufferManager, queryManager, url, nodeId, user, password);

    SUCCEED();
}

/**
 * Test if schema, OPC server url, and node index are the same
 */
TEST_F(OPCSourceTest, OPCSourcePrint) {

    auto opcSource = createOPCSource(test_schema, bufferManager, queryManager, url, nodeId, user, password);

    std::string expected = "OPC_SOURCE(SCHEMA(var:INTEGER ), URL= opc.tcp://localhost:4840, NODE_INDEX= 1, NODE_IDENTIFIER= the.answer. ";

    EXPECT_EQ(opcSource->toString(), expected);

    std::cout << opcSource->toString() << std::endl;

    SUCCEED();
}

/**
 * Tests if obtained value is valid.
 */

TEST_F(OPCSourceTest, OPCSourceValue) {
    std::thread t1(startServer);
    t1.detach();
    auto test_schema = Schema::create()
                ->addField("var", UINT32);
    auto opcSource = createOPCSource(test_schema, bufferManager, queryManager, url, nodeId, user, password);
    auto tuple_buffer = opcSource->receiveData();
    stopServer();
    size_t value = 0;
    auto *tuple = (uint32_t *) tuple_buffer->getBuffer();
    value = *tuple;
    size_t expected = 43;
    NES_DEBUG("OPCSOURCETEST::TEST_F(OPCSourceTest, OPCSourceValue) expected value is: " << expected << ". Received value is: " << value);
    EXPECT_EQ(value, expected);
}
}
#endif
