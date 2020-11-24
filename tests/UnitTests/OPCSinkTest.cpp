/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <gtest/gtest.h>
#ifdef ENABLE_OPC_BUILD
#include <API/Schema.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cstring>
#include <open62541/plugin/pki_default.h>
#include <open62541/server.h>
#include <open62541/server_config_default.h>
#include <string>
#include <thread>

#include <NodeEngine/NodeEngine.hpp>
#include <future>

const std::string& url = "opc.tcp://localhost:4840";
static const UA_NodeId baseDataVariableType = {0, UA_NODEIDTYPE_NUMERIC, {UA_NS0ID_BASEDATAVARIABLETYPE}};
static volatile UA_Boolean running = true;
static UA_Server* server = UA_Server_new();

namespace NES {

class OPCSinkTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES_DEBUG("OPCSINKTEST::SetUpTestCase()"); }

    void SetUp() {
        NES_DEBUG("OPCSINKTEST::SetUp() OPCSinkTest cases set up.");
        NES::setupLogging("OPCSinkTest.log", NES::LOG_DEBUG);
        test_schema = Schema::create()->addField("var", UINT32);
    }

    /* Will be called after a test is executed. */
    void TearDown() { NES_DEBUG("OPCSINKTEST::TearDown() Tear down OPCSourceTest"); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("OPCSINKTEST::TearDownTestCases() Tear down OPCSourceTest test class."); }

    static void addVariable(UA_Server* server) {
        /* Define the attribute of the myInteger variable node */
        UA_VariableAttributes attr = UA_VariableAttributes_default;
        UA_Int32 myInteger = 42;
        UA_Variant_setScalar(&attr.value, &myInteger, &UA_TYPES[UA_TYPES_INT32]);
        attr.description = UA_LOCALIZEDTEXT("en-US", "the answer");
        attr.displayName = UA_LOCALIZEDTEXT("en-US", "the answer");
        attr.dataType = UA_TYPES[UA_TYPES_INT32].typeId;
        attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;

        /* Add the variable node to the information model */
        UA_NodeId myIntegerNodeId = UA_NODEID_STRING(1, "the.answer");
        UA_QualifiedName myIntegerName = UA_QUALIFIEDNAME(1, "the answer");
        UA_NodeId parentNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER);
        UA_NodeId parentReferenceNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES);
        UA_Server_addVariableNode(server, myIntegerNodeId, parentNodeId, parentReferenceNodeId, myIntegerName,
                                  UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE), attr, NULL, NULL);
    }

    static void writeVariable(UA_Server* server) {
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

    static void startServer(std::promise<bool>& p) {
        UA_ServerConfig_setDefault(UA_Server_getConfig(server));
        addVariable(server);
        writeVariable(server);
        p.set_value(true);
        UA_StatusCode retval = UA_Server_run(server, &running);
        std::cout << " retval is=" << retval << std::endl;
    }

    static void stopServer() {
        running = false;
        UA_Server_delete(server);
    }

    SchemaPtr test_schema;
    uint64_t buffer_size;

  protected:
    UA_NodeId nodeId = UA_NODEID_STRING(1, "the.answer");
    const std::string user = "";
    const std::string password = "";
};

/**
 * Tests basic set up of OPC sink
 */
TEST_F(OPCSinkTest, OPCSourceInit) {

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, conf);
    auto opcSink = createOPCSink(test_schema, 0, nodeEngine, url, nodeId, user, password);
    SUCCEED();
}

/**
 * Test if schema, OPC server url, and node index are the same
 */
TEST_F(OPCSinkTest, OPCSourcePrint) {

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, conf);
    auto opcSink = createOPCSink(test_schema, 0, nodeEngine, url, nodeId, user, password);
    std::string expected = "OPC_SINK";
    EXPECT_EQ(opcSink->toString(), expected);
    std::cout << opcSink->toString() << std::endl;
    SUCCEED();
}

/**
 * Tests if obtained value is valid.
 */
TEST_F(OPCSinkTest, OPCSourceValue) {

    std::promise<bool> p;
    std::thread t1([&p]() {
        startServer(p);
    });
    t1.detach();

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, conf);
    auto test_schema = Schema::create()->addField("var", UINT32);
    WorkerContext wctx(NesThread::getId());
    TupleBuffer write_buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    write_buffer.getBuffer<uint32_t>()[0] = 45;
    write_buffer.setNumberOfTuples(1);
    auto opcSink = createOPCSink(test_schema, 0, nodeEngine, url, nodeId, user, password);
    NES_DEBUG("OPCSINKTEST::TEST_F(OPCSinkTest, OPCSinkValue) buffer before write: "
              << UtilityFunctions::prettyPrintTupleBuffer(write_buffer, test_schema));
    opcSink->writeData(write_buffer, wctx);
    NES_DEBUG("OPCSINKTEST::TEST_F(OPCSinkTest, OPCSinkValue) data was written");
    write_buffer.release();
    nodeEngine->stop();
    auto nodeEngine1 = NodeEngine::create("127.0.0.1", 31337, conf);
    auto opcSource = createOPCSource(test_schema, nodeEngine1->getBufferManager(), nodeEngine1->getQueryManager(), url, nodeId,
                                     user, password, 1);
    auto tuple_buffer = opcSource->receiveData();
    stopServer();
    uint64_t value = 0;
    auto* tuple = (uint32_t*) tuple_buffer->getBuffer();
    NES_DEBUG("OPCSINKTEST::TEST_F(OPCSinkTest, OPCSinkValue) Received value is: " << *(uint32_t*) tuple_buffer->getBuffer());
    value = *tuple;
    uint64_t expected = 45;
    NES_DEBUG("OPCSINKTEST::TEST_F(OPCSinkTest, OPCSinkValue) expected value is: " << expected
                                                                                   << ". Received value is: " << value);
    EXPECT_EQ(value, expected);
}
}// namespace NES
#endif
