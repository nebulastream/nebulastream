#include <gtest/gtest.h>
#include <Util/TestUtils.hpp>
#include <SourceSink/PrintSink.hpp>
#include <Services/CoordinatorService.hpp>
#include <SourceSink/ZmqSource.hpp>

#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Util/Logger.hpp>


using namespace NES;

class CoordinatorServiceTest : public testing::Test {
  public:
    std::string queryString =
        "InputQuery::from(default_logical).filter(default_logical[\"value\"] < 42).print(std::cout); ";

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup NES Coordinator test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        std::cout << "Setup NES Coordinator test case." << std::endl;
        NES::setupLogging("CoordinatorServiceTest.log", NES::LOG_DEBUG);

        NESTopologyManager::getInstance().resetNESTopologyPlan();
        const auto& kCoordinatorNode = NESTopologyManager::getInstance()
            .createNESWorkerNode(0, "127.0.0.1", CPUCapacity::HIGH);
        kCoordinatorNode->setPublishPort(4711);
        kCoordinatorNode->setReceivePort(4815);

        coordinatorServicePtr = CoordinatorService::getInstance();
        coordinatorServicePtr->clearQueryCatalogs();
        for (int i = 1; i <= 5; i++) {
            //FIXME: add node properties
            PhysicalStreamConfig streamConf;
            std::string address = ip + ":" + std::to_string(publish_port);
            auto entry = coordinatorServicePtr->registerNode(i, address, 2, /**nodeProperties**/
                                                             "", streamConf, NESNodeType::Sensor);
        }
        NES_DEBUG("FINISHED ADDING 5 Nodes to topology");
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Setup NES Coordinator test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down NES Coordinator test class." << std::endl;
    }

    CoordinatorServicePtr coordinatorServicePtr;
    std::string ip = "127.0.0.1";
    uint16_t receive_port = 0;
    std::string host = "localhost";
    uint16_t publish_port = 4711;
    //std::string sensor_type = "default";
};
/* Test serialization for Schema  */
TEST_F(CoordinatorServiceTest, test_registration_and_topology) {
    string topo = coordinatorServicePtr->getTopologyPlanString();
    string expectedNodes = "raph G {\n"
                          "0[label=\"0 type=Worker\"];\n"
                          "1[label=\"1 type=Sensor(default_physical)\"];\n"
                          "2[label=\"2 type=Sensor(default_physical)\"];\n"
                          "3[label=\"3 type=Sensor(default_physical)\"];\n"
                          "4[label=\"4 type=Sensor(default_physical)\"];\n"
                          "5[label=\"5 type=Sensor(default_physical)\"];\n";
    EXPECT_NE(topo.find(expectedNodes), 0);
}

TEST_F(CoordinatorServiceTest, test_node_properties) {
    //FIXME:provide own properties
    string nodeProps =
        "{\"cpus\":[{\"guest\":0,\"guest_nice\":0,\"idle\":1777049,\"iowait\":3074,\"irq\":0,\"name\":\"cpu\",\"nice\":186,\"softirq\":1276,\"steal\":0,\"system\":15308,\"user\":122112},{\"guest\":0,\"guest_nice\":0,\"idle\":226210,\"iowait\":284,\"irq\":0,\"name\":\"cpu0\",\"nice\":11,\"softirq\":392,\"steal\":0,\"system\":1588,\"user\":11438},{\"guest\":0,\"guest_nice\":0,\"idle\":220835,\"iowait\":307,\"irq\":0,\"name\":\"cpu1\",\"nice\":15,\"softirq\":255,\"steal\":0,\"system\":1902,\"user\":16182},{\"guest\":0,\"guest_nice\":0,\"idle\":220454,\"iowait\":324,\"irq\":0,\"name\":\"cpu2\",\"nice\":14,\"softirq\":126,\"steal\":0,\"system\":1974,\"user\":17030},{\"guest\":0,\"guest_nice\":0,\"idle\":221382,\"iowait\":332,\"irq\":0,\"name\":\"cpu3\",\"nice\":21,\"softirq\":130,\"steal\":0,\"system\":2240,\"user\":15955},{\"guest\":0,\"guest_nice\":0,\"idle\":218898,\"iowait\":564,\"irq\":0,\"name\":\"cpu4\",\"nice\":7,\"softirq\":88,\"steal\":0,\"system\":1957,\"user\":18461},{\"guest\":0,\"guest_nice\":0,\"idle\":222420,\"iowait\":442,\"irq\":0,\"name\":\"cpu5\",\"nice\":103,\"softirq\":90,\"steal\":0,\"system\":1931,\"user\":15007},{\"guest\":0,\"guest_nice\":0,\"idle\":223872,\"iowait\":402,\"irq\":0,\"name\":\"cpu6\",\"nice\":6,\"softirq\":84,\"steal\":0,\"system\":1811,\"user\":13855},{\"guest\":0,\"guest_nice\":0,\"idle\":222973,\"iowait\":415,\"irq\":0,\"name\":\"cpu7\",\"nice\":7,\"softirq\":108,\"steal\":0,\"system\":1901,\"user\":14182}],\"fs\":{\"f_bavail\":8782124,\"f_bfree\":12246175,\"f_blocks\":68080498,\"f_bsize\":4096,\"f_frsize\":4096},\"mem\":{\"bufferram\":457990144,\"freehigh\":0,\"freeram\":6954029056,\"freeswap\":16893603840,\"loads_15min\":46432,\"loads_1min\":87456,\"loads_5min\":69536,\"mem_unit\":1,\"procs\":1512,\"sharedram\":820908032,\"totalhigh\":0,\"totalram\":16534970368,\"totalswap\":16893603840},\"nets\":[{\"hostname\":\"\",\"port\":\"\"},{\"enp0s31f6\":{\"rx_bytes\":0,\"rx_dropped\":0,\"rx_packets\":0,\"tx_bytes\":0,\"tx_dropped\":0,\"tx_packets\":0},\"wlp3s0\":{\"host\":\"192.168.178.37\",\"host6\":\"fe80::52ca:ec35:fc8d:2954%wlp3s0\"}},{\"vmnet1\":{\"host\":\"172.16.158.1\",\"host6\":\"fe80::250:56ff:fec0:1%vmnet1\"},\"wlp3s0\":{\"rx_bytes\":28886252,\"rx_dropped\":0,\"rx_packets\":27532,\"tx_bytes\":3055394,\"tx_dropped\":0,\"tx_packets\":15798}},{\"vmnet1\":{\"rx_bytes\":0,\"rx_dropped\":0,\"rx_packets\":0,\"tx_bytes\":0,\"tx_dropped\":0,\"tx_packets\":95},\"vmnet8\":{\"host\":\"192.168.74.1\",\"host6\":\"fe80::250:56ff:fec0:8%vmnet8\"}},{\"virbr0\":{\"host\":\"192.168.122.1\"},\"vmnet8\":{\"rx_bytes\":0,\"rx_dropped\":0,\"rx_packets\":0,\"tx_bytes\":0,\"tx_dropped\":0,\"tx_packets\":95}},{\"docker0\":{\"host\":\"172.17.0.1\"},\"virbr0\":{\"rx_bytes\":0,\"rx_dropped\":0,\"rx_packets\":0,\"tx_bytes\":0,\"tx_dropped\":0,\"tx_packets\":0}},{\"docker0\":{\"rx_bytes\":0,\"rx_dropped\":0,\"rx_packets\":0,\"tx_bytes\":0,\"tx_dropped\":0,\"tx_packets\":0}}]}";
    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "default_test_node_props";
    std::string address = ip + ":" + std::to_string(publish_port);

    auto entry = coordinatorServicePtr->registerNode(6, address, 2,
                                                     nodeProps, streamConf, NESNodeType::Sensor);

    string expectedProperties = coordinatorServicePtr->getNodePropertiesAsString(
        entry);
    EXPECT_EQ(nodeProps, expectedProperties);

}

TEST_F(CoordinatorServiceTest, test_deregistration_and_topology) {
    //TODO:Test also the links
    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "default_delete_me";
    streamConf.logicalStreamName = "default_delete_me";

    StreamCatalog::instance().addLogicalStream(
        "default_delete_me", Schema::create());
    std::string address = ip + ":" + std::to_string(publish_port);

    auto entry = coordinatorServicePtr->registerNode(6, address, 2, "",
                                                     streamConf, NESNodeType::Sensor);

    EXPECT_NE(entry, nullptr);

    string expectedTopo1 = "raph G {\n"
                           "0[label=\"0 type=Worker\"];\n"
                           "1[label=\"1 type=Sensor(default_physical)\"];\n"
                           "2[label=\"2 type=Sensor(default_physical)\"];\n"
                           "3[label=\"3 type=Sensor(default_physical)\"];\n"
                           "4[label=\"4 type=Sensor(default_physical)\"];\n"
                           "5[label=\"5 type=Sensor(default_physical)\"];\n"
                           "6[label=\"6 type=Sensor(default_delete_me)\"];";
    EXPECT_NE(coordinatorServicePtr->getTopologyPlanString().find(expectedTopo1), 0);

    coordinatorServicePtr->deregisterSensor(entry);
    string expectedTopo2 = "raph G {\n"
                           "0[label=\"0 type=Worker\"];\n"
                           "1[label=\"1 type=Sensor(default_physical)\"];\n"
                           "2[label=\"2 type=Sensor(default_physical)\"];\n"
                           "3[label=\"3 type=Sensor(default_physical)\"];\n"
                           "4[label=\"4 type=Sensor(default_physical)\"];\n"
                           "5[label=\"5 type=Sensor(default_physical)\"];\n";
    EXPECT_NE(coordinatorServicePtr->getTopologyPlanString().find(expectedTopo2), 0);
}

TEST_F(CoordinatorServiceTest, test_register_query) {
    string queryId = coordinatorServicePtr->registerQuery(queryString,
                                                          "BottomUp");
    const map<string, QueryCatalogEntryPtr> mq = coordinatorServicePtr
        ->getRegisteredQueries();
    cout << "size=" << mq.size() << endl;
    EXPECT_TRUE(mq.size() == 1);

    string expectedPlacement =
        "raph G {\n"
        "0[label=\"1 operatorName=SOURCE(OP-1)=>FILTER(OP-2)=>SINK(SYS) nodeName=1\"];\n"
        "1[label=\"0 operatorName=SOURCE(SYS)=>SINK(OP-3) nodeName=0\"];\n"
        "2[label=\"2 operatorName=SOURCE(OP-1)=>FILTER(OP-2)=>SINK(SYS) nodeName=2\"];\n"
        "3[label=\"3 operatorName=SOURCE(OP-1)=>FILTER(OP-2)=>SINK(SYS) nodeName=3\"];\n"
        "4[label=\"4 operatorName=SOURCE(OP-1)=>FILTER(OP-2)=>SINK(SYS) nodeName=4\"];\n"
        "5[label=\"5 operatorName=SOURCE(OP-1)=>FILTER(OP-2)=>SINK(SYS) nodeName=5\"];\n";
    const NESExecutionPlanPtr kExecutionPlan = coordinatorServicePtr
        ->getRegisteredQuery(queryId);

    EXPECT_NE(kExecutionPlan->getTopologyPlanString().find(expectedPlacement), 0);
}

TEST_F(CoordinatorServiceTest, test_register_deregister_query) {
    string queryId = coordinatorServicePtr->registerQuery(queryString,
                                                          "BottomUp");
    EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
    coordinatorServicePtr->deleteQuery(queryId);
    EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
}

TEST_F(CoordinatorServiceTest, test_make_deployment) {
    string queryId = coordinatorServicePtr->registerQuery(queryString,
                                                          "BottomUp");
    EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
    map<NESTopologyEntryPtr, ExecutableTransferObject> etos =
        coordinatorServicePtr->prepareExecutableTransferObject(queryId);
    EXPECT_TRUE(etos.size() == 6);

    for (auto& x : etos) {
        EXPECT_TRUE(x.first);
        string s_eto = SerializationTools::ser_eto(x.second);
        EXPECT_TRUE(!s_eto.empty());
        SerializationTools::parse_eto(s_eto);
    }

    EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
}

TEST_F(CoordinatorServiceTest, test_run_deregister_query) {
    string queryId = coordinatorServicePtr->registerQuery(queryString,
                                                          "BottomUp");
    EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
    map<NESTopologyEntryPtr, ExecutableTransferObject> etos =
        coordinatorServicePtr->prepareExecutableTransferObject(queryId);
    EXPECT_TRUE(etos.size() == 6);

    EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);

    coordinatorServicePtr->deleteQuery(queryId);
    EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
    EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().empty());
}

TEST_F(CoordinatorServiceTest, test_compile_deployment) {
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    string queryId = coordinatorServicePtr->registerQuery(queryString,
                                                          "BottomUp");
    EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
    map<NESTopologyEntryPtr, ExecutableTransferObject> etos =
        coordinatorServicePtr->prepareExecutableTransferObject(queryId);
    EXPECT_TRUE(etos.size() == 6);

    for (auto& x : etos) {
        ExecutableTransferObject eto = x.second;
        QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan(
            createDefaultQueryCompiler(nodeEngine->getQueryManager()));
        EXPECT_TRUE(qep);
    }
    EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
}

TEST_F(CoordinatorServiceTest, DISABLED_test_code_gen) {
    NodeEnginePtr engine = std::make_shared<NodeEngine>();
    engine->start();

    SchemaPtr schema = Schema::create()
        ->addField("id", BasicType::UINT32)
        ->addField("value", BasicType::UINT64);

    Stream def = Stream("default", schema);

    InputQuery& query = InputQuery::from(def).filter(def["value"] < 42).print(
        std::cout);

    auto queryCompiler = createDefaultQueryCompiler(engine->getQueryManager());
    QueryExecutionPlanPtr qep = queryCompiler->compile(query.getRoot());
    // Create new Source and Sink
    DataSourcePtr source = createDefaultDataSourceWithSchemaForOneBuffer(schema, engine->getBufferManager(),
                                                                         engine->getQueryManager());
    source->setNumBuffersToProcess(10);
    qep->addDataSource(source);
    qep->setQueryId("1");
    DataSinkPtr sink = createPrintSinkWithSchema(schema, std::cout);
    qep->addDataSink(sink);

    engine->deployQueryInNodeEngine(qep);
    bool success = TestUtils::checkCompleteOrTimeout(engine, "1", 1);
    EXPECT_TRUE(success);

    engine->undeployQuery("1");
    engine->stop(false);
}

//FIXME: this test times out
TEST_F(CoordinatorServiceTest, DISABLED_test_local_distributed_deployment) {
    auto engine = std::make_unique<NodeEngine>();
    engine->start();
    string queryId = coordinatorServicePtr->registerQuery(queryString,
                                                          "BottomUp");
    EXPECT_EQ(coordinatorServicePtr->getRegisteredQueries().size(), 1);
    map<NESTopologyEntryPtr, ExecutableTransferObject> etos =
        coordinatorServicePtr->prepareExecutableTransferObject(queryId);
    EXPECT_TRUE(etos.size() == 2);

    vector<QueryExecutionPlanPtr> qeps;
    for (auto& x : etos) {
        NESTopologyEntryPtr v = x.first;
        cout << "Deploying QEP for " << v->getEntryTypeString() << endl;
        ExecutableTransferObject eto = x.second;
        QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan(
            createDefaultQueryCompiler(engine->getQueryManager()));

        EXPECT_TRUE(qep);
        engine->deployQueryInNodeEngine(qep);
        qeps.push_back(qep);
    }
    EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
    EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().size() == 1);

    for (const QueryExecutionPlanPtr qep : qeps) {
        assert(0);//hAS TO BE FIXED once thest ist enabled
//        engine->undeployQuery(qep);
    }
    engine->stop(false);

    coordinatorServicePtr->deleteQuery(queryId);
    EXPECT_TRUE(
        coordinatorServicePtr->getRegisteredQueries().empty()
            && coordinatorServicePtr->getRunningQueries().empty());
}

//TODO: Fixme, remove Thread.Sleep
TEST_F(CoordinatorServiceTest, DISABLED_test_sequential_local_distributed_deployment) {
    auto engine = std::make_unique<NodeEngine>();
    engine->start();
    for (int i = 0; i < 15; i++) {
        string queryId = coordinatorServicePtr->registerQuery(queryString,
                                                              "BottomUp");
        EXPECT_EQ(coordinatorServicePtr->getRegisteredQueries().size(), 1);
        map<NESTopologyEntryPtr, ExecutableTransferObject> etos =
            coordinatorServicePtr->prepareExecutableTransferObject(queryId);
        EXPECT_TRUE(etos.size() == 2);

        vector<QueryExecutionPlanPtr> qeps;
        for (auto& x : etos) {
            NESTopologyEntryPtr v = x.first;
            cout << "Deploying QEP for " << v->getEntryTypeString() << endl;
            ExecutableTransferObject eto = x.second;
            QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan(
                createDefaultQueryCompiler(engine->getQueryManager()));
            EXPECT_TRUE(qep);
            engine->deployQueryInNodeEngine(qep);
            qeps.push_back(qep);
        }
        EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
        EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().size() == 1);

        for (const QueryExecutionPlanPtr qep : qeps) {
            assert(0);//hAS TO BE FIXED once thest ist enabled
            //        engine->undeployQuery(qep);
        }

        coordinatorServicePtr->deleteQuery(queryId);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    engine->stop(false);
    EXPECT_TRUE(
        coordinatorServicePtr->getRegisteredQueries().empty()
            && coordinatorServicePtr->getRunningQueries().empty());
}
