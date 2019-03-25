#include <API/Config.hpp>
#include <API/InputQuery.hpp>
#include <API/Schema.hpp>
#include <Topology/FogTopologyManager.hpp>

#include "include/API/InputQuery.hpp"
#include <NodeEngine/NodeEngine.hpp>
#include <Optimizer/FogOptimizer.hpp>
#include <Optimizer/FogRunTime.hpp>
#include <Runtime/CompiledDummyPlan.hpp>
#include <Util/Logger.hpp>

#include <memory>
#include <sstream>

using namespace iotdb;

void printWelcome()
{
    vector<string> logo;
    logo.push_back(R"( __         __)");
    logo.push_back(R"(/  \.-"""-./  \)");
    logo.push_back(R"(\   - DFKI -  /)");
    logo.push_back(R"( |   o   o   |)");
    logo.push_back(R"( \  .-'''-.  /)");
    logo.push_back(R"(  '-\__Y__/-')");
    logo.push_back(R"(     `---`)");

    std::cout << "Welcome to the Grizzly Streaming Compiler ʕ•ᴥ•ʔ" << endl;
    for (auto a : logo)
        cout << a << endl;
}
InputQueryPtr createTestQuery()
{
    Config config =
        Config::create().withParallelism(1).withPreloading().withBufferSize(1000).withNumberOfPassesOverInput(1);

    Schema schema = Schema::create().addField("", INT32);

    /** \brief create a source using the following functions:
     * const DataSourcePtr createTestSource();
     * const DataSourcePtr createBinaryFileSource(const Schema& schema, const std::string& path_to_file);
     * const DataSourcePtr createRemoteTCPSource(const Schema& schema, const std::string& server_ip, int port);
     */
    DataSourcePtr source = createTestSource();

    InputQueryPtr ptr =
        std::make_shared<InputQuery>(InputQuery::create(config, source).filter(PredicatePtr()).printInputQueryPlan());

    return ptr;
}

InputQueryPtr createYSBTestQuery()
{
    Config config =
        Config::create().withParallelism(1).withPreloading().withBufferSize(1000).withNumberOfPassesOverInput(1);

    //	  Schema schema = Schema::create().addField("",INT32);

    /** \brief create a source using the following functions:
     * const DataSourcePtr createTestSource();
     * const DataSourcePtr createBinaryFileSource(const Schema& schema, const std::string& path_to_file);
     * const DataSourcePtr createRemoteTCPSource(const Schema& schema, const std::string& server_ip, int port);
     */
    DataSourcePtr source = createYSBSource(100);

    Schema schema = Schema::create()
                        .addField("user_id", 16)
                        .addField("page_id", 16)
                        .addField("campaign_id", 16)
                        .addField("event_type", 16)
                        .addField("ad_type", 16)
                        .addField("current_ms", UINT64)
                        .addField("ip", INT32);

    //	.filter(Equal("event_type", "view"))                // filter by event type
    //	.window(TumblingProcessingTimeWindow(Counter(100))) // tumbling window of 100 elements
    //	.groupBy("campaign_id")                             // group by campaign id
    //	.aggregate(Count())                                 // count results per key and window
    //	.write("output.csv");
    InputQueryPtr ptr = std::make_shared<InputQuery>(InputQuery::create(config, source)
                                                         .filter(PredicatePtr())
                                                         //			  .window(WindowPtr())
                                                         .printInputQueryPlan());

    return ptr;
}

CompiledDummyPlanPtr createDummyQEP()
{
    CompiledDummyPlanPtr qep(new CompiledDummyPlan());
    return qep;
}

// CompiledTestQueryExecutionPlanPtr createQEP()
//{
//	CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
//	return qep;
//}
//
// CompiledYSBTestQueryExecutionPlanPtr createYSBQEP()
//{
//	CompiledYSBTestQueryExecutionPlanPtr qep(new CompiledYSBTestQueryExecutionPlan());
//    return qep;
//}

void createTestTopo(FogTopologyManager& fMgnr)
{
    FogTopologyWorkerNodePtr f1 = fMgnr.createFogWorkerNode();

    FogTopologySensorNodePtr s1 = fMgnr.createFogSensorNode();

    FogTopologyLinkPtr l1 = fMgnr.createFogNodeLink(s1, f1);

    fMgnr.printTopologyPlan();
}

NodeEnginePtr createTestNode()
{
    NodeEnginePtr node = std::make_shared<NodeEngine>(1);
    JSON props = node->getNodeProperties();
    node->printNodeProperties();
    return node;
}

int main(int argc, const char* argv[])
{
    log4cxx::Logger::getLogger("IOTDB")->setLevel(log4cxx::Level::getInfo());

    printWelcome();
    FogTopologyManager& fMgnr = FogTopologyManager::getInstance();
    createTestTopo(fMgnr);

    // normal test query
    InputQueryPtr query = createTestQuery();
    CompiledDummyPlanPtr qep = createDummyQEP();

    // YSB Query
    //	InputQueryPtr query = createYSBTestQuery();
    //	CompiledYSBTestQueryExecutionPlanPtr qep = createYSBQEP();
    //	qep->setDataSource(query->getSource());
    // skipping LogicalPlanManager

    FogOptimizer& fogOpt = FogOptimizer::getInstance();
    FogExecutionPlanPtr execPlan = fogOpt.map(query, fMgnr.getTopologyPlan());
    fogOpt.optimize(execPlan); // TODO: does nothing atm

    FogRunTime& runtime = FogRunTime::getInstance();
    NodeEnginePtr nodePtr = createTestNode();
    runtime.registerNode(nodePtr);

    runtime.deployQuery(qep);
}
