
#include <benchmark/benchmark.h>
#include <cstring>
#include <set>
#include <API/Query.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyManager.hpp>

using namespace NES;

static void BM_StringCreation(benchmark::State& state) {
    char* src = new char[state.range(0)];
    char* dst = new char[state.range(0)];
    memset(src, 'x', state.range(0));
    for (auto _ : state) {
        memcpy(dst, src, state.range(0));
    }
    state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
    delete[] src;
    delete[] dst;
}

static void BM_SetInsert(benchmark::State& state) {
    std::set<int> data;
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i)
            for (int j = 0; j < state.range(1); ++j)
                data.insert(std::rand());
    }
}

static void BM_QueryTest(benchmark::State& state){
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();

    NESTopologySensorNodePtr sensorNode = topologyManager->createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf,
                                                                     sensorNode);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = plan->getSourceOperators();
}

void setupSensorNodeAndStreamCatalog(TopologyManagerPtr topologyManager, StreamCatalogPtr streamCatalog) {
    NES_INFO("Setup FilterPushDownTest test case.");
    NESTopologySensorNodePtr sensorNode = topologyManager->createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, sensorNode);
    streamCatalog->addPhysicalStream("default_logical", sce);
}
static void BM_FilterPushDown(benchmark::State& state){
    //NES::setupLogging("FilterPushDownTest.log", NES::LOG_DEBUG);
    for (auto _ : state){
        TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
        StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
        setupSensorNodeAndStreamCatalog(topologyManager, streamCatalog);

        // Prepare
        SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
        Query query = Query::from("default_logical")
            .map(Attribute("value") = 40)
            .filter(Attribute("id") < 45)
            .sink(printSinkDescriptor);
        const QueryPlanPtr queryPlan = query.getQueryPlan();

        DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
        auto itr = queryPlanNodeIterator.begin();

        const NodePtr sinkOperator = (*itr);
        ++itr;
        const NodePtr filterOperator = (*itr);
        ++itr;
        const NodePtr mapOperator = (*itr);
        ++itr;
        const NodePtr srcOperator = (*itr);

        // Execute
        FilterPushDownRulePtr filterPushDownRule = FilterPushDownRule::create();
        const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);

        // Validate
        DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
        itr = queryPlanNodeIterator.begin();
    }
}



// Register the function as a benchmark
BENCHMARK(BM_SetInsert)->Ranges({{1024, 8192}, {128, 1024}, {12, 24}})->Unit(benchmark::kMillisecond);
BENCHMARK(BM_StringCreation)->DenseRange(8, 26, 3);
BENCHMARK(BM_FilterPushDown)->Repetitions(20);//->ReportAggregatesOnly(true);
BENCHMARK(BM_QueryTest)->Args();

BENCHMARK_MAIN();