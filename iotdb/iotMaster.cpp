#include <Topology/FogTopologyManager.hpp>
#include "include/API/InputQuery.hpp"
#include <Optimizer/FogOptimizer.hpp>
#include <Optimizer/FogRunTime.hpp>
using namespace iotdb;
/**
 *
 * Open Questions:
 * 		- support half-duplex links?
 *		- how to handle the ids among nodes?
 *		- how does a node knows to whom it is connected?
 *		- make a parrent class for fogentries?
 *
 *	Todo:
 *		- add remove columns/row in matrix
 *		- add node ids
 *		- add unit tests
 *		- make TopologyManager Singleton
 *		- add createFogNode/FogSensor
 *		- make it thread safe
 */

void createTestTopo(FogTopologyManager* fMgnr)
{
	FogTopologyWorkerNodePtr f1 = fMgnr->createFogWorkerNode();

	FogTopologySensorNodePtr s1 = fMgnr->createFogSensorNode();

	FogTopologyLinkPtr l1 = fMgnr->createFogNodeLink(s1, f1);

	fMgnr->printTopologyPlan();
}

InputQuery& createTestQuery()
{
	// define config
	Config config = Config::create().
			withParallelism(1).
			withPreloading().
			withBufferSize(1000).
			withNumberOfPassesOverInput(1);

	// define schema
	Schema schema = Schema::create()
	.addVarSizeField("user_id", APIDataType::Char, 16)
	.addVarSizeField("page_id", APIDataType::Char, 16)
	.addVarSizeField("campaign_id", APIDataType::Char, 16)
	.addVarSizeField("event_type", APIDataType::Char, 9)
	.addVarSizeField("ad_type", APIDataType::Char, 9)
	.addFixSizeField("current_ms", APIDataType::Long)
	.addFixSizeField("ip", APIDataType::Int);

	Source s1 = Source::create()
	.path("/home/zeuchste/git/streaming_code_generator/yahoo_data_generator/yahoo_test_data.bin")
	.inputType(InputType::BinaryFile)
	.sourceType(Rest);

	// streaming query
	InputQuery& query = InputQuery::create(config, schema, s1)
	.filter(Equal("event_type", "view"))                // filter by event type
	.window(TumblingProcessingTimeWindow(Counter(100))) // tumbling window of 100 elements
	.groupBy("campaign_id")                             // group by campaign id
	.aggregate(Count())                                 // count results per key and window
	.write("output.csv");                                // write results to file
//	.execute();

	return query;
}

int main(int argc, const char *argv[]) {
	FogTopologyManager* fMgnr = new FogTopologyManager();
	createTestTopo(fMgnr);

	InputQuery& query = createTestQuery();

	//skipping LogicalPlanManager

	FogOptimizer* fogOpt = new FogOptimizer();
	FogExecutionPlanPtr execPlan = fogOpt->map(query, fMgnr->getTopologyPlan());
	fogOpt->optimize(execPlan);//TODO: does nothing atm

	FogRunTime* runtime = new FogRunTime();
	runtime->deployQuery(execPlan);
}
