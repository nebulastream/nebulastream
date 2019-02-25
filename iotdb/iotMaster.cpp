#include <Topology/FogTopologyManager.hpp>
#include <API/InputQuery.hpp>
#include <API/Config.hpp>
#include <API/Schema.hpp>
#include "include/API/InputQuery.hpp"
#include <Optimizer/FogOptimizer.hpp>
#include <Optimizer/FogRunTime.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Runtime/compiledTestPlan.hpp>

#include <sstream>

using namespace iotdb;


InputQueryPtr createTestQuery()
{
	 Config config = Config::create().
		                  withParallelism(1).
		                  withPreloading().
		                  withBufferSize(1000).
		                  withNumberOfPassesOverInput(1);

	  Schema schema = Schema::create().addField("",INT32);

	  /** \brief create a source using the following functions:
	  * const DataSourcePtr createTestSource();
	  * const DataSourcePtr createBinaryFileSource(const Schema& schema, const std::string& path_to_file);
	  * const DataSourcePtr createRemoteTCPSource(const Schema& schema, const std::string& server_ip, int port);
  */
	DataSourcePtr source = createTestSource();

	InputQueryPtr ptr = std::make_shared<InputQuery>(InputQuery::create(config, source)
		      .filter(PredicatePtr())
		      .printInputQueryPlan());

	return ptr;
}

CompiledTestQueryExecutionPlanPtr createQEP()
{
	CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
    return qep;
}
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

int main(int argc, const char *argv[]) {
	FogTopologyManager& fMgnr = FogTopologyManager::getInstance();
	createTestTopo(fMgnr);

	InputQueryPtr query = createTestQuery();

	//skipping LogicalPlanManager

	FogOptimizer& fogOpt = FogOptimizer::getInstance();
	FogExecutionPlanPtr execPlan = fogOpt.map(query, fMgnr.getTopologyPlan());
	fogOpt.optimize(execPlan);//TODO: does nothing atm

	FogRunTime& runtime = FogRunTime::getInstance();
	NodeEnginePtr nodePtr = createTestNode();
	runtime.registerNode(nodePtr);

	//TODO: will be replaced with FogExecutionPlan
	CompiledTestQueryExecutionPlanPtr qep = createQEP();
	runtime.deployQuery(qep);

}
