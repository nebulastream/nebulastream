#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/NodeProperties.hpp>
#include <string>
//#include <tests/testPlans/compiledTestPlan.hpp>

using namespace std;
namespace iotdb{
//todo: better return ptr ?
JSON NodeEngine::getNodeProperties()
{
	props->readMemStats();
	props->readCpuStats();
	props->readNetworkStats();
	props->readFsStats();

	return props->load();
}


void NodeEngine::deployQuery(QueryExecutionPlanPtr qep)
{
	//TODO:add compile here
	Dispatcher::instance().registerQuery(qep);

	ThreadPool::instance().start(1);

    std::cout << "Waiting 2 seconds " << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
}

}
