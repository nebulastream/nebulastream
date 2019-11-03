#ifndef NODE_ENGINE_H
#define NODE_ENGINE_H

//#include "../Runtime/CompiledDummyPlan.hpp"
#include "NodeProperties.hpp"
//#include "json.hpp"
#include <CodeGen/QueryExecutionPlan.hpp>
#include <API/Config.hpp>
#include <NodeEngine/ThreadPool.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <iostream>
#include <pthread.h>
#include <string>
#include <unistd.h>
#include <vector>
#include <zmq.hpp>


namespace iotdb {
using JSON = nlohmann::json;

class NodeEngine {
public:
	NodeEngine()
	{
		props = new NodeProperties();
	}

	void init();
	void deployQuery(QueryExecutionPlanPtr ptr);
	void undeployQuery(QueryExecutionPlanPtr qep);

	void applyConfig(Config& conf);
	void start();
	void stop();

	JSON getNodePropertiesAsJSON();
	NodeProperties* getNodeProperties();

private:
  NodeProperties* props;
  std::vector<QueryExecutionPlanPtr> qeps;

};

typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

} // namespace iotdb
#endif // NODE_ENGINE_H
