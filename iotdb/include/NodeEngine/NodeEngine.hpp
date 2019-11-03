#ifndef _IOT_NODE_H
#define _IOT_NODE_H

#include "../Runtime/CompiledDummyPlan.hpp"
#include "NodeProperties.hpp"
//#include "json.hpp"
#include <CodeGen/QueryExecutionPlan.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/ThreadPool.hpp>
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
	void start();
	void stop();

	JSON getNodePropertiesAsJSON();
	NodeProperties* getNodeProperties();

private:
  NodeProperties* props;
};

typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

} // namespace iotdb
#endif // _IOT_NODE_H
