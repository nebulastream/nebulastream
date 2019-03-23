#ifndef _IOT_NODE_H
#define _IOT_NODE_H

#include <iostream>
#include <string>
#include <zmq.hpp>
#include <vector>
#include <pthread.h>
#include <unistd.h>
#include "json.hpp"
#include "NodeProperties.hpp"
#include <CodeGen/QueryExecutionPlan.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/ThreadPool.hpp>
#include "../Runtime/CompiledDummyPlan.hpp"

namespace iotdb{
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

	JSON getNodeProperties();

private:
  NodeProperties* props;
};

typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

}
#endif // _IOT_NODE_H
