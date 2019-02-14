#ifndef INCLUDE_OPTIMIZER_FOGRUNTIME_HPP_
#define INCLUDE_OPTIMIZER_FOGRUNTIME_HPP_
#include <Optimizer/FogExecutionPlan.hpp>
#include <NodeEngine/NodeEngine.hpp>

#include <zmq.hpp>
#include <NodeEngine/json.hpp>
#include <map>

namespace iotdb{
using JSON = nlohmann::json;

class FogRunTime{
public:
	FogRunTime(){};
	void deployQuery(FogExecutionPlanPtr plan){};
	void registerNode(NodeEnginePtr ptr);
	void receiveNodeInfo();
	void printNodeInfo(JSON data);

private:
	void startFog();
	void stopFog();
	std::map<size_t, NodeEnginePtr> nodeInfos;

};

}

#endif /* INCLUDE_OPTIMIZER_FOGRUNTIME_HPP_ */
