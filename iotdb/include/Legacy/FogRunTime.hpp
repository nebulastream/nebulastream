#ifndef INCLUDE_OPTIMIZER_FOGRUNTIME_HPP_
#define INCLUDE_OPTIMIZER_FOGRUNTIME_HPP_
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/json.hpp>
#include <map>
#include <zmq.hpp>

#include "../Optimizer/NESExecutionPlan.hpp"
#include "../SourceSink/CompiledDummyPlan.hpp"

namespace NES {
using JSON = nlohmann::json;

class FogRunTime {
  public:
    static FogRunTime& getInstance()
    {
        static FogRunTime instance; // Guaranteed to be destroyed.
                                    // Instantiated on first use.
        return instance;
    }
    FogRunTime(FogRunTime const&);     // Don't Implement
    void operator=(FogRunTime const&); // Don't implement

    void deployQuery(NESExecutionPlan fogPlan);
    void deployQuery(QueryExecutionPlanPtr cPlan);

    void registerNode(NodeEnginePtr ptr);
    void receiveNodeInfo();
    void printNodeInfo(JSON data);

  private:
    FogRunTime(){};

    void startFog();
    void stopFog();
    std::map<size_t, NodeEnginePtr> nodeInfos;
};

} // namespace NES

#endif /* INCLUDE_OPTIMIZER_FOGRUNTIME_HPP_ */
