#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_

#include <memory>

#include <Topology/FogTopologyPlan.hpp>
#include "Util/CPUCapacity.hpp"

/**
 * TODO: add return of create
 */
namespace iotdb {
typedef std::shared_ptr<FogTopologyPlan> FogTopologyPlanPtr;

class FogTopologyManager {
  public:
    static FogTopologyManager& getInstance()
    {
        static FogTopologyManager instance; // Guaranteed to be destroyed.
                                            // Instantiated on first use.
        return instance;
    }
    FogTopologyManager(FogTopologyManager const&); // Don't Implement
    void operator=(FogTopologyManager const&);     // Don't implement

    FogTopologyWorkerNodePtr createFogWorkerNode(CPUCapacity cpuCapacity) { return currentPlan->createFogWorkerNode(cpuCapacity); }

    bool removeFogWorkerNode(FogTopologyWorkerNodePtr ptr) { return currentPlan->removeFogWorkerNode(ptr); }

    bool removeFogSensorNode(FogTopologySensorNodePtr ptr) { return currentPlan->removeFogSensorNode(ptr); }

    FogTopologySensorNodePtr createFogSensorNode(CPUCapacity cpuCapacity) { return currentPlan->createFogSensorNode(cpuCapacity); }

    FogTopologyLinkPtr createFogTopologyLink(FogTopologyEntryPtr pSourceNode, FogTopologyEntryPtr pDestNode)
    {
        return currentPlan->createFogTopologyLink(pSourceNode, pDestNode);
    }

    bool removeFogTopologyLink(FogTopologyLinkPtr linkPtr) { return currentPlan->removeFogTopologyLink(linkPtr); }

    void printTopologyPlan() { std::cout << getTopologyPlanString() << std::endl; }

    std::string getTopologyPlanString() { return currentPlan->getTopologyPlanString(); }

    FogTopologyPlanPtr getTopologyPlan() { return currentPlan; }

    FogTopologyEntryPtr getRootNode() { return currentPlan->getRootNode(); };

    void resetFogTopologyPlan()
    {
        currentPlan.reset(new FogTopologyPlan());
        currentLinkID = 1;
    }

  private:
    FogTopologyManager() { currentPlan = std::make_shared<FogTopologyPlan>(); }

    FogTopologyPlanPtr currentPlan;
};
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_ */
