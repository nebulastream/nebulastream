#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_

#include <Topology/FogTopologyPlan.hpp>
#include <memory>

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

    FogTopologyWorkerNodePtr createFogWorkerNode() { return currentPlan->createFogWorkerNode(); }

    bool removeFogWorkerNode(FogTopologyWorkerNodePtr ptr) { return currentPlan->removeFogWorkerNode(ptr); }

    bool removeFogSensorNode(FogTopologySensorNodePtr ptr) { return currentPlan->removeFogSensorNode(ptr); }

    FogTopologySensorNodePtr createFogSensorNode() { return currentPlan->createFogSensorNode(); }

    FogTopologyLinkPtr createFogNodeLink(FogTopologyEntryPtr pSourceNode, FogTopologyEntryPtr pDestNode)
    {
        return currentPlan->createFogNodeLink(pSourceNode, pDestNode);
    }

    bool removeFogNodeLink(FogTopologyLinkPtr linkPtr) { return currentPlan->removeFogTopologyLink(linkPtr); }

    void printTopologyPlan() { std::cout << getTopologyPlanString() << std::endl; }

    std::string getTopologyPlanString() { return currentPlan->getTopologyPlanString(); }

    FogTopologyPlanPtr getTopologyPlan() { return currentPlan; }

    FogTopologyEntryPtr getRootNode() { return currentPlan->getRootNode(); };

  private:
    FogTopologyManager() { currentPlan = std::make_shared<FogTopologyPlan>(); }

    FogTopologyPlanPtr currentPlan;
};
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_ */
