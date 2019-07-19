#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_

#include <memory>

#include <Topology/FogTopologyPlan.hpp>
#include <cpprest/json.h>
#include "Util/CPUCapacity.hpp"

/**
 * TODO: add return of create
 */
namespace iotdb {
    typedef std::shared_ptr<FogTopologyPlan> FogTopologyPlanPtr;

    using namespace web;

    class FogTopologyManager {
    public:
        static FogTopologyManager &getInstance() {
            static FogTopologyManager instance; // Guaranteed to be destroyed.
            // Instantiated on first use.
            return instance;
        }

        FogTopologyManager(FogTopologyManager const &); // Don't Implement
        void operator=(FogTopologyManager const &);     // Don't implement

        FogTopologyWorkerNodePtr createFogWorkerNode(CPUCapacity cpuCapacity) {
            return currentPlan->createFogWorkerNode(cpuCapacity);
        }

        bool removeFogWorkerNode(FogTopologyWorkerNodePtr ptr) { return currentPlan->removeFogWorkerNode(ptr); }

        bool removeFogSensorNode(FogTopologySensorNodePtr ptr) { return currentPlan->removeFogSensorNode(ptr); }

        FogTopologySensorNodePtr createFogSensorNode(CPUCapacity cpuCapacity) {
            return currentPlan->createFogSensorNode(cpuCapacity);
        }

        FogTopologyLinkPtr createFogTopologyLink(FogTopologyEntryPtr pSourceNode, FogTopologyEntryPtr pDestNode) {
            return currentPlan->createFogTopologyLink(pSourceNode, pDestNode);
        }

        bool removeFogTopologyLink(FogTopologyLinkPtr linkPtr) { return currentPlan->removeFogTopologyLink(linkPtr); }

        void printTopologyPlan() { std::cout << getTopologyPlanString() << std::endl; }

        std::string getTopologyPlanString() { return currentPlan->getTopologyPlanString(); }

        FogTopologyPlanPtr getTopologyPlan() { return currentPlan; }

        FogTopologyEntryPtr getRootNode() { return currentPlan->getRootNode(); };

        /**\brief:
         *          This is a temporary method used for simulating an example topology.
         *
         */
        void createExampleTopology() {

            resetFogTopologyPlan();

            const FogTopologyWorkerNodePtr &sinkNode = createFogWorkerNode(CPUCapacity::HIGH);
            const FogTopologyWorkerNodePtr &workerNode1 = createFogWorkerNode(CPUCapacity::MEDIUM);
            const FogTopologyWorkerNodePtr &workerNode2 = createFogWorkerNode(CPUCapacity::MEDIUM);
            const FogTopologySensorNodePtr &sensorNode1 = createFogSensorNode(CPUCapacity::HIGH);
            const FogTopologySensorNodePtr &sensorNode2 = createFogSensorNode(CPUCapacity::LOW);
            const FogTopologySensorNodePtr &sensorNode3 = createFogSensorNode(CPUCapacity::LOW);
            const FogTopologySensorNodePtr &sensorNode4 = createFogSensorNode(CPUCapacity::MEDIUM);

            createFogTopologyLink(workerNode1, sinkNode);
            createFogTopologyLink(workerNode2, sinkNode);
            createFogTopologyLink(sensorNode1, workerNode1);
            createFogTopologyLink(sensorNode2, workerNode1);
            createFogTopologyLink(sensorNode3, workerNode2);
            createFogTopologyLink(sensorNode4, workerNode2);
        }

        json::value getFogTopologyGraphAsTreeJson() {

            const FogTopologyEntryPtr &rootNode = getRootNode();

            auto topologyAsJson = json::value::object();

            const auto label = std::to_string(rootNode->getId()) + "-" + rootNode->getEntryTypeString();
            topologyAsJson["name"] = json::value::string(label);
            auto children = getChildrenNode(rootNode);
            if (!children.empty()) {
                topologyAsJson["children"] = json::value::array(children);
            }

            return topologyAsJson;
        }

        std::vector<json::value> getChildrenNode(FogTopologyEntryPtr fogParentNode) {

            const FogGraph &fogGraph = getTopologyPlan()->getFogGraph();
            const std::vector<Edge> &edgesToNode = fogGraph.getAllEdgesToNode(fogParentNode);

            std::vector<json::value> children = {};

            if (edgesToNode.empty()) {
                return {};
            }

            for (Edge edge: edgesToNode) {
                const FogTopologyEntryPtr &sourceNode = edge.ptr->getSourceNode();
                if(sourceNode){
                    auto child = json::value::object();
                    const auto label = std::to_string(sourceNode->getId()) + "-" + sourceNode->getEntryTypeString();
                    child["name"] = json::value::string(label);
                    const std::vector<json::value> &grandChildren = getChildrenNode(sourceNode);
                    if(!grandChildren.empty()) {
                        child["children"] = json::value::array(grandChildren);
                    }
                    children.push_back(child);
                }
            }
            return children;
        }

        void resetFogTopologyPlan() {
            currentPlan.reset(new FogTopologyPlan());
            linkID = 1;
        }

    private:
        FogTopologyManager() { currentPlan = std::make_shared<FogTopologyPlan>(); }

        FogTopologyPlanPtr currentPlan;
    };
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_ */
