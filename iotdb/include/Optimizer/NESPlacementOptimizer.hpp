/**\brief:
 *         This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 *
 */

#ifndef NESPLACEMENTOPTIMIZER_HPP
#define NESPLACEMENTOPTIMIZER_HPP

#include <iostream>
#include "../Topology/NESTopologyManager.hpp"
#include "../Topology/NESTopologyPlan.hpp"
#include <Catalogs/StreamCatalog.hpp>
#include "NESExecutionPlan.hpp"

namespace NES {
class NESPlacementOptimizer {
  private:

  public:
    NESPlacementOptimizer() {};

    /**
     * @brief Returns an execution graph based on the input query and nes topology.
     * @param inputQuery
     * @param nesTopologyPlan
     * @return
     */
    virtual NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan) = 0;

    void invalidateUnscheduledOperators(OperatorPtr rootOperator, vector<size_t>& childOperatorIds);

    /**
     * @brief This method will traverse through all the nodes of the graphs and remove any reference to the operator not
     * located on the traversed node.
     *
     * @param nesExecutionPlanPtr
     */
    void removeNonResidentOperators(NESExecutionPlanPtr nesExecutionPlanPtr);

    /**
     * @brief This method will add system generated zmq source and sinks for each execution node.
     * @note We use zmq for internal message transfer therefore the source and sink will be zmq based.
     * @note This method will not append zmq source or sink if the operator chain in an execution node already contains
     * user defined source or sink operator respectively.
     *
     * @param schema
     * @param nesExecutionPlanPtr
     */
    void addSystemGeneratedSourceSinkOperators(const Schema& schema, NESExecutionPlanPtr nesExecutionPlanPtr);

    /**
     * @brief Fill the execution nesExecutionPlanPtr with forward operators in nes topology.
     * @param nesExecutionPlanPtr
     * @param nesTopologyPtr
     */
    void completeExecutionGraphWithNESTopology(NESExecutionPlanPtr nesExecutionPlanPtr, NESTopologyPlanPtr nesTopologyPtr);

    /**
     * @brief Factory method returning different kind of optimizer.
     * @param optimizerName
     * @return instance of type BaseOptimizer
     */
    static std::shared_ptr<NESPlacementOptimizer> getOptimizer(std::string optimizerName);

    /**
     * @brief Get all candidate node from sink to the target source node.
     * @param nesGraphPtr
     * @param targetSource
     * @return deque containing nes nodes with top element being sink node and bottom most being the targetSource node.
     */
    deque<NESTopologyEntryPtr> getCandidateNESNodes(const NESTopologyGraphPtr nesGraphPtr,
                                                    const NESTopologyEntryPtr targetSource) const;

    void convertFwdOptr(const Schema& schema, ExecutionNodePtr executionNodePtr) const;

    /**
     * @brief Add forward operators between source and sink nodes.
     * @param sourceNodes : list of source nodes
     * @param nesTopologyGraphPtr : nes topology graph
     * @param nesExecutionPlanPtr : nes execution plan
     */
    void addForwardOperators(const deque<NESTopologyEntryPtr> sourceNodes, const NESTopologyGraphPtr nesTopologyGraphPtr,
                             NESExecutionPlanPtr nesExecutionPlanPtr) const;
};

}

#endif //NESPLACEMENTOPTIMIZER_HPP
