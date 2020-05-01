#ifndef NES_IMPL_OPTIMIZER_IMPL_HIGHAVAILABILITY_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_HIGHAVAILABILITY_HPP_

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>

namespace NES {

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

/**
 * @brief This Class is responsible for placing operators on the nodes such that there exists R number of redundant
 * paths between the operator and the source node.
 */
class HighAvailabilityStrategy : public BasePlacementStrategy {

  public:
    ~HighAvailabilityStrategy() = default;
    NESExecutionPlanPtr initializeExecutionPlan(QueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

    static std::unique_ptr<HighAvailabilityStrategy> create() {
        return std::make_unique<HighAvailabilityStrategy>(HighAvailabilityStrategy());
    }

  private:
    HighAvailabilityStrategy() = default;

    /**
     * This method is responsible for placing the operators to the nes nodes and generating ExecutionNodes.
     * @param nesExecutionPlanPtr : graph containing the information about the execution nodes.
     * @param nesTopologyGraphPtr : nes Topology graph used for extracting information about the nes topology.
     * @param sourceNodePtr : sensor nodes which can act as source.
     *
     * @throws exception if the operator can't be placed anywhere.
     */
    void placeOperators(NESExecutionPlanPtr nesExecutionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        LogicalOperatorNodePtr sourceOperator, std::vector<NESTopologyEntryPtr> sourceNodes);

    /**
     * @brief Add forward operators between source and sink nodes.
     * @param rootNode : sink node
     * @param nesExecutionPlanPtr : nes execution plan
     */
    void addForwardOperators(std::vector<NESTopologyEntryPtr> pathForPlacement,
                             NESExecutionPlanPtr nesExecutionPlanPtr) const;

    /**
     * @brief Finds all the nodes that can be used for performing FWD operator
     * @param sourceNodes
     * @param rootNode
     * @return
     */
    std::vector<NESTopologyEntryPtr> getCandidateNodesForFwdOperatorPlacement(const std::vector<NESTopologyEntryPtr>& sourceNodes,
                                                                              const NESTopologyEntryPtr rootNode) const;
};

}// namespace NES
#endif//NES_IMPL_OPTIMIZER_IMPL_HIGHAVAILABILITY_HPP_
