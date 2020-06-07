//#ifndef NES_IMPL_OPTIMIZER_IMPL_HIGHTHROUGHPUT_HPP_
//#define NES_IMPL_OPTIMIZER_IMPL_HIGHTHROUGHPUT_HPP_
//
//#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
//
//namespace NES {
//
///**
// * @brief This class is responsible for placing operators on high capacity links such that the overall query throughput
// * will increase.
// */
//class HighThroughputStrategy : public BasePlacementStrategy {
//
//  public:
//    ~HighThroughputStrategy() = default;
//    GlobalExecutionPlanPtr initializeExecutionPlan(QueryPtr inputQuery, StreamCatalogPtr streamCatalog);
//
//    static std::unique_ptr<HighThroughputStrategy> create(NESTopologyPlanPtr nesTopologyPlan) {
//        return std::make_unique<HighThroughputStrategy>(HighThroughputStrategy(nesTopologyPlan));
//    }
//
//  private:
//
//    explicit HighThroughputStrategy(NESTopologyPlanPtr nesTopologyPlan);
//
//    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
//                        LogicalOperatorNodePtr operatorPtr, std::vector<NESTopologyEntryPtr> sourceNodes);
//
//    /**
//     * @brief Finds all the nodes that can be used for performing FWD operator
//     * @param sourceNodes
//     * @param rootNode
//     * @return
//     */
//    std::vector<NESTopologyEntryPtr> getCandidateNodesForFwdOperatorPlacement(const std::vector<NESTopologyEntryPtr>& sourceNodes,
//                                                                              const NESTopologyEntryPtr rootNode) const;
//};
//
//}// namespace NES
//
//#endif//NES_IMPL_OPTIMIZER_IMPL_HIGHTHROUGHPUT_HPP_
