#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>

namespace NES {

LogicalSourceExpansionRule::LogicalSourceExpansionRule(StreamCatalogPtr streamCatalog) : streamCatalog(streamCatalog) {}

LogicalSourceExpansionRulePtr LogicalSourceExpansionRule::create(StreamCatalogPtr streamCatalog) {
    return std::make_shared<LogicalSourceExpansionRule>(LogicalSourceExpansionRule(streamCatalog));
}

QueryPlanPtr LogicalSourceExpansionRule::apply(QueryPlanPtr queryPlan) {
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    for (auto& sourceOperator : sourceOperators) {
        SourceDescriptorPtr sourceDescriptor = sourceOperator->getSourceDescriptor();
        std::vector<TopologyNodePtr> sourceLocations = streamCatalog->getSourceNodesForLogicalStream(sourceDescriptor->getStreamName());
        if (sourceLocations.size() > 1) {
            OperatorNodePtr operatorNode;
            std::vector<OperatorNodePtr> originalRootOperators;
            std::tie(operatorNode, originalRootOperators) = getLogicalGraphToDuplicate(sourceOperator);
            NES_INFO("LogicalSourceExpansionRule: operator " << operatorNode->toString());

            for (uint32_t i = 0; i < sourceLocations.size() - 1; i++) {
                OperatorNodePtr copyOfGraph = operatorNode->duplicate();
                std::vector<NodePtr> rootNodes = copyOfGraph->getAllRootNodes();

                std::vector<NodePtr> family = copyOfGraph->getAndFlattenAllAncestors();

                for (auto& originalRootOperator : originalRootOperators) {
                    auto found = std::find_if(family.begin(), family.end(), [&](NodePtr member) {
                      return member->as<OperatorNode>()->getId() == originalRootOperator->getId();
                    });
                    if (found != family.end()) {
                        for (auto& parent : originalRootOperator->getParents()) {
                            (*found)->as<OperatorNode>()->setId(UtilityFunctions::getNextOperatorId());
                            parent->addChild(*found);
                            family.erase(found);
                        }
                    }
                }

                for (auto& member : family) {
                    member->as<OperatorNode>()->setId(UtilityFunctions::getNextOperatorId());
                }
            }
        }
    }

    return queryPlan;
}

std::tuple<OperatorNodePtr, std::vector<OperatorNodePtr>> LogicalSourceExpansionRule::getLogicalGraphToDuplicate(OperatorNodePtr operatorNode) {
    if (operatorNode->isNAryOperator() || operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        return std::tuple<OperatorNodePtr, std::vector<OperatorNodePtr>>();
    }

    OperatorNodePtr copyOfOperator = operatorNode->copy();
    std::vector<OperatorNodePtr> originalRootOperator;

    for (auto& parent : operatorNode->getParents()) {
        OperatorNodePtr duplicatedParentOperator;
        std::vector<OperatorNodePtr> rootOperators;
        std::tie(duplicatedParentOperator, rootOperators) = getLogicalGraphToDuplicate(parent->as<OperatorNode>());
        if (duplicatedParentOperator) {
            copyOfOperator->addParent(duplicatedParentOperator);
            originalRootOperator.insert(originalRootOperator.end(), rootOperators.begin(), rootOperators.end());
        } else {
            originalRootOperator.push_back(operatorNode);
        }
    }
    return std::make_tuple(copyOfOperator, originalRootOperator);
}

}// namespace NES
