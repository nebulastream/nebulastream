/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <API/Schema.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include "Optimizer/QueryRewrite/DuplicateOperatorEliminationRule.hpp"
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>

namespace NES::Optimizer {

    DuplicateOperatorEliminationRulePtr DuplicateOperatorEliminationRule::create() {
        return std::make_shared<DuplicateOperatorEliminationRule>(DuplicateOperatorEliminationRule());
    }

    DuplicateOperatorEliminationRule::DuplicateOperatorEliminationRule() = default;

    // Function to determine if two filter nodes are duplicates.
    bool DuplicateOperatorEliminationRule::areFiltersDuplicates(const FilterLogicalOperatorNodePtr& filter1, const FilterLogicalOperatorNodePtr& filter2) {
        return filter1->equal(filter2); // equality check for filters
    }

    // Function to determine if two projection nodes are duplicates.
    bool DuplicateOperatorEliminationRule::areProjectionsDuplicates(const ProjectionLogicalOperatorNodePtr& projection1, const ProjectionLogicalOperatorNodePtr& projection2) {
        return projection1->equal(projection2); // equality check for projections
    }

    // Generic method to eliminate duplicate nodes.
    template<typename Container, typename CompareFunc>
    void DuplicateOperatorEliminationRule::eliminateDuplicateNodes(Container& operators, CompareFunc areDuplicates) {
        NES_DEBUG("iterating over all operators to eliminate duplicates..")
        for (auto it = operators.begin(); it != operators.end(); ++it) {
            auto nodeToCompare = *it;
            for (auto it2 = std::next(it); it2 != operators.end(); ++it2) {
                auto nodeComparison = *it2;
                if (areDuplicates(nodeToCompare, nodeComparison)) {
                    NES_DEBUG("found duplicate node {} removing from query plan...", nodeComparison->toString())
                    nodeComparison->removeAndJoinParentAndChildren();
                    nodeToCompare->setInputSchema(nodeToCompare->getChildren()[0]-> template as<OperatorNode>()->getOutputSchema()->copy());
                    nodeToCompare-> template as<OperatorNode>()->setOutputSchema(
                        nodeToCompare->getChildren()[0]-> template as<OperatorNode>()->getOutputSchema()->copy());
                    break; // Assuming we only need to remove one duplicate.
                }
            }
        }
    }

    QueryPlanPtr DuplicateOperatorEliminationRule::apply(QueryPlanPtr queryPlan) {
        const std::vector<OperatorNodePtr> rootOperators = queryPlan->getRootOperators();

        std::set<FilterLogicalOperatorNodePtr> filterOperators;
        std::set<ProjectionLogicalOperatorNodePtr> projectionOperators;

        // Get all filter and projection operators.
        for(const OperatorNodePtr& rootOperator : rootOperators) {
            std::vector<FilterLogicalOperatorNodePtr> filters = rootOperator->getNodesByType<FilterLogicalOperatorNode>();
            filterOperators.insert(filters.begin(), filters.end());

            std::vector<ProjectionLogicalOperatorNodePtr> projections = rootOperator->getNodesByType<ProjectionLogicalOperatorNode>();
            projectionOperators.insert(projections.begin(), projections.end());
        }

        // Group projections by source.
        std::map<uint64_t, std::vector<ProjectionLogicalOperatorNodePtr>> projectionMap;
        for (const auto& projection :  projectionOperators) {
            std::vector<SourceLogicalOperatorNodePtr> srcDescriptors = projection->getNodesByType<SourceLogicalOperatorNode>();
            for (const auto& srcLogicalOperatorNode :  srcDescriptors) {
                projectionMap[srcLogicalOperatorNode->getId()].push_back(projection);
            }
        }

        // Eliminate duplicate projection nodes for every branch separately.
        for (const auto& pair : projectionMap) {
            const std::vector<ProjectionLogicalOperatorNodePtr>& projections = pair.second;
            DuplicateOperatorEliminationRule::eliminateDuplicateNodes(projections,
                                                                      DuplicateOperatorEliminationRule::areProjectionsDuplicates);
        }

        DuplicateOperatorEliminationRule::eliminateDuplicateNodes(filterOperators,
                                                                  DuplicateOperatorEliminationRule::areFiltersDuplicates);
        return queryPlan;
    }

}
