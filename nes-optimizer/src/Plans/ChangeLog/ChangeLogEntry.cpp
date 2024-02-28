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

#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Plans/ChangeLog/ChangeLogEntry.hpp>
#include <stack>
#include <utility>
#include <vector>

namespace NES::Optimizer::Experimental {

ChangeLogEntryPtr ChangeLogEntry::create(std::set<LogicalOperatorPtr> upstreamOperators,
                                         std::set<LogicalOperatorPtr> downstreamOperators) {
    return std::make_shared<ChangeLogEntry>(ChangeLogEntry(std::move(upstreamOperators), std::move(downstreamOperators)));
}

ChangeLogEntry::ChangeLogEntry(std::set<LogicalOperatorPtr> upstreamOperators,
                               std::set<LogicalOperatorPtr> downstreamOperators)
    : upstreamOperators(std::move(upstreamOperators)), downstreamOperators(std::move(downstreamOperators)),
      poSetOfSubQueryPlan(computePoSet()) {}

std::set<OperatorId> ChangeLogEntry::computePoSet() {

    if (upstreamOperators.empty() || downstreamOperators.empty()) {
        return {};
    }

    std::set<OperatorId> poSet;
    std::stack<NodePtr> operatorsToVisit;
    for (const auto& upstreamOperator : upstreamOperators) {

        operatorsToVisit.push(upstreamOperator);
        while (!operatorsToVisit.empty()) {

            auto visitingOperator = operatorsToVisit.top();
            operatorsToVisit.pop();
            // Insert the operator id to the poSet
            auto inserted = poSet.insert(visitingOperator->as<Operator>()->getId());

            //If insertion was not successful then skip the remainder of operation
            // NOTE: this can happen because this operator was already visited.
            if (!inserted.second) {
                // Skip rest of the operation
                continue;
            }

            // Check if the visiting operator is also one of the downstream operators
            if (downstreamOperators.find(visitingOperator->as<LogicalOperator>()) != downstreamOperators.end()) {
                // Skip rest of the operation
                continue;
            }

            auto downStreamOperatorsToVisit = visitingOperator->getParents();

            //If there are more than 1 downstream operators then
            if (downStreamOperatorsToVisit.size() > 1) {
                // Check all downstream operators if they need to be visited or not
                for (const auto& downStreamOperatorToVisit : downStreamOperatorsToVisit) {
                    bool visit = false;
                    // If the operator to visit is one of the input downstream operators then add the operator to visit list
                    if (downstreamOperators.find(downStreamOperatorToVisit->as<LogicalOperator>())
                        != downstreamOperators.end()) {
                        visit = true;
                    } else {// Check if the path is to be explored
                        // visit only those downstream operators that are connected
                        // to the most downstream operators (or root) of the sub-query plan captured by the changelog entry
                        for (const auto& downstreamOperator : downstreamOperators) {
                            if (downStreamOperatorToVisit->as<Operator>()->containAsGrandParent(downstreamOperator)) {
                                visit = true;
                                //skip rest of the checks
                                break;
                            }
                        }
                    }

                    if (visit) {
                        //insert the downstream operator for further visit
                        operatorsToVisit.push(downStreamOperatorToVisit);
                    }
                }
            } else if (downStreamOperatorsToVisit.size() == 1) {
                //insert the downstream operator for further visit
                operatorsToVisit.push(downStreamOperatorsToVisit.at(0));
            }
        }
    }
    return poSet;
}

}// namespace NES::Optimizer::Experimental
