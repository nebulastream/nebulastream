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

#pragma once

#include <memory>
#include <set>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>

namespace NES::Optimizer
{

/**
 * @brief This class will expand the logical query graph by adding information about the physical sources and expand the
 * graph by adding additional replicated operators.
 *
 * Note: Apart from replicating the logical source operator we also replicate its down stream operators till we
 * encounter first blocking operator as defined by method @see LogicalSourceExpansionRule:isBlockingOperator.
 *
 * Note: if expandSourceOnly is set to true then only source operators will be expanded and @see LogicalSourceExpansionRule:isBlockingOperator
 * will be ignored.
 *
 * Example: a query :                       Sink
 *                                           |
 *                                           Map
 *                                           |
 *                                        Source(Car)
 *
 * will be expanded to:                            Sink
 *                                                /     \
 *                                              /        \
 *                                           Map1        Map2
 *                                           |             |
 *                                      Source(Car1)    Source(Car2)
 *
 *
 * Given that logical source car has two physical sources: i.e. car1 and car2
 *
 *                                                     or
 *
 * a query :                                    Sink
 *                                               |
 *                                             Merge
 *                                             /   \
 *                                            /    Filter
 *                                          /        |
 *                                   Source(Car)   Source(Truck)
 *
 * will be expanded to:                             Sink
 *                                                   |
 *                                                 Merge
 *                                              /  / \   \
 *                                            /  /    \    \
 *                                         /   /       \     \
 *                                       /   /          \      \
 *                                    /    /         Filter1   Filter2
 *                                 /     /               \         \
 *                               /      /                 \          \
 *                         Src(Car1) Src(Car2)       Src(Truck1)  Src(Truck2)
 *
 * Given that logical source car has two physical sources: i.e. car1 and car2 and
 * logical source truck has two physical sources: i.e. truck1 and truck2
 *
 */
class LogicalSourceExpansionRule : public BaseRewriteRule
{
public:
    explicit LogicalSourceExpansionRule(std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog);
    virtual ~LogicalSourceExpansionRule() = default;

    std::shared_ptr<QueryPlan> apply(std::shared_ptr<QueryPlan> queryPlan) override;

private:
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;
};
}
