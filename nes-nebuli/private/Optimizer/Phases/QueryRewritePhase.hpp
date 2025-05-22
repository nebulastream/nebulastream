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

namespace NES
{

class QueryPlan;

namespace Configurations
{
class CoordinatorConfiguration;
}
}

namespace NES::Optimizer
{
class QueryRewritePhase;
class AttributeSortRule;
class BinaryOperatorSortRule;
class FilterMergeRule;
class FilterPushDownRule;
class FilterSplitUpRule;
class PredicateReorderingRule;
class ProjectBeforeUnionOperatorRule;
class RenameSourceToProjectOperatorRule;

/**
 * @brief This phase is responsible for re-writing the query plan
 */
class QueryRewritePhase
{
public:
    static std::shared_ptr<QueryRewritePhase> create();

    void execute(std::shared_ptr<QueryPlan>& queryPlan) const;

private:
    explicit QueryRewritePhase();

    std::shared_ptr<AttributeSortRule> attributeSortRule;
    std::shared_ptr<BinaryOperatorSortRule> binaryOperatorSortRule;
    std::shared_ptr<FilterMergeRule> filterMergeRule;
    std::shared_ptr<FilterPushDownRule> filterPushDownRule;
    std::shared_ptr<FilterSplitUpRule> filterSplitUpRule;
    std::shared_ptr<PredicateReorderingRule> predicateReorderingRule;
    std::shared_ptr<ProjectBeforeUnionOperatorRule> projectBeforeUnionOperatorRule;
    std::shared_ptr<RenameSourceToProjectOperatorRule> renameSourceToProjectOperatorRule;
};
}
