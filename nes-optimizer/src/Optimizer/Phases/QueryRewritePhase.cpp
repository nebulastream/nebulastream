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

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Coordinator/ElegantConfigurations.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/QueryRewrite/AttributeSortRule.hpp>
#include <Optimizer/QueryRewrite/BinaryOperatorSortRule.hpp>
#include <Optimizer/QueryRewrite/FilterMergeRule.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Optimizer/QueryRewrite/FilterSplitUpRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Optimizer/QueryRewrite/MapUDFsToOpenCLOperatorsRule.hpp>
#include <Optimizer/QueryRewrite/PredicateReorderingRule.hpp>
#include <Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRule.hpp>
#include <Optimizer/QueryRewrite/RedundancyEliminationRule.hpp>
#include <Optimizer/QueryRewrite/RenameSourceToProjectOperatorRule.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES::Optimizer {

QueryRewritePhasePtr QueryRewritePhase::create(const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration) {

    auto optimizerConfigurations = coordinatorConfiguration->optimizer;

    //If query merger rule is using string based signature or graph isomorphism to identify the sharing opportunities
    //then apply special rewrite rules for improving the match identification
    bool applyRulesImprovingSharingIdentification =
        optimizerConfigurations.queryMergerRule == QueryMergerRule::SyntaxBasedCompleteQueryMergerRule
        || optimizerConfigurations.queryMergerRule == QueryMergerRule::ImprovedHashSignatureBasedCompleteQueryMergerRule
        || optimizerConfigurations.queryMergerRule == QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule
        || optimizerConfigurations.queryMergerRule == QueryMergerRule::HybridCompleteQueryMergerRule;

    //Check if Java UDF acceleration activated
    bool accelerateJavaUDFs = coordinatorConfiguration->elegant.accelerateJavaUDFs;

    return std::make_shared<QueryRewritePhase>(QueryRewritePhase(accelerateJavaUDFs, applyRulesImprovingSharingIdentification));
}

QueryRewritePhase::QueryRewritePhase(bool elegantAccelerationEnabled, bool applyRulesImprovingSharingIdentification)
    : isElegantAccelerationEnabled(elegantAccelerationEnabled),
      applyRulesImprovingSharingIdentification(applyRulesImprovingSharingIdentification) {
    attributeSortRule = AttributeSortRule::create();
    binaryOperatorSortRule = BinaryOperatorSortRule::create();
    filterMergeRule = FilterMergeRule::create();
    filterPushDownRule = FilterPushDownRule::create();
    filterSplitUpRule = FilterSplitUpRule::create();
    redundancyEliminationRule = RedundancyEliminationRule::create();
    mapUDFsToOpenCLOperatorsRule = MapUDFsToOpenCLOperatorsRule::create();
    predicateReorderingRule = PredicateReorderingRule::create();
    projectBeforeUnionOperatorRule = ProjectBeforeUnionOperatorRule::create();
    renameSourceToProjectOperatorRule = RenameSourceToProjectOperatorRule::create();
}

QueryPlanPtr QueryRewritePhase::execute(const QueryPlanPtr& queryPlan) {

    // Duplicate query plan
    auto duplicateQueryPlan = queryPlan->copy();

    // Apply elegant specific rule for accelerating  Java Map UDFs
    if (isElegantAccelerationEnabled) {
        mapUDFsToOpenCLOperatorsRule->apply(duplicateQueryPlan);
    }

    // Apply rules necessary for improving sharing identification
    if (applyRulesImprovingSharingIdentification) {
        duplicateQueryPlan = attributeSortRule->apply(duplicateQueryPlan);
        duplicateQueryPlan = binaryOperatorSortRule->apply(duplicateQueryPlan);
    }

    // Apply rules necessary for enabling query execution when stream alias or union operators are involved
    duplicateQueryPlan = renameSourceToProjectOperatorRule->apply(duplicateQueryPlan);
    duplicateQueryPlan = projectBeforeUnionOperatorRule->apply(duplicateQueryPlan);

    // Apply rule for filter split up
    duplicateQueryPlan = filterSplitUpRule->apply(duplicateQueryPlan);
    // Apply rule for filter push down optimization
    duplicateQueryPlan = filterPushDownRule->apply(duplicateQueryPlan);
    // Apply rule for filter merge
    duplicateQueryPlan = filterMergeRule->apply(duplicateQueryPlan);
    // Apply rule for filter reordering optimization
    return predicateReorderingRule->apply(duplicateQueryPlan);
}

}// namespace NES::Optimizer