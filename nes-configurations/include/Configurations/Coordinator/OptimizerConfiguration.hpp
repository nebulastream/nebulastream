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

#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_COORDINATOR_OPTIMIZERCONFIGURATION_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_COORDINATOR_OPTIMIZERCONFIGURATION_HPP_

#include "Configurations/BaseConfiguration.hpp"
#include "Configurations/ConfigurationsNames.hpp"
#include "Configurations/Enums/DistributedJoinOptimizationMode.hpp"
#include "Configurations/Enums/MemoryLayoutPolicy.hpp"
#include "Configurations/Enums/PlacementAmendmentMode.hpp"
#include "Configurations/Enums/QueryMergerRule.hpp"
#include "Configurations/Validation/BooleanValidation.hpp"
#include "Configurations/Validation/NumberValidation.hpp"
#include <string>

namespace NES::Configurations {

/**
 * @brief ConfigOptions for Coordinator
 */
class OptimizerConfiguration : public BaseConfiguration {
  public:
    OptimizerConfiguration() : BaseConfiguration(){};
    OptimizerConfiguration(std::string name, std::string description) : BaseConfiguration(name, description){};

    /**
     * @brief The rule to be used for performing query merging.
     * Valid options are:
     * SyntaxBasedCompleteQueryMergerRule,
     * SyntaxBasedPartialQueryMergerRule,
     * Z3SignatureBasedCompleteQueryMergerRule,
     * Z3SignatureBasedPartialQueryMergerRule,
     * Z3SignatureBasedPartialQueryMergerBottomUpRule,
     * Z3SignatureBasedBottomUpQueryContainmentRule,
     * Z3SignatureBasedTopDownQueryContainmentMergerRule,
     * HashSignatureBasedCompleteQueryMergerRule,
     * ImprovedHashSignatureBasedCompleteQueryMergerRule,
     * ImprovedHashSignatureBasedPartialQueryMergerRule,
     * HashSignatureBasedPartialQueryMergerRule,
     * DefaultQueryMergerRule,
     * HybridCompleteQueryMergerRule
     */
    EnumOption<Optimizer::QueryMergerRule> queryMergerRule = {QUERY_MERGER_RULE_CONFIG,
                                                              Optimizer::QueryMergerRule::DefaultQueryMergerRule,
                                                              "The rule to be used for performing query merging"};

    /**
     * @brief allow the containment based merging algorithms to identify if a newly arrived query contains an already running SQP
     * if set to true, the containment identification is slower but more accurate
     * also, if the containment identification detects that a newly arrived query contains the already running one
     * this entails un-deployment and re-deployment of the already running SQP
     */
    BoolOption allowExhaustiveContainmentCheck = {
        ALLOW_EXHAUSTIVE_CONTAINMENT_CHECK,
        "false",
        "Allow the containment based merging algorithms to identify if a newly arrived query contains an already running SQP.",
        {std::make_shared<BooleanValidation>()}};

    /**
     * @brief Indicates the memory layout policy and allows the engine to prefer a row or columnar layout.
     * Depending on the concrete workload different memory layouts can be beneficial. Valid options are:
     * FORCE_ROW_LAYOUT -> Enforces a row layout between all operators.
     * FORCE_COLUMN_LAYOUT -> Enforces a column layout between all operators.
     */
    EnumOption<Optimizer::MemoryLayoutPolicy> memoryLayoutPolicy = {
        MEMORY_LAYOUT_POLICY_CONFIG,
        Optimizer::MemoryLayoutPolicy::FORCE_ROW_LAYOUT,
        "selects the memory layout selection policy can be [FORCE_ROW_LAYOUT|FORCE_COLUMN_LAYOUT]"};

    /**
     * @brief Perform only source operator duplication when applying Logical Source Expansion Rewrite Rule.
     */
    BoolOption performOnlySourceOperatorExpansion = {
        PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION,
        "false",
        "Perform only source operator duplication when applying Logical Source Expansion Rewrite Rule. (Default: false)",
        {std::make_shared<BooleanValidation>()}};

    /**
     * @brief Perform advance semantic validation on the incoming queryIdAndCatalogEntryMapping.
     * @warning This option is set to false by default as currently not all operators are supported by Z3 based signature generator.
     * Because of this, in some cases, enabling this check may result in a crash or incorrect behavior.
     */
    BoolOption performAdvanceSemanticValidation = {
        PERFORM_ADVANCE_SEMANTIC_VALIDATION,
        "false",
        "Perform advance semantic validation on the incoming queryIdAndCatalogEntryMapping. (Default: false)",
        {std::make_shared<BooleanValidation>()}};

    /**
     * @brief Enable for distributed windows the NEMO placement where aggregation happens based on the params
     * distributedWindowChildThreshold and distributedWindowCombinerThreshold.
     */
    BoolOption enableNemoPlacement = {
        ENABLE_NEMO_PLACEMENT,
        "false",
        "Enables NEMO distributed window rule to use central windows instead of the distributed windows. (Default: false)",
        {std::make_shared<BooleanValidation>()}};

    /**
     * @brief Indicates the amender mode for performing placement amendment.
     * PESSIMISTIC -> Use a pessimistic 2PL strategy to concurrently amend operator placements.
     * OPTIMISTIC -> Use an optimistic OCC strategy to concurrently amend operator placements.
     */
    EnumOption<Optimizer::PlacementAmendmentMode> placementAmendmentMode = {
        PLACEMENT_AMENDMENT_MODE_CONFIG,
        Optimizer::PlacementAmendmentMode::OPTIMISTIC,
        "selects the placement amender mode to use [PESSIMISTIC|OPTIMISTIC]"};

    /**
     * @brief Set the thread count for running concurrent placement amenders
     */
    UIntOption placementAmendmentThreadCount = {PLACEMENT_AMENDMENT_THREAD_COUNT,
                                                "8",
                                                "set the placement amender thread count",
                                                {std::make_shared<NumberValidation>()}};

    /**
     * @brief Enable incremental placement of running query plans.
     */
    BoolOption enableIncrementalPlacement = {ENABLE_INCREMENTAL_PLACEMENT,
                                             "true",
                                             "Enable reconfiguration of running query plans. (Default: false)",
                                             {std::make_shared<BooleanValidation>()}};

    /**
     * @brief Indicates the optimization mode for distributed joins.
     * NONE -> Perform join at the sink
     * MATRIX -> Use the distributed matrix based partitioning join.
     * NEMO -> Perform partial joins and push them down closer to the sources.
     */
    EnumOption<Optimizer::DistributedJoinOptimizationMode> joinOptimizationMode = {
        DISTRIBUTED_JOIN_OPTIMIZATION_MODE_CONFIG,
        Optimizer::DistributedJoinOptimizationMode::NONE,
        "selects the distributed join optimization mode [NONE|MATRIX|NEMO]"};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&queryMergerRule,
                &memoryLayoutPolicy,
                &performOnlySourceOperatorExpansion,
                &performAdvanceSemanticValidation,
                &enableNemoPlacement,
                &joinOptimizationMode,
                &allowExhaustiveContainmentCheck,
                &placementAmendmentMode,
                &placementAmendmentThreadCount,
                &enableIncrementalPlacement};
    }
};

}// namespace NES::Configurations

#endif// NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_COORDINATOR_OPTIMIZERCONFIGURATION_HPP_
