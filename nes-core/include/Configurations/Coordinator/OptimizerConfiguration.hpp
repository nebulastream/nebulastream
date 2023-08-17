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
#ifndef NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_OPTIMIZERCONFIGURATION_HPP_
#define NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_OPTIMIZERCONFIGURATION_HPP_

#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <utility>

namespace NES::Configurations {

/**
 * @brief ConfigOptions for Coordinator
 */
class OptimizerConfiguration : public BaseConfiguration {
  public:
    OptimizerConfiguration() : BaseConfiguration(){};
    OptimizerConfiguration(std::string name, std::string description) : BaseConfiguration(name, description){};

    /**
     * @brief The number of queryIdAndCatalogEntryMapping to be processed together.
     */
    IntOption queryBatchSize = {QUERY_BATCH_SIZE_CONFIG,
                                1,
                                "The number of queryIdAndCatalogEntryMapping to be processed together"};

    /**
     * @brief The rule to be used for performing query merging.
     * Valid options are:
     * SyntaxBasedCompleteQueryMergerRule,
     * SyntaxBasedPartialQueryMergerRule,
     * Z3SignatureBasedCompleteQueryMergerRule,
     * Z3SignatureBasedPartialQueryMergerRule,
     * Z3SignatureBasedPartialQueryMergerBottomUpRule,
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
     * @brief Indicates the memory layout policy and allows the engine to prefer a row or columnar layout.
     * Depending on the concrete workload different memory layouts can be beneficial. Valid options are:
     * FORCE_ROW_LAYOUT -> Enforces a row layout between all operators.
     * FORCE_COLUMN_LAYOUT -> Enforces a column layout between all operators.
     */
    EnumOption<Optimizer::MemoryLayoutSelectionPhase::MemoryLayoutPolicy> memoryLayoutPolicy = {
        MEMORY_LAYOUT_POLICY_CONFIG,
        Optimizer::MemoryLayoutSelectionPhase::MemoryLayoutPolicy::FORCE_ROW_LAYOUT,
        "selects the memory layout selection policy can be [FORCE_ROW_LAYOUT|FORCE_COLUMN_LAYOUT]"};

    /**
     * @brief Perform only source operator duplication when applying Logical Source Expansion Rewrite Rule.
     */
    BoolOption performOnlySourceOperatorExpansion = {
        PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION,
        false,
        "Perform only source operator duplication when applying Logical Source Expansion Rewrite Rule. (Default: false)"};

    /**
     * @brief Indicates if the distributed window optimization rule should be enabled.
     * This optimization, will enable the distribution of window aggregation across multiple nodes.
     * To this end, the optimizer will create pre-aggregation operators that are located close to the data source.
     */
    BoolOption performDistributedWindowOptimization = {PERFORM_DISTRIBUTED_WINDOW_OPTIMIZATION,
                                                       true, // TODO this has to be set to true, otherwise the Nemo tests fail #4150
                                                       "Enables the distribution of window aggregations."};

    /**
     * @brief Indicated the number of child operators from, which a window operator is distributed.
     */
    IntOption distributedWindowChildThreshold = {DISTRIBUTED_WINDOW_OPTIMIZATION_CHILD_THRESHOLD,
                                                 2,
                                                 "Threshold for the distribution of window aggregations."};

    /**
     * @brief Indicated the number of child nodes from which on we will introduce combine operator between the pre-aggregation operator and the final aggregation.
     */
    IntOption distributedWindowCombinerThreshold = {DISTRIBUTED_WINDOW_OPTIMIZATION_COMBINER_THRESHOLD,
                                                    4,
                                                    "Threshold for the insertion of pre-aggregation operators."};

    /**
     * @brief Perform advance semantic validation on the incoming queryIdAndCatalogEntryMapping.
     * @warning This option is set to false by default as currently not all operators are supported by Z3 based signature generator.
     * Because of this, in some cases, enabling this check may result in a crash or incorrect behavior.
     */
    BoolOption performAdvanceSemanticValidation = {
        PERFORM_ADVANCE_SEMANTIC_VALIDATION,
        false,
        "Perform advance semantic validation on the incoming queryIdAndCatalogEntryMapping. (Default: false)"};

    /**
     * @brief Enable for distributed windows the NEMO placement where aggregation happens based on the params
     * distributedWindowChildThreshold and distributedWindowCombinerThreshold.
     */
    BoolOption enableNemoPlacement = {
        ENABLE_NEMO_PLACEMENT,
        false,
        "Enables NEMO distributed window rule to use central windows instead of the distributed windows. (Default: false)"};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&queryBatchSize,
                &queryMergerRule,
                &memoryLayoutPolicy,
                &performOnlySourceOperatorExpansion,
                &performDistributedWindowOptimization,
                &distributedWindowChildThreshold,
                &distributedWindowCombinerThreshold,
                &performOnlySourceOperatorExpansion,
                &performAdvanceSemanticValidation,
                &enableNemoPlacement};
    }
};

}// namespace NES::Configurations

#endif// NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_OPTIMIZERCONFIGURATION_HPP_
