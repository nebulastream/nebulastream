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
#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_OPTIMIZERCONFIGURATION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_OPTIMIZERCONFIGURATION_HPP_

#include <Configurations/BaseConfiguration.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <utility>
#include <Configurations/ConfigurationOption.hpp>

namespace NES {

namespace Configurations {

/**
 * @brief ConfigOptions for Coordinator
 */
class OptimizerConfiguration : public BaseConfiguration {
  public:
    OptimizerConfiguration() : BaseConfiguration(){};
    OptimizerConfiguration(std::string name, std::string description) : BaseConfiguration(name, description){};

    /**
     * @brief The number of queries to be processed together.
     */
    IntOption queryBatchSize = {QUERY_BATCH_SIZE_CONFIG, 1, "The number of queries to be processed together"};

    /**
     * @brief The rule to be used for performing query merging.
     * Valid options are:
     * SyntaxBasedCompleteQueryMergerRule,
     * SyntaxBasedPartialQueryMergerRule,
     * Z3SignatureBasedCompleteQueryMergerRule,
     * Z3SignatureBasedPartialQueryMergerRule,
     * Z3SignatureBasedPartialQueryMergerBottomUpRule,
     * HashSignatureBasedCompleteQueryMergerRule,
     * ImprovedStringSignatureBasedCompleteQueryMergerRule,
     * ImprovedStringSignatureBasedPartialQueryMergerRule,
     * StringSignatureBasedPartialQueryMergerRule,
     * DefaultQueryMergerRule,
     * HybridCompleteQueryMergerRule
     */
    EnumOption<Optimizer::QueryMergerRule> queryMergerRule = {QUERY_MERGER_RULE_CONFIG,
                                                              Optimizer::QueryMergerRule::DefaultQueryMergerRule,
                                                              "The rule to be used for performing query merging"};

    /**
     * @brief Enable semantic query validation.
     */
    BoolOption enableSemanticQueryValidation = {ENABLE_SEMANTIC_QUERY_VALIDATION_CONFIG,
                                                false,
                                                "Enable semantic query validation feature"};

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

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&queryBatchSize,
                &queryMergerRule,
                &enableSemanticQueryValidation,
                &memoryLayoutPolicy,
                &performOnlySourceOperatorExpansion};
    }
};

}// namespace Configurations
}// namespace NES

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_OPTIMIZERCONFIGURATION_HPP_
