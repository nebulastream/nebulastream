#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_OPTIMIZERCONFIGURATION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_OPTIMIZERCONFIGURATION_HPP_

#include <Configurations/ConfigOptions/BaseConfiguration.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <utility>

namespace NES {

namespace Configurations {

/**
 * @brief ConfigOptions for Coordinator
 */
class OptimizerConfiguration : public BaseConfiguration {
  public:
    OptimizerConfiguration(std::string name, std::string description) : BaseConfiguration(name, description){};

    /**
     * @brief The number of queries to be processed together.
     */
    IntOption queryBatchSize = {"queryBatchSize", 1, "The number of queries to be processed together"};

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
    EnumOption<Optimizer::QueryMergerRule> queryMergerRule = {"queryMergerRule",
                                                              Optimizer::QueryMergerRule::DefaultQueryMergerRule,
                                                              "The rule to be used for performing query merging"};

    /**
     * @brief Enable semantic query validation feature
     */
    BoolOption enableSemanticQueryValidation = {"enableSemanticQueryValidation",
                                                false,
                                                "Enable semantic query validation feature"};

    /**
     * @brief Indicates the memory layout policy and allows the engine to prefer a row or columnar layout.
     * Valid options are:
     * FORCE_ROW_LAYOUT
     * FORCE_COLUMN_LAYOUT
     */
    EnumOption<Optimizer::MemoryLayoutSelectionPhase::MemoryLayoutPolicy> memoryLayoutPolicy = {
        "memoryLayoutPolicy",
        Optimizer::MemoryLayoutSelectionPhase::MemoryLayoutPolicy::FORCE_ROW_LAYOUT,
        "selects the memory layout selection policy can be [FORCE_ROW_LAYOUT|FORCE_COLUMN_LAYOUT]"};

    /**
     * @brief Perform only source operator duplication when applying Logical Source Expansion Rewrite Rule. (Default: false)
     */
    BoolOption performOnlySourceOperatorExpansion = {
        "performOnlySourceOperatorExpansion",
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
