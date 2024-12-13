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

#include <string>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/BooleanValidation.hpp>
#include <Configurations/Validation/NumberValidation.hpp>

namespace NES::Configurations
{

class OptimizerConfiguration : public BaseConfiguration
{
public:
    OptimizerConfiguration() : BaseConfiguration() {};
    OptimizerConfiguration(std::string name, std::string description) : BaseConfiguration(name, description) {};

    /// allow the containment based merging algorithms to identify if a newly arrived query contains an already running SQP
    /// if set to true, the containment identification is slower but more accurate
    /// also, if the containment identification detects that a newly arrived query contains the already running one
    /// this entails un-deployment and re-deployment of the already running SQP
    BoolOption allowExhaustiveContainmentCheck
        = {ALLOW_EXHAUSTIVE_CONTAINMENT_CHECK,
           "false",
           "Allow the containment based merging algorithms to identify if a newly arrived query contains an already running SQP.",
           {std::make_shared<BooleanValidation>()}};

    /// Perform only source operator duplication when applying Logical Source Expansion Rewrite Rule.
    BoolOption performOnlySourceOperatorExpansion
        = {PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION,
           "false",
           "Perform only source operator duplication when applying Logical Source Expansion Rewrite Rule. (Default: false)",
           {std::make_shared<BooleanValidation>()}};

    /// Perform advance semantic validation on the incoming queryIdAndCatalogEntryMapping.
    /// This option is set to false by default as currently not all operators are supported by Z3 based signature generator.
    /// Because of this, in some cases, enabling this check may result in a crash or incorrect behavior.
    BoolOption performAdvanceSemanticValidation
        = {PERFORM_ADVANCE_SEMANTIC_VALIDATION,
           "false",
           "Perform advance semantic validation on the incoming queryIdAndCatalogEntryMapping. (Default: false)",
           {std::make_shared<BooleanValidation>()}};

    /// Enable for distributed windows the NEMO placement where aggregation happens based on the params
    /// distributedWindowChildThreshold and distributedWindowCombinerThreshold.
    BoolOption enableNemoPlacement
        = {ENABLE_NEMO_PLACEMENT,
           "false",
           "Enables NEMO distributed window rule to use central windows instead of the distributed windows. (Default: false)",
           {std::make_shared<BooleanValidation>()}};

    UIntOption placementAmendmentThreadCount
        = {PLACEMENT_AMENDMENT_THREAD_COUNT, "1", "set the placement amender thread count", {std::make_shared<NumberValidation>()}};

    BoolOption enableIncrementalPlacement
        = {ENABLE_INCREMENTAL_PLACEMENT,
           "false",
           "Enable reconfiguration of running query plans. (Default: false)",
           {std::make_shared<BooleanValidation>()}};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &performOnlySourceOperatorExpansion,
            &performAdvanceSemanticValidation,
            &enableNemoPlacement,
            &allowExhaustiveContainmentCheck,
            &placementAmendmentThreadCount,
            &enableIncrementalPlacement};
    }
};

}
