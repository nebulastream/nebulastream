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
#include <string>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Registry.hpp>
#include <ModelCatalog.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{

using PlanRuleRegistryReturnType = Rule<LogicalPlan>;

struct PlanRuleRegistryArguments
{
    QueryOptimizerConfiguration defaultQueryOptimization;
    std::shared_ptr<const SourceCatalog> sourceCatalog;
    std::shared_ptr<const SinkCatalog> sinkCatalog;
    std::shared_ptr<const ModelCatalog> modelCatalog;
};

class PlanRuleRegistry : public BaseRegistry<PlanRuleRegistry, std::string, PlanRuleRegistryReturnType, PlanRuleRegistryArguments>
{
};
}

#define INCLUDED_FROM_REGISTRY_PLAN_RULE
#include <PlanRuleGeneratedRegistrar.inc>
#undef INCLUDED_FROM_REGISTRY_PLAN_RULE
