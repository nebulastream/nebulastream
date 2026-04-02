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

#include <Phases/OperatorPlacer.hpp>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Phases/SemanticAnalyzer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

class QueryOptimizer final
{
public:
    explicit QueryOptimizer(
        const QueryOptimizerConfiguration& defaultQueryOptimization,
        const std::shared_ptr<const SourceCatalog>& sourceCatalog,
        const std::shared_ptr<const SinkCatalog>& sinkCatalog,
        const std::shared_ptr<const WorkerCatalog>& workerCatalog)
        : semanticAnalyzer(sourceCatalog, sinkCatalog)
        , ruleBasedOptimization(defaultQueryOptimization)
        , operatorPlacement(defaultQueryOptimization, sourceCatalog, sinkCatalog, workerCatalog) { };

    [[nodiscard]] DistributedLogicalPlan optimize(LogicalPlan plan) const;

private:
    SemanticAnalyzer semanticAnalyzer;
    RuleBasedOptimizer ruleBasedOptimization;
    OperatorPlacer operatorPlacement;
};

}
