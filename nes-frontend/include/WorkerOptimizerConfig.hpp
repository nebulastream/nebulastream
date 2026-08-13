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

#include <utility>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{

/// The worker config plus the frontend's optimizer config as a top-level sibling (`optimizer.*`)
/// in one schema: fully qualified names keep the roots disjoint, so one literal schema covers
/// both and each root's fromConfig simply picks its own fields from the resolved config.
/// Temporary composition — it dissolves once the catalogs, optimizer, and statement handling
/// move into a dedicated coordinator binary with a config root of its own.
struct WorkerOptimizerConfig
{
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    WorkerOptimizerConfig() = delete;

    WorkerOptimizerConfig(SingleNodeWorkerConfiguration worker, QueryOptimizerConfiguration queryOptimizer)
        : worker(std::move(worker)), queryOptimizer(queryOptimizer)
    {
    }

    SingleNodeWorkerConfiguration worker;
    QueryOptimizerConfiguration queryOptimizer;

    static WorkerOptimizerConfig fromConfig(const InstantiatedConfig& config);
};

}
