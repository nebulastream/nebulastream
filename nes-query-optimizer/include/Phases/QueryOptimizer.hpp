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
#include <utility>
#include <Plans/LogicalPlan.hpp>
#include <Util/Pointers.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <DistributedLogicalPlan.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{
class SourceCatalog;
class SinkCatalog;

class QueryOptimizer final
{
public:
    explicit QueryOptimizer(
        QueryOptimizerConfiguration defaultQueryOptimization,
        SharedPtr<const SourceCatalog> sourceCatalog,
        SharedPtr<const SinkCatalog> sinkCatalog,
        SharedPtr<const WorkerCatalog> workerCatalog)
        : defaultQueryOptimization(std::move(defaultQueryOptimization))
        , sourceCatalog(std::move(sourceCatalog))
        , sinkCatalog(std::move(sinkCatalog))
        , workerCatalog(std::move(workerCatalog)) { };

    /// Takes the query plan as a logical plan and returns a distributed plan with placement and decomposition
    [[nodiscard]] DistributedLogicalPlan optimize(const LogicalPlan& plan) const;

private:
    QueryOptimizerConfiguration defaultQueryOptimization;
    SharedPtr<const SourceCatalog> sourceCatalog;
    SharedPtr<const SinkCatalog> sinkCatalog;
    SharedPtr<const WorkerCatalog> workerCatalog;
};

}
