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

#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{

struct CatalogRef;
class NetworkTopology;

class QueryOptimizer final
{
public:
    explicit QueryOptimizer(
        QueryOptimizerConfiguration defaultQueryOptimization,
        const CatalogRef& catalog,
        const NetworkTopology& topology)
        : defaultQueryOptimization(std::move(defaultQueryOptimization))
        , catalog(catalog)
        , topology(topology) { };

    /// Takes the query plan as a logical plan and returns a distributed plan with placement and decomposition
    [[nodiscard]] std::unordered_map<Host, std::vector<LogicalPlan>> optimize(const LogicalPlan& plan) const;

private:
    QueryOptimizerConfiguration defaultQueryOptimization;
    const CatalogRef& catalog;
    const NetworkTopology& topology;
};

}
