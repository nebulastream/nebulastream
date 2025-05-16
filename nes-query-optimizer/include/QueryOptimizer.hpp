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
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Plans/LogicalPlan.hpp>
#include <PhysicalPlan.hpp>

namespace NES
{

class QueryOptimizer final
{
public:
    explicit QueryOptimizer(NES::Configurations::QueryOptimizerConfiguration conf) : conf(std::move(conf)) { };
    /// Takes the query plan as a logical plan and returns a fully physical plan
    [[nodiscard]] PhysicalPlan optimize(const LogicalPlan& plan);

private:
    NES::Configurations::QueryOptimizerConfiguration conf;
};

}
