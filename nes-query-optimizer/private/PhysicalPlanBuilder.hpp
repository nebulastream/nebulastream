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
#include <vector>
#include <PhysicalPlan.hpp>

namespace NES
{

/// This is the only class that is able to create a physical plan
class PhysicalPlanBuilder final
{
    using Roots = std::vector<std::shared_ptr<PhysicalOperatorWrapper>>;

public:
    explicit PhysicalPlanBuilder(QueryId id);
    void addSinkRoot(std::shared_ptr<PhysicalOperatorWrapper> sink);
    void setExecutionMode(Nautilus::Configurations::ExecutionMode mode);

    [[nodiscard]] PhysicalPlan finalize() &&;

private:
    QueryId queryId;
    Roots sinks;
    Nautilus::Configurations::ExecutionMode executionMode;

    /// Used internally to flip the plan from sink->source tp source->sink
    Roots flip(Roots roots);
};

}
