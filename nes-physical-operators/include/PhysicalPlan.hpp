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
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Pipeline.hpp>

namespace NES
{
struct PhysicalPlan final
{
    PhysicalPlan(QueryId id, std::vector<std::unique_ptr<PhysicalOperatorWrapper>> rootOperator)
        : queryId(id), rootOperator(std::move(rootOperator))
    {
    }
    [[nodiscard]] std::string toString() const;
    friend std::ostream& operator<<(std::ostream& os, const PhysicalPlan& t);

    const QueryId queryId;
    /// Operators might form a tree structure
    std::vector<std::unique_ptr<PhysicalOperatorWrapper>> rootOperator;
};
}
FMT_OSTREAM(NES::PhysicalPlan);
