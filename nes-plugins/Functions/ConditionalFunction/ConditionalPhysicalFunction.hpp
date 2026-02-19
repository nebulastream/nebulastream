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
#include <vector>

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>

namespace NES
{
/// @brief Executes a CASE WHEN expression at runtime.
/// Evaluates conditions in order and returns the result of the first true condition,
/// or the default if none match.
///
/// For a simpler example to use as reference, see ConcatPhysicalFunction.
class ConditionalPhysicalFunction final : public PhysicalFunctionConcept
{
public:
    ConditionalPhysicalFunction(std::vector<PhysicalFunction> inputFns);

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override;

private:
    std::vector<std::pair<PhysicalFunction, PhysicalFunction>> whenThenFns;
    PhysicalFunction elseFn;
};
}
