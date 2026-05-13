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

#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>

namespace NES
{

/// Physical function that formats a VARSIZED format string with `{}` placeholders
/// filled by the remaining child arguments.
///
/// The skeleton currently returns the format string verbatim (i.e. ignores the
/// placeholder args). Students must replace this with real substitution; see the
/// extensive TODO in execute() for guidance.
class FormatPhysicalFunction final
{
public:
    explicit FormatPhysicalFunction(std::vector<PhysicalFunction> childPhysicalFunctions);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    std::vector<PhysicalFunction> childPhysicalFunctions;
};

static_assert(PhysicalFunctionConcept<FormatPhysicalFunction>);

}
