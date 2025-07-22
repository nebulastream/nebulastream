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

#include <Plans/LogicalPlan.hpp>

namespace NES
{

/// @brief This rule removes redundant unions with only a single child.
class RedundantUnionRemovalPhase
{
public:
    RedundantUnionRemovalPhase() = delete;
    ~RedundantUnionRemovalPhase() = delete;
    RedundantUnionRemovalPhase(const RedundantUnionRemovalPhase&) = delete;
    RedundantUnionRemovalPhase(RedundantUnionRemovalPhase&&) = delete;
    RedundantUnionRemovalPhase& operator=(const RedundantUnionRemovalPhase&) = delete;
    RedundantUnionRemovalPhase& operator=(RedundantUnionRemovalPhase&&) = delete;

    static LogicalPlan apply(const LogicalPlan& inputPlan);
};
}
