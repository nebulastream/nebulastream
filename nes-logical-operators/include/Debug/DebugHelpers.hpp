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

#include <string>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>

namespace NES::Debug
{

/// Returns a full recursive debug string representation of a LogicalPlan.
/// Calls explain(ExplainVerbosity::Debug) internally.
std::string dump(const LogicalPlan& plan);

/// Returns a debug string representation of a single LogicalOperator node,
/// including all of its children recursively.
std::string dump(const LogicalOperator& op);

} // namespace NES::Debug
