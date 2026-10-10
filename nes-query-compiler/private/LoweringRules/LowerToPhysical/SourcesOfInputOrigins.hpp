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
#include <Operators/LogicalOperator.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// Maps every input origin of a window-based operator with the given children to the origins of the sources in the plan below it.
/// The plan is the one of this worker, so a network source counts as a source of its own.
SourcesOfInputOrigins getSourcesOfInputOrigins(const std::vector<LogicalOperator>& children);

}
