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

#include <cstddef>
#include <memory>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>

namespace NES
{
/// @brief Represents a query compilation request.
/// The request encapsulates the decomposed query plan and addition properties.
struct QueryCompilationRequest
{
    std::shared_ptr<DecomposedQueryPlan> decomposedQueryPlan;
    size_t bufferSize; /// TODO #403: Use QueryCompiler Configuration instead
    bool debug = false;
    bool optimize = false;
    bool dumpQueryPlans = false;
};
}
