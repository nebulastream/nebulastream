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
#include <Plans/LogicalPlan.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>

namespace NES::LegacyOptimizer
{

/// The type inference phase receives and query plan and infers all input and output schemata for all operators.
class TypeInferencePhase
{
public:
    TypeInferencePhase() = default;

    /// Performs type inference on the given query plan.
    /// This involves the following steps.
    /// 1. Replacing a logical source descriptor with the correct source descriptor form the source catalog.
    /// 2. Propagate the input and output schemas from source operators to the sink operators.
    /// 3. If a operator contains expression, we infer the result dataType of this operators.
    /// @throws TypeInferenceException if inferring the data types into the query failed
    static void apply(LogicalPlan& queryPlan);
};
}
