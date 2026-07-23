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

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using AggregationLogicalFunctionRegistryReturnType = WindowAggregationLogicalFunction;

struct AggregationLogicalFunctionRegistryArguments
{
    std::vector<AggregationFieldAccess> on;
    bool includeNullValues;
};

using AggregationLogicalFunctionFn
    = std::function<AggregationLogicalFunctionRegistryReturnType(AggregationLogicalFunctionRegistryArguments)>;

/// Filled by loadBuiltinPlugins() / plugin registration (see cmake/RuntimeRegistrationUtil.cmake).
/// Entries are static create members on the aggregation function classes (constructor
/// signatures differ between aggregations). Enables name-based construction of aggregation
/// functions, e.g. for parser-side extensibility.
/// Case-insensitive to mirror the retired BaseRegistry.
class AggregationLogicalFunctionRegistry : public RuntimeRegistry<
                                               AggregationLogicalFunctionRegistry,
                                               std::string,
                                               AggregationLogicalFunctionFn,
                                               /*CaseSensitive*/ false>
{
public:
    /// Defined out-of-line (AggregationLogicalFunctionRegistry.cpp) so exactly one instance
    /// exists process-wide even with plugins loaded as shared objects.
    static AggregationLogicalFunctionRegistry& instance();
};

}
