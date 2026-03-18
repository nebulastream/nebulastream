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

/// NOLINT(clang-diagnostic-error)
#ifndef INCLUDED_FROM_REGISTRY_WINDOW_AGGREGATION_FUNCTION
#    error "This file should not be included directly! " \
"Include instead include <WindowAggregationFunctionRegistry.hpp>"
#endif

#include <memory>
#include <string>
#include <Util/Registry.hpp>

namespace NES::AggregationLogicalFunctionGeneratedRegistrar
{

/// declarations of register functions for 'AggregationLogicalFunction'
AggregationLogicalFunctionRegistryReturnType RegisterAvgAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments);
AggregationLogicalFunctionRegistryReturnType RegisterCountAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments);
AggregationLogicalFunctionRegistryReturnType RegisterMaxAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments);
AggregationLogicalFunctionRegistryReturnType RegisterMedianAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments);
AggregationLogicalFunctionRegistryReturnType RegisterMinAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments);
AggregationLogicalFunctionRegistryReturnType RegisterSumAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments);

}

namespace NES
{
template <>
inline void Registrar<AggregationLogicalFunctionRegistry, std::string, AggregationLogicalFunctionRegistryReturnType, AggregationLogicalFunctionRegistryArguments>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::AggregationLogicalFunctionGeneratedRegistrar;
    registry.addEntry("Avg", RegisterAvgAggregationLogicalFunction);
	registry.addEntry("Count", RegisterCountAggregationLogicalFunction);
	registry.addEntry("Max", RegisterMaxAggregationLogicalFunction);
	registry.addEntry("Median", RegisterMedianAggregationLogicalFunction);
	registry.addEntry("Min", RegisterMinAggregationLogicalFunction);
	registry.addEntry("Sum", RegisterSumAggregationLogicalFunction);
	
}

}
