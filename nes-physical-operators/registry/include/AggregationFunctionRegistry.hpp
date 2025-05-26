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
#include <string>
#include <vector>
#include <Aggregation/Function/AggregationFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Util/Registry.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Functions/PhysicalFunction.hpp>

namespace NES
{

using AggregationFunctionRegistryReturnType = std::shared_ptr<AggregationFunction>;
struct AggregationFunctionRegistryArguments
{
    DataType inputType;
    DataType resultType;
    PhysicalFunction inputFunction;
    Record::RecordFieldIdentifier resultFieldIdentifier;
    std::optional<std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>> memProviderPagedVector;
};

class AggregationFunctionRegistry
    : public BaseRegistry<AggregationFunctionRegistry, std::string, AggregationFunctionRegistryReturnType, AggregationFunctionRegistryArguments>
{
};
}


#define INCLUDED_FROM_REGISTRY_AGGREGATION_FUNCTION
#include <AggregationFunctionGeneratedRegistrar.inc>
#undef INCLUDED_FROM_REGISTRY_AGGREGATION_FUNCTION
