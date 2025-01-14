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

#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Runtime::Execution::Aggregation
{

AggregationFunction::AggregationFunction(
    PhysicalTypePtr inputType,
    PhysicalTypePtr resultType,
    std::unique_ptr<Functions::Function> inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier)
    : inputType(std::move(inputType))
    , resultType(std::move(resultType))
    , inputFunction(std::move(inputFunction))
    , resultFieldIdentifier(std::move(resultFieldIdentifier))
{
}

AggregationFunction::~AggregationFunction() = default;
}
