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

#include <memory>
#include <utility>
#include <Aggregation/Function/AggregationFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES
{

AggregationFunction::AggregationFunction(
    std::unique_ptr<PhysicalType> inputType,
    std::unique_ptr<PhysicalType> resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier)
    : inputType(std::move(inputType))
    , resultType(std::move(resultType))
    , inputFunction(std::move(inputFunction))
    , resultFieldIdentifier(std::move(resultFieldIdentifier))
{
}

AggregationFunction::~AggregationFunction() = default;
}
