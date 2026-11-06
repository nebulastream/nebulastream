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

#include <Functions/SplitIndexPhysicalFunction.hpp>

#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <select.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{

namespace
{
constexpr uint64_t NULL_RESULT = std::numeric_limits<uint64_t>::max();

/// Copies the `index`-th part of `input` split at `delimiter` to `outputPtr` (which must hold inputSize bytes).
/// Returns the size of the part, or NULL_RESULT if there is no such part. Mirrors Flink, which splits with
/// commons-lang's splitByWholeSeparatorPreserveAllTokens: empty parts are kept and an empty input has no parts.
/// An empty delimiter does not split, i.e., the whole input is the only part.
uint64_t splitIndex(
    int8_t* inputPtr, const uint64_t inputSize, int8_t* delimiterPtr, const uint64_t delimiterSize, const int64_t index, int8_t* outputPtr)
{
    /// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast) VARSIZED data is handed over as int8_t*
    const std::string_view input{reinterpret_cast<const char*>(inputPtr), inputSize};
    const std::string_view delimiter{reinterpret_cast<const char*>(delimiterPtr), delimiterSize};
    /// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
    if (index < 0 or input.empty())
    {
        return NULL_RESULT;
    }

    size_t start = 0;
    for (int64_t part = 0; part < index; ++part)
    {
        if (delimiter.empty())
        {
            return NULL_RESULT;
        }
        const auto pos = input.find(delimiter, start);
        if (pos == std::string_view::npos)
        {
            return NULL_RESULT;
        }
        start = pos + delimiter.size();
    }
    const auto end = delimiter.empty() ? std::string_view::npos : input.find(delimiter, start);
    const auto result = input.substr(start, end == std::string_view::npos ? std::string_view::npos : end - start);
    if (not result.empty())
    {
        std::memcpy(outputPtr, result.data(), result.size());
    }
    return result.size();
}
}

SplitIndexPhysicalFunction::SplitIndexPhysicalFunction(
    PhysicalFunction stringFunction, PhysicalFunction delimiterFunction, PhysicalFunction indexFunction)
    : stringFunction(std::move(stringFunction)), delimiterFunction(std::move(delimiterFunction)), indexFunction(std::move(indexFunction))
{
}

VarVal SplitIndexPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto stringValue = stringFunction.execute(record, arena);
    const auto delimiterValue = delimiterFunction.execute(record, arena);
    const auto indexValue = indexFunction.execute(record, arena);

    nautilus::val<bool> inputIsNull = false;
    for (const auto* value : {&stringValue, &delimiterValue, &indexValue})
    {
        if (value->isNullable())
        {
            inputIsNull = inputIsNull or value->isNull();
        }
    }

    const auto input = stringValue.getRawValueAs<VariableSizedData>();
    const auto delimiter = delimiterValue.getRawValueAs<VariableSizedData>();
    const auto output = arena.allocateVariableSizedData(nautilus::select(inputIsNull, nautilus::val<uint64_t>{0}, input.getSize()));
    nautilus::val<uint64_t> size = NULL_RESULT;
    if (not inputIsNull)
    {
        const auto index = indexValue.castToType(DataType::Type::INT64).getRawValueAs<nautilus::val<int64_t>>();
        size = nautilus::invoke(
            splitIndex, input.getContent(), input.getSize(), delimiter.getContent(), delimiter.getSize(), index, output.getContent());
    }
    const auto isNull = size == nautilus::val<uint64_t>{NULL_RESULT};
    return VarVal{VariableSizedData(output.getContent(), nautilus::select(isNull, nautilus::val<uint64_t>{0}, size)), true, isNull};
}

/// NOLINTNEXTLINE(readability-identifier-naming)
PhysicalFunctionRegistryReturnType SplitIndexPhysicalFunction::createSPLIT_INDEX(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3, "SPLIT_INDEX function must have exactly three child functions");
    return SplitIndexPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1], arguments.childFunctions[2]);
}
}
