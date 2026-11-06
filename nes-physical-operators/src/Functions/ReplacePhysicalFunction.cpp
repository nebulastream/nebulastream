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

#include <Functions/ReplacePhysicalFunction.hpp>

#include <cstdint>
#include <cstring>
#include <string_view>
#include <utility>
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
std::string_view asStringView(int8_t* data, const uint64_t size)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) VARSIZED data is handed over as int8_t*
    return {reinterpret_cast<const char*>(data), size};
}

/// Returns the size of `input` after replacing every non-overlapping occurrence of `search` by `replacementSize` bytes.
uint64_t
replacedSize(int8_t* inputPtr, const uint64_t inputSize, int8_t* searchPtr, const uint64_t searchSize, const uint64_t replacementSize)
{
    const auto input = asStringView(inputPtr, inputSize);
    const auto search = asStringView(searchPtr, searchSize);
    if (search.empty())
    {
        return inputSize;
    }
    uint64_t occurrences = 0;
    for (auto pos = input.find(search); pos != std::string_view::npos; pos = input.find(search, pos + search.size()))
    {
        ++occurrences;
    }
    return inputSize - (occurrences * searchSize) + (occurrences * replacementSize);
}

/// Writes `input` with every non-overlapping occurrence of `search` replaced by `replacement` to `outputPtr`.
/// The output buffer must hold replacedSize(...) bytes. Returns the number of written bytes.
uint64_t replaceOccurrences(
    int8_t* inputPtr,
    const uint64_t inputSize,
    int8_t* searchPtr,
    const uint64_t searchSize,
    int8_t* replacementPtr,
    const uint64_t replacementSize,
    int8_t* outputPtr)
{
    const auto input = asStringView(inputPtr, inputSize);
    const auto search = asStringView(searchPtr, searchSize);
    const auto replacement = asStringView(replacementPtr, replacementSize);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) VARSIZED data is handed over as int8_t*
    auto* output = reinterpret_cast<char*>(outputPtr);
    uint64_t written = 0;
    const auto append = [&](const std::string_view part)
    {
        if (not part.empty())
        {
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) the caller sized the buffer via replacedSize
            std::memcpy(output + written, part.data(), part.size());
            written += part.size();
        }
    };
    if (search.empty())
    {
        append(input);
        return written;
    }
    size_t start = 0;
    for (auto pos = input.find(search); pos != std::string_view::npos; pos = input.find(search, start))
    {
        append(input.substr(start, pos - start));
        append(replacement);
        start = pos + search.size();
    }
    append(input.substr(start));
    return written;
}
}

ReplacePhysicalFunction::ReplacePhysicalFunction(
    PhysicalFunction stringFunction, PhysicalFunction searchFunction, PhysicalFunction replacementFunction)
    : stringFunction(std::move(stringFunction))
    , searchFunction(std::move(searchFunction))
    , replacementFunction(std::move(replacementFunction))
{
}

VarVal ReplacePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto stringValue = stringFunction.execute(record, arena);
    const auto searchValue = searchFunction.execute(record, arena);
    const auto replacementValue = replacementFunction.execute(record, arena);

    bool nullable = false;
    nautilus::val<bool> isNull = false;
    for (const auto* value : {&stringValue, &searchValue, &replacementValue})
    {
        if (value->isNullable())
        {
            nullable = true;
            isNull = isNull or value->isNull();
        }
    }

    const auto input = stringValue.getRawValueAs<VariableSizedData>();
    const auto search = searchValue.getRawValueAs<VariableSizedData>();
    const auto replacement = replacementValue.getRawValueAs<VariableSizedData>();
    if (isNull)
    {
        return VarVal{arena.allocateVariableSizedData(nautilus::val<uint64_t>{0}), nullable, isNull};
    }

    const auto outputSize
        = nautilus::invoke(replacedSize, input.getContent(), input.getSize(), search.getContent(), search.getSize(), replacement.getSize());
    const auto output = arena.allocateVariableSizedData(outputSize);
    const auto written = nautilus::invoke(
        replaceOccurrences,
        input.getContent(),
        input.getSize(),
        search.getContent(),
        search.getSize(),
        replacement.getContent(),
        replacement.getSize(),
        output.getContent());
    return VarVal{VariableSizedData(output.getContent(), written), nullable, isNull};
}

/// NOLINTNEXTLINE(readability-identifier-naming)
PhysicalFunctionRegistryReturnType ReplacePhysicalFunction::createREPLACE(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3, "REPLACE function must have exactly three child functions");
    return ReplacePhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1], arguments.childFunctions[2]);
}
}
