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

#include <Functions/RegexpExtractPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
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
constexpr size_t MAX_CACHED_REGEXES = 64;

/// Returns the compiled regex for `pattern`, or nullptr if the pattern is invalid.
/// Compiled regexes are cached per thread, so a constant pattern is compiled once per worker thread.
const std::regex* compiledRegex(const std::string_view pattern)
{
    thread_local std::unordered_map<std::string, std::optional<std::regex>> cache;
    const std::string key{pattern};
    if (const auto it = cache.find(key); it != cache.end())
    {
        return it->second ? &*it->second : nullptr;
    }
    if (cache.size() >= MAX_CACHED_REGEXES)
    {
        cache.clear();
    }
    std::optional<std::regex> compiled;
    try
    {
        compiled.emplace(key, std::regex::ECMAScript);
    }
    catch (const std::regex_error&)
    {
        compiled.reset();
    }
    const auto& entry = cache.emplace(key, std::move(compiled)).first->second;
    return entry ? &*entry : nullptr;
}

/// Copies capture group `group` of the first match of `regex` in `input` to `outputPtr` (which must hold inputSize bytes).
/// Returns the size of the group, or NULL_RESULT if there is no match, the group does not exist or did not participate,
/// or the regex is invalid.
uint64_t regexpExtract(
    int8_t* inputPtr, const uint64_t inputSize, int8_t* regexPtr, const uint64_t regexSize, const int64_t group, int8_t* outputPtr)
{
    /// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast) VARSIZED data is handed over as int8_t*
    const auto* inputBegin = reinterpret_cast<const char*>(inputPtr);
    const std::string_view pattern{reinterpret_cast<const char*>(regexPtr), regexSize};
    /// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
    const auto* regex = compiledRegex(pattern);
    if (regex == nullptr or group < 0 or static_cast<size_t>(group) > regex->mark_count())
    {
        return NULL_RESULT;
    }
    std::cmatch match;
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) end of the input range
    if (not std::regex_search(inputBegin, inputBegin + inputSize, match, *regex))
    {
        return NULL_RESULT;
    }
    const auto& subMatch = match[static_cast<size_t>(group)];
    if (not subMatch.matched)
    {
        return NULL_RESULT;
    }
    const auto length = static_cast<uint64_t>(subMatch.length());
    if (length > 0)
    {
        std::memcpy(outputPtr, subMatch.first, length);
    }
    return length;
}
}

RegexpExtractPhysicalFunction::RegexpExtractPhysicalFunction(
    PhysicalFunction stringFunction, PhysicalFunction regexFunction, std::optional<PhysicalFunction> groupFunction)
    : stringFunction(std::move(stringFunction)), regexFunction(std::move(regexFunction)), groupFunction(std::move(groupFunction))
{
}

VarVal RegexpExtractPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto stringValue = stringFunction.execute(record, arena);
    const auto regexValue = regexFunction.execute(record, arena);
    const auto groupValue = groupFunction ? groupFunction->execute(record, arena) : VarVal{nautilus::val<int64_t>{0}};

    nautilus::val<bool> inputIsNull = false;
    for (const auto* value : {&stringValue, &regexValue, &groupValue})
    {
        if (value->isNullable())
        {
            inputIsNull = inputIsNull or value->isNull();
        }
    }

    const auto input = stringValue.getRawValueAs<VariableSizedData>();
    const auto regex = regexValue.getRawValueAs<VariableSizedData>();
    const auto output = arena.allocateVariableSizedData(nautilus::select(inputIsNull, nautilus::val<uint64_t>{0}, input.getSize()));
    nautilus::val<uint64_t> size = NULL_RESULT;
    if (not inputIsNull)
    {
        const auto group = groupValue.castToType(DataType::Type::INT64).getRawValueAs<nautilus::val<int64_t>>();
        size = nautilus::invoke(
            regexpExtract, input.getContent(), input.getSize(), regex.getContent(), regex.getSize(), group, output.getContent());
    }
    const auto isNull = size == nautilus::val<uint64_t>{NULL_RESULT};
    return VarVal{VariableSizedData(output.getContent(), nautilus::select(isNull, nautilus::val<uint64_t>{0}, size)), true, isNull};
}

/// NOLINTNEXTLINE(readability-identifier-naming)
PhysicalFunctionRegistryReturnType RegexpExtractPhysicalFunction::createREGEXP_EXTRACT(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.childFunctions.size() == 2 or arguments.childFunctions.size() == 3,
        "REGEXP_EXTRACT function must have two or three child functions");
    std::optional<PhysicalFunction> groupFunction;
    if (arguments.childFunctions.size() == 3)
    {
        groupFunction = arguments.childFunctions[2];
    }
    return RegexpExtractPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1], std::move(groupFunction));
}
}
