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

#include <Functions/DateFormatPhysicalFunction.hpp>

#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
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
/// Upper bound of output bytes per pattern byte: a single pattern letter expands to at most six characters
/// (a negative five-digit year), every other pattern byte to at most one.
constexpr uint64_t MAX_OUTPUT_BYTES_PER_PATTERN_BYTE = 7;

/// Writes `value` zero-padded to at least `minDigits` digits and returns the number of written bytes.
size_t writeNumber(std::span<char> output, int64_t value, size_t minDigits)
{
    std::array<char, 24> digits{};
    const bool negative = value < 0;
    const auto [end, errc] = std::to_chars(digits.data(), digits.data() + digits.size(), negative ? -value : value);
    INVARIANT(errc == std::errc{}, "formatting a date component must not fail");
    const auto numDigits = static_cast<size_t>(end - digits.data());
    size_t written = 0;
    if (negative)
    {
        output[written++] = '-';
    }
    for (size_t i = numDigits; i < minDigits; ++i)
    {
        output[written++] = '0';
    }
    for (size_t i = 0; i < numDigits; ++i)
    {
        output[written++] = digits.at(i);
    }
    return written;
}

/// Formats a unix timestamp in milliseconds (UTC) following the numeric subset of Java's SimpleDateFormat.
/// Returns the number of bytes written to `outputPtr`, which must hold MAX_OUTPUT_BYTES_PER_PATTERN_BYTE * patternSize bytes.
uint64_t formatTimestamp(const uint64_t millis, int8_t* patternPtr, const uint64_t patternSize, int8_t* outputPtr)
{
    using namespace std::chrono;
    const sys_time<milliseconds> timePoint{milliseconds{static_cast<int64_t>(millis)}};
    const auto day = floor<days>(timePoint);
    const year_month_day date{day};
    const hh_mm_ss time{timePoint - day};

    /// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast) VARSIZED data is handed over as int8_t*
    const std::span pattern{reinterpret_cast<const char*>(patternPtr), patternSize};
    const std::span output{reinterpret_cast<char*>(outputPtr), patternSize * MAX_OUTPUT_BYTES_PER_PATTERN_BYTE};
    /// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)

    size_t in = 0;
    size_t out = 0;
    while (in < pattern.size())
    {
        const char letter = pattern[in];
        if (letter == '\'')
        {
            /// Quoted literal text; two consecutive quotes produce a single quote.
            if (in + 1 < pattern.size() && pattern[in + 1] == '\'')
            {
                output[out++] = '\'';
                in += 2;
                continue;
            }
            ++in;
            while (in < pattern.size())
            {
                if (pattern[in] == '\'')
                {
                    if (in + 1 < pattern.size() && pattern[in + 1] == '\'')
                    {
                        output[out++] = '\'';
                        in += 2;
                        continue;
                    }
                    ++in;
                    break;
                }
                output[out++] = pattern[in++];
            }
            continue;
        }

        size_t count = 1;
        while (in + count < pattern.size() && pattern[in + count] == letter)
        {
            ++count;
        }
        const auto target = output.subspan(out);
        switch (letter)
        {
            case 'y': {
                const auto year = static_cast<int64_t>(static_cast<int>(date.year()));
                out += count == 2 ? writeNumber(target, year % 100, 2) : writeNumber(target, year, count);
                break;
            }
            case 'M':
                out += writeNumber(target, static_cast<unsigned>(date.month()), count);
                break;
            case 'd':
                out += writeNumber(target, static_cast<unsigned>(date.day()), count);
                break;
            case 'H':
                out += writeNumber(target, time.hours().count(), count);
                break;
            case 'm':
                out += writeNumber(target, time.minutes().count(), count);
                break;
            case 's':
                out += writeNumber(target, time.seconds().count(), count);
                break;
            case 'S':
                out += writeNumber(target, time.subseconds().count(), count);
                break;
            default:
                /// Any other character is copied verbatim.
                for (size_t i = 0; i < count; ++i)
                {
                    output[out++] = letter;
                }
        }
        in += count;
    }
    return out;
}
}

DateFormatPhysicalFunction::DateFormatPhysicalFunction(PhysicalFunction timestampFunction, PhysicalFunction patternFunction)
    : timestampFunction(std::move(timestampFunction)), patternFunction(std::move(patternFunction))
{
}

VarVal DateFormatPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto timestamp = timestampFunction.execute(record, arena);
    const auto pattern = patternFunction.execute(record, arena);
    const auto nullable = timestamp.isNullable() or pattern.isNullable();
    nautilus::val<bool> isNull = false;
    if (timestamp.isNullable())
    {
        isNull = isNull or timestamp.isNull();
    }
    if (pattern.isNullable())
    {
        isNull = isNull or pattern.isNull();
    }

    const auto patternData = pattern.getRawValueAs<VariableSizedData>();
    const auto capacity = nautilus::select(
        isNull, nautilus::val<uint64_t>{0}, patternData.getSize() * nautilus::val<uint64_t>{MAX_OUTPUT_BYTES_PER_PATTERN_BYTE});
    const auto output = arena.allocateVariableSizedData(capacity);
    if (isNull)
    {
        return VarVal{output, nullable, isNull};
    }

    const auto size = nautilus::invoke(
        formatTimestamp,
        timestamp.getRawValueAs<nautilus::val<uint64_t>>(),
        patternData.getContent(),
        patternData.getSize(),
        output.getContent());
    return VarVal{VariableSizedData(output.getContent(), size), nullable, isNull};
}

/// NOLINTNEXTLINE(readability-identifier-naming)
PhysicalFunctionRegistryReturnType DateFormatPhysicalFunction::createDATE_FORMAT(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "DATE_FORMAT function must have exactly two child functions");
    return DateFormatPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1]);
}
}
