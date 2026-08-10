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

#include <DefaultTimeDeserializers.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/StructData.hpp>
#include <DataTypes/VarVal.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ValueDeserializer.hpp>
#include <ValueDeserializerRegistry.hpp>
#include <ValueDeserializerUtil.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>
#include <val_std.hpp>

namespace NES::DefaultTimeDeserializer
{
struct TruncateSpacesResult
{
    const int8_t* ptr;
    uint64_t size;
};

void truncateTrailingSpaces(const int8_t* ptr, const uint64_t size, TruncateSpacesResult* result)
{
    std::string_view uncutString{reinterpret_cast<const char*>(ptr), size};
    if (const auto lastNonWs = uncutString.find_last_not_of(" \t\n\r"); lastNonWs != std::string_view::npos)
    {
        uncutString = uncutString.substr(0, lastNonWs + 1);
    }
    result->ptr = reinterpret_cast<const int8_t*>(uncutString.data());
    result->size = uncutString.size();
}

int32_t getDaysSinceUnixEpoch(const int8_t* datePtr, const uint64_t dateSize)
{
    const std::string_view dateView{reinterpret_cast<const char*>(datePtr), dateSize};
    const int32_t year = (dateView[0] - '0') * 1000 + (dateView[1] - '0') * 100 + (dateView[2] - '0') * 10 + (dateView[3] - '0');
    const uint32_t month = (dateView[5] - '0') * 10 + (dateView[6] - '0');
    const uint32_t day = (dateView[8] - '0') * 10 + (dateView[9] - '0');

    const std::chrono::year_month_day date{std::chrono::year{year}, std::chrono::month{month}, std::chrono::day{day}};
    return std::chrono::sys_days{date}.time_since_epoch().count();
}

int64_t getMicroSecondsSinceMidnight(const int8_t* timePtr, const uint64_t timeSize)
{
    const std::string_view timeView{reinterpret_cast<const char*>(timePtr), timeSize};
    /// Get the hour
    const uint32_t hour = (timeView[0] - '0') * 10 + (timeView[1] - '0');
    /// Get the minutes
    const uint32_t minute = (timeView[3] - '0') * 10 + (timeView[4] - '0');
    /// Get the seconds
    const uint32_t seconds = (timeView[6] - '0') * 10 + (timeView[7] - '0');

    const std::string_view secondsView = timeView.substr(6);
    /// Check for a point
    const size_t dot = secondsView.find('.');
    uint32_t microseconds = 0;
    if (dot != std::string::npos)
    {
        const std::string_view fraction = secondsView.substr(dot + 1);
        /// We support up to microseconds of precision
        for (size_t i = 0; i < std::min(static_cast<size_t>(6), fraction.size()); ++i)
        {
            microseconds = microseconds * 10 + (fraction[i] - '0');
        }

        /// Pad with 0's for microseconds precision
        for (size_t i = fraction.size(); i < 6; i++)
        {
            microseconds *= 10;
        }
    }

    return microseconds + seconds * 1'000'000 + minute * 60'000'000 + hour * 3'600'000'000;
}

int64_t getMicroSecondsSinceUnixEpoch(const int8_t* timestampPtr, const uint64_t timestampSize)
{
    const std::string_view timeStampView{reinterpret_cast<const char*>(timestampPtr), timestampSize};
    const size_t gap = timeStampView.find(' ');
    const std::string_view datePart = timeStampView.substr(0, gap);
    const std::string_view timePart = timeStampView.substr(gap + 1);

    const int32_t daysSinceUnixEpoch = getDaysSinceUnixEpoch(reinterpret_cast<const int8_t*>(datePart.data()), datePart.size());
    const int64_t microSecondsSinceMidnight
        = getMicroSecondsSinceMidnight(reinterpret_cast<const int8_t*>(timePart.data()), timePart.size());

    return daysSinceUnixEpoch * 86'400'000'000 + microSecondsSinceMidnight;
}

template <typename T>
void writeFlatValToBuffer(int8_t* bufferAddress, T val)
{
    *reinterpret_cast<T*>(bufferAddress) = val;
}
}

namespace NES
{

VarVal DefaultDateValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const std::unordered_map<DeserializerKey, std::string>&,
    const DataType& valueType,
    ArenaRef& arena) const
{
    /// Remove quotes and cut trailing spaces if needed
    nautilus::val<const int8_t*> trueFieldAddress = fieldAddress;
    quoted ? fieldAddress + nautilus::val<uint64_t>{1} : fieldAddress;
    nautilus::val<uint64_t> trueFieldSize = fieldSize;
    quoted ? fieldSize - 2 : fieldSize;
    if (hasTrailingSpaces)
    {
        nautilus::val<DefaultTimeDeserializer::TruncateSpacesResult> truncatedField;
        nautilus::invoke(DefaultTimeDeserializer::truncateTrailingSpaces, fieldAddress, fieldSize, &truncatedField);
        trueFieldAddress = truncatedField.get(&DefaultTimeDeserializer::TruncateSpacesResult::ptr);
        trueFieldSize = truncatedField.get(&DefaultTimeDeserializer::TruncateSpacesResult::size);
    }
    trueFieldAddress = quoted ? trueFieldAddress + nautilus::val<uint64_t>{1} : trueFieldAddress;
    trueFieldSize = quoted ? trueFieldSize - 2 : trueFieldSize;
    const nautilus::val<int32_t> daysSinceEpoch
        = nautilus::invoke(DefaultTimeDeserializer::getDaysSinceUnixEpoch, trueFieldAddress, trueFieldSize);

    /// Create a buffer for the value
    const nautilus::val<int8_t*> buffer = arena.allocateMemory(valueType.getSizeInBytesWithoutNull());
    nautilus::invoke(DefaultTimeDeserializer::writeFlatValToBuffer<int32_t>, buffer, daysSinceEpoch);
    const StructData structData{buffer, valueType.fields};
    return VarVal{structData, false, false};
}

void DefaultDateValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const std::unordered_map<DeserializerKey, std::string>&,
    const DataType&,
    ArenaRef&,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    /// Remove quotes and cut trailing spaces if needed
    nautilus::val<const int8_t*> trueFieldAddress = fieldAddress;
    quoted ? fieldAddress + nautilus::val<uint64_t>{1} : fieldAddress;
    nautilus::val<uint64_t> trueFieldSize = fieldSize;
    quoted ? fieldSize - 2 : fieldSize;
    if (hasTrailingSpaces)
    {
        nautilus::val<DefaultTimeDeserializer::TruncateSpacesResult> truncatedField;
        nautilus::invoke(DefaultTimeDeserializer::truncateTrailingSpaces, fieldAddress, fieldSize, &truncatedField);
        trueFieldAddress = truncatedField.get(&DefaultTimeDeserializer::TruncateSpacesResult::ptr);
        trueFieldSize = truncatedField.get(&DefaultTimeDeserializer::TruncateSpacesResult::size);
    }
    trueFieldAddress = quoted ? trueFieldAddress + nautilus::val<uint64_t>{1} : trueFieldAddress;
    trueFieldSize = quoted ? trueFieldSize - 2 : trueFieldSize;
    const nautilus::val<int32_t> daysSinceEpoch
        = nautilus::invoke(DefaultTimeDeserializer::getDaysSinceUnixEpoch, trueFieldAddress, trueFieldSize);

    nautilus::invoke(DefaultTimeDeserializer::writeFlatValToBuffer<int32_t>, bufferAddress, daysSinceEpoch);
}

VarVal DefaultTimeValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const std::unordered_map<DeserializerKey, std::string>&,
    const DataType& valueType,
    ArenaRef& arena) const
{
    /// Remove quotes and cut trailing spaces if needed
    nautilus::val<const int8_t*> trueFieldAddress = fieldAddress;
    quoted ? fieldAddress + nautilus::val<uint64_t>{1} : fieldAddress;
    nautilus::val<uint64_t> trueFieldSize = fieldSize;
    quoted ? fieldSize - 2 : fieldSize;
    if (hasTrailingSpaces)
    {
        nautilus::val<DefaultTimeDeserializer::TruncateSpacesResult> truncatedField;
        nautilus::invoke(DefaultTimeDeserializer::truncateTrailingSpaces, fieldAddress, fieldSize, &truncatedField);
        trueFieldAddress = truncatedField.get(&DefaultTimeDeserializer::TruncateSpacesResult::ptr);
        trueFieldSize = truncatedField.get(&DefaultTimeDeserializer::TruncateSpacesResult::size);
    }
    trueFieldAddress = quoted ? trueFieldAddress + nautilus::val<uint64_t>{1} : trueFieldAddress;
    trueFieldSize = quoted ? trueFieldSize - 2 : trueFieldSize;
    const nautilus::val<int64_t> microSecondsSinceMidnight
        = nautilus::invoke(DefaultTimeDeserializer::getMicroSecondsSinceMidnight, trueFieldAddress, trueFieldSize);

    /// Create a buffer for the value
    const nautilus::val<int8_t*> buffer = arena.allocateMemory(valueType.getSizeInBytesWithoutNull());
    nautilus::invoke(DefaultTimeDeserializer::writeFlatValToBuffer<int64_t>, buffer, microSecondsSinceMidnight);
    const StructData structData{buffer, valueType.fields};
    return VarVal{structData, false, false};
}

void DefaultTimeValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const std::unordered_map<DeserializerKey, std::string>&,
    const DataType&,
    ArenaRef&,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    /// Remove quotes and cut trailing spaces if needed
    nautilus::val<const int8_t*> trueFieldAddress = fieldAddress;
    quoted ? fieldAddress + nautilus::val<uint64_t>{1} : fieldAddress;
    nautilus::val<uint64_t> trueFieldSize = fieldSize;
    quoted ? fieldSize - 2 : fieldSize;
    if (hasTrailingSpaces)
    {
        nautilus::val<DefaultTimeDeserializer::TruncateSpacesResult> truncatedField;
        nautilus::invoke(DefaultTimeDeserializer::truncateTrailingSpaces, fieldAddress, fieldSize, &truncatedField);
        trueFieldAddress = truncatedField.get(&DefaultTimeDeserializer::TruncateSpacesResult::ptr);
        trueFieldSize = truncatedField.get(&DefaultTimeDeserializer::TruncateSpacesResult::size);
    }
    trueFieldAddress = quoted ? trueFieldAddress + nautilus::val<uint64_t>{1} : trueFieldAddress;
    trueFieldSize = quoted ? trueFieldSize - 2 : trueFieldSize;
    const nautilus::val<int64_t> microSecondsSinceMidnight
        = nautilus::invoke(DefaultTimeDeserializer::getMicroSecondsSinceMidnight, trueFieldAddress, trueFieldSize);

    nautilus::invoke(DefaultTimeDeserializer::writeFlatValToBuffer<int64_t>, bufferAddress, microSecondsSinceMidnight);
}

VarVal DefaultTimestampValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const std::unordered_map<DeserializerKey, std::string>&,
    const DataType& valueType,
    ArenaRef& arena) const
{
    /// Remove quotes and cut trailing spaces if needed
    nautilus::val<const int8_t*> trueFieldAddress = fieldAddress;
    quoted ? fieldAddress + nautilus::val<uint64_t>{1} : fieldAddress;
    nautilus::val<uint64_t> trueFieldSize = fieldSize;
    quoted ? fieldSize - 2 : fieldSize;
    if (hasTrailingSpaces)
    {
        nautilus::val<DefaultTimeDeserializer::TruncateSpacesResult> truncatedField;
        nautilus::invoke(DefaultTimeDeserializer::truncateTrailingSpaces, fieldAddress, fieldSize, &truncatedField);
        trueFieldAddress = truncatedField.get(&DefaultTimeDeserializer::TruncateSpacesResult::ptr);
        trueFieldSize = truncatedField.get(&DefaultTimeDeserializer::TruncateSpacesResult::size);
    }
    trueFieldAddress = quoted ? trueFieldAddress + nautilus::val<uint64_t>{1} : trueFieldAddress;
    trueFieldSize = quoted ? trueFieldSize - 2 : trueFieldSize;
    const nautilus::val<int64_t> microSecondsSinceUnixEpoch
        = nautilus::invoke(DefaultTimeDeserializer::getMicroSecondsSinceUnixEpoch, trueFieldAddress, trueFieldSize);

    const nautilus::val<int8_t*> buffer = arena.allocateMemory(valueType.getSizeInBytesWithoutNull());
    nautilus::invoke(DefaultTimeDeserializer::writeFlatValToBuffer<int64_t>, buffer, microSecondsSinceUnixEpoch);
    const StructData structData{buffer, valueType.fields};
    return VarVal{structData, false, false};
}

void DefaultTimestampValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const std::unordered_map<DeserializerKey, std::string>&,
    const DataType&,
    ArenaRef&,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    /// Remove quotes and cut trailing spaces if needed
    nautilus::val<const int8_t*> trueFieldAddress = fieldAddress;
    quoted ? fieldAddress + nautilus::val<uint64_t>{1} : fieldAddress;
    nautilus::val<uint64_t> trueFieldSize = fieldSize;
    quoted ? fieldSize - 2 : fieldSize;
    if (hasTrailingSpaces)
    {
        nautilus::val<DefaultTimeDeserializer::TruncateSpacesResult> truncatedField;
        nautilus::invoke(DefaultTimeDeserializer::truncateTrailingSpaces, fieldAddress, fieldSize, &truncatedField);
        trueFieldAddress = truncatedField.get(&DefaultTimeDeserializer::TruncateSpacesResult::ptr);
        trueFieldSize = truncatedField.get(&DefaultTimeDeserializer::TruncateSpacesResult::size);
    }
    trueFieldAddress = quoted ? trueFieldAddress + nautilus::val<uint64_t>{1} : trueFieldAddress;
    trueFieldSize = quoted ? trueFieldSize - 2 : trueFieldSize;
    const nautilus::val<int64_t> microSecondsSinceUnixEpoch
        = nautilus::invoke(DefaultTimeDeserializer::getMicroSecondsSinceUnixEpoch, trueFieldAddress, trueFieldSize);

    nautilus::invoke(DefaultTimeDeserializer::writeFlatValToBuffer<int64_t>, bufferAddress, microSecondsSinceUnixEpoch);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultDateValueDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<DefaultDateValueDeserializer>(args.quoted, args.hasTrailingSpaces);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultTimeValueDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<DefaultTimeValueDeserializer>(args.quoted, args.hasTrailingSpaces);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultTimestampValueDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<DefaultTimestampValueDeserializer>(args.quoted, args.hasTrailingSpaces);
}
}
