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

#include <DefaultTimeSerializers.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <simdjson.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/StructData.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <OutputFormatters/ValueSerializer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ValueSerializerRegistry.hpp>
#include <function.hpp>
#include <select.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES::DefaultTimeSerializer
{
/// Serialize the number of days since unix epoch to a year-month-day date
uint64_t serializeDate(
    const int32_t daysSinceUnixEpoch,
    const bool quoted,
    const uint64_t remainingSpace,
    TupleBuffer* buffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress)
{
    /// Use chrono to convert days since epoch to year-month-day format
    std::chrono::sys_days date{std::chrono::days{daysSinceUnixEpoch}};
    std::chrono::year_month_day ymd{date};

    const std::string dateString = std::format("{:%Y-%m-%d}", ymd);
    const std::string finalString = quoted ? "\"" + dateString + "\"" : dateString;
    return writeValueToBuffer(finalString.data(), finalString.size(), remainingSpace, buffer, bufferProvider, bufferStartingAddress);
}

uint64_t serializeTime(
    const int64_t microSecondsSinceMidnight,
    const bool quoted,
    const uint64_t remainingSpace,
    TupleBuffer* buffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress)
{
    /// Use chrono to convert micro seconds since midnight to hour:minute:second format
    std::chrono::microseconds microSecs{microSecondsSinceMidnight};

    const std::chrono::hours hours = duration_cast<std::chrono::hours>(microSecs);
    microSecs -= hours;

    const std::chrono::minutes minutes = duration_cast<std::chrono::minutes>(microSecs);
    microSecs -= minutes;

    const std::chrono::seconds seconds = duration_cast<std::chrono::seconds>(microSecs);
    microSecs -= seconds;

    if (microSecs.count() == 0)
    {
        const std::string timeString = std::format("{:02}:{:02}:{:02}", hours.count(), minutes.count(), seconds.count());
        const std::string finalString = quoted ? "\"" + timeString + "\"" : timeString;
        return writeValueToBuffer(finalString.data(), finalString.size(), remainingSpace, buffer, bufferProvider, bufferStartingAddress);
    }
    const std::string timeString
        = std::format("{:02}:{:02}:{:02}.{:06}", hours.count(), minutes.count(), seconds.count(), microSecs.count());
    const std::string finalString = quoted ? "\"" + timeString + "\"" : timeString;
    return writeValueToBuffer(finalString.data(), finalString.size(), remainingSpace, buffer, bufferProvider, bufferStartingAddress);
}

uint64_t serializeTimestamp(
    int64_t microSecondsSinceUnixEpoch,
    const bool quoted,
    const uint64_t remainingSpace,
    TupleBuffer* buffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress)
{
    /// Write leading quote if needed
    uint64_t written = 0;
    if (quoted)
    {
        written += writeValueToBuffer("\"", 1, remainingSpace - written, buffer, bufferProvider, bufferStartingAddress + written);
    }
    /// Write date
    const int32_t daysSinceEpoch = microSecondsSinceUnixEpoch / 86'400'000'000;
    written += serializeDate(daysSinceEpoch, false, remainingSpace - written, buffer, bufferProvider, bufferStartingAddress + written);

    /// Write blank
    written += writeValueToBuffer(" ", 1, remainingSpace - written, buffer, bufferProvider, bufferStartingAddress + written);

    /// Write time
    const int64_t microSecondsSinceMidnight = microSecondsSinceUnixEpoch % 86'400'000'000;
    written += serializeTime(
        microSecondsSinceMidnight, false, remainingSpace - written, buffer, bufferProvider, bufferStartingAddress + written);

    /// Write ending quote if needed
    if (quoted)
    {
        written += writeValueToBuffer("\"", 1, remainingSpace - written, buffer, bufferProvider, bufferStartingAddress + written);
    }
    return written;
}
}

namespace NES
{
nautilus::val<uint64_t> DefaultDateValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<SerializerKey, std::string>&,
    const DataType&) const
{
    const StructData castedVal = value.getRawValueAs<StructData>();
    const VarVal daysSinceUnixEpoch = castedVal.at(0);
    const nautilus::val<int32_t> daysAsNVal = daysSinceUnixEpoch.getRawValueAs<nautilus::val<int32_t>>();
    return nautilus::invoke(
        DefaultTimeSerializer::serializeDate,
        daysAsNVal,
        nautilus::val<bool>{quoted},
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider,
        startingAddress);
}

nautilus::val<uint64_t> DefaultTimeValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<SerializerKey, std::string>&,
    const DataType&) const
{
    const StructData castedVal = value.getRawValueAs<StructData>();
    const VarVal microSecondsSinceMidnight = castedVal.at(0);
    const nautilus::val<int64_t> msAsNVal = microSecondsSinceMidnight.getRawValueAs<nautilus::val<int64_t>>();
    return nautilus::invoke(
        DefaultTimeSerializer::serializeTime,
        msAsNVal,
        nautilus::val<bool>{quoted},
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider,
        startingAddress);
}

nautilus::val<uint64_t> DefaultTimestampValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<SerializerKey, std::string>&,
    const DataType&) const
{
    const StructData castedVal = value.getRawValueAs<StructData>();
    const VarVal microSecondsSinceUnixEpoch = castedVal.at(0);
    const nautilus::val<int64_t> msAsNVal = microSecondsSinceUnixEpoch.getRawValueAs<nautilus::val<int64_t>>();
    return nautilus::invoke(
        DefaultTimeSerializer::serializeTimestamp,
        msAsNVal,
        nautilus::val<bool>{quoted},
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider,
        startingAddress);
}

ValueSerializerRegistryReturnType
ValueSerializerGeneratedRegistrar::RegisterDefaultDateValueSerializer(ValueSerializerRegistryArguments args)
{
    return std::make_unique<DefaultDateValueSerializer>(args.quoted);
}

ValueSerializerRegistryReturnType
ValueSerializerGeneratedRegistrar::RegisterDefaultTimeValueSerializer(ValueSerializerRegistryArguments args)
{
    return std::make_unique<DefaultTimeValueSerializer>(args.quoted);
}

ValueSerializerRegistryReturnType
ValueSerializerGeneratedRegistrar::RegisterDefaultTimestampValueSerializer(ValueSerializerRegistryArguments args)
{
    return std::make_unique<DefaultTimestampValueSerializer>(args.quoted);
}
}
