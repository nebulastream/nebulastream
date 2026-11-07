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
#include <DataTypes/TimeUnit.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>

namespace NES
{
struct ExecutionContext;
}

namespace NES
{

/// @brief A time function, infers the timestamp of an record.
/// For ingestion time, this is determined by the creation ts in the buffer.
/// For event time, this is inferred by a field in the record.
class TimeFunction
{
public:
    virtual void open(ExecutionContext& ctx, RecordBuffer& buffer) const = 0;
    virtual nautilus::val<Timestamp> getTs(ExecutionContext& ctx, Record& record) const = 0;
    virtual ~TimeFunction() = default;

    [[nodiscard]] virtual std::unique_ptr<TimeFunction> clone() const = 0;
    [[nodiscard]] static std::unique_ptr<TimeFunction> create(const Windowing::BoundTimeCharacteristic& timeCharacteristic);
};

class EventTimeFunction final : public TimeFunction
{
public:
    explicit EventTimeFunction(PhysicalFunction timestampFunction, const Windowing::TimeUnit& unit);
    void open(ExecutionContext& ctx, RecordBuffer& buffer) const override;
    nautilus::val<Timestamp> getTs(ExecutionContext& ctx, Record& record) const override;

    [[nodiscard]] std::unique_ptr<TimeFunction> clone() const override
    {
        return std::make_unique<EventTimeFunction>(timestampFunction, unit);
    }

private:
    Windowing::TimeUnit unit;
    PhysicalFunction timestampFunction;
};

class IngestionTimeFunction final : public TimeFunction
{
public:
    void open(ExecutionContext& ctx, RecordBuffer& buffer) const override;
    nautilus::val<Timestamp> getTs(ExecutionContext& ctx, Record& record) const override;

    [[nodiscard]] std::unique_ptr<TimeFunction> clone() const override { return std::make_unique<IngestionTimeFunction>(); }
};

/// Clamps the timestamp of another time function to the watermark of the record's buffer. An unbounded operator uses its slices
/// only to batch its output, so a record whose timestamp lies behind the watermark (e.g. an unbounded join emits matches with the
/// timestamps of its older inputs) goes to the earliest slice that cannot have been triggered yet instead of being out of order.
/// The watermark of a buffer is at least that of every earlier buffer of its origin, so no slice at or after it has been triggered.
class WatermarkClampedTimeFunction final : public TimeFunction
{
public:
    explicit WatermarkClampedTimeFunction(std::unique_ptr<TimeFunction> timeFunction);
    void open(ExecutionContext& ctx, RecordBuffer& buffer) const override;
    nautilus::val<Timestamp> getTs(ExecutionContext& ctx, Record& record) const override;
    [[nodiscard]] std::unique_ptr<TimeFunction> clone() const override;

private:
    std::unique_ptr<TimeFunction> timeFunction;
};

}
