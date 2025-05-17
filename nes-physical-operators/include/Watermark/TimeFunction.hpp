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
#include <API/TimeUnit.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>


namespace NES
{
struct ExecutionContext;
}

namespace NES
{
using namespace Nautilus;

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

}
