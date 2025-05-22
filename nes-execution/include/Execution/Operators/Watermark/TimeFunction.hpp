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

#include <Execution/Functions/Function.hpp>
#include <Measures/TimeUnit.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Time/Timestamp.hpp>

namespace NES::Runtime::Execution
{
struct ExecutionContext;
}

namespace NES::Runtime::Execution::Operators
{
using namespace Nautilus;

/// @brief A time function, infers the timestamp of an record.
/// For ingestion time, this is determined by the creation ts in the buffer.
/// For event time, this is inferred by a field in the record.
class TimeFunction
{
public:
    virtual void open(Execution::ExecutionContext& ctx, RecordBuffer& buffer) = 0;
    virtual nautilus::val<Timestamp> getTs(Execution::ExecutionContext& ctx, Record& record) = 0;
    virtual ~TimeFunction() = default;
};

/**
 * @brief Time function for event time windows
 */
class EventTimeFunction final : public TimeFunction
{
public:
    explicit EventTimeFunction(std::unique_ptr<Functions::Function> timestampFunction, Windowing::TimeUnit unit);
    void open(Execution::ExecutionContext& ctx, RecordBuffer& buffer) override;
    nautilus::val<Timestamp> getTs(Execution::ExecutionContext& ctx, Record& record) override;

private:
    Windowing::TimeUnit unit;
    std::unique_ptr<Functions::Function> timestampFunction;
};

/**
 * @brief Time function for ingestion time windows
 */
class IngestionTimeFunction final : public TimeFunction
{
public:
    void open(ExecutionContext& ctx, RecordBuffer& buffer) override;
    nautilus::val<Timestamp> getTs(ExecutionContext& ctx, Record& record) override;
};

}
