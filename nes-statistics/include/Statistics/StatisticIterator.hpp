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

#include <cstdint>
#include <functional>
#include <span>
#include <DataTypes/DataType.hpp>
#include <Operators/Statistic/StatisticBlobType.hpp>

#include <Interface/Record.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// One decoded column of a statistic payload as the probe declared it, resolved to its physical field name.
/// A scalar statistic has exactly one; a synopsis has as many as its decoder emits per record.
struct StatisticPayloadField
{
    Record::RecordFieldIdentifier name;
    DataType type;
};

class StatisticIterator
{
public:
    explicit StatisticIterator(StatisticBlobType typeName);
    virtual ~StatisticIterator() = default;
    StatisticIterator(const StatisticIterator&) = delete;
    StatisticIterator& operator=(const StatisticIterator&) = delete;
    StatisticIterator(StatisticIterator&&) = delete;
    StatisticIterator& operator=(StatisticIterator&&) = delete;

    [[nodiscard]] const StatisticBlobType& getStatisticBlobType() const;

    /// The size every payload of this type has, or 0 for a payload whose size depends on its own header. The probe
    /// checks a fixed size against the stored blob before decoding it; a variable-size payload has to check itself
    /// in validate().
    [[nodiscard]] virtual uint64_t getExpectedPayloadSizeInBytes() const = 0;

    /// Called once per loaded statistic, before any traced code touches the payload. Throws CannotProbeStatistic if
    /// the bytes cannot be decoded by this iterator. forEachRecord is traced Nautilus code that only ever sees a
    /// pointer, so it can neither throw nor compare the payload against its own header; this is where a
    /// variable-size payload earns the right to be trusted there. The default accepts everything.
    virtual void validate(std::span<const int8_t> payload) const;

    virtual void forEachRecord(const nautilus::val<int8_t*>& payload, const std::function<void(Record&)>& emit) const = 0;

private:
    StatisticBlobType typeName;
};

}
