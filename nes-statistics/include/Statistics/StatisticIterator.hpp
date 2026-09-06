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
#include <Operators/Statistic/StatisticBlobType.hpp>

#include <Interface/Record.hpp>
#include <val_ptr.hpp>

namespace NES
{

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

    [[nodiscard]] virtual uint64_t getExpectedPayloadSizeInBytes() const = 0;

    virtual void forEachRecord(const nautilus::val<int8_t*>& payload, const std::function<void(Record&)>& emit) const = 0;

private:
    StatisticBlobType typeName;
};

}
