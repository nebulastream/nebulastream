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
#include <memory>
#include <span>

#include <Interface/Record.hpp>
#include <Statistics/StatisticIterator.hpp>
#include <val_ptr.hpp>

namespace NES
{

struct StatisticIteratorRegistryArguments;

/// Decodes a stored equi-width histogram into one record per bin: [binStart, binCounter, binEnd]. The bounds are
/// computed from the blob header rather than read out of it, which is why the blob does not store them.
///
/// binEnd is exclusive except for the last bin, where it is maxValue and included; see EquiWidthHistogramBlobBinReader
/// for what that means for the width of the last bin.
class EquiWidthHistogramStatisticIterator final : public StatisticIterator
{
public:
    EquiWidthHistogramStatisticIterator(
        Record::RecordFieldIdentifier binStartFieldName,
        Record::RecordFieldIdentifier binCounterFieldName,
        Record::RecordFieldIdentifier binEndFieldName);

    /// Throws InvalidQuerySyntax unless the probe declared exactly the three UINT64 columns a bin has. The names are
    /// the caller's; the position is what gives each one its meaning.
    static std::shared_ptr<StatisticIterator> create(StatisticIteratorRegistryArguments arguments);

    /// 0: the payload size follows from the blob's own header, so validate() is what checks it.
    [[nodiscard]] uint64_t getExpectedPayloadSizeInBytes() const override;
    void validate(std::span<const int8_t> payload) const override;
    void forEachRecord(const nautilus::val<int8_t*>& payload, const std::function<void(Record&)>& emit) const override;

private:
    Record::RecordFieldIdentifier binStartFieldName;
    Record::RecordFieldIdentifier binCounterFieldName;
    Record::RecordFieldIdentifier binEndFieldName;
};

}
