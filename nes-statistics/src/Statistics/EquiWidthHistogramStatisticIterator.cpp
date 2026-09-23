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

#include <Statistics/EquiWidthHistogramStatisticIterator.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/Record.hpp>
#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Operators/Windows/Aggregations/EquiWidthHistogramAggregationLogicalFunction.hpp>
#include <Statistics/EquiWidthHistogramBlob.hpp>
#include <Statistics/StatisticIterator.hpp>
#include <ErrorHandling.hpp>
#include <StatisticIteratorRegistry.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

EquiWidthHistogramStatisticIterator::EquiWidthHistogramStatisticIterator(
    Record::RecordFieldIdentifier binStartFieldName,
    Record::RecordFieldIdentifier binCounterFieldName,
    Record::RecordFieldIdentifier binEndFieldName)
    : StatisticIterator(StatisticBlobType{EquiWidthHistogramAggregationLogicalFunction::NAME})
    , binStartFieldName(std::move(binStartFieldName))
    , binCounterFieldName(std::move(binCounterFieldName))
    , binEndFieldName(std::move(binEndFieldName))
{
}

std::shared_ptr<StatisticIterator> EquiWidthHistogramStatisticIterator::create(StatisticIteratorRegistryArguments arguments)
{
    static constexpr auto CALL = "EQUIWIDTHHISTOGRAM_PROBE(statisticId, <binStart> uint64, <binCounter> uint64, <binEnd> uint64)";
    auto& payloadFields = arguments.payloadFields;
    if (payloadFields.size() != 3)
    {
        throw InvalidQuerySyntax(
            "A histogram bin is a start, a counter and an end, so its probe declares exactly three columns, as {}; got {}",
            CALL,
            payloadFields.size());
    }
    for (const auto& field : payloadFields)
    {
        if (field.type.type != DataType::Type::UINT64 or field.type.nullable)
        {
            throw InvalidQuerySyntax(
                "A histogram bin is three non-nullable uint64 columns, as {}, but {} was declared as {}", CALL, field.name, field.type);
        }
    }
    return std::make_shared<EquiWidthHistogramStatisticIterator>(
        std::move(payloadFields[0].name), std::move(payloadFields[1].name), std::move(payloadFields[2].name));
}

uint64_t EquiWidthHistogramStatisticIterator::getExpectedPayloadSizeInBytes() const
{
    return 0;
}

void EquiWidthHistogramStatisticIterator::validate(const std::span<const int8_t> payload) const
{
    validateEquiWidthHistogramBlob(payload);
}

void EquiWidthHistogramStatisticIterator::forEachRecord(
    const nautilus::val<int8_t*>& payload, const std::function<void(Record&)>& emit) const
{
    /// validate() has already checked the header against the payload size, so the bin count can be trusted here.
    const EquiWidthHistogramBlobBinReader reader{payload};
    const auto numberOfBins = reader.getNumberOfBins();
    for (nautilus::val<uint64_t> bin = 0; bin < numberOfBins; bin = bin + 1)
    {
        Record binRecord;
        binRecord.write(binStartFieldName, VarVal{reader.getBinStart(bin)});
        binRecord.write(binCounterFieldName, VarVal{reader.getBinCounter(bin)});
        binRecord.write(binEndFieldName, VarVal{reader.getBinEnd(bin)});
        emit(binRecord);
    }
}

}
