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

#include <Statistics/ScalarStatisticIterator.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/Record.hpp>
#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Statistics/StatisticIterator.hpp>
#include <ErrorHandling.hpp>
#include <StatisticIteratorRegistry.hpp>
#include <val_ptr.hpp>

namespace NES
{

ScalarStatisticIterator::ScalarStatisticIterator(
    StatisticBlobType typeName, DataType valueType, Record::RecordFieldIdentifier outputValueFieldName)
    : StatisticIterator(std::move(typeName)), valueType(std::move(valueType)), outputValueFieldName(std::move(outputValueFieldName))
{
}

std::shared_ptr<StatisticIterator> ScalarStatisticIterator::create(StatisticIteratorRegistryArguments arguments)
{
    if (arguments.payloadFields.size() != 1)
    {
        throw InvalidQuerySyntax(
            "{} reduces a window to a single value, so its probe declares exactly one column, e.g. {}_PROBE(id, "
            "<value> <type>); got {}",
            arguments.typeName.getRawValue(),
            arguments.typeName.getRawValue(),
            arguments.payloadFields.size());
    }
    auto& field = arguments.payloadFields.front();
    return std::make_shared<ScalarStatisticIterator>(std::move(arguments.typeName), std::move(field.type), std::move(field.name));
}

uint64_t ScalarStatisticIterator::getExpectedPayloadSizeInBytes() const
{
    return valueType.getSizeInBytesWithoutNull();
}

void ScalarStatisticIterator::forEachRecord(const nautilus::val<int8_t*>& payload, const std::function<void(Record&)>& emit) const
{
    Record statisticRecord;
    statisticRecord.write(outputValueFieldName, VarVal::readNonNullableVarValFromMemory(payload, valueType));
    emit(statisticRecord);
}

}
