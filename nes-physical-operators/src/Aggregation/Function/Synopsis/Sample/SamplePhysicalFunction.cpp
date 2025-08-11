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

#include <Aggregation/Function/Synopsis/Sample/SamplePhysicalFunction.hpp>

#include <DataTypes/DataType.hpp>
#include <Util/Common.hpp>

namespace NES
{

SamplePhysicalFunction::SamplePhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector,
    const uint64_t sampleSize,
    const Schema& sampleSchema)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , memProviderPagedVector(std::move(memProviderPagedVector))
    , sampleSize(sampleSize)
    , sampleSchema(sampleSchema)
{
}

nautilus::val<uint64_t> SamplePhysicalFunction::getRecordDataSize(const Record& record) const
{
    const auto schema = memProviderPagedVector->getMemoryLayout()->getSchema();
    auto recordDataSize = nautilus::val<uint64_t>(schema.getSizeOfSchemaInBytes());

    for (const auto& fieldName : schema.getFieldNames())
    {
        const auto field = schema.getFieldByName(fieldName).value();

        if (field.dataType.type == DataType::Type::VARSIZED)
        {
            const auto textValue = record.read(fieldName).cast<VariableSizedData>();
            recordDataSize += textValue.getContentSize();
        }
    }

    return recordDataSize;
}

}
