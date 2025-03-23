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

#include <Execution/Operators/Streaming/Aggregation/Function/Synopsis/Sample/SampleFunction.hpp>
#include <Util/Common.hpp>

namespace NES::Runtime::Execution::Aggregation::Synopsis
{

SampleFunction::SampleFunction(
    std::shared_ptr<PhysicalType> inputType,
    std::shared_ptr<PhysicalType> resultType,
    std::unique_ptr<Functions::Function> inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector,
    const uint64_t sampleSize)
    : AggregationFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , memProviderPagedVector(std::move(memProviderPagedVector))
    , sampleSize(sampleSize)
    , sampleDataSize(0)
{
}

nautilus::val<uint64_t> SampleFunction::getRecordDataSize(const Record& record) const
{
    const auto schema = memProviderPagedVector->getMemoryLayout()->getSchema();
    auto recordDataSize = nautilus::val<uint64_t>(schema->getSchemaSizeInBytes());

    for (const auto& fieldName : schema->getFieldNames())
    {
        const auto* const field = schema->getFieldByName(fieldName)->get();

        if (NES::Util::instanceOf<VariableSizedData>(field->getDataType()))
        {
            const auto textValue = record.read(fieldName).cast<VariableSizedData>();
            recordDataSize += textValue.getSize();
        }
    }

    return recordDataSize;
}

}
