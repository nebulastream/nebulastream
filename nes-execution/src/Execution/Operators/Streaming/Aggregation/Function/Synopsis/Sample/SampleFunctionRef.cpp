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

#include <Execution/Operators/Streaming/Aggregation/Function/Synopsis/Sample/SampleFunctionRef.hpp>
#include <Util/Common.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/VariableSizedDataPhysicalType.hpp>

namespace NES::Runtime::Execution::Aggregation::Synopsis
{

uint32_t storeVarSizedDataProxy(int8_t* memArea, int8_t* dataRef)
{
    const auto dataSize = *reinterpret_cast<uint32_t*>(dataRef);
    std::memcpy(memArea, dataRef, dataSize + sizeof(uint32_t));
    return dataSize;
}

SampleFunctionRef::SampleFunctionRef(
    ArenaRef& arena,
    const std::shared_ptr<Schema>& schema,
    const nautilus::val<uint64_t>& sampleSize,
    const nautilus::val<uint64_t>& sampleDataSize)
    : schema(schema), sampleSize(sampleSize)
{
    /// Set sampleDataSize member as given dataSize plus size of sample metadata
    this->sampleDataSize = sampleDataSize + nautilus::val<uint64_t>(sizeof(uint64_t));

    /// Allocate space for length field of VariableSizedData, as well as the calculated sampleDataSize
    memArea = arena.allocateMemory(nautilus::val<uint64_t>(sizeof(uint32_t)) + this->sampleDataSize);
    currWritePtr = memArea;

    /// Store the sampleDataSize in the first 4 bytes of the memArea to interpret it as VariableSizedData later
    const VarVal dataSizeVarVal(this->sampleDataSize);
    dataSizeVarVal.writeToMemory(currWritePtr);
    currWritePtr += nautilus::val<uint64_t>(sizeof(uint32_t));

    /// Store the sampleSize as metadata in the next 8 bytes of the sample
    const VarVal metadataVarVal{sampleSize};
    metadataVarVal.writeToMemory(currWritePtr);
    currWritePtr += nautilus::val<uint64_t>(sizeof(uint64_t));

    currReadPtr = currWritePtr;
}

SampleFunctionRef::SampleFunctionRef(const VariableSizedData& sample, const std::shared_ptr<Schema>& schema) : schema(schema)
{
    memArea = sample.getReference();
    currReadPtr = memArea;

    /// Get sampleDataSize from the first 4 bytes of the VariableSizedData object
    sampleDataSize = Nautilus::Util::readValueFromMemRef<uint32_t>(currReadPtr);
    currReadPtr += nautilus::val<uint64_t>(sizeof(uint32_t));

    /// Get sampleSize from metadata from the next 8 bytes of the sample
    sampleSize = Nautilus::Util::readValueFromMemRef<uint64_t>(currReadPtr);
    currReadPtr += nautilus::val<uint64_t>(sizeof(uint64_t));

    currWritePtr = currReadPtr;
}

void SampleFunctionRef::writeRecord(const Record& record)
{
    if (currWritePtr >= memArea + nautilus::val<uint64_t>(sizeof(uint32_t)) + sampleDataSize)
    {
        throw UnsupportedOperation("Write operation attempts to write beyond allocated memory area");
    }

    const auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    for (const auto& fieldName : schema->getFieldNames())
    {
        const auto* const field = schema->getFieldByName(fieldName)->get();
        const auto dataType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        const auto& value = record.read(fieldName);

        if (NES::Util::instanceOf<VariableSizedDataPhysicalType>(dataType))
        {
            const auto textValue = value.cast<VariableSizedData>();
            currWritePtr += invoke(storeVarSizedDataProxy, currWritePtr, textValue.getReference());
        }
        else if (NES::Util::instanceOf<BasicPhysicalType>(dataType))
        {
            const auto storeFunction = Nautilus::Util::storeValueFunctionMap.find(NES::Util::as<BasicPhysicalType>(dataType)->nativeType);
            (void)storeFunction->second(value, currWritePtr);
        }

        currWritePtr += nautilus::val<uint64_t>(dataType->size());
    }
}

Record SampleFunctionRef::readRecord()
{
    if (currReadPtr >= memArea + nautilus::val<uint64_t>(sizeof(uint32_t)) + sampleDataSize)
    {
        throw UnsupportedOperation("Read operation attempts to read beyond allocated memory area");
    }

    Record record;
    const auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    for (const auto& fieldName : schema->getFieldNames())
    {
        const auto* const field = schema->getFieldByName(fieldName)->get();
        const auto dataType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        if (NES::Util::instanceOf<VariableSizedDataPhysicalType>(dataType))
        {
            const auto varSizedData = VariableSizedData(currReadPtr);
            record.write(fieldName, VarVal(varSizedData));
            currReadPtr += varSizedData.getSize();
        }
        else if (NES::Util::instanceOf<BasicPhysicalType>(dataType))
        {
            record.write(fieldName, VarVal::readVarValFromMemory(currReadPtr, dataType));
        }

        currReadPtr += nautilus::val<uint64_t>(dataType->size());
    }

    return record;
}

void SampleFunctionRef::setReadPtrToPos(const nautilus::val<uint64_t>& pos)
{
    if (pos >= sampleSize)
    {
        throw UnsupportedOperation("Position exceeds sample size");
    }

    /// Set currReadPtr to address of first record
    currReadPtr = memArea + nautilus::val<uint64_t>(sizeof(uint32_t) + sizeof(uint64_t));
    const auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    for (auto recordIdx = nautilus::val<uint64_t>(0UL); recordIdx < pos; ++recordIdx)
    {
        for (const auto& fieldName : schema->getFieldNames())
        {
            const auto* const field = schema->getFieldByName(fieldName)->get();
            const auto dataType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

            if (NES::Util::instanceOf<VariableSizedDataPhysicalType>(dataType))
            {
                const auto textSize = Nautilus::Util::readValueFromMemRef<uint32_t>(currReadPtr);
                currReadPtr += textSize;
            }

            currReadPtr += nautilus::val<uint64_t>(dataType->size());
        }
    }
}

VarVal SampleFunctionRef::getSample() const
{
    return VarVal(VariableSizedData(memArea));
}

}
