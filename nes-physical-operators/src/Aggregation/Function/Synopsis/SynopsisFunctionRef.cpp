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

#include <Aggregation/Function/Synopsis/SynopsisFunctionRef.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

uint32_t storeVarSizedDataProxy(int8_t* memArea, int8_t* dataRef)
{
    const auto dataSize = *reinterpret_cast<uint32_t*>(dataRef);
    std::memcpy(memArea, dataRef, dataSize + sizeof(uint32_t));
    return dataSize;
}

SynopsisFunctionRef::SynopsisFunctionRef(const Schema& schema) : schema(schema)
{
}

void SynopsisFunctionRef::initializeForWriting(
    ArenaRef& arena,
    const nautilus::val<uint32_t>& synopsisDataSize,
    const nautilus::val<uint32_t>& synopsisMetaDataSize,
    const VarVal& synopsisMetaData)
{
    /// Allocate space for length field of VariableSizedData and synopsisMetaDataSize, as well as the actual synopsisMetaData and synopsisData
    totalMemAreaSize = nautilus::val<uint64_t>(2 * sizeof(uint32_t)) + synopsisDataSize + synopsisMetaDataSize;
    startOfMemArea = arena.allocateMemory(totalMemAreaSize);
    currWritePtr = startOfMemArea;

    /// Store the accumulated synopsisDataSize and synopsisMetaDataSize in the first 4 bytes of the startOfMemArea to interpret it as VariableSizedData later
    const VarVal totalSynopsisSizeVarVal(totalMemAreaSize - nautilus::val<uint64_t>(sizeof(uint32_t)));
    totalSynopsisSizeVarVal.writeToMemory(currWritePtr);
    currWritePtr += nautilus::val<uint64_t>(sizeof(uint32_t));

    /// Store the synopsisMetaDataSize in the next 4 bytes of the startOfMemArea
    const VarVal metaDataSizeVarVal(synopsisMetaDataSize);
    metaDataSizeVarVal.writeToMemory(currWritePtr);
    currWritePtr += nautilus::val<uint64_t>(sizeof(uint32_t));

    /// Store the synopsisMetaData in the next bytes
    synopsisMetaData.writeToMemory(currWritePtr);
    currWritePtr += synopsisMetaDataSize;

    currReadPtr = currWritePtr;
    isInitializedForWriting = true;
}

void SynopsisFunctionRef::initializeForReading(const nautilus::val<int8_t*>& memArea)
{
    /// Get total synopsis size (including synopsisMetaDataSize and synopsisMetaData) from the first 4 bytes of the VariableSizedData object
    startOfMemArea = memArea;
    currReadPtr = startOfMemArea;
    totalMemAreaSize = Nautilus::Util::readValueFromMemRef<uint32_t>(currReadPtr) + nautilus::val<uint64_t>(sizeof(uint32_t));
    currReadPtr += nautilus::val<uint64_t>(sizeof(uint32_t));

    /// Get synopsisMetaDataSize from the next 4 bytes of the VariableSizedData object
    const auto synopsisMetaDataSize = Nautilus::Util::readValueFromMemRef<uint32_t>(currReadPtr);
    currReadPtr += nautilus::val<uint64_t>(sizeof(uint32_t)) + synopsisMetaDataSize;

    currWritePtr = currReadPtr;
    isInitializedForReading = true;
}

void SynopsisFunctionRef::writeRecord(const Record& record)
{
    PRECONDITION(isInitializedForWriting, "SynopsisFunctionRef is not initialized for writing. Call initializeForWriting() first.");
    if (currWritePtr >= startOfMemArea + totalMemAreaSize)
    {
        /// TODO Change to some Bug exception (all throws in this file)
        throw UnknownOperation("Write operation attempts to write beyond allocated memory area");
    }

    for (const auto& fieldName : schema.getFieldNames())
    {
        const auto field = schema.getFieldByName(fieldName).value();
        const auto dataType = field.dataType;
        const auto& value = record.read(fieldName);

        if (dataType.type == DataType::Type::VARSIZED)
        {
            const auto textValue = value.cast<VariableSizedData>();
            currWritePtr += invoke(storeVarSizedDataProxy, currWritePtr, textValue.getReference());
        }
        else
        {
            const auto storeFunction = Nautilus::Util::storeValueFunctionMap.find(dataType.type);
            (void)storeFunction->second(value, currWritePtr);
        }

        currWritePtr += nautilus::val<uint64_t>(dataType.getSizeInBytes());
    }
}

Record SynopsisFunctionRef::readNextRecord()
{
    PRECONDITION(isInitializedForReading, "SynopsisFunctionRef is not initialized for reading. Call initializeForReading() first.");
    if (currReadPtr >= startOfMemArea + totalMemAreaSize)
    {
        throw UnknownOperation("Read operation attempts to read beyond allocated memory area");
    }

    Record record;

    for (const auto& fieldName : schema.getFieldNames())
    {
        const auto field = schema.getFieldByName(fieldName).value();
        const auto dataType = field.dataType;

        if (dataType.type == DataType::Type::VARSIZED)
        {
            const auto varSizedData = VariableSizedData(currReadPtr);
            record.write(fieldName, varSizedData);
            currReadPtr += varSizedData.getContentSize();
        }
        else
        {
            record.write(fieldName, VarVal::readVarValFromMemory(currReadPtr, dataType.type));
        }
        auto temp = dataType.getSizeInBytes();
        currReadPtr += nautilus::val<uint64_t>(temp);
    }

    return record;
}

void SynopsisFunctionRef::setReadPtrToRecordIdx(const nautilus::val<uint64_t>& idx)
{
    /// Set currReadPtr to address of first record
    currReadPtr = startOfMemArea + nautilus::val<uint64_t>(sizeof(uint32_t) + sizeof(uint64_t));

    for (auto recordIdx = nautilus::val<uint64_t>(0UL); recordIdx < idx; ++recordIdx)
    {
        for (const auto& fieldName : schema.getFieldNames())
        {
            const auto field = schema.getFieldByName(fieldName).value();
            const auto dataType = field.dataType;

            if (dataType.type == DataType::Type::VARSIZED)
            {
                const auto textSize = Nautilus::Util::readValueFromMemRef<uint32_t>(currReadPtr);
                currReadPtr += textSize;
            }

            currReadPtr += nautilus::val<uint64_t>(dataType.getSizeInBytes());
            if (currReadPtr >= startOfMemArea + totalMemAreaSize)
            {
                throw UnknownOperation("Set read pointer operation attempts to read beyond allocated memory area");
            }
        }
    }
}

VariableSizedData SynopsisFunctionRef::getSynopsis() const
{
    return VariableSizedData(startOfMemArea);
}

VariableSizedData SynopsisFunctionRef::getMetaData() const
{
    return VariableSizedData(startOfMemArea + nautilus::val<uint64_t>(sizeof(uint32_t)));
}

}
