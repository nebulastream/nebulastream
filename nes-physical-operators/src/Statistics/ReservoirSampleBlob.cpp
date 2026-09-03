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

#include <Statistics/ReservoirSampleBlob.hpp>

#include <cstdint>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Record.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

void writeReservoirSampleBlobHeader(
    const nautilus::val<int8_t*>& blobMemArea, const nautilus::val<uint64_t>& tupleCount, const nautilus::val<uint64_t>& dataSizeBytes)
{
    VarVal{tupleCount}.writeToMemory(blobMemArea + nautilus::val<uint64_t>{ReservoirSampleBlob::TUPLE_COUNT_OFFSET});
    VarVal{dataSizeBytes}.writeToMemory(blobMemArea + nautilus::val<uint64_t>{ReservoirSampleBlob::DATA_SIZE_OFFSET});
}

nautilus::val<uint64_t> readReservoirSampleBlobTupleCount(const nautilus::val<int8_t*>& blobMemArea)
{
    return readValueFromMemRef<uint64_t>(blobMemArea + nautilus::val<uint64_t>{ReservoirSampleBlob::TUPLE_COUNT_OFFSET});
}

nautilus::val<int8_t*> getReservoirSampleBlobDataArea(const nautilus::val<int8_t*>& blobMemArea)
{
    return blobMemArea + nautilus::val<uint64_t>{ReservoirSampleBlob::HEADER_SIZE};
}

ReservoirSampleBlobRowReader::ReservoirSampleBlobRowReader(
    const nautilus::val<int8_t*>& blobMemArea, const std::vector<ReservoirSampleField>& fields)
    : cursor(getReservoirSampleBlobDataArea(blobMemArea)), fields(fields)
{
}

Record ReservoirSampleBlobRowReader::readNextTuple()
{
    Record record;
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, type] = fields[i];
        if (type.type == DataType::Type::VARSIZED)
        {
            const auto contentSize = static_cast<nautilus::val<uint64_t>>(readValueFromMemRef<uint32_t>(cursor));
            const auto contentPtr = cursor + nautilus::val<uint64_t>{ReservoirSampleBlob::VARSIZED_LENGTH_PREFIX_SIZE};
            record.write(name, VarVal{VariableSizedData{contentPtr, contentSize}});
            cursor = contentPtr + contentSize;
        }
        else
        {
            record.write(name, VarVal::readNonNullableVarValFromMemory(cursor, type));
            cursor = cursor + nautilus::val<uint64_t>{type.getSizeInBytesWithoutNull()};
        }
    }
    return record;
}

}
