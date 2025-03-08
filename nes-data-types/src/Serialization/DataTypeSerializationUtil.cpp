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

#include <memory>
#include <string>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SerializableDataType.pb.h>

namespace NES
{

SerializableDataType* DataTypeSerializationUtil::serializeDataType(const DataType& dataType, SerializableDataType* serializedDataType)
{
    auto serializedPhysicalDataType = SerializableDataType_SerializablePhysicalDataType().New();
    auto serializedPhysicalTypeEnum = SerializableDataType_SerializablePhysicalDataType_Type();
    SerializableDataType_SerializablePhysicalDataType_Type_Parse(
        magic_enum::enum_name(dataType.physicalType.type), &serializedPhysicalTypeEnum);
    serializedPhysicalDataType->set_type(serializedPhysicalTypeEnum);
    serializedPhysicalDataType->set_sizeinbits(dataType.physicalType.sizeInBits);
    serializedPhysicalDataType->set_issigned(dataType.physicalType.isSigned);
    serializedDataType->set_allocated_physicaltype(serializedPhysicalDataType);
    NES_TRACE("DataTypeSerializationUtil:: serialized {} to {}", dataType, serializedDataType->SerializeAsString());
    return serializedDataType;
}

DataType DataTypeSerializationUtil::deserializeDataType(const SerializableDataType& serializedDataType)
{
    NES_TRACE("DataTypeSerializationUtil:: de-serialized {}", serializedDataType.DebugString());
    DataType deserializedDataType;
    deserializedDataType.physicalType = PhysicalType{
        .type = magic_enum::enum_value<PhysicalType::Type>(serializedDataType.physicaltype().type()),
        .sizeInBits = serializedDataType.physicaltype().sizeinbits(),
        .isSigned = serializedDataType.physicaltype().issigned()};

    // if (serializedDataType.type() == SerializableDataType_Type_UNDEFINED)
    return deserializedDataType;
}

}
