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

#include <Serialization/DataTypeSerializationUtil.hpp>

#include <type_traits>
#include <DataTypes/DataType.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SerializableDataType.pb.h>

namespace NES
{

SerializableDataType* DataTypeSerializationUtil::serializeDataType(const DataType& dataType, SerializableDataType* serializedDataType)
{
    auto serializedPhysicalTypeEnum = SerializableDataType_Type();
    SerializableDataType_Type_Parse(magic_enum::enum_name(dataType.type), &serializedPhysicalTypeEnum);
    serializedDataType->set_type(serializedPhysicalTypeEnum);
    serializedDataType->set_isnullable(dataType.isNullable);
    return serializedDataType;
}

DataType DataTypeSerializationUtil::deserializeDataType(const SerializableDataType& serializedDataType)
{
    auto type = magic_enum::enum_cast<DataType::Type>(serializedDataType.type());
    if (!type)
    {
        /// from https://protobuf.dev/programming-guides/proto3/#enum:
        ///
        /// During deserialization, unrecognized enum values will be preserved in the message [...]
        /// In [...] C++ and Go, the unknown enum value is simply stored as its underlying integer representation.
        throw CannotDeserialize(
            "Encountered illegal enum value for {}! Got {} but enum only goes up to {}",
            magic_enum::enum_type_name<DataType::Type>(),
            static_cast<std::underlying_type_t<DataType::Type>>(serializedDataType.type()),
            magic_enum::enum_values<DataType::Type>().size());
    }
    const DataType deserializedDataType = DataType{.type = *type, .isNullable = serializedDataType.isnullable()};
    return deserializedDataType;
}

}
