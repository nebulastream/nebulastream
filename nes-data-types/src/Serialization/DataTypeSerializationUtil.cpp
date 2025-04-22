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
    auto serializedPhysicalTypeEnum = SerializableDataType_Type();
    SerializableDataType_Type_Parse(magic_enum::enum_name(dataType.type), &serializedPhysicalTypeEnum);
    serializedDataType->set_type(serializedPhysicalTypeEnum);
    return serializedDataType;
}

DataType DataTypeSerializationUtil::deserializeDataType(const SerializableDataType& serializedDataType)
{
    const DataType deserializedDataType = DataType{.type = magic_enum::enum_value<DataType::Type>(serializedDataType.type())};
    return deserializedDataType;
}

}
