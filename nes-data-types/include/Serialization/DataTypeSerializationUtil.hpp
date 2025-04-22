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

#pragma once

#include <memory>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
class SerializableDataType;


/// The DataTypeSerializationUtil offers functionality to serialize and de-serialize data types and value types to a
/// corresponding protobuffer object.
class DataTypeSerializationUtil
{
public:
    static SerializableDataType* serializeDataType(const std::shared_ptr<DataType>& dataType, SerializableDataType* serializedDataType);
    static std::shared_ptr<DataType> deserializeDataType(const SerializableDataType& serializedDataType);
};
}
