/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_UTIL_DATATYPESERIALIZATIONUTIL_HPP_
#define NES_INCLUDE_UTIL_DATATYPESERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

class SerializableDataType;
class SerializableDataValue;

/**
 * @brief The DataTypeSerializationUtil offers functionality to serialize and de-serialize data types and value types to a
 * corresponding protobuffer object.
 */
class DataTypeSerializationUtil {
  public:
    /**
     * @brief Serializes a data type and all its children to a SerializableDataType object.
     * @param dataType The data type.
     * @param serializedDataType The corresponding protobuff object, which is used to capture the state of the object.
     * @return the modified serializedDataType
     */
    static SerializableDataType* serializeDataType(DataTypePtr dataType, SerializableDataType* serializedDataType);

    /**
    * @brief De-serializes the SerializableDataType and all its children to a DataTypePtr
    * @param serializedDataType the serialized data type.
    * @return DataTypePtr
    */
    static DataTypePtr deserializeDataType(SerializableDataType* serializedDataType);

    /**
     * @brief Serializes a value type and all its children to a SerializableDataValue object.
     * @param valueType The data value type.
     * @param serializedDataValue The corresponding protobuff object, which is used to capture the state of the object.
     * @return the modified serializedDataValue
     */
    static SerializableDataValue* serializeDataValue(ValueTypePtr valueType, SerializableDataValue* serializedDataValue);

    /**
    * @brief De-serializes the SerializableDataValue and all its children to a ValueTypePtr
    * @param serializedDataValue the serialized data value type.
    * @return ValueTypePtr
    */
    static ValueTypePtr deserializeDataValue(SerializableDataValue* serializedDataValue);
};
}// namespace NES

#endif//NES_INCLUDE_UTIL_DATATYPESERIALIZATIONUTIL_HPP_
