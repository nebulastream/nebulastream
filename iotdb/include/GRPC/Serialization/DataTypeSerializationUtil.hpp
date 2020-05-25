#ifndef NES_INCLUDE_UTIL_DATATYPESERIALIZATIONUTIL_HPP_
#define NES_INCLUDE_UTIL_DATATYPESERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

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
