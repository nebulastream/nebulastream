#ifndef NES_INCLUDE_UTIL_DATATYPESERIALIZATIONUTIL_HPP_
#define NES_INCLUDE_UTIL_DATATYPESERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class SerializableDataType;
class SerializableDataValue;

class DataTypeSerializationUtil {
  public:
    static SerializableDataType* serializeDataType(DataTypePtr dataType, SerializableDataType* serializedDataType);
    static DataTypePtr deserializeDataType(SerializableDataType* serializedDataType);
    static SerializableDataValue* serializeDataValue(ValueTypePtr valueType, SerializableDataValue* serializedDataValue);
    static ValueTypePtr deserializeDataValue(SerializableDataValue* serializedDataValue);
};
}// namespace NES

#endif//NES_INCLUDE_UTIL_DATATYPESERIALIZATIONUTIL_HPP_
