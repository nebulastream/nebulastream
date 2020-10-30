#ifndef NES_OPTIMIZER_UTILS_DATATYPETOFOL_HPP
#define NES_OPTIMIZER_UTILS_DATATYPETOFOL_HPP

#include <memory>

namespace z3 {
class expr;
class context;
}// namespace z3

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

class DataTypeToFOL {
  public:
    /**
     * @brief Serializes a data type and all its children to a SerializableDataType object.
     * @param dataType The data type.
     * @param serializedDataType The corresponding protobuff object, which is used to capture the state of the object.
     * @return the modified serializedDataType
     */
    static z3::expr serializeDataType(std::string fieldName, DataTypePtr dataType, z3::context& context);

    /**
     * @brief Serializes a value type and all its children to a SerializableDataValue object.
     * @param valueType The data value type.
     * @param serializedDataValue The corresponding protobuff object, which is used to capture the state of the object.
     * @return the modified serializedDataValue
     */
    static z3::expr serializeDataValue(ValueTypePtr valueType, z3::context& context);
};
}// namespace NES

#endif//NES_OPTIMIZER_UTILS_DATATYPETOFOL_HPP
