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

/**
 * @brief This class is used for converting a data field or value into Z3 expression.
 */
class DataTypeToZ3Expr {
  public:
    /**
     * @brief Create Z3 expression for field of specific datatype
     * @param fieldName: name of the filed
     * @param dataType: the type of the field
     * @param context: the z3 context
     * @return expression for the field
     */
    static z3::expr createForField(std::string fieldName, DataTypePtr dataType, z3::context& context);

    /**
     * @brief Create Z3 expression for data value of specific type
     * @param valueType: the input value
     * @param context: Z3 context
     * @return expression for the data value
     */
    static z3::expr createForDataValue(ValueTypePtr valueType, z3::context& context);
};
}// namespace NES

#endif//NES_OPTIMIZER_UTILS_DATATYPETOFOL_HPP
