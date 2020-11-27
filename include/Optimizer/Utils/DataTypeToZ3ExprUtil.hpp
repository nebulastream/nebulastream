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

#ifndef NES_OPTIMIZER_UTILS_DATATYPETOZ3EXPRUTIL_HPP
#define NES_OPTIMIZER_UTILS_DATATYPETOZ3EXPRUTIL_HPP

#include <memory>

namespace z3 {
class expr;
typedef std::shared_ptr<expr> ExprPtr;

class context;
typedef std::shared_ptr<context> ContextPtr;
}// namespace z3

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

}// namespace NES

namespace NES::Optimizer {

class ReturnValue;
typedef std::shared_ptr<ReturnValue> ReturnValuePtr;

/**
 * @brief This class is used for converting a data field or value into Z3 expression.
 */
class DataTypeToZ3ExprUtil {
  public:
    /**
     * @brief Create Z3 expression for field of specific datatype
     * @param fieldName: name of the filed
     * @param dataType: the type of the field
     * @param context: the z3 context
     * @return expression for the field
     */
    static ReturnValuePtr createForField(std::string fieldName, DataTypePtr dataType, z3::ContextPtr context);

    /**
     * @brief Create Z3 expression for data value of specific type
     * @param valueType: the input value
     * @param context: Z3 context
     * @return expression for the data value
     */
    static ReturnValuePtr createForDataValue(ValueTypePtr valueType, z3::ContextPtr context);
};
}// namespace NES::Optimizer

#endif//NES_OPTIMIZER_UTILS_DATATYPETOZ3EXPRUTIL_HPP
