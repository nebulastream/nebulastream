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

#ifndef NES_INCLUDE_OPTIMIZER_UTILS_DATA_TYPE_TO_Z3_EXPR_UTIL_HPP_
#define NES_INCLUDE_OPTIMIZER_UTILS_DATA_TYPE_TO_Z3_EXPR_UTIL_HPP_

#include <memory>

namespace z3 {
class expr;
using ExprPtr = std::shared_ptr<expr>;

class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

class DataType;
using DataTypePtr = std::shared_ptr<DataType>;

class ValueType;
using ValueTypePtr = std::shared_ptr<ValueType>;

}// namespace NES

namespace NES::Optimizer {

class Z3ExprAndFieldMap;
using Z3ExprAndFieldMapPtr = std::shared_ptr<Z3ExprAndFieldMap>;

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
     * @return expression and field map for the field
     */
    static Z3ExprAndFieldMapPtr
    createForField(const std::string& fieldName, const DataTypePtr& dataType, const z3::ContextPtr& context);

    /**
     * @brief Create Z3 expression for data value of specific type
     * @param valueType: the input value
     * @param context: Z3 context
     * @return expression and field map for the data value
     */
    static Z3ExprAndFieldMapPtr createForDataValue(const ValueTypePtr& valueType, const z3::ContextPtr& context);
};
}// namespace NES::Optimizer

#endif  // NES_INCLUDE_OPTIMIZER_UTILS_DATA_TYPE_TO_Z3_EXPR_UTIL_HPP_
