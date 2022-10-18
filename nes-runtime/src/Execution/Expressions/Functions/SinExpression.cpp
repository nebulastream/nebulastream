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

#include <Execution/Expressions/Functions/SinExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>



 namespace NES::Runtime::Execution::Expressions {



             SinExpression::SinExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& subExpression)
    : subExpression(subExpression) {}



             /**

  * @brief This method calculates the sine of X.

  * This function is basically a wrapper for std::sin and enables us to use it in our execution engine framework.

  * @param x double

  * @param y double

  * @return double

  */

             double calculateSin(double x) { return std::sin(x); }



             Value<> SinExpression::execute(NES::Nautilus::Record& record) const {

                 // Evaluate the sub expression and retrieve the value.

                 Value subValue = subExpression->execute(record);

                 // As we don't know the exact type of value here, we have to check the type and then call the function.

                 // subValue.as<Int8>() makes an explicit cast from Value to Value<Int8>.

                 // In all cases we can call the same calculateSin function as under the hood C++ can do an implicit cast from

                 // primitive integer types to the double argument.

                 // Later we will introduce implicit casts on this level to hide this casting boilerplate code.

                 if (subValue->isType<Int8>()) {

                     // call the calculateSin function with the correct type

                     return FunctionCall<>("calculateSin", calculateSin, subValue.as<Int8>());

                 } else if (subValue->isType<Int16>()) {

                     return FunctionCall<>("calculateSin", calculateSin, subValue.as<Int16>());

                 } else if (subValue->isType<Int32>()) {

                     return FunctionCall<>("calculateSin", calculateSin, subValue.as<Int32>());

                 } else if (subValue->isType<Int64>()) {

                     return FunctionCall<>("calculateSin", calculateSin, subValue.as<Int64>());

                 } else if (subValue->isType<UInt8>()) {

                     return FunctionCall<>("calculateSin", calculateSin, subValue.as<UInt8>());

                 } else if (subValue->isType<UInt16>()) {

                     return FunctionCall<>("calculateSin", calculateSin, subValue.as<UInt16>());

                 } else if (subValue->isType<UInt32>()) {

                     return FunctionCall<>("calculateSin", calculateSin, subValue.as<UInt32>());

                 } else if (subValue->isType<UInt64>()) {

                     return FunctionCall<>("calculateSin", calculateSin, subValue.as<UInt64>());

                 } else if (subValue->isType<Float>()) {

                     return FunctionCall<>("calculateSin", calculateSin, subValue.as<Float>());

                 } else if (subValue->isType<Double>()) {

                     return FunctionCall<>("calculateSin", calculateSin, subValue.as<Double>());

                 } else {

                     // If no type was applicable we throw an exception.

                     NES_THROW_RUNTIME_ERROR("This expression is only defined on a numeric input argument that is ether Integer or Float.");

                 }

             }



     }// namespace NES::Runtime::Execution::Expressions