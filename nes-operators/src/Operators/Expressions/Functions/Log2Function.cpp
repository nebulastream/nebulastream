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

#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/Expressions/Functions/LogicalFunctionRegistry.hpp>

namespace NES {

/*
 * Defines the log2 function and registers it to the FunctionRegistry.
 */
class Log2Function : public UnaryLogicalFunction {
  public:
    [[nodiscard]] DataTypePtr inferUnary(const DataTypePtr& input) const override {
        if (!input->isNumeric()) {
            NES_THROW_RUNTIME_ERROR("LogExpressions can only be evaluated on numeric values.");
        }
        // Output values can become highly negative for inputs close to +0. Set Double as output stamp.
        return DataTypeFactory::createDouble();
    }
};

[[maybe_unused]] const static LogicalFunctionRegistry::Add<Log2Function> logFunction("log2");

}// namespace NES