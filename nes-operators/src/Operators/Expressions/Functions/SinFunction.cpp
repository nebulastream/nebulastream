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
#include <Nodes/Expressions/Functions/LogicalFunctionRegistry.hpp>

namespace NES {

/*
* Defines the sin function and registers it to the FunctionRegistry.
*/
class SinFunction : public UnaryLogicalFunction {
  public:
    [[nodiscard]] DataTypePtr inferUnary(const DataTypePtr& input) const override {
        if (!input->isNumeric()) {
            NES_THROW_RUNTIME_ERROR("SinExpressions can only be evaluated on numeric values.");
        }
        return DataTypeFactory::createDouble();
    }
};

[[maybe_unused]] static LogicalFunctionRegistry::Add<SinFunction> sinFunction("sin");

}// namespace NES