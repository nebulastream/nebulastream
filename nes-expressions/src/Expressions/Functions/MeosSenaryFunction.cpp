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
#include <Expressions/Functions/LogicalFunctionRegistry.hpp>

namespace NES {

/*
* Defines the read function and registers it to the FunctionRegistry.
*/
class MeosSenaryFunction : public SenaryLogicalFunction {
  public:
    [[nodiscard]] DataTypePtr
    inferSenary(const DataTypePtr& one, const DataTypePtr& two, const DataTypePtr& three, const DataTypePtr& four, const DataTypePtr& five, const DataTypePtr& six) const override {
        if (!one->isNumeric() || !two->isNumeric() || !three->isNumeric() || !four->isNumeric() || !five->isNumeric() || !six->isNumeric()) {
            NES_THROW_RUNTIME_ERROR("MeosSenaryFunction can only be evaluated on numeric values.");
        }
        // Output values can become highly negative for inputs close to +0. Set Double as output stamp.
        return DataTypeFactory::createDouble();
    }
};

[[maybe_unused]] const static LogicalFunctionRegistry::Add<MeosSenaryFunction> MeosSenaryFunction("sintersects");

}// namespace NES
