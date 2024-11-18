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
#include "Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp"
#include <API/Expressions/Expressions.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/MEOS/MeosOperator.hpp>
#include <Execution/Operators/MEOS/MeosOperatorHandler.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMeosOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PluginRegistry.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::Execution::Operators {

class MeosOperatorPlugin : public NES::Runtime::Execution::Expressions::FunctionExpressionProvider {
  public:
    MeosOperatorPlugin() { NES_INFO("Load MeosOperatorPlugin"); }

    Expressions::ExpressionPtr createExpression(const std::vector<Expressions::ExpressionPtr>& args) {
        if (args.size() != 3) {
            throw std::invalid_argument("MeosOperator requires exactly 3 arguments");
        }
        return MeosOperator::create(args[0], args[1], args[2]);
    }
};

// Register the plugin
[[maybe_unused]] static Util::PluginRegistry::Add<MeosOperatorPlugin> MeosOperatorPlugin("meosT");

};// namespace NES::Runtime::Execution::Operators
