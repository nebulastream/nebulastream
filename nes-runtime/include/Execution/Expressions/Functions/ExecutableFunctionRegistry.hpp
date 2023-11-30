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

#include <Execution/Expressions/Expression.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PluginRegistry.hpp>
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_EXECUTABLEFUNCTIONREGISTRY_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_EXECUTABLEFUNCTIONREGISTRY_HPP_

namespace NES::Runtime::Execution::Expressions {

/**
 * @brief The function provider, is the base class, which registers an expression in the engine.
 */
class FunctionExpressionProvider {
  public:
    /**
     * @brief Creates a new function expression, which a set of arguments.
     * @param args aruments for the function expression
     * @return std::unique_ptr<Expression>
     */
    virtual std::unique_ptr<Expression> create(std::vector<ExpressionPtr>& args) = 0;
    virtual ~FunctionExpressionProvider() = default;
};

/**
 * @brief A function provider for unary function expressions.
 * @tparam T
 */
template<typename T>
class UnaryFunctionProvider : public FunctionExpressionProvider {
  public:
    std::unique_ptr<Expression> create(std::vector<ExpressionPtr>& args) override {
        NES_ASSERT(args.size() == 1, "A unary function should receive one argument");
        return std::make_unique<T>(args[0]);
    };
};

/**
 * @brief A function provider for binary function expressions.
 * @tparam T
 */
template<typename T>
class BinaryFunctionProvider : public FunctionExpressionProvider {
  public:
    std::unique_ptr<Expression> create(std::vector<ExpressionPtr>& args) override {
        NES_ASSERT(args.size() == 2, "A binary function should receive two arguments");
        return std::make_unique<T>(args[0], args[1]);
    };
};

/**
 * @brief The ExecutableFunctionRegistry manages all executable function expressions for the engine.
 */
using ExecutableFunctionRegistry = Util::PluginFactory<FunctionExpressionProvider>;

}// namespace NES::Runtime::Execution::Expressions

#endif  // NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_EXECUTABLEFUNCTIONREGISTRY_HPP_
