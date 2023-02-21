//
// Created by pgrulich on 21.02.23.
//
#include <Execution/Expressions/Expression.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PluginRegistry.hpp>
#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_EXECUTABLEFUNCTIONREGISTRY_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_EXECUTABLEFUNCTIONREGISTRY_HPP_

namespace NES::Runtime::Execution::Expressions {

class FunctionProvider {
  public:
    virtual std::unique_ptr<Expression> create(std::vector<ExpressionPtr>& args) = 0;
    virtual ~FunctionProvider() = default;
};

template<typename T>
class UnaryFunctionProvider : public FunctionProvider {
  public:
    std::unique_ptr<Expression> create(std::vector<ExpressionPtr>& args) override {
        NES_ASSERT(args.size() == 1, "A unary function should receive one argument");
        return std::make_unique<T>(args[0]);
    };
};

template<typename T>
class BinaryFunctionProvider : public FunctionProvider {
  public:
    std::unique_ptr<Expression> create(std::vector<ExpressionPtr>& args) override {
        NES_ASSERT(args.size() == 2, "A binary function should receive two arguments");
        return std::make_unique<T>(args[0], args[1]);
    };
};

using ExecutableFunctionRegistry = Util::PluginFactory<FunctionProvider>;

}// namespace NES::Runtime::Execution::Expressions

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_EXECUTABLEFUNCTIONREGISTRY_HPP_
