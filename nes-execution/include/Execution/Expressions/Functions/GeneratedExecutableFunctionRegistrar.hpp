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

#pragma once
#ifndef INCLUDED_FROM_EXECUTABLE_FUNCTION_REGISTRY
#    error "This file should not be included directly! " \
        "Include instead include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>"
#endif

namespace NES::Runtime::Execution::Expressions
{
std::unique_ptr<Expression> RegisterAbsExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterAcosExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterAsinExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterAtan2Expression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterAtanExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterBitcounterExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterCbrtExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterCeilExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterCosExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterCotExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterDegreesExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterExpExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterFactorialExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterFloorExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterGammaExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterLGammaExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterLnExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterLog2Expression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterLog10Expression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterMaxExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterMinExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterModExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterPowerExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterRadiansExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterRandomExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterSignExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterSinExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterSqrtExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterTanExpression(const std::vector<ExpressionPtr>&);
std::unique_ptr<Expression> RegisterTruncExpression(const std::vector<ExpressionPtr>&);
}

namespace NES
{
template <>
inline void
Registrar<std::string, Runtime::Execution::Expressions::Expression, const std::vector<Runtime::Execution::Expressions::ExpressionPtr>&>::
    registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::Runtime::Execution::Expressions;

    registry.registerPlugin("abs", RegisterAbsExpression);
    registry.registerPlugin("acos", RegisterAcosExpression);
    registry.registerPlugin("asin", RegisterAsinExpression);
    registry.registerPlugin("atan", RegisterAtanExpression);
    registry.registerPlugin("atan2", RegisterAtan2Expression);
    registry.registerPlugin("bitcounter", RegisterBitcounterExpression);
    registry.registerPlugin("cbrt", RegisterCbrtExpression);
    registry.registerPlugin("ceil", RegisterCeilExpression);
    registry.registerPlugin("cos", RegisterCosExpression);
    registry.registerPlugin("cot", RegisterCotExpression);
    registry.registerPlugin("degrees", RegisterDegreesExpression);
    registry.registerPlugin("exp", RegisterExpExpression);
    registry.registerPlugin("factorial", RegisterFloorExpression);
    registry.registerPlugin("floor", RegisterFloorExpression);
    registry.registerPlugin("gamma", RegisterGammaExpression);
    registry.registerPlugin("lGamma", RegisterLGammaExpression);
    registry.registerPlugin("ln", RegisterLnExpression);
    registry.registerPlugin("log2", RegisterLog2Expression);
    registry.registerPlugin("log10", RegisterLog10Expression);
    registry.registerPlugin("max", RegisterMaxExpression);
    registry.registerPlugin("min", RegisterMinExpression);
    registry.registerPlugin("mod", RegisterModExpression);
    registry.registerPlugin("power", RegisterPowerExpression);
    registry.registerPlugin("radians", RegisterRadiansExpression);
    registry.registerPlugin("random", RegisterRandomExpression);
    registry.registerPlugin("sign", RegisterSignExpression);
    registry.registerPlugin("sin", RegisterSinExpression);
    registry.registerPlugin("sqrt", RegisterSqrtExpression);
    registry.registerPlugin("tan", RegisterTanExpression);
    registry.registerPlugin("trunc", RegisterTruncExpression);
}
}
