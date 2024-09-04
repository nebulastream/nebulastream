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
        "Include instead include <Execution/Functions/Functions/GeneratedExecutableFunctionRegistrar.hpp>"
#endif

namespace NES::Runtime::Execution::Functions
{

std::unique_ptr<Function> RegisterTanFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterGammaFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterLnFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterCotFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterLog2Function(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterCeilFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterSqrtFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterMinFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterAsinFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterLog10Function(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterAcosFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterLGammaFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterCosFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterMaxFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterAtanFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterDegreesFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterRandomFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterAtan2Function(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterPowerFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterSinFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterBitcounterFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterAbsFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterCbrtFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterFloorFunction(const std::vector<FunctionPtr>&);

std::unique_ptr<Function> RegisterModFunction(const std::vector<FunctionPtr>&);
std::unique_ptr<Function> RegisterRadiansFunction(const std::vector<FunctionPtr>&);
} /// namespace NES::Runtime::Execution::Functions

namespace NES
{
template <>
inline void
Registrar<std::string, Runtime::Execution::Functions::Function, const std::vector<Runtime::Execution::Functions::FunctionPtr>&>::
    registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::Runtime::Execution::Functions;
    /// External register functions
    ///-todo

    /// Internal register functions
    registry.registerPlugin("tan", RegisterTanFunction);

    registry.registerPlugin("gamma", RegisterGammaFunction);

    registry.registerPlugin("ln", RegisterLnFunction);

    registry.registerPlugin("cot", RegisterCotFunction);

    registry.registerPlugin("log2", RegisterLog2Function);

    registry.registerPlugin("ceil", RegisterCeilFunction);

    registry.registerPlugin("sqrt", RegisterSqrtFunction);

    registry.registerPlugin("min", RegisterMinFunction);

    registry.registerPlugin("abs", RegisterAbsFunction);
    registry.registerPlugin("acos", RegisterAcosFunction);
    registry.registerPlugin("asin", RegisterAsinFunction);

    registry.registerPlugin("log10", RegisterLog10Function);

    registry.registerPlugin("acos", RegisterAcosFunction);

    registry.registerPlugin("lGamma", RegisterLGammaFunction);

    registry.registerPlugin("cos", RegisterCosFunction);

    registry.registerPlugin("max", RegisterMaxFunction);

    registry.registerPlugin("atan", RegisterAtanFunction);

    registry.registerPlugin("degrees", RegisterDegreesFunction);

    registry.registerPlugin("random", RegisterRandomFunction);

    registry.registerPlugin("atan2", RegisterAtan2Function);

    registry.registerPlugin("power", RegisterPowerFunction);

    registry.registerPlugin("sin", RegisterSinFunction);

    registry.registerPlugin("bitcounter", RegisterBitcounterFunction);

    registry.registerPlugin("abs", RegisterAbsFunction);

    registry.registerPlugin("cbrt", RegisterCbrtFunction);
    registry.registerPlugin("floor", RegisterFloorFunction);
    registry.registerPlugin("mod", RegisterModFunction);
    registry.registerPlugin("radians", RegisterRadiansFunction);
    registry.registerPlugin("random", RegisterRandomFunction);
    registry.registerPlugin("sign", RegisterSignFunction);
    registry.registerPlugin("sin", RegisterSinFunction);
    registry.registerPlugin("sqrt", RegisterSqrtFunction);
    registry.registerPlugin("tan", RegisterTanFunction);
    registry.registerPlugin("trunc", RegisterTruncFunction);
    registry.registerPlugin("ceil", RegisterCeilFunction);
    registry.registerPlugin("cos", RegisterCosFunction);
    registry.registerPlugin("cot", RegisterCotFunction);
    registry.registerPlugin("degrees", RegisterDegreesFunction);
    registry.registerPlugin("exp", RegisterExpFunction);
    registry.registerPlugin("factorial", RegisterFloorFunction);
    registry.registerPlugin("gamma", RegisterGammaFunction);
    registry.registerPlugin("lGamma", RegisterLGammaFunction);
    registry.registerPlugin("ln", RegisterLnFunction);
    registry.registerPlugin("log2", RegisterLog2Function);
    registry.registerPlugin("log10", RegisterLog10Function);
    registry.registerPlugin("max", RegisterMaxFunction);
    registry.registerPlugin("min", RegisterMinFunction);
    registry.registerPlugin("power", RegisterPowerFunction);
}
}
