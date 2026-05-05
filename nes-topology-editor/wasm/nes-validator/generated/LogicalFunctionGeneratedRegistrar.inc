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

/// NOLINT(clang-diagnostic-error)
#ifndef INCLUDED_FROM_REGISTRY_LOGICAL_FUNCTION
#    error "This file should not be included directly! " \
"Include instead include <LogicalFunctionRegistry.hpp>"
#endif

#include <memory>
#include <string>
#include <Util/Registry.hpp>

namespace NES::LogicalFunctionGeneratedRegistrar
{

/// declarations of register functions for 'InputFormatters'
LogicalFunctionRegistryReturnType RegisterAndLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterEqualsLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterNegateLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterOrLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterIsNullLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterAbsLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterAddLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterCeilLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterDivLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterExpLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterFloorLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterModLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterMulLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterPowLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterRoundLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterSqrtLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterSubLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterGreaterLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterGreaterEqualsLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterLessLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterLessEqualsLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterConstantValueLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterFieldAccessLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterConcatLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterFROM_BASE64LogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterTO_BASE64LogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterCastToTypeLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterCastToUnixTsLogicalFunction(LogicalFunctionRegistryArguments);
LogicalFunctionRegistryReturnType RegisterCastFromUnixTsLogicalFunction(LogicalFunctionRegistryArguments);

}

namespace NES
{
template <>
inline void Registrar<LogicalFunctionRegistry, std::string, LogicalFunctionRegistryReturnType, LogicalFunctionRegistryArguments>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::LogicalFunctionGeneratedRegistrar;
    registry.addEntry("And", RegisterAndLogicalFunction);
	registry.addEntry("Equals", RegisterEqualsLogicalFunction);
	registry.addEntry("Negate", RegisterNegateLogicalFunction);
	registry.addEntry("Or", RegisterOrLogicalFunction);
	registry.addEntry("IsNull", RegisterIsNullLogicalFunction);
	registry.addEntry("Abs", RegisterAbsLogicalFunction);
	registry.addEntry("Add", RegisterAddLogicalFunction);
	registry.addEntry("Ceil", RegisterCeilLogicalFunction);
	registry.addEntry("Div", RegisterDivLogicalFunction);
	registry.addEntry("Exp", RegisterExpLogicalFunction);
	registry.addEntry("Floor", RegisterFloorLogicalFunction);
	registry.addEntry("Mod", RegisterModLogicalFunction);
	registry.addEntry("Mul", RegisterMulLogicalFunction);
	registry.addEntry("Pow", RegisterPowLogicalFunction);
	registry.addEntry("Round", RegisterRoundLogicalFunction);
	registry.addEntry("Sqrt", RegisterSqrtLogicalFunction);
	registry.addEntry("Sub", RegisterSubLogicalFunction);
	registry.addEntry("Greater", RegisterGreaterLogicalFunction);
	registry.addEntry("GreaterEquals", RegisterGreaterEqualsLogicalFunction);
	registry.addEntry("Less", RegisterLessLogicalFunction);
	registry.addEntry("LessEquals", RegisterLessEqualsLogicalFunction);
	registry.addEntry("ConstantValue", RegisterConstantValueLogicalFunction);
	registry.addEntry("FieldAccess", RegisterFieldAccessLogicalFunction);
	registry.addEntry("Concat", RegisterConcatLogicalFunction);
	registry.addEntry("FROM_BASE64", RegisterFROM_BASE64LogicalFunction);
	registry.addEntry("TO_BASE64", RegisterTO_BASE64LogicalFunction);
	registry.addEntry("CastToType", RegisterCastToTypeLogicalFunction);
	registry.addEntry("CastToUnixTs", RegisterCastToUnixTsLogicalFunction);
	registry.addEntry("CastFromUnixTs", RegisterCastFromUnixTsLogicalFunction);
	
}

}
