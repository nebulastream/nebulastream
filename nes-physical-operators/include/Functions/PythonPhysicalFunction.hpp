/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0
*/
#pragma once

#include <string>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>

namespace NES
{
class CompilationContext;

class PythonPhysicalFunction final
{
public:
    PythonPhysicalFunction(
        std::vector<std::string> parameterNames,
        std::string body,
        std::vector<PhysicalFunction> arguments,
        std::vector<DataType> argumentTypes,
        DataType returnType,
        const std::vector<std::string>& importPaths);

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;
    void setupSelf(CompilationContext& compilationContext) const;

private:
    std::vector<PhysicalFunction> arguments;
    std::vector<DataType> argumentTypes;
    DataType returnType;
    std::string symbolName;
    std::string llvmBitcode;
};

static_assert(PhysicalFunctionConcept<PythonPhysicalFunction>);
}
