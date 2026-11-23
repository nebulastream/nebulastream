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

#include <memory>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{
namespace HiPy
{
struct Program;
}

/// A Python UDF compiled by HiPy (COMPILER HIPY, HiPyUdfCompiler): HiPy's IR, a module of typed functions whose operations are
/// constants, named builtins (scalar.int.add, scalar.string.find, range.iter, ...), if/else with yielded results, records,
/// function references and calls. execute() interprets the IR on Nautilus values, so Nautilus traces the UDF into the query's
/// pipeline like any physical function: ints, floats and bools are val<> arithmetic, strings are VariableSizedData, an if/else
/// is Nautilus control flow, a loop builtin is a Nautilus loop over the loop function, and string builtins call runtime
/// functions. A NULL argument gives a NULL result without a call.
class HiPyPhysicalFunction final
{
public:
    /// `compiled.pipelineBitcode` holds the JSON that nes_hipy_compile.py wrote: {"entry": name, "module": HiPy IR}.
    HiPyPhysicalFunction(
        const CompiledInlineUdf& compiled, std::vector<PhysicalFunction> arguments, std::vector<DataType> argumentTypes, DataType returnType);

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    std::shared_ptr<const HiPy::Program> program;
    std::vector<PhysicalFunction> arguments;
    std::vector<DataType> argumentTypes;
    DataType returnType;
};

static_assert(PhysicalFunctionConcept<HiPyPhysicalFunction>);
}
