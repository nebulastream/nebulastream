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

#include <cstdint>
#include <string>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <UdfDescriptor.hpp>

namespace llvm
{
class Module;
}

namespace NES
{

/// Throws QueryCompilerError unless the module defines `symbol` and calls only what the worker provides: the functions of
/// PythonUdfRuntimeSymbolNames, the native functions of the loaded Codon plugins, and LLVM intrinsics. compilePythonUdf
/// checks this, and the worker checks it again before it runs bitcode that another process compiled.
void validatePythonUdfModule(const llvm::Module& module, const std::string& symbol);

/// Loads the Codon plugins that NES ships (once per process): Codon reads their modules when it compiles a UDF, and the
/// worker needs their native functions for a compiled UDF that calls them, also if another process compiled it.
void loadCodonPlugins();

/// The interpreter's entry point of a compiled Python UDF whose pipeline entry point is `symbol`.
[[nodiscard]] std::string interpreterSymbolName(const std::string& symbol);

/// Compiles a Python UDF body with Codon into LLVM bitcode for the query pipeline and for the interpreter, with the entry
/// points `symbol` and interpreterSymbolName(symbol). The entry points take `argumentTypes`; the body receives the arguments
/// for which `bodyNullable` is set as Optional, and for a NULL in another nullable argument the result is NULL without a
/// call. Throws QueryCompilerError if Codon cannot compile it.
[[nodiscard]] CompiledInlineUdf compilePythonUdf(
    const std::string& symbol,
    const std::vector<std::string>& parameterNames,
    const std::string& body,
    const std::vector<DataType>& argumentTypes,
    const DataType& returnType,
    const std::vector<std::string>& importPaths,
    const std::vector<uint64_t>& bytesArguments,
    const std::vector<bool>& bodyNullable);

/// A UDF named by its entry point, expressed as the inline function the Codon path compiles: the ENTRYPOINT module is
/// imported and called from a generated body.
[[nodiscard]] InlineUdfSource codonUdfSource(const UdfDescriptor& descriptor, const std::vector<DataType>& inputTypes);

/// Where Codon looks for imported modules: `configured` plus NES_UDF_PATH.
[[nodiscard]] std::vector<std::string> codonImportPaths(const std::vector<std::string>& configured);

}
