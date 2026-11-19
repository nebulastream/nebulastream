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

#include <PythonUdfCompiler.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <mutex>
#include <ratio>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <NESCodonPlugin/Registry.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/Analysis/CGSCCPassManager.h>
#include <llvm/Analysis/LoopAnalysisManager.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/Bitcode/BitcodeWriter.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/GlobalValue.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/PassManager.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/Casting.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Transforms/IPO/GlobalDCE.h>
#include <llvm/Transforms/IPO/StripDeadPrototypes.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <ErrorHandling.hpp>
#include <PythonUdfAbi.hpp>
#include <PythonUdfFilesystem.hpp>
#include <UdfBridgeRegistry.hpp>
#include <UdfDescriptor.hpp>

/// Argument names, types and values are parallel vectors of the same length, indexed by the same loop counter.
/// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)

namespace NES
{
std::string interpreterSymbolName(const std::string& symbol)
{
    return symbol + "_interpreter";
}

namespace
{
double elapsedMilliseconds(const std::chrono::steady_clock::time_point start, const std::chrono::steady_clock::time_point end)
{
    return std::chrono::duration<double, std::milli>{end - start}.count();
}

/// NOLINTNEXTLINE(fuchsia-default-arguments-declarations)
std::string join(const std::vector<std::string>& values, const std::string_view delimiter = ", ")
{
    std::string result;
    for (const auto& value : values)
    {
        if (!result.empty())
        {
            result += delimiter;
        }
        result += value;
    }
    return result;
}

std::string codonTypeName(const DataType& type)
{
    switch (type.type)
    {
        case DataType::Type::UINT8:
            return "u8";
        case DataType::Type::UINT16:
            return "u16";
        case DataType::Type::UINT32:
            return "u32";
        case DataType::Type::UINT64:
            return "u64";
        case DataType::Type::INT8:
            return "i8";
        case DataType::Type::INT16:
            return "i16";
        case DataType::Type::INT32:
            return "i32";
        case DataType::Type::INT64:
            return "i64";
        case DataType::Type::FLOAT32:
            return "float32";
        case DataType::Type::FLOAT64:
            return "float";
        case DataType::Type::BOOLEAN:
            return "bool";
        case DataType::Type::VARSIZED:
            return "str";
        case DataType::Type::CHAR:
            return "u8";
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Unsupported Python UDF type {}", type);
    }
    std::unreachable();
}

std::string pythonTypeName(const DataType& type)
{
    switch (type.type)
    {
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
            return "int";
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
            return "float";
        case DataType::Type::BOOLEAN:
            return "bool";
        case DataType::Type::VARSIZED:
        case DataType::Type::CHAR:
            return "str";
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Unsupported Python UDF type {}", type);
    }
    std::unreachable();
}

std::string indentPythonBody(const std::string_view body)
{
    std::string indented;
    size_t position = 0;
    while (position < body.size())
    {
        const auto lineEnd = body.find('\n', position);
        indented += "    ";
        indented.append(body.substr(position, lineEnd == std::string_view::npos ? body.size() - position : lineEnd - position));
        indented += '\n';
        if (lineEnd == std::string_view::npos)
        {
            break;
        }
        position = lineEnd + 1;
    }
    return indented;
}

/// Splits the body's leading import statements off so they run at module scope. Codon then executes the imported modules'
/// top-level code once in the module initializer (see addModuleInitializationPrologue in PythonUdfFilesystem.cpp) rather than lazily during the
/// first invocation, whose arena does not outlive it. An import after another statement stays in the body, and Codon
/// rejects the UDF (see rejectFunctionLocalImports in PythonUdfFilesystem.cpp).
/// Blank lines before and between the imports are skipped: a body written as `AS $python$` and a newline starts with one.
/// The imports may be indented, as the body may be indented as a whole; they move to module scope without the indentation.
std::pair<std::string, std::string> splitLeadingImports(const std::string_view body)
{
    std::string imports;
    size_t position = 0;
    while (position < body.size())
    {
        const auto lineEnd = body.find('\n', position);
        const auto line = body.substr(position, lineEnd == std::string_view::npos ? std::string_view::npos : lineEnd - position);
        const auto statementStart = line.find_first_not_of(" \t\r");
        const auto statement = statementStart == std::string_view::npos ? std::string_view{} : line.substr(statementStart);
        if (!statement.empty() && !statement.starts_with("import ") && !statement.starts_with("from "))
        {
            break;
        }
        if (!statement.empty())
        {
            imports.append(statement).append("\n");
        }
        position = lineEnd == std::string_view::npos ? body.size() : lineEnd + 1;
    }
    auto remaining = std::string{body.substr(position)};
    return {std::move(imports), remaining.find_first_not_of(" \t\r\n") == std::string::npos ? std::string{"pass"} : std::move(remaining)};
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity)
/// A VARSIZED or CHAR argument declared with BYTES reaches the body as bytes (nes_compat.codon). Any other VARSIZED argument
/// reaches it as a str, which must hold valid UTF-8, as in the Python executions: the str methods work on code points.
/// `argumentTypes` are the types the entry point takes; `bodyNullable` says which arguments the body receives as Optional.
/// An argument that the entry point takes as nullable and the body does not gives a NULL result for NULL, without a call.
std::string createPythonUdfSource(
    const std::string& symbol,
    const std::vector<std::string>& parameterNames,
    const std::string_view body,
    const std::vector<DataType>& argumentTypes,
    const DataType& returnType,
    const std::vector<uint64_t>& bytesArguments,
    const std::vector<bool>& bodyNullable)
{
    PRECONDITION(parameterNames.size() == argumentTypes.size(), "Python UDF argument type and parameter name counts differ");
    PRECONDITION(bodyNullable.size() == argumentTypes.size(), "Python UDF argument type and nullability counts differ");
    std::vector<std::string> nullGuards;

    std::vector<std::string> parameters;
    std::vector<std::string> abiParameters;
    std::vector<std::string> bodyArguments;
    parameters.reserve(parameterNames.size());
    bodyArguments.reserve(parameterNames.size());
    for (size_t index = 0; index < parameterNames.size(); ++index)
    {
        const bool isText = argumentTypes[index].type == DataType::Type::VARSIZED || argumentTypes[index].type == DataType::Type::CHAR;
        const bool asBytes = isText && std::ranges::contains(bytesArguments, index);
        const auto pythonType = asBytes ? std::string{"bytes"} : pythonTypeName(argumentTypes[index]);
        parameters.emplace_back(
            fmt::format(
                "{}: {}{}{}", parameterNames[index], bodyNullable[index] ? "Optional[" : "", pythonType, bodyNullable[index] ? "]" : ""));
        if (argumentTypes[index].nullable && !bodyNullable[index])
        {
            nullGuards.emplace_back(fmt::format("bool(_arg{}_is_null)", index));
        }
        if (argumentTypes[index].type == DataType::Type::VARSIZED)
        {
            abiParameters.emplace_back(fmt::format("_arg{}_ptr: Ptr[byte]", index));
            abiParameters.emplace_back(fmt::format("_arg{}_size: i64", index));
            const auto value = fmt::format("{}(str(_arg{}_ptr, int(_arg{}_size)))", asBytes ? "bytes" : "_nes_text", index, index);
            if (argumentTypes[index].nullable)
            {
                abiParameters.emplace_back(fmt::format("_arg{}_is_null: u8", index));
            }
            if (bodyNullable[index])
            {
                bodyArguments.emplace_back(
                    fmt::format("Optional[{}]() if bool(_arg{}_is_null) else Optional[{}]({})", pythonType, index, pythonType, value));
            }
            else
            {
                bodyArguments.emplace_back(value);
            }
        }
        else
        {
            abiParameters.emplace_back(fmt::format("_arg{}: {}", index, codonTypeName(argumentTypes[index])));
            /// A CHAR crosses the ABI as its byte and reaches the body as a one-character str, or with BYTES as one byte.
            const auto converter = argumentTypes[index].type == DataType::Type::CHAR
                ? std::string{asBytes ? "_nes_char_bytes" : "_nes_char_str"}
                : pythonType;
            if (argumentTypes[index].nullable)
            {
                abiParameters.emplace_back(fmt::format("_arg{}_is_null: u8", index));
            }
            if (bodyNullable[index])
            {
                bodyArguments.emplace_back(
                    fmt::format(
                        "Optional[{}]() if bool(_arg{}_is_null) else Optional[{}]({}(_arg{}))",
                        pythonType,
                        index,
                        pythonType,
                        converter,
                        index));
            }
            else
            {
                bodyArguments.emplace_back(fmt::format("{}(_arg{})", converter, index));
            }
        }
    }

    const auto bodySymbol = symbol + "_body";
    /// A VARSIZED or CHAR result may be a str or bytes, as in the Python executions, so the body has no return annotation and
    /// Codon infers its type. _nes_result_bytes takes the bytes of either and rejects other types when Codon compiles it.
    const bool textResult = returnType.type == DataType::Type::VARSIZED || returnType.type == DataType::Type::CHAR;
    const auto returnAnnotation = textResult
        ? std::string{}
        : fmt::format(" -> {}{}{}", returnType.nullable ? "Optional[" : "", pythonTypeName(returnType), returnType.nullable ? "]" : "");
    const auto [imports, functionBody] = splitLeadingImports(body);
    auto source = imports;
    source += fmt::format(
        "@tuple\n"
        "class UdfErrorHolder:\n"
        "    error_ptr: Ptr[byte]\n"
        "    error_size: i64\n\n"
        "from nes_compat import _nes_check_utf8\n\n"
        "def _nes_text(value: str) -> str:\n"
        "    _nes_check_utf8(value)\n"
        "    return value\n\n"
        "def _nes_char_str(value: u8) -> str:\n"
        "    _data = Ptr[byte](1)\n"
        "    _data[0] = byte(int(value))\n"
        "    return str(_data, 1)\n\n"
        "def _nes_char_bytes(value: u8) -> bytes:\n"
        "    return bytes(_nes_char_str(value))\n\n"
        "def _nes_result_bytes(value) -> str:\n"
        "    if isinstance(value, Optional[str]) or isinstance(value, Optional[bytes]):\n"
        "        return _nes_result_bytes(value.__val__())\n"
        "    elif isinstance(value, str):\n"
        "        return value\n"
        "    elif isinstance(value, bytes):\n"
        "        return value._value\n"
        "    else:\n"
        "        compile_error(\"a VARSIZED or CHAR UDF must return str or bytes\")\n\n"
        "def _nes_char_byte(value: str) -> u8:\n"
        "    if value.len != 1:\n"
        "        raise ValueError(f\"a CHAR UDF must return one byte, not {{value.len}}\")\n"
        "    return u8(int(value.ptr[0]))\n\n"
        "def {}({}){}:\n{}\n@export\n",
        bodySymbol,
        join(parameters),
        returnAnnotation,
        indentPythonBody(functionBody));

    const auto abiResultType = returnType.type == DataType::Type::BOOLEAN ? std::string{"u8"} : codonTypeName(returnType);
    if (returnType.type == DataType::Type::VARSIZED)
    {
        abiParameters.emplace_back("_result_ptr: Ptr[Ptr[byte]]");
        abiParameters.emplace_back("_result_size: Ptr[i64]");
    }
    else
    {
        abiParameters.emplace_back(fmt::format("_result: Ptr[{}]", abiResultType));
    }
    if (returnType.nullable)
    {
        abiParameters.emplace_back("_result_is_null: Ptr[u8]");
    }
    abiParameters.emplace_back("_error: Ptr[UdfErrorHolder]");

    source += fmt::format("def {}({}) -> i8:\n    _status = i8(0)\n    try:\n", symbol, join(abiParameters));
    if (!nullGuards.empty())
    {
        PRECONDITION(returnType.nullable, "A Python UDF that returns NULL for a NULL argument needs a nullable result");
        source += fmt::format("        if {}:\n", join(nullGuards, " or "));
        source += "            _result_is_null[0] = u8(1)\n";
        if (returnType.type == DataType::Type::VARSIZED)
        {
            source += "            _result_ptr[0] = Ptr[byte]()\n";
            source += "            _result_size[0] = i64(0)\n";
        }
        source += "            return _status\n";
    }
    if (returnType.nullable)
    {
        source += fmt::format("        _result_value = {}({})\n", bodySymbol, join(bodyArguments));
        source += "        if _result_value is None:\n";
        source += "            _result_is_null[0] = u8(1)\n";
        if (returnType.type == DataType::Type::VARSIZED)
        {
            source += "            _result_ptr[0] = Ptr[byte]()\n";
            source += "            _result_size[0] = i64(0)\n";
            source += "        else:\n";
            source += "            _result_is_null[0] = u8(0)\n";
            source += "            _result_bytes = _nes_result_bytes(_result_value)\n";
            source += "            _result_ptr[0] = _result_bytes.ptr\n";
            source += "            _result_size[0] = i64(_result_bytes.len)\n";
        }
        else if (returnType.type == DataType::Type::BOOLEAN)
        {
            source += "        else:\n";
            source += "            _result_is_null[0] = u8(0)\n";
            source += "            _result[0] = u8(1) if _result_value else u8(0)\n";
        }
        else if (returnType.type == DataType::Type::CHAR)
        {
            source += "        else:\n";
            source += "            _result_is_null[0] = u8(0)\n";
            source += "            _result[0] = _nes_char_byte(_nes_result_bytes(_result_value))\n";
        }
        else
        {
            source += "        else:\n";
            source += "            _result_is_null[0] = u8(0)\n";
            source += fmt::format("            _result[0] = {}(_result_value)\n", abiResultType);
        }
    }
    else if (returnType.type == DataType::Type::VARSIZED)
    {
        source += fmt::format("        _result_value = _nes_result_bytes({}({}))\n", bodySymbol, join(bodyArguments));
        source += "        _result_ptr[0] = _result_value.ptr\n";
        source += "        _result_size[0] = i64(_result_value.len)\n";
    }
    else if (returnType.type == DataType::Type::BOOLEAN)
    {
        source += fmt::format("        _result[0] = u8(1) if {}({}) else u8(0)\n", bodySymbol, join(bodyArguments));
    }
    else if (returnType.type == DataType::Type::CHAR)
    {
        source += fmt::format("        _result[0] = _nes_char_byte(_nes_result_bytes({}({})))\n", bodySymbol, join(bodyArguments));
    }
    else
    {
        source += fmt::format("        _result[0] = {}({}({}))\n", abiResultType, bodySymbol, join(bodyArguments));
    }
    source += "    except Exception as _exception:\n";
    source += "        _message = str(_exception)\n";
    source += "        _error[0] = UdfErrorHolder(_message.ptr, i64(_message.len))\n";
    source += "        _status = i8(1)\n";
    source += "    return _status\n";
    return source;
}

void replaceCodonStdoutLoads(llvm::Module& module)
{
    std::vector<llvm::LoadInst*> stdoutLoads;
    for (auto& global : module.globals())
    {
        if (!global.getName().starts_with("..default.std.internal.builtin.print."))
        {
            continue;
        }
        for (auto* user : global.users())
        {
            if (auto* load = llvm::dyn_cast<llvm::LoadInst>(user))
            {
                stdoutLoads.emplace_back(load);
            }
        }
    }
    for (auto* load : stdoutLoads)
    {
        llvm::IRBuilder<> builder(load);
        const auto stdoutFunction = module.getOrInsertFunction("seq_stdout", load->getType());
        auto* stdoutValue = builder.CreateCall(stdoutFunction);
        load->replaceAllUsesWith(stdoutValue);
        load->eraseFromParent();
    }
}

void addPythonUdfInterpreterAdapter(
    llvm::Module& module, const std::string& symbol, const std::vector<DataType>& argumentTypes, const DataType& returnType)
{
    auto* typedEntryPoint = module.getFunction(symbol);
    if (typedEntryPoint == nullptr || typedEntryPoint->isDeclaration())
    {
        throw QueryCompilerError("Codon did not emit exported function '{}'", symbol);
    }

    auto& context = module.getContext();
    auto* pointerType = llvm::PointerType::getUnqual(context);
    auto* int64Type = llvm::Type::getInt64Ty(context);
    auto* int8Type = llvm::Type::getInt8Ty(context);
    auto* abiValueType = llvm::StructType::create(context, {pointerType, int64Type, int8Type}, "nes.python_udf_abi_value");
    auto* adapterType = llvm::FunctionType::get(int8Type, {pointerType, pointerType, pointerType}, false);
    auto* adapter = llvm::Function::Create(adapterType, llvm::GlobalValue::ExternalLinkage, interpreterSymbolName(symbol), module);
    auto* entryBlock = llvm::BasicBlock::Create(context, "entry", adapter);
    llvm::IRBuilder<> builder(entryBlock);

    auto* adapterArgument = adapter->arg_begin();
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    llvm::Value* arguments = adapterArgument++;
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    llvm::Value* result = adapterArgument++;
    llvm::Value* error = adapterArgument;
    arguments->setName("arguments");
    result->setName("result");
    error->setName("error");

    const auto fieldPointer = [&](llvm::Value* base, const size_t valueIndex, const unsigned fieldIndex)
    {
        auto* value = builder.CreateConstInBoundsGEP1_64(abiValueType, base, valueIndex);
        return builder.CreateStructGEP(abiValueType, value, fieldIndex);
    };
    const auto loadField = [&](llvm::Value* base, const size_t valueIndex, const unsigned fieldIndex, llvm::Type* fieldType)
    { return builder.CreateLoad(fieldType, fieldPointer(base, valueIndex, fieldIndex)); };

    llvm::SmallVector<llvm::Value*> typedArguments;
    typedArguments.reserve(typedEntryPoint->arg_size());
    size_t typedArgumentIndex = 0;
    for (size_t index = 0; index < argumentTypes.size(); ++index)
    {
        const auto& argumentType = argumentTypes[index];
        auto* data = loadField(arguments, index, 0, pointerType);
        if (argumentType.type == DataType::Type::VARSIZED)
        {
            typedArguments.emplace_back(data);
            ++typedArgumentIndex;
            typedArguments.emplace_back(loadField(arguments, index, 1, int64Type));
            ++typedArgumentIndex;
        }
        else
        {
            if (typedArgumentIndex >= typedEntryPoint->arg_size())
            {
                throw QueryCompilerError("Codon emitted an invalid argument signature for Python UDF '{}'", symbol);
            }
            auto* scalarType = typedEntryPoint->getFunctionType()->getParamType(typedArgumentIndex++);
            typedArguments.emplace_back(builder.CreateLoad(scalarType, data));
        }
        if (argumentType.nullable)
        {
            typedArguments.emplace_back(loadField(arguments, index, 2, int8Type));
            ++typedArgumentIndex;
        }
    }

    if (returnType.type == DataType::Type::VARSIZED)
    {
        typedArguments.emplace_back(fieldPointer(result, 0, 0));
        typedArguments.emplace_back(fieldPointer(result, 0, 1));
        typedArgumentIndex += 2;
    }
    else
    {
        typedArguments.emplace_back(loadField(result, 0, 0, pointerType));
        ++typedArgumentIndex;
    }
    if (returnType.nullable)
    {
        typedArguments.emplace_back(fieldPointer(result, 0, 2));
        ++typedArgumentIndex;
    }
    typedArguments.emplace_back(error);
    ++typedArgumentIndex;

    if (typedArgumentIndex != typedEntryPoint->arg_size())
    {
        throw QueryCompilerError("Codon emitted an invalid argument signature for Python UDF '{}'", symbol);
    }
    builder.CreateRet(builder.CreateCall(typedEntryPoint, typedArguments));
}

void internalizePythonUdfModule(llvm::Module& module, const std::string& symbol)
{
    for (auto& function : module.functions())
    {
        if (!function.isDeclaration() && function.getName() != symbol)
        {
            function.setLinkage(llvm::GlobalValue::InternalLinkage);
        }
    }
    for (auto& global : module.globals())
    {
        if (!global.isDeclaration() && !global.getName().starts_with("llvm."))
        {
            global.setLinkage(llvm::GlobalValue::InternalLinkage);
        }
    }

    llvm::LoopAnalysisManager loopAnalysisManager;
    llvm::FunctionAnalysisManager functionAnalysisManager;
    llvm::CGSCCAnalysisManager cgsccAnalysisManager;
    llvm::ModuleAnalysisManager moduleAnalysisManager;
    llvm::PassBuilder passBuilder;
    passBuilder.registerModuleAnalyses(moduleAnalysisManager);
    passBuilder.registerCGSCCAnalyses(cgsccAnalysisManager);
    passBuilder.registerFunctionAnalyses(functionAnalysisManager);
    passBuilder.registerLoopAnalyses(loopAnalysisManager);
    passBuilder.crossRegisterProxies(loopAnalysisManager, functionAnalysisManager, cgsccAnalysisManager, moduleAnalysisManager);
    llvm::ModulePassManager cleanupPasses;
    cleanupPasses.addPass(llvm::GlobalDCEPass());
    cleanupPasses.addPass(llvm::StripDeadPrototypesPass());
    cleanupPasses.run(module, moduleAnalysisManager);
}


std::string writeBitcode(const llvm::Module& module)
{
    llvm::SmallVector<char, 0> bitcode;
    llvm::raw_svector_ostream bitcodeStream(bitcode);
    llvm::WriteBitcodeToFile(module, bitcodeStream);
    return {bitcode.data(), bitcode.size()};
}

/// The Codon plugins are built next to the worker and their build-time paths are compiled in. A worker that runs somewhere
/// else, e.g. in a container image, finds them in the `nes-codon-plugins` directory next to its executable instead.
std::string codonPluginPath(const std::string& builtPath)
{
    if (std::filesystem::exists(builtPath))
    {
        return builtPath;
    }
    const auto besideExecutable = currentExecutableDirectory() / "nes-codon-plugins" / std::filesystem::path{builtPath}.filename();
    return std::filesystem::exists(besideExecutable) ? besideExecutable.string() : builtPath;
}

/// The directory of the generated UDF sources, which the debug information of the compiled UDFs refers to.
std::filesystem::path pythonUdfSourceDirectory()
{
    /// NOLINTNEXTLINE(concurrency-mt-unsafe)
    if (const char* directory = std::getenv("NES_PYTHON_UDF_SOURCE_DIR"); directory != nullptr && *directory != '\0')
    {
        return directory;
    }
    return std::filesystem::temp_directory_path() / "nes-python-udf-sources";
}

}

void validatePythonUdfModule(const llvm::Module& module, const std::string& symbol)
{
    static const std::unordered_set<std::string_view> BannedFunctions{
        "seq_alloc_uncollectable", "seq_alloc_atomic_uncollectable", "seq_register_finalizer"};
    /// The libc character classification and case mapping functions Codon's str methods call, and modf, which numpy's
    /// module initialization calls and the LLVM optimizations remove elsewhere; stateless, resolved from the process.
    static const std::unordered_set<std::string_view> AllowedProcessFunctions{
        "isdigit", "isspace", "isupper", "islower", "isalpha", "isalnum", "toupper", "tolower", "modf"};
    const auto pluginFunctions = getCodonPluginNativeSymbols();

    const auto* entryPoint = module.getFunction(symbol);
    if (entryPoint == nullptr || entryPoint->isDeclaration())
    {
        throw QueryCompilerError("Codon did not emit exported function '{}'", symbol);
    }

    std::vector<std::string> unsupportedExternalFunctions;
    for (const auto& function : module.functions())
    {
        const std::string_view name{function.getName().data(), function.getName().size()};
        if (BannedFunctions.contains(name) || name.starts_with("GC_") || name.starts_with("seq_gc_"))
        {
            throw QueryCompilerError(
                "Python UDF uses unsupported Codon runtime function '{}'; allocations escaping the UDF invocation and direct "
                "garbage-collector access are not supported",
                name);
        }
        if (function.isDeclaration() && !name.starts_with("llvm.") && !AllowedProcessFunctions.contains(name)
            && !std::ranges::contains(PythonUdfRuntimeSymbolNames, name) && !pluginFunctions.contains(std::string{name}))
        {
            unsupportedExternalFunctions.emplace_back(name);
        }
    }
    if (!unsupportedExternalFunctions.empty())
    {
        std::ranges::sort(unsupportedExternalFunctions);
        throw QueryCompilerError(
            "Python UDF requires unsupported external functions {}; only the NES Codon compatibility ABI is available",
            join(unsupportedExternalFunctions));
    }
    for (const auto& global : module.globals())
    {
        if (global.isDeclaration() && !global.getName().starts_with("llvm."))
        {
            throw QueryCompilerError(
                "Python UDF requires unsupported external global '{}'; only the NES Codon compatibility ABI is available",
                global.getName().str());
        }
    }

    std::string verificationMessage;
    llvm::raw_string_ostream verificationStream(verificationMessage);
    if (llvm::verifyModule(module, &verificationStream))
    {
        throw QueryCompilerError("Codon emitted an invalid LLVM module: {}", verificationMessage);
    }
}

void loadCodonPlugins()
{
    loadCodonPlugin(codonPluginPath(NES_CODON_BLAS_PLUGIN_PATH));
    loadCodonPlugin(codonPluginPath(NES_CODON_OPENCV_PLUGIN_PATH));
    loadCodonPlugin(codonPluginPath(NES_CODON_COMPAT_PLUGIN_PATH));
    loadCodonPlugin(codonPluginPath(NES_CODON_JSON_PLUGIN_PATH));
    loadCodonPlugin(codonPluginPath(NES_CODON_RE_PLUGIN_PATH));
    loadCodonPlugin(codonPluginPath(NES_CODON_UNICODE_PLUGIN_PATH));
}

CompiledInlineUdf compilePythonUdf(
    const std::string& symbol,
    const std::vector<std::string>& parameterNames,
    const std::string& body,
    const std::vector<DataType>& argumentTypes,
    const DataType& returnType,
    const std::vector<std::string>& importPaths,
    const std::vector<uint64_t>& bytesArguments,
    const std::vector<bool>& bodyNullable)
{
    static std::mutex codonCompilerMutex;
    const auto mutexWaitStart = std::chrono::steady_clock::now();
    const std::scoped_lock lock{codonCompilerMutex};
    const auto compilationStart = std::chrono::steady_clock::now();
    try
    {
        loadCodonPlugins();
        const auto source = createPythonUdfSource(symbol, parameterNames, body, argumentTypes, returnType, bytesArguments, bodyNullable);
        /// Named by the source, so that workers that compile the same UDF write the same file.
        const auto sourceDirectory = pythonUdfSourceDirectory();
        std::filesystem::create_directories(sourceDirectory);
        const auto sourcePath = (sourceDirectory / fmt::format("{:016x}.py", std::hash<std::string>{}(source))).string();
        const auto codonResult = compileWithCodon(sourcePath, source, symbol, importPaths);
        const auto& codonBitcode = codonResult.llvmBitcode;

        llvm::LLVMContext llvmContext;
        auto parsedModule = llvm::parseBitcodeFile(
            llvm::MemoryBufferRef{llvm::StringRef{codonBitcode.data(), codonBitcode.size()}, sourcePath}, llvmContext);
        if (!parsedModule)
        {
            throw QueryCompilerError(
                "Could not read LLVM bitcode for Python UDF '{}': {}", symbol, llvm::toString(parsedModule.takeError()));
        }
        const auto cleanupStart = std::chrono::steady_clock::now();
        auto pipelineModule = std::move(*parsedModule);
        replaceCodonStdoutLoads(*pipelineModule);
        addPythonUdfInterpreterAdapter(*pipelineModule, symbol, argumentTypes, returnType);
        auto interpreterModule = llvm::CloneModule(*pipelineModule);
        internalizePythonUdfModule(*pipelineModule, symbol);
        validatePythonUdfModule(*pipelineModule, symbol);
        internalizePythonUdfModule(*interpreterModule, interpreterSymbolName(symbol));
        validatePythonUdfModule(*interpreterModule, interpreterSymbolName(symbol));

        const auto bitcodeStart = std::chrono::steady_clock::now();
        auto pipelineBitcode = writeBitcode(*pipelineModule);
        auto interpreterBitcode = writeBitcode(*interpreterModule);
        const auto compilationEnd = std::chrono::steady_clock::now();
        fmt::print(
            stderr,
            "[PythonUDF] Compiled '{}' with Codon in {:.3f} ms (mutex wait: {:.3f} ms, parse: {:.3f} ms, compile: {:.3f} "
            "ms, LLVM optimize: {:.3f} ms, cleanup: {:.3f} ms, bitcode: {:.3f} ms; debug source: '{}')\n",
            symbol,
            elapsedMilliseconds(compilationStart, compilationEnd),
            elapsedMilliseconds(mutexWaitStart, compilationStart),
            codonResult.parseMilliseconds,
            codonResult.compileMilliseconds,
            codonResult.optimizeMilliseconds,
            elapsedMilliseconds(cleanupStart, bitcodeStart),
            elapsedMilliseconds(bitcodeStart, compilationEnd),
            sourcePath);
        return {.symbol = symbol, .pipelineBitcode = std::move(pipelineBitcode), .interpreterBitcode = std::move(interpreterBitcode)};
    }
    catch (const Exception&)
    {
        throw;
    }
    catch (const std::exception& exception)
    {
        throw QueryCompilerError("Could not compile Python UDF '{}': {}", symbol, exception.what());
    }
}

/// A UDF named by its entry point, expressed as the inline function the Codon path compiles: the ENTRYPOINT module is
/// imported and called from a generated body. A nullable argument reaches the entry point as Optional[T], like any body.
InlineUdfSource codonUdfSource(const UdfDescriptor& descriptor, const std::vector<DataType>& inputTypes)
{
    const auto entrypoint = splitEntrypoint(descriptor.getEntrypoint());
    INVARIANT(entrypoint.has_value(), "Codon UDF '{}' has no 'module.function' entry point", descriptor.getName());
    INVARIANT(
        inputTypes.size() == descriptor.getArgTypes().size(), "UDF '{}' called with a wrong number of arguments", descriptor.getName());

    InlineUdfSource call;
    std::vector<std::string> entryArguments;
    for (size_t index = 0; index < inputTypes.size(); ++index)
    {
        call.parameterNames.emplace_back(fmt::format("_p{}", index));
        /// The parameters are bytes or str as the declaration says (BYTES), see createPythonUdfSource.
        entryArguments.emplace_back(fmt::format("_p{}", index));
    }
    call.body = fmt::format(
        "from {} import {} as _nes_udf_entry\nfrom nes_compat import _nes_result\n"
        "return _nes_result(_nes_udf_entry({}))",
        entrypoint->first,
        entrypoint->second,
        fmt::join(entryArguments, ", "));
    return call;
}

/// Where Codon looks for the ENTRYPOINT module: the configured python_udf_import_paths plus NES_UDF_PATH, the module
/// search path the CPython bridge uses, so a single directory of UDF modules serves every backend.
std::vector<std::string> codonImportPaths(const std::vector<std::string>& configured)
{
    auto paths = configured;
    const auto* const udfPath = std::getenv("NES_UDF_PATH"); /// NOLINT(concurrency-mt-unsafe)
    if (udfPath == nullptr)
    {
        return paths;
    }
    const std::string_view remaining{udfPath};
    size_t begin = 0;
    while (begin <= remaining.size())
    {
        const auto end = remaining.find(':', begin);
        const auto directory = std::string{remaining.substr(begin, end == std::string_view::npos ? std::string_view::npos : end - begin)};
        if (!directory.empty() && std::ranges::find(paths, directory) == paths.end())
        {
            paths.push_back(directory);
        }
        if (end == std::string_view::npos)
        {
            break;
        }
        begin = end + 1;
    }
    return paths;
}

}
/// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
