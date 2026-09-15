/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0
*/
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <unistd.h>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PythonPhysicalFunction.hpp>
#include <Functions/PythonUDF/PythonUdfFilesystem.hpp>
#include <Functions/PythonUDF/PythonUdfRuntime.hpp>
#include <Interface/Record.hpp>
#include <NESCodonPlugin/Registry.hpp>
#include <fmt/format.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/Bitcode/BitcodeWriter.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Transforms/IPO/GlobalDCE.h>
#include <llvm/Transforms/IPO/StripDeadPrototypes.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <nautilus/select.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/tracing/TracingUtil.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_std.hpp>
#include <Arena.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>

namespace NES
{
struct PythonUdfInterpreterExecutable
{
    using Function = nautilus::engine::ModuleFunction<uint8_t(PythonUdfAbiValue*, PythonUdfAbiValue*, UdfErrorHolder*)>;

    explicit PythonUdfInterpreterExecutable(Function function) : function(std::move(function)) { }

    Function function;
};

namespace
{
std::atomic_uint64_t nextPythonUdfId = 0;

double elapsedMilliseconds(const std::chrono::steady_clock::time_point start, const std::chrono::steady_clock::time_point end)
{
    return std::chrono::duration<double, std::milli>{end - start}.count();
}

static_assert(offsetof(UdfErrorHolder, errorPtr) == 0);
static_assert(offsetof(UdfErrorHolder, errorSize) == sizeof(int8_t*));
static_assert(offsetof(PythonUdfAbiValue, data) == 0);
static_assert(offsetof(PythonUdfAbiValue, size) == sizeof(int8_t*));
static_assert(offsetof(PythonUdfAbiValue, isNull) == sizeof(int8_t*) + sizeof(uint64_t));
static_assert(sizeof(PythonUdfAbiValue) == sizeof(int8_t*) + (2 * sizeof(uint64_t)));

std::string interpreterSymbolName(const std::string& symbol)
{
    return symbol + "_interpreter";
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
            return "str";
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Unsupported Python UDF type {}", type);
    }
    std::unreachable();
}

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

std::string createPythonUdfSource(
    const std::string& symbol,
    const std::vector<std::string>& parameterNames,
    const std::string_view body,
    const std::vector<DataType>& argumentTypes,
    const DataType& returnType)
{
    PRECONDITION(parameterNames.size() == argumentTypes.size(), "Python UDF argument type and parameter name counts differ");

    std::vector<std::string> parameters;
    std::vector<std::string> abiParameters;
    std::vector<std::string> bodyArguments;
    parameters.reserve(parameterNames.size());
    bodyArguments.reserve(parameterNames.size());
    for (size_t index = 0; index < parameterNames.size(); ++index)
    {
        const auto pythonType = pythonTypeName(argumentTypes[index]);
        parameters.emplace_back(fmt::format(
            "{}: {}{}{}",
            parameterNames[index],
            argumentTypes[index].nullable ? "Optional[" : "",
            pythonType,
            argumentTypes[index].nullable ? "]" : ""));
        if (argumentTypes[index].type == DataType::Type::VARSIZED)
        {
            abiParameters.emplace_back(fmt::format("_arg{}_ptr: Ptr[byte]", index));
            abiParameters.emplace_back(fmt::format("_arg{}_size: i64", index));
            if (argumentTypes[index].nullable)
            {
                abiParameters.emplace_back(fmt::format("_arg{}_is_null: u8", index));
                bodyArguments.emplace_back(fmt::format(
                    "Optional[str]() if bool(_arg{}_is_null) else Optional[str](str(_arg{}_ptr, int(_arg{}_size)))",
                    index,
                    index,
                    index,
                    index));
            }
            else
            {
                bodyArguments.emplace_back(fmt::format("str(_arg{}_ptr, int(_arg{}_size))", index, index));
            }
        }
        else
        {
            abiParameters.emplace_back(fmt::format("_arg{}: {}", index, codonTypeName(argumentTypes[index])));
            if (argumentTypes[index].nullable)
            {
                abiParameters.emplace_back(fmt::format("_arg{}_is_null: u8", index));
                bodyArguments.emplace_back(fmt::format(
                    "Optional[{}]() if bool(_arg{}_is_null) else Optional[{}]({}(_arg{}))",
                    pythonType,
                    index,
                    pythonType,
                    pythonType,
                    index));
            }
            else
            {
                bodyArguments.emplace_back(fmt::format("{}(_arg{})", pythonType, index));
            }
        }
    }

    const auto bodySymbol = symbol + "_body";
    const auto pythonResultType = pythonTypeName(returnType);
    auto source = fmt::format(
        "@tuple\n"
        "class UdfErrorHolder:\n"
        "    error_ptr: Ptr[byte]\n"
        "    error_size: i64\n\n"
        "def {}({}) -> {}{}{}:\n{}\n@export\n",
        bodySymbol,
        join(parameters),
        returnType.nullable ? "Optional[" : "",
        pythonResultType,
        returnType.nullable ? "]" : "",
        indentPythonBody(body));

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
            source += "            _result_value = str(_result_value)\n";
            source += "            _result_ptr[0] = _result_value.ptr\n";
            source += "            _result_size[0] = i64(len(_result_value))\n";
        }
        else if (returnType.type == DataType::Type::BOOLEAN)
        {
            source += "        else:\n";
            source += "            _result_is_null[0] = u8(0)\n";
            source += "            _result[0] = u8(1) if _result_value else u8(0)\n";
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
        source += fmt::format("        _result_value = str({}({}))\n", bodySymbol, join(bodyArguments));
        source += "        _result_ptr[0] = _result_value.ptr\n";
        source += "        _result_size[0] = i64(len(_result_value))\n";
    }
    else if (returnType.type == DataType::Type::BOOLEAN)
    {
        source += fmt::format("        _result[0] = u8(1) if {}({}) else u8(0)\n", bodySymbol, join(bodyArguments));
    }
    else
    {
        source += fmt::format("        _result[0] = {}({}({}))\n", abiResultType, bodySymbol, join(bodyArguments));
    }
    source += "    except Exception as _exception:\n";
    source += "        _message = str(_exception)\n";
    source += "        _error[0] = UdfErrorHolder(_message.ptr, i64(len(_message)))\n";
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

    auto adapterArgument = adapter->arg_begin();
    llvm::Value* arguments = adapterArgument++;
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

void validatePythonUdfModule(const llvm::Module& module, const std::string& symbol)
{
    static const std::unordered_set<std::string_view> bannedFunctions{
        "seq_alloc_uncollectable", "seq_alloc_atomic_uncollectable", "seq_register_finalizer"};
    static const std::unordered_set<std::string_view> allowedProcessFunctions{"isspace"};
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
        if (bannedFunctions.contains(name) || name.starts_with("GC_") || name.starts_with("seq_gc_"))
        {
            throw QueryCompilerError(
                "Python UDF uses unsupported Codon runtime function '{}'; allocations escaping the UDF invocation and direct "
                "garbage-collector access are not supported",
                name);
        }
        if (function.isDeclaration() && !name.starts_with("llvm.") && !allowedProcessFunctions.contains(name)
            && !isPythonUdfRuntimeSymbol(name) && !pluginFunctions.contains(std::string{name}))
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

struct CompiledPythonUdf
{
    std::string pipelineBitcode;
    std::string interpreterBitcode;
};

std::string writeBitcode(const llvm::Module& module)
{
    llvm::SmallVector<char, 0> bitcode;
    llvm::raw_svector_ostream bitcodeStream(bitcode);
    llvm::WriteBitcodeToFile(module, bitcodeStream);
    return {bitcode.data(), bitcode.size()};
}

CompiledPythonUdf compilePythonUdf(
    const std::string& symbol,
    const std::vector<std::string>& parameterNames,
    const std::string& body,
    const std::vector<DataType>& argumentTypes,
    const DataType& returnType,
    const std::vector<std::string>& importPaths)
{
    static std::mutex codonCompilerMutex;
    const auto mutexWaitStart = std::chrono::steady_clock::now();
    const std::scoped_lock lock{codonCompilerMutex};
    const auto compilationStart = std::chrono::steady_clock::now();
    try
    {
        loadCodonPlugin(NES_CODON_BLAS_PLUGIN_PATH);
        loadCodonPlugin(NES_CODON_OPENCV_PLUGIN_PATH);
        const auto source = createPythonUdfSource(symbol, parameterNames, body, argumentTypes, returnType);
        const auto sourcePath
            = (std::filesystem::temp_directory_path() / fmt::format("{}_{}.py", symbol, static_cast<uint64_t>(::getpid()))).string();
        const auto codonResult = compileWithCodon(sourcePath, source, importPaths);
        llvm::LLVMContext llvmContext;
        auto parsedModule = llvm::parseBitcodeFile(
            llvm::MemoryBufferRef{llvm::StringRef{codonResult.llvmBitcode.data(), codonResult.llvmBitcode.size()}, sourcePath},
            llvmContext);
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
        const auto interpreterSymbol = interpreterSymbolName(symbol);
        internalizePythonUdfModule(*interpreterModule, interpreterSymbol);
        validatePythonUdfModule(*interpreterModule, interpreterSymbol);

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
        return {std::move(pipelineBitcode), std::move(interpreterBitcode)};
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

using PythonAbiValue = std::variant<
    nautilus::val<uint8_t>,
    nautilus::val<uint16_t>,
    nautilus::val<uint32_t>,
    nautilus::val<uint64_t>,
    nautilus::val<int8_t>,
    nautilus::val<int16_t>,
    nautilus::val<int32_t>,
    nautilus::val<int64_t>,
    nautilus::val<float>,
    nautilus::val<double>,
    nautilus::val<uint8_t*>,
    nautilus::val<uint16_t*>,
    nautilus::val<uint32_t*>,
    nautilus::val<uint64_t*>,
    nautilus::val<int8_t*>,
    nautilus::val<int16_t*>,
    nautilus::val<int32_t*>,
    nautilus::val<int64_t*>,
    nautilus::val<float*>,
    nautilus::val<double*>,
    nautilus::val<int8_t**>,
    nautilus::val<PythonUdfAbiValue*>,
    nautilus::val<UdfErrorHolder*>>;

template <typename R>
nautilus::val<R> dynamicInvoke(const std::string& symbol, const std::vector<PythonAbiValue>& arguments)
{
    PRECONDITION(nautilus::tracing::inTracer(), "A Python UDF can only be invoked while tracing");
    std::vector<nautilus::tracing::TypedValueRef> argumentReferences;
    argumentReferences.reserve(arguments.size());
    for (const auto& argument : arguments)
    {
        std::visit([&](const auto& value) { argumentReferences.emplace_back(value.getState()); }, argument);
    }
    auto& result = nautilus::tracing::traceCall(
        symbol, nautilus::tracing::TypeResolver<R>::to_type(), argumentReferences, nautilus::FunctionAttributes{});
    return nautilus::val<R>{result};
}

void appendPythonArgument(std::vector<PythonAbiValue>& arguments, const VarVal& argument, const DataType& type)
{
#define NES_APPEND_PYTHON(TYPE, CPP_TYPE) \
    case DataType::Type::TYPE: \
        arguments.emplace_back(argument.getRawValueAs<nautilus::val<CPP_TYPE>>()); \
        break
    switch (type.type)
    {
        NES_APPEND_PYTHON(UINT8, uint8_t);
        NES_APPEND_PYTHON(UINT16, uint16_t);
        NES_APPEND_PYTHON(UINT32, uint32_t);
        NES_APPEND_PYTHON(UINT64, uint64_t);
        NES_APPEND_PYTHON(INT8, int8_t);
        NES_APPEND_PYTHON(INT16, int16_t);
        NES_APPEND_PYTHON(INT32, int32_t);
        NES_APPEND_PYTHON(INT64, int64_t);
        NES_APPEND_PYTHON(FLOAT32, float);
        NES_APPEND_PYTHON(FLOAT64, double);
        case DataType::Type::BOOLEAN:
            arguments.emplace_back(
                nautilus::select(argument.getRawValueAs<nautilus::val<bool>>(), nautilus::val<uint8_t>{1}, nautilus::val<uint8_t>{0}));
            break;
        case DataType::Type::VARSIZED: {
            const auto string = argument.getRawValueAs<VariableSizedData>();
            arguments.emplace_back(string.getContent());
            arguments.emplace_back(string.getSize());
            break;
        }
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Unsupported Python UDF argument type {}", type);
    }
    if (type.nullable)
    {
        arguments.emplace_back(nautilus::select(argument.isNull(), nautilus::val<uint8_t>{1}, nautilus::val<uint8_t>{0}));
    }
#undef NES_APPEND_PYTHON
}

[[noreturn]] void throwPythonUdfError(UdfErrorHolder* error)
{
    const auto* characters = reinterpret_cast<const char*>(error->errorPtr);
    throw UnknownException("Python UDF failed: {}", std::string_view{characters, error->errorSize});
}

void checkPythonUdfStatus(const uint8_t status, UdfErrorHolder* error)
{
    if (status != 0)
    {
        throwPythonUdfError(error);
    }
}

void invokePythonWithErrorHandling(const std::string& symbol, ArenaRef& arena, std::vector<PythonAbiValue>& arguments)
{
    nautilus::val<UdfErrorHolder> errorHolder;
    arguments.emplace_back(&errorHolder);
    nautilus::invoke(activatePythonUdfArena, arena.getArena());
    const auto status = dynamicInvoke<uint8_t>(symbol, arguments);
    nautilus::invoke(checkPythonUdfStatus, status, &errorHolder);
}

template <typename R>
struct PythonScalarResult
{
    nautilus::val<R> value;
    nautilus::val<bool> isNull;
};

template <typename R>
PythonScalarResult<R>
invokePythonScalar(const std::string& symbol, const bool nullable, ArenaRef& arena, std::vector<PythonAbiValue>& arguments)
{
    auto resultPointer = static_cast<nautilus::val<R*>>(arena.allocateMemory(nautilus::val<size_t>{sizeof(R)}));
    auto resultIsNull = static_cast<nautilus::val<uint8_t*>>(arena.allocateMemory(nautilus::val<size_t>{sizeof(uint8_t)}));
    *resultPointer = nautilus::val<R>{0};
    *resultIsNull = nautilus::val<uint8_t>{0};
    arguments.emplace_back(resultPointer);
    if (nullable)
    {
        arguments.emplace_back(resultIsNull);
    }
    invokePythonWithErrorHandling(symbol, arena, arguments);
    return PythonScalarResult<R>{*resultPointer, *resultIsNull != nautilus::val<uint8_t>{0}};
}

VarVal invokePython(const std::string& symbol, const DataType& returnType, ArenaRef& arena, std::vector<PythonAbiValue>& arguments)
{
#define NES_INVOKE_PYTHON(TYPE, CPP_TYPE) \
    case DataType::Type::TYPE: { \
        const auto [value, isNull] = invokePythonScalar<CPP_TYPE>(symbol, returnType.nullable, arena, arguments); \
        result.emplace(value, returnType.nullable, isNull); \
        break; \
    }
    std::optional<VarVal> result;
    switch (returnType.type)
    {
        NES_INVOKE_PYTHON(UINT8, uint8_t);
        NES_INVOKE_PYTHON(UINT16, uint16_t);
        NES_INVOKE_PYTHON(UINT32, uint32_t);
        NES_INVOKE_PYTHON(UINT64, uint64_t);
        NES_INVOKE_PYTHON(INT8, int8_t);
        NES_INVOKE_PYTHON(INT16, int16_t);
        NES_INVOKE_PYTHON(INT32, int32_t);
        NES_INVOKE_PYTHON(INT64, int64_t);
        NES_INVOKE_PYTHON(FLOAT32, float);
        NES_INVOKE_PYTHON(FLOAT64, double);
        case DataType::Type::BOOLEAN: {
            const auto [value, isNull] = invokePythonScalar<uint8_t>(symbol, returnType.nullable, arena, arguments);
            result.emplace(value != nautilus::val<uint8_t>{0}, returnType.nullable, isNull);
            break;
        }
        case DataType::Type::VARSIZED: {
            /// Codon strings are {length, data} values backed by Codon's runtime. Flatten the result into out-parameters and
            /// copy it immediately so no Codon-owned pointer escapes this invocation.
            auto resultPointer = static_cast<nautilus::val<int8_t**>>(arena.allocateMemory(nautilus::val<size_t>{sizeof(int8_t*)}));
            auto resultSize = static_cast<nautilus::val<uint64_t*>>(arena.allocateMemory(nautilus::val<size_t>{sizeof(uint64_t)}));
            auto resultIsNull = static_cast<nautilus::val<uint8_t*>>(arena.allocateMemory(nautilus::val<size_t>{sizeof(uint8_t)}));
            *resultIsNull = nautilus::val<uint8_t>{0};
            arguments.emplace_back(resultPointer);
            arguments.emplace_back(resultSize);
            if (returnType.nullable)
            {
                arguments.emplace_back(resultIsNull);
            }
            invokePythonWithErrorHandling(symbol, arena, arguments);
            const nautilus::val<int8_t*> returnedContent = *resultPointer;
            const nautilus::val<uint64_t> returnedSize = *resultSize;
            const auto copiedResult = arena.allocateVariableSizedData(returnedSize);
            nautilus::memcpy(copiedResult.getContent(), returnedContent, returnedSize);
            result.emplace(copiedResult, returnType.nullable, *resultIsNull != nautilus::val<uint8_t>{0});
            break;
        }
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Unsupported Python UDF return type {}", returnType);
    }
#undef NES_INVOKE_PYTHON
    INVARIANT(result.has_value(), "Python UDF result was not initialized");
    return std::move(*result);
}

std::shared_ptr<PythonUdfInterpreterExecutable>
compilePythonUdfInterpreter(const std::string& symbol, const std::string& interpreterBitcode)
{
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", true);
    options.setOption("engine.backend", std::string{"mlir"});
    options.setOption("engine.compilationStrategy", std::string{"legacy"});
    options.setOption("mlir.enableMultithreading", false);
    options.setOption("mlir.inline_invoke_calls", true);
    options.setOption("mlir.debug.enable", true);
    nautilus::engine::NautilusEngine engine{options};
    const auto udfSymbol = interpreterSymbolName(symbol);
    engine.registerUDF(udfSymbol, interpreterBitcode);
    for (const auto& [name, address] : getPythonUdfRuntimeSymbols())
    {
        engine.registerExternalSymbol(name, address);
    }
    for (const auto& [name, address] : getCodonPluginNativeSymbols())
    {
        engine.registerExternalSymbol(name, address);
    }

    auto module = engine.createModule();
    std::function<nautilus::val<uint8_t>(
        nautilus::val<PythonUdfAbiValue*>, nautilus::val<PythonUdfAbiValue*>, nautilus::val<UdfErrorHolder*>)>
        trampoline
        = [udfSymbol](
              nautilus::val<PythonUdfAbiValue*> arguments, nautilus::val<PythonUdfAbiValue*> result, nautilus::val<UdfErrorHolder*> error)
    {
        std::vector<PythonAbiValue> abiArguments;
        abiArguments.emplace_back(arguments);
        abiArguments.emplace_back(result);
        abiArguments.emplace_back(error);
        return dynamicInvoke<uint8_t>(udfSymbol, abiArguments);
    };
    static constexpr std::string_view trampolineName = "invokePythonUdf";
    module.registerFunction(std::string{trampolineName}, std::move(trampoline));
    auto compiledModule = module.compile();
    return std::make_shared<PythonUdfInterpreterExecutable>(
        compiledModule.getFunction<uint8_t(PythonUdfAbiValue*, PythonUdfAbiValue*, UdfErrorHolder*)>(std::string{trampolineName}));
}

struct alignas(uint64_t) PythonUdfScalarStorage
{
    std::array<std::byte, sizeof(uint64_t)> bytes{};
};

template <typename T>
void storeInterpreterArgument(PythonUdfAbiValue& destination, PythonUdfScalarStorage& storage, const nautilus::val<T>& source)
{
    static_assert(sizeof(T) <= sizeof(storage.bytes));
    const auto rawValue = nautilus::details::RawValueResolver<T>::getRawValue(source);
    std::memcpy(storage.bytes.data(), &rawValue, sizeof(rawValue));
    destination.data = reinterpret_cast<int8_t*>(storage.bytes.data());
    destination.size = sizeof(rawValue);
}

void appendInterpreterArgument(
    PythonUdfAbiValue& destination, PythonUdfScalarStorage& storage, const VarVal& argument, const DataType& type)
{
#define NES_STORE_INTERPRETER_ARGUMENT(TYPE, CPP_TYPE) \
    case DataType::Type::TYPE: \
        storeInterpreterArgument(destination, storage, argument.getRawValueAs<nautilus::val<CPP_TYPE>>()); \
        break
    switch (type.type)
    {
        NES_STORE_INTERPRETER_ARGUMENT(UINT8, uint8_t);
        NES_STORE_INTERPRETER_ARGUMENT(UINT16, uint16_t);
        NES_STORE_INTERPRETER_ARGUMENT(UINT32, uint32_t);
        NES_STORE_INTERPRETER_ARGUMENT(UINT64, uint64_t);
        NES_STORE_INTERPRETER_ARGUMENT(INT8, int8_t);
        NES_STORE_INTERPRETER_ARGUMENT(INT16, int16_t);
        NES_STORE_INTERPRETER_ARGUMENT(INT32, int32_t);
        NES_STORE_INTERPRETER_ARGUMENT(INT64, int64_t);
        NES_STORE_INTERPRETER_ARGUMENT(FLOAT32, float);
        NES_STORE_INTERPRETER_ARGUMENT(FLOAT64, double);
        case DataType::Type::BOOLEAN: {
            const auto value = argument.getRawValueAs<nautilus::val<bool>>();
            const uint8_t rawValue = nautilus::details::RawValueResolver<bool>::getRawValue(value) ? 1 : 0;
            std::memcpy(storage.bytes.data(), &rawValue, sizeof(rawValue));
            destination.data = reinterpret_cast<int8_t*>(storage.bytes.data());
            destination.size = sizeof(rawValue);
            break;
        }
        case DataType::Type::VARSIZED: {
            const auto string = argument.getRawValueAs<VariableSizedData>();
            destination.data = nautilus::details::RawValueResolver<int8_t*>::getRawValue(string.getContent());
            destination.size = nautilus::details::RawValueResolver<uint64_t>::getRawValue(string.getSize());
            break;
        }
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Unsupported Python UDF argument type {}", type);
    }
    destination.isNull = type.nullable && nautilus::details::RawValueResolver<bool>::getRawValue(argument.isNull());
#undef NES_STORE_INTERPRETER_ARGUMENT
}

template <typename T>
T loadInterpreterScalarResult(const PythonUdfScalarStorage& storage)
{
    T result{};
    std::memcpy(&result, storage.bytes.data(), sizeof(result));
    return result;
}

VarVal invokePythonInterpreter(
    const PythonUdfInterpreterExecutable& executable,
    const std::vector<VarVal>& values,
    const std::vector<DataType>& argumentTypes,
    const DataType& returnType,
    ArenaRef& arena)
{
    std::vector<PythonUdfScalarStorage> argumentStorage(values.size());
    std::vector<PythonUdfAbiValue> abiArguments(values.size());
    for (size_t index = 0; index < values.size(); ++index)
    {
        appendInterpreterArgument(abiArguments[index], argumentStorage[index], values[index], argumentTypes[index]);
    }

    PythonUdfScalarStorage resultStorage;
    PythonUdfAbiValue result{reinterpret_cast<int8_t*>(resultStorage.bytes.data()), sizeof(resultStorage.bytes), 0};
    UdfErrorHolder error{};
    activatePythonUdfArena(nautilus::details::RawValueResolver<Arena*>::getRawValue(arena.getArena()));
    checkPythonUdfStatus(executable.function(abiArguments.data(), &result, &error), &error);
    const auto isNull = result.isNull != 0;

#define NES_LOAD_INTERPRETER_RESULT(TYPE, CPP_TYPE) \
    case DataType::Type::TYPE: \
        return VarVal \
        { \
            loadInterpreterScalarResult<CPP_TYPE>(resultStorage), returnType.nullable, isNull \
        }
    switch (returnType.type)
    {
        NES_LOAD_INTERPRETER_RESULT(UINT8, uint8_t);
        NES_LOAD_INTERPRETER_RESULT(UINT16, uint16_t);
        NES_LOAD_INTERPRETER_RESULT(UINT32, uint32_t);
        NES_LOAD_INTERPRETER_RESULT(UINT64, uint64_t);
        NES_LOAD_INTERPRETER_RESULT(INT8, int8_t);
        NES_LOAD_INTERPRETER_RESULT(INT16, int16_t);
        NES_LOAD_INTERPRETER_RESULT(INT32, int32_t);
        NES_LOAD_INTERPRETER_RESULT(INT64, int64_t);
        NES_LOAD_INTERPRETER_RESULT(FLOAT32, float);
        NES_LOAD_INTERPRETER_RESULT(FLOAT64, double);
        case DataType::Type::BOOLEAN:
            return VarVal{loadInterpreterScalarResult<uint8_t>(resultStorage) != 0, returnType.nullable, isNull};
        case DataType::Type::VARSIZED: {
            const auto copiedResult = arena.allocateVariableSizedData(nautilus::val<uint64_t>{result.size});
            nautilus::memcpy(copiedResult.getContent(), nautilus::val<int8_t*>{result.data}, nautilus::val<uint64_t>{result.size});
            return VarVal{copiedResult, returnType.nullable, isNull};
        }
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Unsupported Python UDF return type {}", returnType);
    }
#undef NES_LOAD_INTERPRETER_RESULT
    std::unreachable();
}
}

PythonPhysicalFunction::PythonPhysicalFunction(
    std::vector<std::string> parameterNames,
    std::string body,
    std::vector<PhysicalFunction> arguments,
    std::vector<DataType> argumentTypes,
    DataType returnType,
    const std::vector<std::string>& importPaths)
    : arguments(std::move(arguments))
    , argumentTypes(std::move(argumentTypes))
    , returnType(returnType)
    , symbolName(fmt::format("__nes_python_udf_{}", nextPythonUdfId.fetch_add(1)))
{
    PRECONDITION(this->arguments.size() == this->argumentTypes.size(), "Python UDF argument and type counts differ");
    PRECONDITION(!this->arguments.empty(), "Python UDF requires at least one argument");
    auto compiledUdf = compilePythonUdf(symbolName, parameterNames, body, this->argumentTypes, returnType, importPaths);
    llvmBitcode = std::move(compiledUdf.pipelineBitcode);
    interpreterLlvmBitcode = std::move(compiledUdf.interpreterBitcode);
}

void PythonPhysicalFunction::setupSelf(CompilationContext& compilationContext) const
{
    if (!compilationContext.isCompiled())
    {
        interpreterExecutable = compilePythonUdfInterpreter(symbolName, interpreterLlvmBitcode);
        return;
    }
    for (const auto& [name, address] : getPythonUdfRuntimeSymbols())
    {
        compilationContext.registerExternalSymbol(name, address);
    }
    for (const auto& [name, address] : getCodonPluginNativeSymbols())
    {
        compilationContext.registerExternalSymbol(name, address);
    }
    compilationContext.registerUDF(symbolName, llvmBitcode);
}

VarVal PythonPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> values;
    values.reserve(arguments.size());
    for (const auto& argument : arguments)
    {
        values.emplace_back(argument.execute(record, arena));
    }

    if (!nautilus::tracing::inTracer())
    {
        INVARIANT(interpreterExecutable != nullptr, "Python UDF interpreter trampoline was not compiled during setup");
        return invokePythonInterpreter(*interpreterExecutable, values, argumentTypes, returnType, arena);
    }

    std::vector<PythonAbiValue> abiArguments;
    abiArguments.reserve(values.size() * 2 + 3);
    for (size_t index = 0; index < values.size(); ++index)
    {
        appendPythonArgument(abiArguments, values[index], argumentTypes[index]);
    }

    return invokePython(symbolName, returnType, arena, abiArguments);
}
}
