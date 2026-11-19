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

#include <PythonPhysicalFunction.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <ios>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <ratio>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <unistd.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <NESCodonPlugin/Registry.hpp>
#include <Util/Logger/Logger.hpp>
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
#include <nautilus/select.hpp>
#include <nautilus/tracing/TracingUtil.hpp>
#include <nautilus/val_std.hpp>
#include <tracing/TypedValueRef.hpp>
#include <tracing/Types.hpp>
#include <Arena.hpp>
#include <CompilationContext.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <Module.hpp>
#include <PythonUdfCompiler.hpp>
#include <PythonUdfRuntime.hpp>
#include <UdfBridgeRegistry.hpp>
#include <function.hpp>
#include <options.hpp>
#include <val_arith.hpp>
#include <val_base.hpp>
#include <val_bool.hpp>
#include <val_details.hpp>
#include <val_memcpy.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

/// Argument names, types and values are parallel vectors of the same length, indexed by the same loop counter.
/// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)

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
/// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic_uint64_t nextPythonUdfId = 0;

static_assert(offsetof(UdfErrorHolder, errorPtr) == 0);
static_assert(offsetof(UdfErrorHolder, errorSize) == sizeof(int8_t*));
static_assert(offsetof(PythonUdfAbiValue, data) == 0);
static_assert(offsetof(PythonUdfAbiValue, size) == sizeof(int8_t*));
static_assert(offsetof(PythonUdfAbiValue, isNull) == sizeof(int8_t*) + sizeof(uint64_t));
static_assert(sizeof(PythonUdfAbiValue) == sizeof(int8_t*) + (2 * sizeof(uint64_t)));

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

std::string writeBitcode(const llvm::Module& module)
{
    llvm::SmallVector<char, 0> bitcode;
    llvm::raw_svector_ostream bitcodeStream(bitcode);
    llvm::WriteBitcodeToFile(module, bitcodeStream);
    return {bitcode.data(), bitcode.size()};
}

/// The bitcode of a compiled UDF with its entry point `from` renamed to `to`, after checking that it uses only what this
/// worker provides.
std::string renameEntryPoint(const std::string& bitcode, const std::string& from, const std::string& to)
{
    llvm::LLVMContext context;
    auto parsed = llvm::parseBitcodeFile(llvm::MemoryBufferRef{llvm::StringRef{bitcode.data(), bitcode.size()}, from}, context);
    if (!parsed)
    {
        throw QueryCompilerError("Could not read LLVM bitcode for Python UDF '{}': {}", from, llvm::toString(parsed.takeError()));
    }
    auto& module = **parsed;
    auto* entryPoint = module.getFunction(from);
    if (entryPoint == nullptr || entryPoint->isDeclaration())
    {
        throw QueryCompilerError("The compiled Python UDF has no entry point '{}'", from);
    }
    entryPoint->setName(to);
    validatePythonUdfModule(module, to);
    return writeBitcode(module);
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
/// CPP_TYPE is a type that is used as a template argument, where it cannot be parenthesized.
/// NOLINTBEGIN(bugprone-macro-parentheses,cppcoreguidelines-macro-usage)
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
            arguments.emplace_back(argument.castToType(DataType::Type::UINT8).getRawValueAs<nautilus::val<uint8_t>>());
            break;
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Unsupported Python UDF argument type {}", type);
    }
    if (type.nullable)
    {
        arguments.emplace_back(nautilus::select(argument.isNull(), nautilus::val<uint8_t>{1}, nautilus::val<uint8_t>{0}));
    }
#undef NES_APPEND_PYTHON
    /// NOLINTEND(bugprone-macro-parentheses,cppcoreguidelines-macro-usage)
}

[[noreturn]] void throwPythonUdfError(UdfErrorHolder* error)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    const auto* characters = reinterpret_cast<const char*>(error->errorPtr);
    throw UdfExecutionError("Python UDF failed: {}", std::string_view{characters, error->errorSize});
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
/// CPP_TYPE is a type that is used as a template argument, where it cannot be parenthesized.
/// NOLINTBEGIN(bugprone-macro-parentheses,cppcoreguidelines-macro-usage)
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
        case DataType::Type::CHAR: {
            const auto [value, isNull] = invokePythonScalar<uint8_t>(symbol, returnType.nullable, arena, arguments);
            result.emplace(VarVal{value, returnType.nullable, isNull}.castToType(DataType::Type::CHAR));
            break;
        }
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Unsupported Python UDF return type {}", returnType);
    }
#undef NES_INVOKE_PYTHON
    /// NOLINTEND(bugprone-macro-parentheses,cppcoreguidelines-macro-usage)
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
    static constexpr std::string_view TrampolineName = "invokePythonUdf";
    module.registerFunction(std::string{TrampolineName}, std::move(trampoline));
    auto compiledModule = module.compile();
    return std::make_shared<PythonUdfInterpreterExecutable>(
        compiledModule.getFunction<uint8_t(PythonUdfAbiValue*, PythonUdfAbiValue*, UdfErrorHolder*)>(std::string{TrampolineName}));
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
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    destination.data = reinterpret_cast<int8_t*>(storage.bytes.data());
    destination.size = sizeof(rawValue);
}

void appendInterpreterArgument(
    PythonUdfAbiValue& destination, PythonUdfScalarStorage& storage, const VarVal& argument, const DataType& type)
{
/// CPP_TYPE is a type that is used as a template argument, where it cannot be parenthesized.
/// NOLINTBEGIN(bugprone-macro-parentheses,cppcoreguidelines-macro-usage)
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
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
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
            storeInterpreterArgument(
                destination, storage, argument.castToType(DataType::Type::UINT8).getRawValueAs<nautilus::val<uint8_t>>());
            break;
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Unsupported Python UDF argument type {}", type);
    }
    destination.isNull = static_cast<uint8_t>(type.nullable && nautilus::details::RawValueResolver<bool>::getRawValue(argument.isNull()));
#undef NES_STORE_INTERPRETER_ARGUMENT
    /// NOLINTEND(bugprone-macro-parentheses,cppcoreguidelines-macro-usage)
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
    PythonUdfAbiValue result{
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        .data = reinterpret_cast<int8_t*>(resultStorage.bytes.data()),
        .size = sizeof(resultStorage.bytes),
        .isNull = 0};
    UdfErrorHolder error{};
    activatePythonUdfArena(nautilus::details::RawValueResolver<Arena*>::getRawValue(arena.getArena()));
    checkPythonUdfStatus(executable.function(abiArguments.data(), &result, &error), &error);
    const auto isNull = result.isNull != 0;

/// CPP_TYPE is a type that is used as a template argument, where it cannot be parenthesized.
/// NOLINTBEGIN(bugprone-macro-parentheses,cppcoreguidelines-macro-usage)
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
            return VarVal{loadInterpreterScalarResult<uint8_t>(resultStorage), returnType.nullable, isNull}.castToType(
                DataType::Type::CHAR);
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Unsupported Python UDF return type {}", returnType);
    }
#undef NES_LOAD_INTERPRETER_RESULT
    /// NOLINTEND(bugprone-macro-parentheses,cppcoreguidelines-macro-usage)
    std::unreachable();
}
}

PythonPhysicalFunction::PythonPhysicalFunction(
    const CompiledInlineUdf& compiled, std::vector<PhysicalFunction> arguments, std::vector<DataType> argumentTypes, DataType returnType)
    : arguments(std::move(arguments))
    , argumentTypes(std::move(argumentTypes))
    , returnType(returnType)
    , symbolName(fmt::format("__nes_python_udf_{}", nextPythonUdfId.fetch_add(1)))
{
    PRECONDITION(this->arguments.size() == this->argumentTypes.size(), "Python UDF argument and type counts differ");
    PRECONDITION(!this->arguments.empty(), "Python UDF requires at least one argument");
    /// The UDF may call native functions of the Codon plugins, which this process loads only when it compiles a UDF itself.
    loadCodonPlugins();
    /// Each call gets its own entry points, so that several calls of one UDF can be part of one pipeline.
    llvmBitcode = renameEntryPoint(compiled.pipelineBitcode, compiled.symbol, symbolName);
    interpreterLlvmBitcode
        = renameEntryPoint(compiled.interpreterBitcode, interpreterSymbolName(compiled.symbol), interpreterSymbolName(symbolName));
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
    abiArguments.reserve((values.size() * 2) + 3);
    for (size_t index = 0; index < values.size(); ++index)
    {
        appendPythonArgument(abiArguments, values[index], argumentTypes[index]);
    }

    return invokePython(symbolName, returnType, arena, abiArguments);
}
}

/// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
