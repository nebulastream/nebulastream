/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0
*/
#include <Functions/PythonPhysicalFunction.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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
#include <unwind.h>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Record.hpp>
#include <codon/cir/llvm/optimize.h>
#include <codon/compiler/compiler.h>
#include <fmt/format.h>
#include <llvm/ADT/SmallVector.h>
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
namespace
{
std::atomic_uint64_t nextPythonUdfId = 0;
thread_local Arena* currentPythonUdfArena = nullptr;

double elapsedMilliseconds(const std::chrono::steady_clock::time_point start, const std::chrono::steady_clock::time_point end)
{
    return std::chrono::duration<double, std::milli>{end - start}.count();
}

void setCurrentPythonUdfArena(Arena* arena)
{
    currentPythonUdfArena = arena;
}

void* allocatePythonUdfMemory(const size_t size)
{
    PRECONDITION(currentPythonUdfArena != nullptr, "Python UDF allocation attempted without an active NES arena");
    constexpr auto alignment = alignof(std::max_align_t);
    constexpr auto alignmentMask = alignment - 1;
    PRECONDITION(size <= std::numeric_limits<size_t>::max() - alignmentMask, "Python UDF allocation size overflow");
    const auto allocationSize = std::max<size_t>(size, 1) + alignmentMask;
    const auto memory = currentPythonUdfArena->allocateMemory(allocationSize);
    const auto address = reinterpret_cast<uintptr_t>(memory.data());
    return reinterpret_cast<void*>((address + alignmentMask) & ~alignmentMask);
}
}

struct UdfErrorHolder
{
    int8_t* errorPtr;
    uint64_t errorSize;
};

/// These symbols implement the subset of Codon's runtime allocation ABI supported by NES. They are resolved both by the
/// cleaned Codon LLVM module and by Codon runtime helpers such as seq_alloc_exc.
extern "C" __attribute__((visibility("default"), used)) void* seq_alloc(const size_t size)
{
    return allocatePythonUdfMemory(size);
}

extern "C" __attribute__((visibility("default"), used)) void* seq_alloc_atomic(const size_t size)
{
    return allocatePythonUdfMemory(size);
}

extern "C" __attribute__((visibility("default"), used)) void* seq_realloc(const void* oldMemory, const size_t newSize, const size_t oldSize)
{
    auto* newMemory = allocatePythonUdfMemory(newSize);
    if (oldMemory != nullptr && newSize != 0)
    {
        std::memcpy(newMemory, oldMemory, std::min(oldSize, newSize));
    }
    return newMemory;
}

extern "C" __attribute__((visibility("default"), used)) void seq_free(void*)
{
    /// Individual arena allocations cannot be freed. They are released together when the pipeline arena is destroyed.
}

namespace
{
/// Minimal Codon 0.19.6 runtime ABI used by compiled Python UDFs. The exception
/// unwinding code follows Codon's Apache-2.0-licensed runtime implementation.
constexpr uint64_t CodonExceptionClass = 0x6F626A0073657100ULL;

struct CodonString
{
    int64_t length;
    char* data;
};

struct CodonBacktrace
{
    void* frames;
    size_t count;
};

struct CodonTypeInfo
{
    int64_t id;
    int64_t* parentIds;
    int64_t parentIdCount;
    CodonString rawName;
};

struct CodonRttiObject
{
    void* data;
    CodonTypeInfo* type;
};

struct CodonBaseExceptionType
{
    int type;
};

struct CodonBaseException
{
    void* object;
    CodonBacktrace backtrace;
    _Unwind_Exception unwindException;
};

CodonString copyCodonString(const std::string_view value)
{
    auto* data = static_cast<char*>(allocatePythonUdfMemory(value.size()));
    if (!value.empty())
    {
        std::memcpy(data, value.data(), value.size());
    }
    return {static_cast<int64_t>(value.size()), data};
}

template <typename T>
CodonString formatCodonValue(const T value, const CodonString format, bool* error, const std::string_view defaultFormat = "{}")
{
    *error = false;
    try
    {
        if (format.length == 0)
        {
            return copyCodonString(fmt::vformat(defaultFormat, fmt::make_format_args(value)));
        }
        const std::string pattern = fmt::format("{{:{}}}", std::string_view{format.data, static_cast<size_t>(format.length)});
        return copyCodonString(fmt::vformat(pattern, fmt::make_format_args(value)));
    }
    catch (const std::exception& exception)
    {
        *error = true;
        return copyCodonString(exception.what());
    }
}

template <typename T>
uintptr_t readType(const uint8_t*& position)
{
    T value;
    std::memcpy(&value, position, sizeof(value));
    position += sizeof(value);
    return static_cast<uintptr_t>(value);
}

uintptr_t readUleb128(const uint8_t*& position)
{
    uintptr_t result = 0;
    uintptr_t shift = 0;
    uint8_t byte;
    do
    {
        byte = *position++;
        result |= static_cast<uintptr_t>(byte & 0x7F) << shift;
        shift += 7;
    } while ((byte & 0x80) != 0);
    return result;
}

uintptr_t readSleb128(const uint8_t*& position)
{
    uintptr_t result = 0;
    uintptr_t shift = 0;
    uint8_t byte;
    do
    {
        byte = *position++;
        result |= static_cast<uintptr_t>(byte & 0x7F) << shift;
        shift += 7;
    } while ((byte & 0x80) != 0);
    if ((byte & 0x40) != 0 && shift < sizeof(result) * 8)
    {
        result |= ~uintptr_t{0} << shift;
    }
    return result;
}

constexpr uint8_t DwarfOmit = 0xFF;
constexpr uint8_t DwarfAbsolutePointer = 0x00;
constexpr uint8_t DwarfUleb128 = 0x01;
constexpr uint8_t DwarfUdata2 = 0x02;
constexpr uint8_t DwarfUdata4 = 0x03;
constexpr uint8_t DwarfUdata8 = 0x04;
constexpr uint8_t DwarfSleb128 = 0x09;
constexpr uint8_t DwarfSdata2 = 0x0A;
constexpr uint8_t DwarfSdata4 = 0x0B;
constexpr uint8_t DwarfSdata8 = 0x0C;
constexpr uint8_t DwarfPcRelative = 0x10;
constexpr uint8_t DwarfIndirect = 0x80;

unsigned encodingSize(const uint8_t encoding)
{
    if (encoding == DwarfOmit)
    {
        return 0;
    }
    switch (encoding & 0x0F)
    {
        case DwarfAbsolutePointer:
            return sizeof(uintptr_t);
        case DwarfUdata2:
        case DwarfSdata2:
            return sizeof(uint16_t);
        case DwarfUdata4:
        case DwarfSdata4:
            return sizeof(uint32_t);
        case DwarfUdata8:
        case DwarfSdata8:
            return sizeof(uint64_t);
        default:
            std::abort();
    }
}

uintptr_t readEncodedPointer(const uint8_t*& position, const uint8_t encoding)
{
    if (encoding == DwarfOmit)
    {
        return 0;
    }
    const auto* originalPosition = position;
    uintptr_t result;
    switch (encoding & 0x0F)
    {
        case DwarfAbsolutePointer:
            result = readType<uintptr_t>(position);
            break;
        case DwarfUleb128:
            result = readUleb128(position);
            break;
        case DwarfSleb128:
            result = readSleb128(position);
            break;
        case DwarfUdata2:
            result = readType<uint16_t>(position);
            break;
        case DwarfUdata4:
            result = readType<uint32_t>(position);
            break;
        case DwarfUdata8:
            result = readType<uint64_t>(position);
            break;
        case DwarfSdata2:
            result = readType<int16_t>(position);
            break;
        case DwarfSdata4:
            result = readType<int32_t>(position);
            break;
        case DwarfSdata8:
            result = readType<int64_t>(position);
            break;
        default:
            std::abort();
    }
    switch (encoding & 0x70)
    {
        case DwarfAbsolutePointer:
            break;
        case DwarfPcRelative:
            result += reinterpret_cast<uintptr_t>(originalPosition);
            break;
        default:
            std::abort();
    }
    if ((encoding & DwarfIndirect) != 0)
    {
        result = *reinterpret_cast<const uintptr_t*>(result);
    }
    return result;
}

bool isCodonInstance(void* object, const int64_t type)
{
    const auto* info = static_cast<CodonRttiObject*>(object)->type;
    if (info->id == type)
    {
        return true;
    }
    for (auto* parent = info->parentIds; parent != nullptr && *parent != 0; ++parent)
    {
        if (*parent == type)
        {
            return true;
        }
    }
    return false;
}

bool matchCodonAction(
    int64_t& resultAction,
    const uint8_t typeEncoding,
    const uint8_t* classInfo,
    const uintptr_t actionEntry,
    const uint64_t exceptionClass,
    _Unwind_Exception* exceptionObject)
{
    if (exceptionObject == nullptr || exceptionClass != CodonExceptionClass)
    {
        return false;
    }
    const auto offset = -static_cast<ptrdiff_t>(offsetof(CodonBaseException, unwindException));
    auto* exception = reinterpret_cast<CodonBaseException*>(reinterpret_cast<char*>(exceptionObject) + offset);
    auto* actionPosition = reinterpret_cast<const uint8_t*>(actionEntry);
    while (true)
    {
        const auto typeOffset = static_cast<int64_t>(readSleb128(actionPosition));
        assert(typeOffset >= 0);
        auto* nextActionPosition = actionPosition;
        const auto nextActionOffset = static_cast<int64_t>(readSleb128(nextActionPosition));
        if (typeOffset > 0)
        {
            const auto* entry = classInfo - typeOffset * encodingSize(typeEncoding);
            const auto pointer = readEncodedPointer(entry, typeEncoding);
            const auto type = reinterpret_cast<const CodonBaseExceptionType*>(pointer)->type;
            if (type == 0 || isCodonInstance(exception->object, type))
            {
                resultAction = type;
                return true;
            }
        }
        if (nextActionOffset == 0)
        {
            return false;
        }
        actionPosition += nextActionOffset;
    }
}

_Unwind_Reason_Code handleCodonLsda(
    const uint8_t* lsda,
    const _Unwind_Action actions,
    const uint64_t exceptionClass,
    _Unwind_Exception* exceptionObject,
    _Unwind_Context* context)
{
    if (lsda == nullptr)
    {
        return _URC_CONTINUE_UNWIND;
    }
    const auto functionStart = _Unwind_GetRegionStart(context);
    const auto pcOffset = _Unwind_GetIP(context) - 1 - functionStart;
    const auto lpStartEncoding = *lsda++;
    if (lpStartEncoding != DwarfOmit)
    {
        readEncodedPointer(lsda, lpStartEncoding);
    }
    const auto typeEncoding = *lsda++;
    const uint8_t* classInfo = nullptr;
    if (typeEncoding != DwarfOmit)
    {
        const auto classInfoOffset = readUleb128(lsda);
        classInfo = lsda + classInfoOffset;
    }
    const auto callSiteEncoding = *lsda++;
    const auto callSiteTableLength = readUleb128(lsda);
    const auto* callSiteTableEnd = lsda + callSiteTableLength;
    const auto* actionTable = callSiteTableEnd;
    while (lsda < callSiteTableEnd)
    {
        const auto start = readEncodedPointer(lsda, callSiteEncoding);
        const auto length = readEncodedPointer(lsda, callSiteEncoding);
        const auto landingPad = readEncodedPointer(lsda, callSiteEncoding);
        auto actionEntry = readUleb128(lsda);
        if (exceptionClass != CodonExceptionClass)
        {
            actionEntry = 0;
        }
        if (landingPad == 0 || pcOffset < start || pcOffset >= start + length)
        {
            continue;
        }
        if (actionEntry != 0)
        {
            actionEntry += reinterpret_cast<uintptr_t>(actionTable) - 1;
        }
        int64_t actionValue = 0;
        const auto matched
            = actionEntry != 0 && matchCodonAction(actionValue, typeEncoding, classInfo, actionEntry, exceptionClass, exceptionObject);
        if ((actions & _UA_SEARCH_PHASE) != 0)
        {
            return matched ? _URC_HANDLER_FOUND : _URC_CONTINUE_UNWIND;
        }
        _Unwind_SetGR(context, __builtin_eh_return_data_regno(0), reinterpret_cast<uintptr_t>(exceptionObject));
        _Unwind_SetGR(context, __builtin_eh_return_data_regno(1), matched ? static_cast<uintptr_t>(actionValue) : 0);
        _Unwind_SetIP(context, functionStart + landingPad);
        return _URC_INSTALL_CONTEXT;
    }
    return _URC_CONTINUE_UNWIND;
}
}

extern "C" __attribute__((visibility("default"), used)) int64_t seq_exc_offset()
{
    return -static_cast<int64_t>(offsetof(CodonBaseException, unwindException));
}

extern "C" __attribute__((visibility("default"), used)) void* seq_alloc_exc(void* object)
{
    auto* exception = static_cast<CodonBaseException*>(seq_alloc(sizeof(CodonBaseException)));
    std::memset(exception, 0, sizeof(*exception));
    exception->object = object;
    exception->unwindException.exception_class = CodonExceptionClass;
    exception->unwindException.exception_cleanup = [](_Unwind_Reason_Code, _Unwind_Exception*) { };
    return &exception->unwindException;
}

extern "C" __attribute__((visibility("default"), used, noreturn)) void seq_throw(void* exception)
{
    static_cast<void>(_Unwind_RaiseException(static_cast<_Unwind_Exception*>(exception)));
    std::abort();
}

extern "C" __attribute__((visibility("default"), used)) _Unwind_Reason_Code seq_personality(
    int, const _Unwind_Action actions, const uint64_t exceptionClass, _Unwind_Exception* exceptionObject, _Unwind_Context* context)
{
    const auto* lsda = static_cast<const uint8_t*>(_Unwind_GetLanguageSpecificData(context));
    return handleCodonLsda(lsda, actions, exceptionClass, exceptionObject, context);
}

extern "C" __attribute__((visibility("default"), used)) int64_t seq_int_from_str(const CodonString string, const char** end, const int base)
{
    int64_t result = 0;
    const auto conversion = std::from_chars(string.data, string.data + string.length, result, base);
    *end = conversion.ec == std::errc{} ? conversion.ptr : string.data;
    return result;
}

extern "C" __attribute__((visibility("default"), used)) CodonString seq_str_int(const int64_t value, const CodonString format, bool* error)
{
    return formatCodonValue(value, format, error);
}

extern "C" __attribute__((visibility("default"), used)) CodonString seq_str_uint(const int64_t value, const CodonString format, bool* error)
{
    return formatCodonValue(static_cast<uint64_t>(value), format, error);
}

extern "C" __attribute__((visibility("default"), used)) CodonString seq_str_float(const double value, const CodonString format, bool* error)
{
    return formatCodonValue(value, format, error, "{:g}");
}

extern "C" __attribute__((visibility("default"), used)) double seq_float_from_str(const CodonString string, const char** end)
{
    double result = 0;
    const auto conversion = std::from_chars(string.data, string.data + string.length, result);
    *end = conversion.ec == std::errc{} || conversion.ec == std::errc::result_out_of_range ? conversion.ptr : string.data;
    return result;
}

extern "C" __attribute__((visibility("default"), used)) void* seq_stdout()
{
    return stdout;
}

/// Codon's NumPy Generator owns a lock even when the generator is local to one
/// invocation. NES does not permit Codon threading, and no UDF value may escape
/// its invocation, so these locks cannot contend in the supported POC subset.
extern "C" __attribute__((visibility("default"), used)) void* seq_lock_new()
{
    return allocatePythonUdfMemory(1);
}

extern "C" __attribute__((visibility("default"), used)) bool seq_lock_acquire(void*, bool, double)
{
    return true;
}

extern "C" __attribute__((visibility("default"), used)) void seq_lock_release(void*)
{
}

extern "C" __attribute__((visibility("default"), used)) int64_t seq_pid()
{
    return static_cast<int64_t>(::getpid());
}

extern "C" __attribute__((visibility("default"), used)) int64_t seq_time()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

extern "C" __attribute__((visibility("default"), used)) int64_t seq_time_monotonic()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

namespace
{
static_assert(offsetof(UdfErrorHolder, errorPtr) == 0);
static_assert(offsetof(UdfErrorHolder, errorSize) == sizeof(int8_t*));

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
        parameters.emplace_back(fmt::format("{}: {}", parameterNames[index], pythonTypeName(argumentTypes[index])));
        if (argumentTypes[index].type == DataType::Type::VARSIZED)
        {
            abiParameters.emplace_back(fmt::format("_arg{}_ptr: Ptr[byte]", index));
            abiParameters.emplace_back(fmt::format("_arg{}_size: i64", index));
            bodyArguments.emplace_back(fmt::format("str(_arg{}_ptr, int(_arg{}_size))", index, index));
        }
        else
        {
            abiParameters.emplace_back(fmt::format("_arg{}: {}", index, codonTypeName(argumentTypes[index])));
            bodyArguments.emplace_back(fmt::format("{}(_arg{})", pythonTypeName(argumentTypes[index]), index));
        }
    }

    const auto bodySymbol = symbol + "_body";
    auto source = fmt::format(
        "@tuple\n"
        "class UdfErrorHolder:\n"
        "    error_ptr: Ptr[byte]\n"
        "    error_size: i64\n\n"
        "def {}({}):\n{}\n@export\n",
        bodySymbol,
        join(parameters),
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
    abiParameters.emplace_back("_error: Ptr[UdfErrorHolder]");

    source += fmt::format("def {}({}) -> i8:\n    _status = i8(0)\n    try:\n", symbol, join(abiParameters));
    if (returnType.type == DataType::Type::VARSIZED)
    {
        source += fmt::format("        _result_value = str({}({}))\n", bodySymbol, join(bodyArguments));
        source += "        _result_ptr[0] = _result_value.ptr\n";
        source += "        _result_size[0] = i64(len(_result_value))\n";
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
    static const std::unordered_set<std::string_view> allowedExternalFunctions{
        "isspace",   "seq_alloc",        "seq_alloc_atomic",  "seq_alloc_exc", "seq_exc_offset",   "seq_float_from_str",
        "seq_free",  "seq_int_from_str", "seq_lock_acquire",  "seq_lock_new",  "seq_lock_release", "seq_personality",
        "seq_pid",   "seq_realloc",      "seq_stdout",        "seq_str_float", "seq_str_int",      "seq_str_uint",
        "seq_throw", "seq_time",         "seq_time_monotonic"};

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
        if (function.isDeclaration() && !name.starts_with("llvm.") && !allowedExternalFunctions.contains(name))
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

std::string compilePythonUdf(
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
        codon::ir::setNativeOptimizationEnabled(false);
        const auto source = createPythonUdfSource(symbol, parameterNames, body, argumentTypes, returnType);
        const auto sourcePath = (std::filesystem::temp_directory_path() / fmt::format("{}.py", symbol)).string();
        auto filesystem = std::make_shared<codon::ast::ResourceFilesystem>("nes-worker", sourcePath, !importPaths.empty());
        for (const auto& importPath : importPaths)
        {
            filesystem->add_search_path(importPath);
        }
        codon::Compiler compiler("nes-worker", codon::Compiler::Mode::RELEASE, std::vector<std::string>{}, false, false, false, filesystem);
        const auto parseStart = std::chrono::steady_clock::now();
        if (auto error = compiler.parseCode(sourcePath, source))
        {
            throw QueryCompilerError("Could not parse Python UDF '{}': {}", symbol, llvm::toString(std::move(error)));
        }
        const auto compileStart = std::chrono::steady_clock::now();
        if (auto error = compiler.compile())
        {
            throw QueryCompilerError("Could not compile Python UDF '{}': {}", symbol, llvm::toString(std::move(error)));
        }
        const auto optimizeStart = std::chrono::steady_clock::now();
        compiler.getLLVMVisitor()->optimizeLLVM();
        const auto cleanupStart = std::chrono::steady_clock::now();
        auto* module = compiler.getLLVMVisitor()->getModule();
        INVARIANT(module != nullptr, "Codon did not create an LLVM module");
        replaceCodonStdoutLoads(*module);
        internalizePythonUdfModule(*module, symbol);
        validatePythonUdfModule(*module, symbol);

        const auto bitcodeStart = std::chrono::steady_clock::now();
        llvm::SmallVector<char, 0> bitcode;
        llvm::raw_svector_ostream bitcodeStream(bitcode);
        llvm::WriteBitcodeToFile(*module, bitcodeStream);
        const auto compilationEnd = std::chrono::steady_clock::now();
        NES_INFO(
            "Compiled Python UDF '{}' with Codon in {:.3f} ms (mutex wait: {:.3f} ms, parse: {:.3f} ms, compile: {:.3f} ms, "
            "LLVM optimize: {:.3f} ms, cleanup: {:.3f} ms, bitcode: {:.3f} ms)",
            symbol,
            elapsedMilliseconds(compilationStart, compilationEnd),
            elapsedMilliseconds(mutexWaitStart, compilationStart),
            elapsedMilliseconds(parseStart, compileStart),
            elapsedMilliseconds(compileStart, optimizeStart),
            elapsedMilliseconds(optimizeStart, cleanupStart),
            elapsedMilliseconds(cleanupStart, bitcodeStart),
            elapsedMilliseconds(bitcodeStart, compilationEnd));
        return {bitcode.data(), bitcode.size()};
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

VarVal makeNull(const DataType& type)
{
    const nautilus::val<bool> isNull{true};
    std::optional<VarVal> result;
    switch (type.type)
    {
        case DataType::Type::UINT8:
            result.emplace(nautilus::val<uint8_t>{0}, true, isNull);
            break;
        case DataType::Type::UINT16:
            result.emplace(nautilus::val<uint16_t>{0}, true, isNull);
            break;
        case DataType::Type::UINT32:
            result.emplace(nautilus::val<uint32_t>{0}, true, isNull);
            break;
        case DataType::Type::UINT64:
            result.emplace(nautilus::val<uint64_t>{0}, true, isNull);
            break;
        case DataType::Type::INT8:
            result.emplace(nautilus::val<int8_t>{0}, true, isNull);
            break;
        case DataType::Type::INT16:
            result.emplace(nautilus::val<int16_t>{0}, true, isNull);
            break;
        case DataType::Type::INT32:
            result.emplace(nautilus::val<int32_t>{0}, true, isNull);
            break;
        case DataType::Type::INT64:
            result.emplace(nautilus::val<int64_t>{0}, true, isNull);
            break;
        case DataType::Type::FLOAT32:
            result.emplace(nautilus::val<float>{0}, true, isNull);
            break;
        case DataType::Type::FLOAT64:
            result.emplace(nautilus::val<double>{0}, true, isNull);
            break;
        case DataType::Type::BOOLEAN:
            result.emplace(nautilus::val<bool>{false}, true, isNull);
            break;
        case DataType::Type::VARSIZED:
            result.emplace(VariableSizedData{nautilus::val<int8_t*>{nullptr}, nautilus::val<uint64_t>{0}}, true, isNull);
            break;
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Unsupported Python UDF return type {}", type);
    }
    INVARIANT(result.has_value(), "Python UDF NULL result was not initialized");
    return std::move(*result);
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
    nautilus::invoke(setCurrentPythonUdfArena, arena.getArena());
    const auto status = dynamicInvoke<uint8_t>(symbol, arguments);
    nautilus::invoke(checkPythonUdfStatus, status, &errorHolder);
}

template <typename R>
nautilus::val<R> invokePythonScalar(const std::string& symbol, ArenaRef& arena, std::vector<PythonAbiValue>& arguments)
{
    auto resultPointer = static_cast<nautilus::val<R*>>(arena.allocateMemory(nautilus::val<size_t>{sizeof(R)}));
    arguments.emplace_back(resultPointer);
    invokePythonWithErrorHandling(symbol, arena, arguments);
    return *resultPointer;
}

VarVal invokePython(const std::string& symbol, const DataType& returnType, ArenaRef& arena, std::vector<PythonAbiValue>& arguments)
{
#define NES_INVOKE_PYTHON(TYPE, CPP_TYPE) \
    case DataType::Type::TYPE: \
        result.emplace(invokePythonScalar<CPP_TYPE>(symbol, arena, arguments), returnType.nullable, nautilus::val<bool>{false}); \
        break
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
            const auto value = invokePythonScalar<uint8_t>(symbol, arena, arguments);
            result.emplace(value != nautilus::val<uint8_t>{0}, returnType.nullable, nautilus::val<bool>{false});
            break;
        }
        case DataType::Type::VARSIZED: {
            /// Codon strings are {length, data} values backed by Codon's runtime. Flatten the result into out-parameters and
            /// copy it immediately so no Codon-owned pointer escapes this invocation.
            auto resultPointer = static_cast<nautilus::val<int8_t**>>(arena.allocateMemory(nautilus::val<size_t>{sizeof(int8_t*)}));
            auto resultSize = static_cast<nautilus::val<uint64_t*>>(arena.allocateMemory(nautilus::val<size_t>{sizeof(uint64_t)}));
            arguments.emplace_back(resultPointer);
            arguments.emplace_back(resultSize);
            invokePythonWithErrorHandling(symbol, arena, arguments);
            const nautilus::val<int8_t*> returnedContent = *resultPointer;
            const nautilus::val<uint64_t> returnedSize = *resultSize;
            const auto copiedResult = arena.allocateVariableSizedData(returnedSize);
            nautilus::memcpy(copiedResult.getContent(), returnedContent, returnedSize);
            result.emplace(copiedResult, returnType.nullable, nautilus::val<bool>{false});
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
    , llvmBitcode(compilePythonUdf(symbolName, parameterNames, body, this->argumentTypes, returnType, importPaths))
{
    PRECONDITION(this->arguments.size() == this->argumentTypes.size(), "Python UDF argument and type counts differ");
    PRECONDITION(!this->arguments.empty(), "Python UDF requires at least one argument");
}

void PythonPhysicalFunction::setupSelf(CompilationContext& compilationContext) const
{
    compilationContext.registerUDF(symbolName, llvmBitcode);
}

VarVal PythonPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> values;
    values.reserve(arguments.size());
    nautilus::val<bool> hasNull{false};
    for (const auto& argument : arguments)
    {
        values.emplace_back(argument.execute(record, arena));
        if (values.back().isNullable())
        {
            hasNull = hasNull || values.back().isNull();
        }
    }

    std::vector<PythonAbiValue> abiArguments;
    abiArguments.reserve(values.size() * 2 + 3);
    for (size_t index = 0; index < values.size(); ++index)
    {
        appendPythonArgument(abiArguments, values[index], argumentTypes[index]);
    }

    auto result = makeNull(returnType);
    if (!hasNull)
    {
        result = invokePython(symbolName, returnType, arena, abiArguments);
    }
    return result;
}
}
