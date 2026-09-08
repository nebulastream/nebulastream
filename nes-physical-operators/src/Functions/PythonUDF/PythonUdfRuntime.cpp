// Licensed under the Apache License, Version 2.0 (the "License");

#include <Functions/PythonUDF/PythonUdfRuntime.hpp>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <unistd.h>
#include <unwind.h>
#include <fmt/format.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
thread_local Arena* currentPythonUdfArena = nullptr;

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

void activatePythonUdfArena(Arena* arena)
{
    setCurrentPythonUdfArena(arena);
}

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

std::span<const PythonUdfRuntimeSymbol> getPythonUdfRuntimeSymbols()
{
    static const PythonUdfRuntimeSymbol symbols[]{
        {"seq_alloc", reinterpret_cast<void*>(&seq_alloc)},
        {"seq_alloc_atomic", reinterpret_cast<void*>(&seq_alloc_atomic)},
        {"seq_realloc", reinterpret_cast<void*>(&seq_realloc)},
        {"seq_free", reinterpret_cast<void*>(&seq_free)},
        {"seq_exc_offset", reinterpret_cast<void*>(&seq_exc_offset)},
        {"seq_alloc_exc", reinterpret_cast<void*>(&seq_alloc_exc)},
        {"seq_throw", reinterpret_cast<void*>(&seq_throw)},
        {"seq_personality", reinterpret_cast<void*>(&seq_personality)},
        {"seq_int_from_str", reinterpret_cast<void*>(&seq_int_from_str)},
        {"seq_str_int", reinterpret_cast<void*>(&seq_str_int)},
        {"seq_str_uint", reinterpret_cast<void*>(&seq_str_uint)},
        {"seq_str_float", reinterpret_cast<void*>(&seq_str_float)},
        {"seq_float_from_str", reinterpret_cast<void*>(&seq_float_from_str)},
        {"seq_stdout", reinterpret_cast<void*>(&seq_stdout)},
        {"seq_lock_new", reinterpret_cast<void*>(&seq_lock_new)},
        {"seq_lock_acquire", reinterpret_cast<void*>(&seq_lock_acquire)},
        {"seq_lock_release", reinterpret_cast<void*>(&seq_lock_release)},
        {"seq_pid", reinterpret_cast<void*>(&seq_pid)},
        {"seq_time", reinterpret_cast<void*>(&seq_time)},
        {"seq_time_monotonic", reinterpret_cast<void*>(&seq_time_monotonic)},
    };
    return symbols;
}

bool isPythonUdfRuntimeSymbol(const std::string_view name)
{
    return std::ranges::any_of(getPythonUdfRuntimeSymbols(), [name](const auto& symbol) { return symbol.name == name; });
}
}
