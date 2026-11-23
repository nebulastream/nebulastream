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

#include <PythonUdfRuntime.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>
#include <strings.h>
#include <unistd.h>
#include <unwind.h>
#include <Util/Sanitizer.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PythonUdfAbi.hpp>

/// Codon runtime ABI: pointers passed as integers, LEB128 decoding loops, and the C symbol table handed to Codon.
/// NOLINTBEGIN(performance-no-int-to-ptr,cppcoreguidelines-avoid-do-while,modernize-avoid-c-arrays,clang-analyzer-core.NullDereference,cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic,cppcoreguidelines-avoid-non-const-global-variables,readability-magic-numbers,hicpp-signed-bitwise,readability-identifier-naming)

namespace NES
{
namespace
{
/// Set while a UDF module's globals are initialized: those allocations back module-level state that outlives every
/// invocation, so they bypass the per-invocation scratch region. Bounded by the module's globals; never freed.
thread_local bool persistentPythonUdfAllocation = false;

/// The memory of one UDF call, for Codon (seq_alloc) and for Numba's runtime (NRT): a region of the thread that
/// activatePythonUdfArena resets at the start of each call. An allocation is a pointer increment and a free does nothing.
/// This is correct because no object of a call outlives it: NES copies a VARSIZED result right after the call, reads an
/// error message before the next call, and module-level state is allocated persistently (persistentPythonUdfAllocation).
/// Before, the memory came from the NES arena of the input buffer, which takes a pooled buffer from the buffer manager
/// whenever its buffer is full and keeps every block until the input buffer is done (E72, E73). The region is reused by
/// every call, so it stays in the cache. An object that a UDF keeps in a global after the call was already invalid with the
/// arena, after the input buffer. Under ASan, reset() poisons the region, so such an access is reported at the next call.
class CallScratch
{
public:
    void* allocate(const size_t size)
    {
        const auto needed = alignUp(std::max<size_t>(size, 1));
        if (needed > ChunkSize)
        {
            large.emplace_back(std::make_unique<std::byte[]>(needed));
            return large.back().get();
        }
        if (chunks.empty() || offset + needed > ChunkSize)
        {
            ++chunk;
            if (chunk >= chunks.size())
            {
                chunks.emplace_back(std::make_unique<std::byte[]>(ChunkSize));
                chunk = chunks.size() - 1;
                ASAN_POISON_MEMORY_REGION(chunks.back().get(), ChunkSize);
            }
            offset = 0;
        }
        auto* block = chunks[chunk].get() + offset;
        offset += needed;
        ASAN_UNPOISON_MEMORY_REGION(block, size);
        return block;
    }

    /// The last block of the current chunk grows in place if the chunk has room; other blocks move.
    void* reallocate(void* memory, const size_t newSize, const size_t oldSize)
    {
        if (memory == nullptr)
        {
            return allocate(newSize);
        }
        if (newSize <= oldSize)
        {
            return memory;
        }
        auto* const block = static_cast<std::byte*>(memory);
        if (!chunks.empty())
        {
            auto* const chunkBegin = chunks[chunk].get();
            const auto blockOffset = static_cast<size_t>(block - chunkBegin);
            if (block >= chunkBegin && block < chunkBegin + ChunkSize && blockOffset + alignUp(std::max<size_t>(oldSize, 1)) == offset
                && blockOffset + alignUp(newSize) <= ChunkSize)
            {
                offset = blockOffset + alignUp(newSize);
                ASAN_UNPOISON_MEMORY_REGION(block, newSize);
                return memory;
            }
        }
        auto* grown = allocate(newSize);
        std::memcpy(grown, memory, oldSize);
        return grown;
    }

    /// Frees the large blocks and makes the chunks free again; keeps the chunks for the next call.
    void reset()
    {
        large.clear();
        for (size_t index = 0; index <= chunk && index < chunks.size(); ++index)
        {
            ASAN_POISON_MEMORY_REGION(chunks[index].get(), ChunkSize);
        }
        chunk = 0;
        offset = 0;
    }

private:
    static constexpr size_t ChunkSize = 256 * 1024;

    static size_t alignUp(const size_t size)
    {
        constexpr auto mask = alignof(std::max_align_t) - 1;
        return (size + mask) & ~mask;
    }

    std::vector<std::unique_ptr<std::byte[]>> chunks;
    std::vector<std::unique_ptr<std::byte[]>> large;
    size_t chunk = 0;
    size_t offset = 0;
};

thread_local CallScratch callScratch;

void* allocatePythonUdfMemory(const size_t size)
{
    if (persistentPythonUdfAllocation)
    {
        return std::calloc(1, std::max<size_t>(size, 1));
    }
    return callScratch.allocate(size);
}
}

void activatePythonUdfArena(Arena*)
{
    callScratch.reset();
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

extern "C" __attribute__((visibility("default"), used)) void* seq_realloc(void* oldMemory, const size_t newSize, const size_t oldSize)
{
    if (persistentPythonUdfAllocation)
    {
        auto* newMemory = allocatePythonUdfMemory(newSize);
        if (oldMemory != nullptr && newSize != 0)
        {
            std::memcpy(newMemory, oldMemory, std::min(oldSize, newSize));
        }
        return newMemory;
    }
    return callScratch.reallocate(oldMemory, newSize, oldSize);
}

extern "C" __attribute__((visibility("default"), used)) void seq_free(void*)
{
    /// Individual allocations are not freed. The scratch region is reset at the start of the next call.
}

extern "C" void NRT_MemSys_set_allocator(void* (*malloc)(size_t), void* (*realloc)(void*, size_t), void (*free)(void*));

namespace
{
/// Numba's runtime (NRT) allocates each object with malloc and frees it when its reference count becomes zero. NES gives it
/// the scratch region of the call too (E73). NRT's realloc does not pass the old size, so a header before each block holds
/// it. Numba's typed dict and list call libc directly and are not affected.
struct alignas(std::max_align_t) NumbaBlockHeader
{
    size_t size;
};

void* numbaAllocate(const size_t size)
{
    auto* header = static_cast<NumbaBlockHeader*>(callScratch.allocate(sizeof(NumbaBlockHeader) + size));
    header->size = size;
    return header + 1;
}

void numbaFree(void*)
{
}

void* numbaReallocate(void* memory, const size_t size)
{
    if (memory == nullptr)
    {
        return numbaAllocate(size);
    }
    auto* header = static_cast<NumbaBlockHeader*>(memory) - 1;
    auto* moved = static_cast<NumbaBlockHeader*>(
        callScratch.reallocate(header, sizeof(NumbaBlockHeader) + size, sizeof(NumbaBlockHeader) + header->size));
    moved->size = size;
    return moved + 1;
}
}

void useArenaForNumbaRuntime()
{
    static std::once_flag installed;
    std::call_once(installed, [] { NRT_MemSys_set_allocator(numbaAllocate, numbaReallocate, numbaFree); });
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
    return {.length = static_cast<int64_t>(value.size()), .data = data};
}

template <typename T>
/// NOLINTNEXTLINE(fuchsia-default-arguments-declarations)
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

/// repr() of a float as CPython writes it: the shortest digits that read back as the same value, in fixed notation when
/// the decimal point falls within 16 digits of the first digit (with ".0" for an integral value), else as d.ddde+XX.
std::string pythonFloatRepr(const double value)
{
    if (std::isnan(value))
    {
        return "nan";
    }
    if (std::isinf(value))
    {
        return value > 0 ? "inf" : "-inf";
    }
    std::array<char, 32> buffer{};
    const auto [end, error] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value, std::chars_format::scientific);
    INVARIANT(error == std::errc{}, "to_chars of a double cannot fail with 32 bytes");
    const std::string_view scientific{buffer.data(), end};
    const auto exponentPosition = scientific.find('e');
    const bool negative = scientific.front() == '-';
    std::string digits;
    for (const char character : scientific.substr(negative ? 1 : 0, exponentPosition - (negative ? 1 : 0)))
    {
        if (character != '.')
        {
            digits.push_back(character);
        }
    }
    /// from_chars does not accept the '+' that to_chars writes before a positive exponent.
    const auto* exponentStart = scientific.data() + exponentPosition + 1;
    exponentStart += *exponentStart == '+' ? 1 : 0;
    int exponent = 0;
    std::from_chars(exponentStart, scientific.data() + scientific.size(), exponent);
    /// The position of the decimal point after the first digit, as in CPython's format_float_short.
    const int decimalPoint = exponent + 1;
    std::string result = negative ? "-" : "";
    if (decimalPoint > -4 && decimalPoint <= 16)
    {
        if (decimalPoint <= 0)
        {
            result += "0." + std::string(static_cast<size_t>(-decimalPoint), '0') + digits;
        }
        else if (static_cast<size_t>(decimalPoint) >= digits.size())
        {
            result += digits + std::string(static_cast<size_t>(decimalPoint) - digits.size(), '0') + ".0";
        }
        else
        {
            result += digits.substr(0, decimalPoint) + "." + digits.substr(decimalPoint);
        }
        return result;
    }
    result += digits.substr(0, 1);
    if (digits.size() > 1)
    {
        result += "." + digits.substr(1);
    }
    return result + fmt::format("e{}{:02d}", exponent < 0 ? '-' : '+', std::abs(exponent));
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
    uint8_t byte = 0;
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
    uint8_t byte = 0;
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
    uintptr_t result = 0;
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
    const auto* actionPosition = reinterpret_cast<const uint8_t*>(actionEntry);
    while (true)
    {
        const auto typeOffset = static_cast<int64_t>(readSleb128(actionPosition));
        assert(typeOffset >= 0);
        const auto* nextActionPosition = actionPosition;
        const auto nextActionOffset = static_cast<int64_t>(readSleb128(nextActionPosition));
        if (typeOffset > 0)
        {
            const auto* entry = classInfo - (typeOffset * encodingSize(typeEncoding));
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
    /// Without a format spec (str, repr, f"{x}"), the float is written as CPython's repr does. With a spec, fmt formats it.
    if (format.length == 0)
    {
        *error = false;
        return copyCodonString(pythonFloatRepr(value));
    }
    return formatCodonValue(value, format, error);
}

/// A str with a format spec (f"{s:>10}", format(s, "^8")): fmt applies the spec to the text.
extern "C" __attribute__((visibility("default"), used)) CodonString seq_str_str(const CodonString value, const CodonString format, bool* error)
{
    return formatCodonValue(std::string_view{value.data, static_cast<size_t>(value.length)}, format, error);
}

extern "C" __attribute__((visibility("default"), used)) double seq_float_from_str(const CodonString string, const char** end)
{
    const auto [result, suffix] = NES::from_chars_prefix<double>(std::string_view(string.data, string.length));
    *end = suffix.data();
    return result.value_or(0.0);
}

extern "C" __attribute__((visibility("default"), used)) void* seq_stdout()
{
    return stdout;
}

/// Referenced by Codon's module initializer (sys.stdin / sys.stderr).
extern "C" __attribute__((visibility("default"), used)) void* seq_stdin()
{
    return stdin;
}

extern "C" __attribute__((visibility("default"), used)) void* seq_stderr()
{
    return stderr;
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
/// Runs Codon's module initializer (its top-level code, including the stdlib's module globals) once per compiled UDF
/// module. Called at the start of every invocation (see addModuleInitializationPrologue); `initialized` is a module global.
void codonEnsureModuleInitialized(void (*initialize)(), uint8_t* initialized)
{
    const std::atomic_ref<uint8_t> flag{*initialized};
    if (flag.load(std::memory_order_acquire) != 0)
    {
        return;
    }
    static std::mutex initializationMutex;
    const std::scoped_lock lock{initializationMutex};
    if (flag.load(std::memory_order_relaxed) != 0)
    {
        return;
    }

    /// Resets the allocation mode even if the initializer throws.
    struct PersistentAllocationScope
    {
        PersistentAllocationScope() { persistentPythonUdfAllocation = true; }

        ~PersistentAllocationScope() { persistentPythonUdfAllocation = false; }

        PersistentAllocationScope(const PersistentAllocationScope&) = delete;
        PersistentAllocationScope& operator=(const PersistentAllocationScope&) = delete;
    };

    {
        const PersistentAllocationScope scope;
        initialize();
    }
    flag.store(1, std::memory_order_release);
}
}

std::span<const PythonUdfRuntimeSymbol> getPythonUdfRuntimeSymbols()
{
    static const PythonUdfRuntimeSymbol symbols[]{
        {.name = "seq_alloc", .address = reinterpret_cast<void*>(&seq_alloc)},
        {.name = "seq_alloc_atomic", .address = reinterpret_cast<void*>(&seq_alloc_atomic)},
        {.name = "seq_realloc", .address = reinterpret_cast<void*>(&seq_realloc)},
        {.name = "seq_free", .address = reinterpret_cast<void*>(&seq_free)},
        {.name = "seq_exc_offset", .address = reinterpret_cast<void*>(&seq_exc_offset)},
        {.name = "seq_alloc_exc", .address = reinterpret_cast<void*>(&seq_alloc_exc)},
        {.name = "seq_throw", .address = reinterpret_cast<void*>(&seq_throw)},
        {.name = "seq_personality", .address = reinterpret_cast<void*>(&seq_personality)},
        {.name = "seq_int_from_str", .address = reinterpret_cast<void*>(&seq_int_from_str)},
        {.name = "seq_str_int", .address = reinterpret_cast<void*>(&seq_str_int)},
        {.name = "seq_str_uint", .address = reinterpret_cast<void*>(&seq_str_uint)},
        {.name = "seq_str_float", .address = reinterpret_cast<void*>(&seq_str_float)},
        {.name = "seq_str_str", .address = reinterpret_cast<void*>(&seq_str_str)},
        {.name = "seq_float_from_str", .address = reinterpret_cast<void*>(&seq_float_from_str)},
        {.name = "seq_stdout", .address = reinterpret_cast<void*>(&seq_stdout)},
        {.name = "seq_stdin", .address = reinterpret_cast<void*>(&seq_stdin)},
        {.name = "seq_stderr", .address = reinterpret_cast<void*>(&seq_stderr)},
        {.name = "seq_lock_new", .address = reinterpret_cast<void*>(&seq_lock_new)},
        {.name = "seq_lock_acquire", .address = reinterpret_cast<void*>(&seq_lock_acquire)},
        {.name = "seq_lock_release", .address = reinterpret_cast<void*>(&seq_lock_release)},
        {.name = "seq_pid", .address = reinterpret_cast<void*>(&seq_pid)},
        {.name = "seq_time", .address = reinterpret_cast<void*>(&seq_time)},
        {.name = "seq_time_monotonic", .address = reinterpret_cast<void*>(&seq_time_monotonic)},
        {.name = "nes_codon_ensure_module_initialized", .address = reinterpret_cast<void*>(&codonEnsureModuleInitialized)},
        /// Pure libc functions that Codon's stdlib calls directly: str.lower/upper, str search and comparison, float hashing, re's character classes.
        {.name = "tolower", .address = reinterpret_cast<void*>(static_cast<int (*)(int)>(&::tolower))},
        {.name = "toupper", .address = reinterpret_cast<void*>(static_cast<int (*)(int)>(&::toupper))},
        {.name = "memchr", .address = reinterpret_cast<void*>(static_cast<const void* (*)(const void*, int, size_t)>(&std::memchr))},
        {.name = "bcmp", .address = reinterpret_cast<void*>(&::bcmp)},
        {.name = "frexp", .address = reinterpret_cast<void*>(static_cast<double (*)(double, int*)>(&std::frexp))},
        {.name = "isalnum", .address = reinterpret_cast<void*>(static_cast<int (*)(int)>(&::isalnum))},
        {.name = "isalpha", .address = reinterpret_cast<void*>(static_cast<int (*)(int)>(&::isalpha))},
        {.name = "isdigit", .address = reinterpret_cast<void*>(static_cast<int (*)(int)>(&::isdigit))},
        {.name = "islower", .address = reinterpret_cast<void*>(static_cast<int (*)(int)>(&::islower))},
        {.name = "isspace", .address = reinterpret_cast<void*>(static_cast<int (*)(int)>(&::isspace))},
        {.name = "isupper", .address = reinterpret_cast<void*>(static_cast<int (*)(int)>(&::isupper))},
    };
    /// The compiler validates UDFs against PythonUdfRuntimeSymbolNames, so the table must define exactly those names.
    static const bool matchesAbi = std::ranges::equal(
        std::span{symbols} | std::views::transform([](const PythonUdfRuntimeSymbol& symbol) { return symbol.name; }),
        PythonUdfRuntimeSymbolNames);
    INVARIANT(matchesAbi, "The Python UDF runtime symbols differ from PythonUdfRuntimeSymbolNames");
    return symbols;
}
}

/// NOLINTEND(performance-no-int-to-ptr,cppcoreguidelines-avoid-do-while,modernize-avoid-c-arrays,clang-analyzer-core.NullDereference,cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic,cppcoreguidelines-avoid-non-const-global-variables,readability-magic-numbers,hicpp-signed-bitwise,readability-identifier-naming)
