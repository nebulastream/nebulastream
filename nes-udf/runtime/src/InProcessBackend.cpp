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

#include <InProcessBackend.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <dlfcn.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <UdfAbi.h>
#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <UdfBackend.hpp>
#include <UdfDescriptor.hpp>

/// Loads the bridge with dlopen or dlmopen and calls it through the C ABI of UdfAbi.h: C types, C arrays, and malloc'ed strings
/// the bridge hands over.
/// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic,cppcoreguidelines-pro-bounds-avoid-unchecked-container-access,concurrency-mt-unsafe,google-runtime-int,cppcoreguidelines-owning-memory,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay,bugprone-multi-level-implicit-pointer-conversion,cppcoreguidelines-avoid-non-const-global-variables)

namespace NES
{

namespace
{
/// Fixed-width scalar slot; a VARSIZED argument slot instead holds a pointer to the content bytes.
constexpr std::size_t SLOT_BYTES = 8;

using InitializeUdfFn = UdfInstance* (*)(const char*, int, const int*, int, char**);
using InitializeUdfFromSourceFn = UdfInstance* (*)(const char*, const char*, int, const int*, int, char**);
using ExecuteUdfRowFn = int (*)(UdfInstance*, const void* const*, const long long*, const int*, void*, char**, long long*, int*, char**);
using ExecuteUdfBatchFn = int (*)(UdfInstance*, ArrowArray*, ArrowArray*, char**);
using EnterUdfRowFn = int (*)(int);
using ExitUdfRowFn = void (*)(int);
using CleanupUdfFn = void (*)(UdfInstance*);
using FreeFn = void (*)(void*);
using InitializeUdfStageFn = UdfStageInstance* (*)(int, UdfInstance* const*, char**);
using ExecuteUdfStageRowFn = int (*)(UdfStageInstance*, const void* const*, const long long*, const int*, void*, char**, long long*, int*, char**);
using ExecuteUdfStageBatchFn = int (*)(UdfStageInstance*, ArrowArray*, ArrowArray*, char**);
using CleanupUdfStageFn = void (*)(UdfStageInstance*);

/// Maps a NES type to its UDF ABI type code. UNDEFINED is rejected at UDF registration, so reaching it here is a bug.
int toUdfTypeCode(const DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::BOOLEAN:
            return UDF_BOOL;
        case DataType::Type::INT8:
            return UDF_INT8;
        case DataType::Type::INT16:
            return UDF_INT16;
        case DataType::Type::INT32:
            return UDF_INT32;
        case DataType::Type::INT64:
            return UDF_INT64;
        case DataType::Type::UINT8:
            return UDF_UINT8;
        case DataType::Type::UINT16:
            return UDF_UINT16;
        case DataType::Type::UINT32:
            return UDF_UINT32;
        case DataType::Type::UINT64:
            return UDF_UINT64;
        case DataType::Type::FLOAT32:
            return UDF_FLOAT32;
        case DataType::Type::FLOAT64:
            return UDF_FLOAT64;
        case DataType::Type::VARSIZED:
            return UDF_VARSIZED;
        case DataType::Type::CHAR:
            return UDF_CHAR;
        case DataType::Type::UNDEFINED:
            break;
    }
    throw CannotLoadUdf("UDF type (code {}) cannot be represented in the UDF ABI", static_cast<int>(type));
}

/// Resolves the entry points of a loaded bridge. Throws CannotLoadUdf if a required one is missing.
BridgeLibrary resolve(void* handle, const std::filesystem::path& bridgePath)
{
    BridgeLibrary library{
        .handle = handle,
        .initialize = dlsym(handle, "initialize_udf"),
        .initializeFromSource = dlsym(handle, "initialize_udf_from_source"),
        .executeRow = dlsym(handle, "execute_udf_row"),
        .executeBatch = dlsym(handle, "execute_udf_batch"),
        .enterRow = dlsym(handle, "enter_udf_row"),
        .exitRow = dlsym(handle, "exit_udf_row"),
        .cleanup = dlsym(handle, "cleanup_udf"),
        /// Found in the C library of the bridge's link namespace.
        .free = dlsym(handle, "free"),
        .initializeStage = dlsym(handle, "initialize_udf_stage"),
        .executeStageRow = dlsym(handle, "execute_udf_stage_row"),
        .executeStageBatch = dlsym(handle, "execute_udf_stage_batch"),
        .cleanupStage = dlsym(handle, "cleanup_udf_stage")};
    if (library.initialize == nullptr || library.executeRow == nullptr || library.executeBatch == nullptr || library.enterRow == nullptr
        || library.exitRow == nullptr || library.cleanup == nullptr || library.free == nullptr)
    {
        throw CannotLoadUdf(
            "UDF library '{}' does not export the required symbols "
            "initialize_udf/execute_udf_row/execute_udf_batch/enter_udf_row/exit_udf_row/cleanup_udf",
            bridgePath.string());
    }
    return library;
}

/// The bridge, loaded once into the worker's own link namespace.
/// RTLD_LOCAL: a bridge embeds a whole language runtime, whose symbols must not serve anything else in the worker.
/// Never dlclose'd: a bridge may init an embedded runtime once per process and never finalize it (e.g. Py_InitializeEx), and
/// unmapping the .so while that runtime's state is still live crashes at exit. The OS reclaims the mapping when the process exits.
const BridgeLibrary& sharedLibrary(const std::filesystem::path& bridgePath)
{
    static std::mutex mutex;
    static std::map<std::filesystem::path, std::unique_ptr<BridgeLibrary>> libraries;
    const std::scoped_lock lock(mutex);
    auto& library = libraries[bridgePath];
    if (library == nullptr)
    {
        void* handle = dlopen(bridgePath.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr)
        {
            throw CannotLoadUdf("Failed to load UDF library '{}': {}", bridgePath.string(), dlerror());
        }
        library = std::make_unique<BridgeLibrary>(resolve(handle, bridgePath));
    }
    return *library;
}

/// A link namespace with the bridges loaded into it and the UDFs initialized there. One thread at a time uses it.
struct LinkNamespace
{
    std::optional<Lmid_t> id; /// created by its first bridge
    std::map<std::filesystem::path, const BridgeLibrary*> libraries;
    /// Every UDF initialized and every stage fused in this namespace, by the id of its backend or InProcessFusedUdfs.
    std::unordered_map<std::uint64_t, std::pair<const BridgeLibrary*, UdfInstance*>> udfs;
    std::unordered_map<std::uint64_t, std::pair<const BridgeLibrary*, UdfStageInstance*>> stages;
};

/// glibc has room for 15 link namespaces besides the worker's (DL_NNS) and cannot unload a namespace with a language runtime in it,
/// so namespaces are not bound to a thread but lent to it: a thread borrows one on its first UDF call and returns it when it exits.
/// The next thread then reuses the bridges and UDFs in it. Namespaces live as long as the process, like sharedLibrary's bridge.
class NamespacePool
{
public:
    static LinkNamespace& ofThisThread()
    {
        thread_local const Lease lease;
        return *lease.borrowed;
    }

private:
    struct Lease
    {
        LinkNamespace* borrowed;

        Lease() : borrowed(take()) { }

        ~Lease()
        {
            const std::scoped_lock lock(mutex);
            idle.push_back(borrowed);
        }

        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&&) = delete;
        Lease& operator=(Lease&&) = delete;
    };

    static LinkNamespace* take()
    {
        const std::scoped_lock lock(mutex);
        if (idle.empty())
        {
            return new LinkNamespace(); /// lives as long as the process, see above
        }
        auto* const linkNamespace = idle.back();
        idle.pop_back();
        return linkNamespace;
    }

    static inline std::mutex mutex;
    static inline std::vector<LinkNamespace*> idle;
};

/// The bridge, loaded into `linkNamespace`, which the first bridge creates.
const BridgeLibrary& namespaceLibrary(LinkNamespace& linkNamespace, const std::filesystem::path& bridgePath)
{
    if (const auto it = linkNamespace.libraries.find(bridgePath); it != linkNamespace.libraries.end())
    {
        return *it->second;
    }
    void* handle = dlmopen(linkNamespace.id.value_or(LM_ID_NEWLM), bridgePath.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr)
    {
        throw CannotLoadUdf(
            "Failed to load UDF library '{}' into a link namespace of its own: {}. glibc has room for 15 namespaces, one per "
            "thread running a UDF at the same time, and the static TLS of the libraries in them may need more room, see the "
            "tunable glibc.rtld.optional_static_tls",
            bridgePath.string(),
            dlerror());
    }
    if (!linkNamespace.id.has_value())
    {
        Lmid_t created = LM_ID_BASE;
        dlinfo(handle, RTLD_DI_LMID, &created);
        linkNamespace.id = created;
    }
    const auto* const library = new BridgeLibrary(resolve(handle, bridgePath)); /// lives as long as the process, see above
    linkNamespace.libraries.emplace(bridgePath, library);
    return *library;
}

std::atomic<std::uint64_t> nextBackendId{0};

/// Frees a string the bridge handed over with the bridge's `free`.
void freeString(const BridgeLibrary& library, char* string)
{
    reinterpret_cast<FreeFn>(library.free)(string);
}

std::string takeMessage(const BridgeLibrary& library, char* message)
{
    std::string text = message != nullptr ? message : "unknown error";
    freeString(library, message);
    return text;
}
}

InProcessBackend::InProcessBackend(Definition definition, const UdfInterpreter interpreter)
    : definition(std::move(definition)), interpreter(interpreter), id(nextBackendId++)
{
    if (interpreter == UdfInterpreter::DLMOPEN)
    {
        /// Fails the query now if the UDF cannot load, rather than on its first record.
        static_cast<void>(current());
        return;
    }
    library = &sharedLibrary(this->definition.bridgePath);
    udfHandle = initialize(*library);
}

UdfInstance* InProcessBackend::initialize(const BridgeLibrary& bridge) const
{
    char* errorMessage = nullptr;
    UdfInstance* handle = nullptr;
    const auto argCount = static_cast<int>(definition.argTypeCodes.size());
    if (definition.entrypoint.empty())
    {
        if (bridge.initializeFromSource == nullptr)
        {
            throw CannotLoadUdf(
                "UDF bridge '{}' does not export the required symbol initialize_udf_from_source", definition.bridgePath.string());
        }
        handle = reinterpret_cast<InitializeUdfFromSourceFn>(bridge.initializeFromSource)(
            definition.source.c_str(),
            definition.functionName.c_str(),
            argCount,
            definition.argTypeCodes.data(),
            definition.returnTypeCode,
            &errorMessage);
    }
    else
    {
        handle = reinterpret_cast<InitializeUdfFn>(bridge.initialize)(
            definition.entrypoint.c_str(), argCount, definition.argTypeCodes.data(), definition.returnTypeCode, &errorMessage);
    }
    if (handle == nullptr)
    {
        throw CannotLoadUdf(
            "Failed to initialize UDF '{}': {}",
            definition.entrypoint.empty() ? definition.functionName : definition.entrypoint,
            takeMessage(bridge, errorMessage));
    }
    return handle;
}

std::pair<const BridgeLibrary*, UdfInstance*> InProcessBackend::current()
{
    if (interpreter != UdfInterpreter::DLMOPEN)
    {
        return {library, udfHandle};
    }
    auto& linkNamespace = NamespacePool::ofThisThread();
    if (const auto it = linkNamespace.udfs.find(id); it != linkNamespace.udfs.end())
    {
        return it->second;
    }
    const auto& bridge = namespaceLibrary(linkNamespace, definition.bridgePath);
    const std::pair entry{&bridge, initialize(bridge)};
    linkNamespace.udfs.emplace(id, entry);
    const std::scoped_lock lock(handlesMutex);
    handles.push_back(entry);
    return entry;
}

InProcessBackend::~InProcessBackend()
{
    if (library != nullptr)
    {
        reinterpret_cast<CleanupUdfFn>(library->cleanup)(udfHandle);
    }
    /// The handles of DLMOPEN stay in the namespaces' maps, keyed by an id no later backend reuses.
    for (const auto& [bridge, handle] : handles)
    {
        reinterpret_cast<CleanupUdfFn>(bridge->cleanup)(handle);
    }
}

int InProcessBackend::enterRow()
{
    return reinterpret_cast<EnterUdfRowFn>(current().first->enterRow)(interpreter == UdfInterpreter::SUBINTERPRETER ? 1 : 0);
}

void InProcessBackend::exitRow(const int state)
{
    reinterpret_cast<ExitUdfRowFn>(current().first->exitRow)(state);
}

const BridgeLibrary& InProcessBackend::invokeUdf(
    const std::int8_t* argValues,
    const std::int8_t* argLens,
    const std::int8_t* argNulls,
    std::int8_t* resultScalar,
    char** resultString,
    long long* resultStringLen,
    int* resultNull)
{
    const auto [bridge, handle] = current();
    const auto argCount = definition.argTypeCodes.size();
    /// Per thread rather than per backend, so that threads with interpreters of their own call in parallel.
    thread_local std::vector<const void*> argPointers;
    thread_local std::vector<long long> argLengths;
    thread_local std::vector<int> argNullFlags;
    argPointers.resize(argCount);
    argLengths.resize(argCount);
    argNullFlags.resize(argCount);
    for (std::size_t i = 0; i < argCount; ++i)
    {
        const std::int8_t* const slot = argValues + (i * SLOT_BYTES);
        if (definition.argTypeCodes[i] == UDF_VARSIZED)
        {
            /// The slot holds a pointer to the content bytes; the ABI wants that pointer directly.
            std::memcpy(&argPointers[i], slot, sizeof(const void*));
            std::memcpy(&argLengths[i], argLens + (i * SLOT_BYTES), sizeof(long long));
        }
        else
        {
            argPointers[i] = slot;
            argLengths[i] = 0;
        }
        argNullFlags[i] = static_cast<unsigned char>(argNulls[i]);
    }

    char* errorMessage = nullptr;
    const int status = reinterpret_cast<ExecuteUdfRowFn>(bridge->executeRow)(
        handle,
        argPointers.data(),
        argLengths.data(),
        argNullFlags.data(),
        resultScalar,
        resultString,
        resultStringLen,
        resultNull,
        &errorMessage);

    if (status == UDF_ERROR)
    {
        freeString(*bridge, *resultString);
        *resultString = nullptr;
        throw UdfExecutionError("{}", takeMessage(*bridge, errorMessage));
    }
    return *bridge;
}

void InProcessBackend::executeScalarRow(
    const std::int8_t* argValues, const std::int8_t* argLens, const std::int8_t* argNulls, std::int8_t* resultScalar, int* resultNull)
{
    char* resultString = nullptr;
    long long resultStringLen = 0;
    const auto& bridge = invokeUdf(argValues, argLens, argNulls, resultScalar, &resultString, &resultStringLen, resultNull);
    freeString(bridge, resultString); /// a scalar-return UDF never sets result_string
}

void InProcessBackend::executeVarsizedRow(
    const std::int8_t* argValues, const std::int8_t* argLens, const std::int8_t* argNulls, std::vector<std::int8_t>& result, int* resultNull)
{
    std::int8_t scalarUnused[SLOT_BYTES] = {};
    char* resultString = nullptr;
    long long resultStringLen = 0;
    const auto& bridge = invokeUdf(argValues, argLens, argNulls, scalarUnused, &resultString, &resultStringLen, resultNull);
    result.clear();
    if (*resultNull == 0 && resultString != nullptr)
    {
        if (resultStringLen < 0)
        {
            freeString(bridge, resultString);
            throw UdfExecutionError("UDF returned a result of negative length {}", resultStringLen);
        }
        result.assign(resultString, resultString + resultStringLen);
    }
    freeString(bridge, resultString);
}

void InProcessBackend::executeBatch(ArrowArray* arguments, ArrowArray* result)
{
    const auto [bridge, handle] = current();
    char* errorMessage = nullptr;
    if (reinterpret_cast<ExecuteUdfBatchFn>(bridge->executeBatch)(handle, arguments, result, &errorMessage) == UDF_ERROR)
    {
        throw UdfExecutionError("{}", takeMessage(*bridge, errorMessage));
    }
}

InProcessFusedUdfs::InProcessFusedUdfs(std::vector<std::shared_ptr<InProcessBackend>> udfs) : udfs(std::move(udfs)), id(nextBackendId++)
{
    PRECONDITION(!this->udfs.empty(), "Fusing UDFs needs at least one UDF");
    for (const auto& udf : this->udfs)
    {
        argumentCount += udf->getArgTypeCodes().size();
    }
    const auto [bridge, handle] = current();
    if (this->udfs.front()->getInterpreter() != UdfInterpreter::DLMOPEN)
    {
        library = bridge;
        stageHandle = handle;
    }
}

UdfStageInstance* InProcessFusedUdfs::initialize(const BridgeLibrary& bridge, const std::vector<UdfInstance*>& udfHandles) const
{
    if (bridge.initializeStage == nullptr || bridge.executeStageRow == nullptr || bridge.executeStageBatch == nullptr
        || bridge.cleanupStage == nullptr)
    {
        throw CannotLoadUdf(
            "UDF bridge cannot fuse UDFs: it does not export "
            "initialize_udf_stage/execute_udf_stage_row/execute_udf_stage_batch/cleanup_udf_stage");
    }
    char* errorMessage = nullptr;
    UdfStageInstance* const handle
        = reinterpret_cast<InitializeUdfStageFn>(bridge.initializeStage)(static_cast<int>(udfHandles.size()), udfHandles.data(), &errorMessage);
    if (handle == nullptr)
    {
        throw CannotLoadUdf("Failed to fuse {} UDFs: {}", udfHandles.size(), takeMessage(bridge, errorMessage));
    }
    return handle;
}

std::pair<const BridgeLibrary*, UdfStageInstance*> InProcessFusedUdfs::current()
{
    if (library != nullptr)
    {
        return {library, stageHandle};
    }
    const bool perNamespace = udfs.front()->getInterpreter() == UdfInterpreter::DLMOPEN;
    auto* const linkNamespace = perNamespace ? &NamespacePool::ofThisThread() : nullptr;
    if (linkNamespace != nullptr)
    {
        if (const auto it = linkNamespace->stages.find(id); it != linkNamespace->stages.end())
        {
            return it->second;
        }
    }
    const BridgeLibrary* bridge = nullptr;
    std::vector<UdfInstance*> udfHandles;
    for (const auto& udf : udfs)
    {
        const auto [udfBridge, udfHandle] = udf->current();
        INVARIANT(bridge == nullptr || bridge == udfBridge, "Fused UDFs must share one bridge library");
        bridge = udfBridge;
        udfHandles.push_back(udfHandle);
    }
    const std::pair entry{bridge, initialize(*bridge, udfHandles)};
    if (linkNamespace != nullptr)
    {
        linkNamespace->stages.emplace(id, entry);
        const std::scoped_lock lock(handlesMutex);
        handles.push_back(entry);
    }
    return entry;
}

InProcessFusedUdfs::~InProcessFusedUdfs()
{
    if (library != nullptr)
    {
        reinterpret_cast<CleanupUdfStageFn>(library->cleanupStage)(stageHandle);
    }
    for (const auto& [bridge, handle] : handles)
    {
        reinterpret_cast<CleanupUdfStageFn>(bridge->cleanupStage)(handle);
    }
}

void InProcessFusedUdfs::executeRow(
    const std::int8_t* argValues,
    const std::int8_t* argLens,
    const std::int8_t* argNulls,
    std::int8_t* results,
    std::int8_t* resultNulls,
    const std::span<std::vector<std::int8_t>> varsizedResults)
{
    const auto [bridge, handle] = current();
    /// Per thread rather than per stage, so that threads with interpreters of their own call in parallel.
    thread_local std::vector<const void*> argPointers;
    thread_local std::vector<long long> argLengths;
    thread_local std::vector<int> argNullFlags;
    thread_local std::vector<char*> resultStrings;
    thread_local std::vector<long long> resultStringLens;
    thread_local std::vector<int> resultNullFlags;
    argPointers.resize(argumentCount);
    argLengths.resize(argumentCount);
    argNullFlags.resize(argumentCount);
    resultStrings.assign(udfs.size(), nullptr);
    resultStringLens.assign(udfs.size(), 0);
    resultNullFlags.assign(udfs.size(), 0);

    std::size_t argument = 0;
    for (const auto& udf : udfs)
    {
        for (const int typeCode : udf->getArgTypeCodes())
        {
            const std::int8_t* const slot = argValues + (argument * SLOT_BYTES);
            if (typeCode == UDF_VARSIZED)
            {
                std::memcpy(&argPointers[argument], slot, sizeof(const void*));
                std::memcpy(&argLengths[argument], argLens + (argument * SLOT_BYTES), sizeof(long long));
            }
            else
            {
                argPointers[argument] = slot;
                argLengths[argument] = 0;
            }
            argNullFlags[argument] = static_cast<unsigned char>(argNulls[argument]);
            ++argument;
        }
    }

    char* errorMessage = nullptr;
    const int status = reinterpret_cast<ExecuteUdfStageRowFn>(bridge->executeStageRow)(
        handle,
        argPointers.data(),
        argLengths.data(),
        argNullFlags.data(),
        results,
        resultStrings.data(),
        resultStringLens.data(),
        resultNullFlags.data(),
        &errorMessage);
    if (status == UDF_ERROR)
    {
        throw UdfExecutionError("{}", takeMessage(*bridge, errorMessage));
    }

    /// Copies the VARSIZED results out, and frees all result strings, also on a throw.
    struct FreeResultStrings
    {
        const BridgeLibrary& bridge;

        ~FreeResultStrings()
        {
            for (auto*& string : resultStrings)
            {
                freeString(bridge, string);
                string = nullptr;
            }
        }
    } const freeResultStrings{*bridge};
    for (std::size_t udf = 0; udf < udfs.size(); ++udf)
    {
        resultNulls[udf] = static_cast<std::int8_t>(resultNullFlags[udf]);
        if (udfs[udf]->getReturnTypeCode() != UDF_VARSIZED)
        {
            continue;
        }
        auto& result = varsizedResults[udf];
        result.clear();
        if (resultNullFlags[udf] == 0 && resultStrings[udf] != nullptr)
        {
            if (resultStringLens[udf] < 0)
            {
                throw UdfExecutionError("UDF returned a result of negative length {}", resultStringLens[udf]);
            }
            result.assign(resultStrings[udf], resultStrings[udf] + resultStringLens[udf]);
        }
    }
}

void InProcessFusedUdfs::executeBatch(ArrowArray* arguments, ArrowArray* result)
{
    const auto [bridge, handle] = current();
    char* errorMessage = nullptr;
    if (reinterpret_cast<ExecuteUdfStageBatchFn>(bridge->executeStageBatch)(handle, arguments, result, &errorMessage) == UDF_ERROR)
    {
        throw UdfExecutionError("{}", takeMessage(*bridge, errorMessage));
    }
}

std::shared_ptr<UdfBackend> UdfBackend::create(const UdfDescriptor& descriptor, const UdfInterpreter interpreter)
{
    if (descriptor.getExecution() != UdfExecution::InProcess)
    {
        throw CannotLoadUdf("UDF '{}' does not run through an in-process bridge", descriptor.getName());
    }
    InProcessBackend::Definition definition;
    definition.bridgePath = descriptor.getPath();
    definition.entrypoint = descriptor.getEntrypoint();
    definition.argTypeCodes = descriptor.getArgTypes() | std::views::transform([](const auto& type) { return toUdfTypeCode(type.type); })
        | std::ranges::to<std::vector>();
    definition.returnTypeCode = toUdfTypeCode(descriptor.getReturnType().type);
    if (const auto& source = descriptor.getSource())
    {
        static constexpr std::string_view InlineFunctionName = "nes_inline_udf";
        definition.entrypoint.clear();
        definition.functionName = InlineFunctionName;
        definition.source = fmt::format("def {}({}):\n", InlineFunctionName, fmt::join(source->parameterNames, ", "));
        /// One level deeper than the body's own indentation, whichever that is. Lines in a multi-line string literal shift too.
        for (const auto line : std::views::split(std::string_view{source->body}, '\n'))
        {
            definition.source += fmt::format("    {}\n", std::string_view{line});
        }
    }
    return std::make_shared<InProcessBackend>(std::move(definition), interpreter);
}

}

/// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic,cppcoreguidelines-pro-bounds-avoid-unchecked-container-access,concurrency-mt-unsafe,google-runtime-int,cppcoreguidelines-owning-memory,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay,bugprone-multi-level-implicit-pointer-conversion,cppcoreguidelines-avoid-non-const-global-variables)
