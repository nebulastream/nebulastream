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

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <dlfcn.h>

#include <UdfAbi.h>
#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <UdfBackend.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

namespace
{
/// Fixed-width scalar slot; a VARSIZED argument slot instead holds a pointer to the content bytes.
constexpr std::size_t SLOT_BYTES = 8;

using InitializeUdfFn = int (*)(const char*, int, const int*, int, char**);
using InitializeUdfFromSourceFn = int (*)(const char*, const char*, int, const int*, int, char**);
using ExecuteUdfRowFn = int (*)(int, const void* const*, const long long*, const int*, void*, char**, long long*, int*, char**);
using CleanupUdfFn = void (*)(int);

/// Maps a NES type to its UDF ABI type code. CHAR/UNDEFINED are rejected at UDF registration, so
/// reaching them here is a bug.
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
        case DataType::Type::UNDEFINED:
            break;
    }
    throw CannotLoadUdf("UDF type (code {}) cannot be represented in the UDF ABI", static_cast<int>(type));
}
}

void* InProcessBackend::loadBridge(const std::filesystem::path& bridgePath)
{
    void* handle = dlopen(bridgePath.c_str(), RTLD_NOW | RTLD_GLOBAL);
    if (handle == nullptr)
    {
        throw CannotLoadUdf("Failed to load UDF library '{}': {}", bridgePath.string(), dlerror());
    }
    executeFn = dlsym(handle, "execute_udf_row");
    cleanupFn = dlsym(handle, "cleanup_udf");
    if (executeFn == nullptr || cleanupFn == nullptr)
    {
        dlclose(handle);
        throw CannotLoadUdf(
            "UDF library '{}' does not export the required symbols execute_udf_row/cleanup_udf", bridgePath.string());
    }
    return handle;
}

InProcessBackend::InProcessBackend(const UdfDescriptor& descriptor) : argCount(descriptor.getArgTypes().size())
{
    libHandle = loadBridge(descriptor.getPath());
    auto* const initializeFn = reinterpret_cast<InitializeUdfFn>(dlsym(libHandle, "initialize_udf"));
    if (initializeFn == nullptr)
    {
        throw CannotLoadUdf("UDF library '{}' does not export the required symbol initialize_udf", descriptor.getPath().string());
    }

    argTypeCodes.reserve(argCount);
    for (const auto& argType : descriptor.getArgTypes())
    {
        argTypeCodes.push_back(toUdfTypeCode(argType.type));
    }
    const int returnTypeCode = toUdfTypeCode(descriptor.getReturnType().type);

    char* errorMessage = nullptr;
    udfHandle
        = initializeFn(descriptor.getEntrypoint().c_str(), static_cast<int>(argCount), argTypeCodes.data(), returnTypeCode, &errorMessage);
    if (udfHandle < 0)
    {
        const std::string message = errorMessage != nullptr ? errorMessage : "unknown error";
        std::free(errorMessage);
        /// Not dlclose()'d -- see the "Never dlclose" comment on the destructor below. initializeFn may
        /// have already initialized an embedded runtime (e.g. Py_InitializeEx) that never gets finalized;
        /// unmapping the .so here wipes its "already initialized" state (e.g. the bridge's std::once_flag)
        /// without the runtime itself knowing, so the next dlopen of this same path re-runs init on an
        /// already-live-but-orphaned runtime and crashes.
        throw CannotLoadUdf("Failed to initialize UDF '{}': {}", descriptor.getEntrypoint(), message);
    }

    argPointers.resize(argCount);
    argLengths.resize(argCount);
    argNullFlags.resize(argCount);
}

InProcessBackend::InProcessBackend(
    std::filesystem::path bridgePath,
    const std::string& source,
    const std::string& functionName,
    const std::vector<DataType>& argTypes,
    const DataType& returnType)
    : argCount(argTypes.size())
{
    libHandle = loadBridge(bridgePath);
    auto* const initializeFn = reinterpret_cast<InitializeUdfFromSourceFn>(dlsym(libHandle, "initialize_udf_from_source"));
    if (initializeFn == nullptr)
    {
        throw CannotLoadUdf(
            "UDF bridge '{}' does not export the required symbol initialize_udf_from_source", bridgePath.string());
    }

    argTypeCodes.reserve(argCount);
    for (const auto& argType : argTypes)
    {
        argTypeCodes.push_back(toUdfTypeCode(argType.type));
    }
    const int returnTypeCode = toUdfTypeCode(returnType.type);

    char* errorMessage = nullptr;
    udfHandle = initializeFn(
        source.c_str(), functionName.c_str(), static_cast<int>(argCount), argTypeCodes.data(), returnTypeCode, &errorMessage);
    if (udfHandle < 0)
    {
        const std::string message = errorMessage != nullptr ? errorMessage : "unknown error";
        std::free(errorMessage);
        /// See the "Never dlclose" comment above/below.
        throw CannotLoadUdf("Failed to initialize inline UDF '{}': {}", functionName, message);
    }

    argPointers.resize(argCount);
    argLengths.resize(argCount);
    argNullFlags.resize(argCount);
}

InProcessBackend::~InProcessBackend()
{
    if (cleanupFn != nullptr && udfHandle >= 0)
    {
        reinterpret_cast<CleanupUdfFn>(cleanupFn)(udfHandle);
    }
    /// Never dlclose(libHandle): a bridge may init an embedded runtime once per process and never
    /// finalize it (e.g. Py_InitializeEx). Unmapping the .so while that runtime's state is still live
    /// crashes at exit. The OS reclaims the mapping when the process exits.
}

void InProcessBackend::invokeUdf(
    const std::int8_t* argValues,
    const std::int8_t* argLens,
    const std::int8_t* argNulls,
    std::int8_t* resultScalar,
    char** resultString,
    long long* resultStringLen,
    int* resultNull)
{
    for (std::size_t i = 0; i < argCount; ++i)
    {
        const std::int8_t* const slot = argValues + (i * SLOT_BYTES);
        if (argTypeCodes[i] == UDF_VARSIZED)
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
        argNullFlags[i] = argNulls[i];
    }

    char* errorMessage = nullptr;
    const int status = reinterpret_cast<ExecuteUdfRowFn>(executeFn)(
        udfHandle,
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
        const std::string message = errorMessage != nullptr ? errorMessage : "unknown error";
        std::free(errorMessage);
        std::free(*resultString);
        *resultString = nullptr;
        throw UdfExecutionError("{}", message);
    }
}

void InProcessBackend::executeScalarRow(
    const std::int8_t* argValues, const std::int8_t* argLens, const std::int8_t* argNulls, std::int8_t* resultScalar, int* resultNull)
{
    const std::lock_guard lock(mutex);
    char* resultString = nullptr;
    long long resultStringLen = 0;
    invokeUdf(argValues, argLens, argNulls, resultScalar, &resultString, &resultStringLen, resultNull);
    std::free(resultString); /// a scalar-return UDF never sets result_string
}

std::uint64_t InProcessBackend::executeVarsizedRow(
    const std::int8_t* argValues,
    const std::int8_t* argLens,
    const std::int8_t* argNulls,
    std::int8_t* resultBuffer,
    std::uint64_t maxResultLen,
    int* resultNull)
{
    const std::lock_guard lock(mutex);
    std::int8_t scalarUnused[SLOT_BYTES] = {};
    char* resultString = nullptr;
    long long resultStringLen = 0;
    invokeUdf(argValues, argLens, argNulls, scalarUnused, &resultString, &resultStringLen, resultNull);

    if (*resultNull != 0 || resultString == nullptr)
    {
        std::free(resultString);
        return 0;
    }
    if (resultStringLen < 0 || static_cast<std::uint64_t>(resultStringLen) > maxResultLen)
    {
        const long long length = resultStringLen;
        std::free(resultString);
        throw UdfExecutionError("UDF returned a {}-byte result, exceeding the {}-byte limit", length, maxResultLen);
    }
    std::memcpy(resultBuffer, resultString, static_cast<std::size_t>(resultStringLen));
    std::free(resultString);
    return static_cast<std::uint64_t>(resultStringLen);
}

std::shared_ptr<UdfBackend> UdfBackend::create(const UdfDescriptor& descriptor)
{
    if (descriptor.getExecution() != UdfExecution::InProcess)
    {
        throw CannotLoadUdf("UDF '{}' does not run through an in-process bridge", descriptor.getName());
    }
    return std::make_shared<InProcessBackend>(descriptor);
}

std::shared_ptr<UdfBackend> UdfBackend::createFromSource(
    std::filesystem::path bridgePath, std::string source, std::string functionName, std::vector<DataType> argTypes, DataType returnType)
{
    return std::make_shared<InProcessBackend>(std::move(bridgePath), source, functionName, argTypes, returnType);
}

}
