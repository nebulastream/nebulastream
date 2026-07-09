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
#include <memory>
#include <mutex>
#include <string>
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

InProcessBackend::InProcessBackend(const UdfDescriptor& descriptor) : argCount(descriptor.getArgTypes().size())
{
    libHandle = dlopen(descriptor.getPath().c_str(), RTLD_NOW | RTLD_GLOBAL);
    if (libHandle == nullptr)
    {
        throw CannotLoadUdf("Failed to load UDF library '{}': {}", descriptor.getPath().string(), dlerror());
    }

    auto* const initializeFn = reinterpret_cast<InitializeUdfFn>(dlsym(libHandle, "initialize_udf"));
    executeFn = dlsym(libHandle, "execute_udf_row");
    cleanupFn = dlsym(libHandle, "cleanup_udf");
    if (initializeFn == nullptr || executeFn == nullptr || cleanupFn == nullptr)
    {
        dlclose(libHandle);
        throw CannotLoadUdf(
            "UDF library '{}' does not export the required symbols initialize_udf/execute_udf_row/cleanup_udf",
            descriptor.getPath().string());
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
        dlclose(libHandle);
        throw CannotLoadUdf("Failed to initialize UDF '{}': {}", descriptor.getEntrypoint(), message);
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
    if (libHandle != nullptr)
    {
        dlclose(libHandle);
    }
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
    return std::make_shared<InProcessBackend>(descriptor);
}

}
