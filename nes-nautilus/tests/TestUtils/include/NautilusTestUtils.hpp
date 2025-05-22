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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/NautilusBackend.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/Engine.hpp>
#include <ErrorHandling.hpp>
#include <options.hpp>
#include <static.hpp>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES::Nautilus::TestUtils
{

/// We need a simple wrapper around the Record class to be able to compare the records with the fields.
/// Otherwise, we would have to implement the operator< directly in the Record class.
struct RecordWithFields
{
    RecordWithFields(const Record& record, std::vector<Record::RecordFieldIdentifier>& fields) : record(record), fields(fields) { }

    bool operator<(const RecordWithFields& other) const
    {
        for (const auto& fieldIdentifier : nautilus::static_iterable(fields))
        {
            const auto thisVal = record.read(fieldIdentifier);
            const auto otherVal = other.record.read(fieldIdentifier);
            if (thisVal == otherVal)
            {
                continue;
            }
            return static_cast<bool>(thisVal < otherVal);
        }
        return false;
    }

    Record record;
    std::vector<Record::RecordFieldIdentifier> fields;
};

/// We store the name of a nautilus function and the backend type in this struct
/// We use this information for being able to access a (pre-)compiled/traced function and not having to recompile it all the time
struct NameAndNautilusBackend
{
    NameAndNautilusBackend(std::string_view functionName, const Configurations::NautilusBackend backend)
        : functionName(std::move(functionName)), backend(backend)
    {
    }

    bool operator==(const NameAndNautilusBackend& other) const { return functionName == other.functionName && backend == other.backend; }

    bool operator<(const NameAndNautilusBackend& other) const
    {
        if (functionName == other.functionName)
        {
            return backend < other.backend;
        }
        return functionName < other.functionName;
    }

    [[nodiscard]] std::size_t hash() const
    {
        std::size_t hashValue = std::hash<int>{}(static_cast<int>(backend)); /// Hash the enum
        hashValue ^= std::hash<std::string>{}(functionName) << 1UL; /// Hash the string and combine with the enum hash
        return hashValue;
    }

    std::string functionName;
    Configurations::NautilusBackend backend;
};

/// Struct that stores a min and max value.
/// Test use it for defining a range for random values.
struct MinMaxValue
{
    uint64_t min;
    uint64_t max;
};

/// Base function wrapper class
class FunctionWrapperBase
{
public:
    FunctionWrapperBase() = default;
    virtual ~FunctionWrapperBase() = default;
};

/// Function wrapper class so that we can store multiple different nautilus functions in a map
template <typename R, typename... FunctionArguments>
class FunctionWrapper final : public FunctionWrapperBase
{
public:
    explicit FunctionWrapper(nautilus::engine::CallableFunction<R, FunctionArguments...>&& function)
        : FunctionWrapperBase(), func(std::move(function))
    {
    }

    ~FunctionWrapper() override = default;
    nautilus::engine::CallableFunction<R, FunctionArguments...> func;
};

class NautilusTestUtils
{
public:
    static constexpr std::string_view FUNCTION_CREATE_MONOTONIC_VALUES_FOR_BUFFER = "createMonotonicValues";
    static constexpr std::string_view FUNCTION_INSERT_INTO_PAGED_VECTOR = "insertIntoPagedVector";
    static constexpr std::string_view FUNCTION_READ_FROM_PAGED_VECTOR = "readFromPagedVector";


    /// Returns a MurMur3 hash function
    static std::unique_ptr<Interface::HashFunction> getMurMurHashFunction();

    /// Creates a schema from the provided basic types. The field names will be field<counter> with the counter starting at typeIdxOffset
    /// For example, the call createSchemaFromBasicTypes({BasicType::INT_32, BasicType::FLOAT}, 1) will create a schema with the fields field1 and field2
    static std::shared_ptr<Schema> createSchemaFromBasicTypes(const std::vector<BasicType>& basicTypes);
    static std::shared_ptr<Schema> createSchemaFromBasicTypes(const std::vector<BasicType>& basicTypes, uint64_t typeIdxOffset);

    /// Creates monotonic increasing values for each field. This means that each field in each tuple has a new and increased value
    std::vector<Memory::TupleBuffer> createMonotonicallyIncreasingValues(
        const std::shared_ptr<Schema>& schema,
        uint64_t numberOfTuples,
        Memory::BufferManager& bufferManager,
        uint64_t seed,
        uint64_t minSizeVarSizedData,
        uint64_t maxSizeVarSizedData);
    std::vector<Memory::TupleBuffer> createMonotonicallyIncreasingValues(
        const std::shared_ptr<Schema>& schema, uint64_t numberOfTuples, Memory::BufferManager& bufferManager, uint64_t minSizeVarSizedData);
    std::vector<Memory::TupleBuffer> createMonotonicallyIncreasingValues(
        const std::shared_ptr<Schema>& schema, uint64_t numberOfTuples, Memory::BufferManager& bufferManager);

    void compileFillBufferFunction(
        std::string_view functionName,
        Configurations::NautilusBackend backend,
        nautilus::engine::Options& options,
        const std::shared_ptr<Schema>& schema,
        const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProviderInputBuffer);

    /// Compares two records and if they are not equal returning a string. If the records are equal, return nullopt
    static std::string
    compareRecords(const Record& recordLeft, const Record& recordRight, const std::vector<Record::RecordFieldIdentifier>& projection);

    /// Calls an already compiled function. If the method does not exist, we throw an PRECONDITION violation
    template <typename R, typename... FunctionArguments>
    void callCompiledFunction(const NameAndNautilusBackend& nameAndBackend, FunctionArguments... arguments)
    {
        PRECONDITION(compiledFunctions.contains(nameAndBackend), "Expected that a query for {} exists", nameAndBackend.functionName);
        const auto& callableFunction = compiledFunctions.at(nameAndBackend);
        auto* castedFunction = dynamic_cast<FunctionWrapper<R, FunctionArguments...>*>(callableFunction.get());
        castedFunction->func(arguments...);
    }

    /// Compares two buffers and returns a string with the differences. If the buffers are equal, return an empty string
    static std::string compareRecordBuffers(
        const std::vector<Memory::TupleBuffer>& actualRecords,
        const std::vector<Memory::TupleBuffer>& expectedRecords,
        const Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider& memoryProviderActualBuffer,
        const Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider& memoryProviderInputBuffer);


protected:
    /// The idea behind this map is that we can batch/(pre-)compile and trace functions and store them in this map.
    /// Allowing us to not have to recompile/trace the same function in multiple different (parameterized) tests
    /// This map can and will be filled in this class but also in the tests themselves.
    std::map<NameAndNautilusBackend, std::unique_ptr<FunctionWrapperBase>> compiledFunctions;
};

}
