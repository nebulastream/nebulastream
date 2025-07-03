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

#include <NautilusTestUtils.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Util.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <nautilus/Engine.hpp>
#include <nautilus/function.hpp>
#include <nautilus/options.hpp>
#include <nautilus/static.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <std/sstream.h>
#include <ErrorHandling.hpp>

namespace NES::Nautilus::TestUtils
{

std::unique_ptr<Interface::HashFunction> NautilusTestUtils::getMurMurHashFunction()
{
    return std::make_unique<Interface::MurMur3HashFunction>();
}

std::vector<Memory::TupleBuffer> NautilusTestUtils::createMonotonicallyIncreasingValues(
    const Schema& schema, const uint64_t numberOfTuples, Memory::BufferManager& bufferManager, const uint64_t minSizeVarSizedData)
{
    /// We log the seed to gain reproducibility of the test
    const auto seed = std::random_device()();
    NES_INFO("Seed for creating values: {}", seed);

    constexpr auto maxSizeVarSizedData = 20;
    return createMonotonicallyIncreasingValues(schema, numberOfTuples, bufferManager, seed, minSizeVarSizedData, maxSizeVarSizedData);
}

std::vector<Memory::TupleBuffer> NautilusTestUtils::createMonotonicallyIncreasingValues(
    const Schema& schema, const uint64_t numberOfTuples, Memory::BufferManager& bufferManager)
{
    constexpr auto minSizeVarSizedData = 10;
    return createMonotonicallyIncreasingValues(schema, numberOfTuples, bufferManager, minSizeVarSizedData);
}

std::vector<Memory::TupleBuffer> NautilusTestUtils::createMonotonicallyIncreasingValues(
    const Schema& schema,
    const uint64_t numberOfTuples,
    Memory::BufferManager& bufferManager,
    const uint64_t seed,
    const uint64_t minSizeVarSizedData,
    const uint64_t maxSizeVarSizedData)
{
    /// Creating here the memory provider for the tuple buffers that store the data
    const auto memoryProviderInputBuffer
        = Interface::MemoryProvider::TupleBufferMemoryProvider::create(bufferManager.getBufferSize(), schema);


    /// If we have large number of tuples, we should compile the query otherwise, it is faster to run it in the interpreter.
    /// We set the threshold to be 10k tuples.
    constexpr auto thresholdForCompile = 10 * 1000;
    auto backend
        = numberOfTuples > thresholdForCompile ? Configurations::ExecutionMode::COMPILER : Configurations::ExecutionMode::INTERPRETER;
    if (not compiledFunctions.contains({FUNCTION_CREATE_MONOTONIC_VALUES_FOR_BUFFER, backend}))
    {
        nautilus::engine::Options options;
        const auto compilation = backend == Configurations::ExecutionMode::COMPILER;
        options.setOption("engine.Compilation", compilation);
        const nautilus::engine::NautilusEngine engine(options);
        compileFillBufferFunction(FUNCTION_CREATE_MONOTONIC_VALUES_FOR_BUFFER, backend, options, schema, memoryProviderInputBuffer);
    }

    /// Lambda function that creates a vector from 0 to n-1 and then shuffles it
    const auto createShuffledVector = [seed](const uint64_t n)
    {
        std::vector<uint64_t> vec(n);
        std::iota(vec.begin(), vec.end(), 0); /// NOLINT(modernize-use-ranges) as libcxx does not have ranges::iota
        std::ranges::shuffle(vec, std::default_random_engine(seed));
        return vec;
    };

    /// Now, we have to call the compiled function to fill the buffer with the values.
    /// We are using the buffer manager to get a buffer of a fixed size.
    /// Therefore, we have to iterate in a loop and fill multiple buffers until we have created the required numberofTuples.
    std::vector<Memory::TupleBuffer> buffers;
    const auto capacity = memoryProviderInputBuffer->getMemoryLayout()->getCapacity();
    INVARIANT(capacity > 0, "Capacity should be larger than 0");

    for (uint64_t i = 0; i < numberOfTuples; i = i + capacity)
    {
        /// Depending on the capacity and the number of tuples, we might have to reduce the number of tuples to fill for this iteration
        const auto tuplesToFill = std::min(capacity, numberOfTuples - i);
        auto buffer = bufferManager.getBufferBlocking();
        auto outputBufferIndex = createShuffledVector(tuplesToFill);
        const auto sizeVarSizedData = (rand() % (maxSizeVarSizedData + 1 - minSizeVarSizedData)) + minSizeVarSizedData;
        callCompiledFunction<void, Memory::TupleBuffer*, Memory::AbstractBufferProvider*, uint64_t, uint64_t, uint64_t, uint64_t*>(
            {FUNCTION_CREATE_MONOTONIC_VALUES_FOR_BUFFER, backend},
            std::addressof(buffer),
            std::addressof(bufferManager),
            tuplesToFill,
            i + 1,
            sizeVarSizedData,
            outputBufferIndex.data());
        buffers.push_back(buffer);
    }

    /// Returning the buffers with the values but before that shuffle the buffers to have a random order
    /// To ensure that this randomness is deterministic, we write the seed to our log
    std::ranges::shuffle(buffers, std::default_random_engine(seed));
    return buffers;
}

Schema NautilusTestUtils::createSchemaFromBasicTypes(const std::vector<DataType::Type>& basicTypes)
{
    constexpr auto typeIdxOffset = 0;
    return createSchemaFromBasicTypes(basicTypes, typeIdxOffset);
}

Schema NautilusTestUtils::createSchemaFromBasicTypes(const std::vector<DataType::Type>& basicTypes, const uint64_t typeIdxOffset)
{
    /// Creating a schema for the memory provider
    auto schema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
    for (const auto& [typeIdx, type] : views::enumerate(basicTypes))
    {
        const auto nameOfField = Record::RecordFieldIdentifier("field" + std::to_string(typeIdx + typeIdxOffset));
        schema.addField(nameOfField, type);
    }
    return schema;
}

void NautilusTestUtils::compileFillBufferFunction(
    std::string_view functionName,
    Configurations::ExecutionMode backend,
    nautilus::engine::Options& options,
    const Schema& schema,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProviderInputBuffer)
{
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    const std::function tmp = [=](nautilus::val<Memory::TupleBuffer*> buffer,
                                  nautilus::val<Memory::AbstractBufferProvider*> bufferProvider,
                                  nautilus::val<uint64_t> numberOfTuplesToFill,
                                  nautilus::val<uint64_t> startForValues,
                                  nautilus::val<uint64_t> sizeVarSizedDataVal,
                                  nautilus::val<uint64_t*> outputIndex)
    {
        RecordBuffer recordBuffer(buffer);
        nautilus::val<uint64_t> value = std::move(startForValues);
        for (nautilus::val<uint64_t> i = 0; i < numberOfTuplesToFill; i = i + 1)
        {
            Record record;
            for (nautilus::static_val<size_t> fieldIndex = 0; fieldIndex < schema.getNumberOfFields(); ++fieldIndex)
            {
                const auto field = schema.getFieldAt(fieldIndex);
                const auto physicalType = field.dataType;
                const auto fieldName = field.name;
                if (not field.dataType.isType(DataType::Type::VARSIZED))
                {
                    const auto varValue = Nautilus::Util::createNautilusConstValue(value, physicalType.type);
                    record.write(fieldName, VarVal(value));
                    value += 1;
                }
                else if (field.dataType.isType(DataType::Type::VARSIZED))
                {
                    const auto pointerToVarSizedData = nautilus::invoke(
                        +[](const Memory::TupleBuffer* inputBuffer, Memory::AbstractBufferProvider* bufferProviderVal, const uint64_t size)
                        {
                            /// Creating a random string of the given size
                            auto randchar = []() -> char
                            {
                                const std::string charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
                                const size_t maxIndex = (charset.size() - 1);
                                return charset[rand() % maxIndex];
                            };
                            std::string randomString(size, 0);
                            std::generate_n(randomString.begin(), size, randchar);

                            /// Adding the random string to the buffer and returning the pointer to the data
                            const auto varSizedPosition
                                = Memory::MemoryLayouts::writeVarSizedData(*inputBuffer, randomString, *bufferProviderVal).value();
                            const auto varSizedDataBuffer = inputBuffer->loadChildBuffer(varSizedPosition);
                            return varSizedDataBuffer.getBuffer();
                        },
                        recordBuffer.getReference(),
                        bufferProvider,
                        sizeVarSizedDataVal);

                    record.write(fieldName, VarVal(VariableSizedData(pointerToVarSizedData, VariableSizedData::Owned(true))));
                }
                else
                {
                    throw UnknownDataType("Unsupported data type {}", physicalType);
                }
            }
            auto currentIndex = nautilus::val<uint64_t>(outputIndex[i]);
            memoryProviderInputBuffer->writeRecord(currentIndex, recordBuffer, record, bufferProvider);
            recordBuffer.setNumRecords(i + 1);
        }
    };
    /// NOLINTEND(performance-unnecessary-value-param)

    const bool compilation = (backend == Configurations::ExecutionMode::COMPILER);
    options.setOption("engine.Compilation", compilation);
    auto engine = nautilus::engine::NautilusEngine(options);
    auto compiledFunction = engine.registerFunction(tmp);

    compiledFunctions[{functionName, backend}] = std::make_unique<
        FunctionWrapper<void, Memory::TupleBuffer*, Memory::AbstractBufferProvider*, uint64_t, uint64_t, uint64_t, uint64_t*>>(
        std::move(compiledFunction));
}

std::string NautilusTestUtils::compareRecords(
    const Record& recordLeft, const Record& recordRight, const std::vector<Record::RecordFieldIdentifier>& projection)
{
    bool printErrorMessage = false;
    nautilus::val<std::stringstream> ss;
    for (const auto& fieldIdentifier : nautilus::static_iterable(projection))
    {
        const auto& valueLeft = recordLeft.read(fieldIdentifier);
        const auto& valueRight = recordRight.read(fieldIdentifier);
        ss << fieldIdentifier.c_str() << " (" << valueLeft;
        if (valueLeft != valueRight)
        {
            printErrorMessage = true;
            ss << " != ";
        }
        else
        {
            ss << " == ";
        }
        ss << valueRight << ") ";
    }
    return printErrorMessage ? ss.str().c_str().value : "";
}

}
