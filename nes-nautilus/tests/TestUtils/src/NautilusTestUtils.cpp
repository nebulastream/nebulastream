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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Configurations/Enums/NautilusBackend.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Common.hpp>
#include <Util/Ranges.hpp>
#include <std/sstream.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/VariableSizedDataPhysicalType.hpp>

#include <nautilus/Engine.hpp>
#include <nautilus/function.hpp>
#include <nautilus/options.hpp>
#include <nautilus/static.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <NautilusTestUtils.hpp>

namespace NES::Nautilus
{

std::unique_ptr<Interface::HashFunction> NautilusTestUtils::getMurMurHashFunction()
{
    return std::make_unique<Interface::MurMur3HashFunction>();
}

std::vector<Memory::TupleBuffer> NautilusTestUtils::createMonotonicIncreasingValues(
    const SchemaPtr& schema,
    const uint64_t numberOfTuples,
    const QueryCompilation::NautilusBackend& backend,
    Memory::BufferManager& bufferManager,
    const uint64_t minSizeVarSizedData)
{
    constexpr auto maxSizeVarSizedData = 20;
    return createMonotonicIncreasingValues(schema, numberOfTuples, backend, bufferManager, minSizeVarSizedData, maxSizeVarSizedData);
}

std::vector<Memory::TupleBuffer> NautilusTestUtils::createMonotonicIncreasingValues(
    const SchemaPtr& schema,
    const uint64_t numberOfTuples,
    const QueryCompilation::NautilusBackend& backend,
    Memory::BufferManager& bufferManager)
{
    constexpr auto minSizeVarSizedData = 10;
    return createMonotonicIncreasingValues(schema, numberOfTuples, backend, bufferManager, minSizeVarSizedData);
}

std::vector<Memory::TupleBuffer> NautilusTestUtils::createMonotonicIncreasingValues(
    const SchemaPtr& schema,
    const uint64_t numberOfTuples,
    const QueryCompilation::NautilusBackend& backend,
    Memory::BufferManager& bufferManager,
    const uint64_t minSizeVarSizedData,
    const uint64_t maxSizeVarSizedData)
{
    /// Creating here the memory provider for the tuple buffers that store the data
    const auto memoryProviderInputBuffer
        = Interface::MemoryProvider::TupleBufferMemoryProvider::create(bufferManager.getBufferSize(), schema);


    /// If we have large number of tuples, we should compile the query otherwise, it is faster to run it in the interpreter.
    /// We set the threshold to be 10k tuples.
    if (not compiledFunctions.contains({CREATE_MONOTONIC_VALUES_FOR_BUFFER, backend}))
    {
        constexpr auto thresholdForCompile = 1 * 1000;
        nautilus::engine::Options options;
        const auto compilation = numberOfTuples > thresholdForCompile;
        options.setOption("engine.Compilation", compilation);
        const nautilus::engine::NautilusEngine engine(options);

        compileFillBufferFunction(CREATE_MONOTONIC_VALUES_FOR_BUFFER, backend, options, schema, memoryProviderInputBuffer);
    }


    /// Now, we have to call the compiled function to fill the buffer with the values.
    /// We are using the buffer manager to get a buffer of a fixed size.
    /// Therefore, we have to iterate in a loop and fill multiple buffers until we have created the required numberofTuples.
    std::vector<Memory::TupleBuffer> buffers;
    const auto capacity = memoryProviderInputBuffer->getMemoryLayoutPtr()->getCapacity();
    INVARIANT(capacity > 0, "Capacity should be larger than 0");

    for (uint64_t i = 0; i < numberOfTuples; i = i + capacity)
    {
        /// Depending on the capacity and the number of tuples, we might have to reduce the number of tuples to fill for this iteration
        const auto tuplesToFill = std::min(capacity, numberOfTuples - i);
        auto buffer = bufferManager.getBufferBlocking();
        const auto sizeVarSizedData = rand() % (maxSizeVarSizedData + 1 - minSizeVarSizedData) + minSizeVarSizedData;
        callCompiledFunction<void, Memory::TupleBuffer*, Memory::AbstractBufferProvider*, uint64_t, uint64_t, uint64_t>(
            {CREATE_MONOTONIC_VALUES_FOR_BUFFER, backend},
            std::addressof(buffer),
            std::addressof(bufferManager),
            tuplesToFill,
            i + 1,
            sizeVarSizedData);
        buffers.push_back(buffer);
    }

    return buffers;
}

std::shared_ptr<Schema> NautilusTestUtils::createSchemaFromBasicTypes(const std::vector<BasicType>& basicTypes)
{
    constexpr auto typeIdxOffset = 0;
    return createSchemaFromBasicTypes(basicTypes, typeIdxOffset);
}

std::shared_ptr<Schema>
NautilusTestUtils::createSchemaFromBasicTypes(const std::vector<BasicType>& basicTypes, const uint64_t typeIdxOffset)
{
    /// Creating a schema for the memory provider
    const auto schema = Schema::create();
    for (const auto& [typeIdx, type] : views::enumerate(basicTypes))
    {
        schema->addField(Record::RecordFieldIdentifier("field" + std::to_string(typeIdx + typeIdxOffset)), type);
    }
    return schema;
}

void NautilusTestUtils::compileFillBufferFunction(
    std::string_view functionName,
    QueryCompilation::NautilusBackend backend,
    nautilus::engine::Options& options,
    const SchemaPtr& schema,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProviderInputBuffer)
{
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    const std::function tmp = [=](nautilus::val<Memory::TupleBuffer*> buffer,
                                  nautilus::val<Memory::AbstractBufferProvider*> bufferProvider,
                                  nautilus::val<uint64_t> numberOfTuplesToFill,
                                  nautilus::val<uint64_t> startForValues,
                                  nautilus::val<uint64_t> sizeVarSizedDataVal)
    {
        RecordBuffer recordBuffer(buffer);
        nautilus::val<uint64_t> value = std::move(startForValues);
        for (nautilus::val<uint64_t> i = 0; i < numberOfTuplesToFill; i = i + 1)
        {
            Record record;
            for (nautilus::static_val<size_t> fieldIndex = 0; fieldIndex < schema->getFieldCount(); ++fieldIndex)
            {
                DefaultPhysicalTypeFactory const physicalTypeFactory;
                const auto field = schema->getFieldByIndex(fieldIndex);
                const auto type = physicalTypeFactory.getPhysicalType(field->getDataType());
                const auto fieldName = field->getName();
                if (NES::Util::instanceOf<BasicPhysicalType>(type))
                {
                    const auto basicType = NES::Util::as<BasicPhysicalType>(type);
                    switch (basicType->nativeType)
                    {
                        case BasicPhysicalType::NativeType::INT_64:
                            record.write(fieldName, VarVal(static_cast<nautilus::val<int64_t>>(value)));
                            break;
                        case BasicPhysicalType::NativeType::INT_32:
                            record.write(fieldName, VarVal(static_cast<nautilus::val<int32_t>>(value)));
                            break;
                        case BasicPhysicalType::NativeType::INT_16:
                            record.write(fieldName, VarVal(static_cast<nautilus::val<int16_t>>(value)));
                            break;
                        case BasicPhysicalType::NativeType::INT_8:
                            record.write(fieldName, VarVal(static_cast<nautilus::val<int8_t>>(value)));
                            break;
                        case BasicPhysicalType::NativeType::UINT_64:
                            record.write(fieldName, VarVal(static_cast<nautilus::val<uint64_t>>(value)));
                            break;
                        case BasicPhysicalType::NativeType::UINT_32:
                            record.write(fieldName, VarVal(static_cast<nautilus::val<uint32_t>>(value)));
                            break;
                        case BasicPhysicalType::NativeType::UINT_16:
                            record.write(fieldName, VarVal(static_cast<nautilus::val<uint16_t>>(value)));
                            break;
                        case BasicPhysicalType::NativeType::UINT_8:
                            record.write(fieldName, VarVal(static_cast<nautilus::val<uint8_t>>(value)));
                            break;
                        case BasicPhysicalType::NativeType::FLOAT:
                            record.write(fieldName, VarVal(static_cast<nautilus::val<float>>(value)));
                            break;
                        case BasicPhysicalType::NativeType::DOUBLE:
                            record.write(fieldName, VarVal(static_cast<nautilus::val<double>>(value)));
                            break;
                        default:
                            throw UnsupportedOperation("Unsupported data type {}", basicType->toString());
                    }
                    value += 1;
                }
                else if (NES::Util::instanceOf<VariableSizedDataPhysicalType>(type))
                {
                    const auto pointerToVarSizedData = nautilus::invoke(
                        +[](const Memory::TupleBuffer* inputBuffer, Memory::AbstractBufferProvider* bufferProviderVal, const uint64_t size)
                        {
                            /// Creating a random string of the given size
                            auto randchar = []() -> char
                            {
                                constexpr char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
                                constexpr size_t maxIndex = (sizeof(charset) - 1);
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

                    record.write(fieldName, VarVal(VariableSizedData(pointerToVarSizedData)));
                }
                else
                {
                    throw UnsupportedOperation("Unsupported data type {}", type->toString());
                }
            }
            memoryProviderInputBuffer->writeRecord(i, recordBuffer, record);
            recordBuffer.setNumRecords(i + 1);
        }
    };
    const bool compilation = (backend == QueryCompilation::NautilusBackend::COMPILER);
    options.setOption("engine.Compilation", compilation);
    auto engine = nautilus::engine::NautilusEngine(options);
    auto compiledFunction = engine.registerFunction(tmp);

    compiledFunctions[{functionName, backend}]
        = std::make_unique<FunctionWrapper<void, Memory::TupleBuffer*, Memory::AbstractBufferProvider*, uint64_t, uint64_t, uint64_t>>(
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
