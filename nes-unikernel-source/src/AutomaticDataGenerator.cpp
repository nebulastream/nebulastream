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

#include "AutomaticDataGenerator.h"
#include "Common/PhysicalTypes/BasicPhysicalType.hpp"
#include "Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp"
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>

std::vector<NES::Runtime::TupleBuffer> AutomaticDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::vector<NES::Runtime::TupleBuffer> buffers;
    buffers.reserve(numberOfBuffers);
    auto memoryLayout = getMemoryLayout(bufferSize);

    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto tuples_in_buffer = 0;
        auto buffer = allocateBuffer();
        auto dynamicBuffer = NES::Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        auto numberOfTuple = numberOfTupleToCreate.value_or(dynamicBuffer.getCapacity());
        numberOfTuple = std::min(numberOfTuple, dynamicBuffer.getCapacity());

        for (uint64_t currentRecord = 0; currentRecord < numberOfTuple; currentRecord++) {
            auto currentTuple = dynamicBuffer[currentRecord];
            for (auto& field : schema->fields) {
                auto dynamicField = currentTuple[field->getName()];
                generators.at(field->getName())->generate(dynamicField, buffer);
            }
            tuples_in_buffer++;
        }
        dynamicBuffer.setNumberOfTuples(tuples_in_buffer);
        buffers.push_back(buffer);
    }

    return buffers;
}
NES::SchemaPtr AutomaticDataGenerator::getSchema() { return schema; }
std::string AutomaticDataGenerator::getName() { return "Automatic Data Generator"; }
std::string AutomaticDataGenerator::toString() { return "Automatic Data Generator"; }
std::unique_ptr<AutomaticDataGenerator> AutomaticDataGenerator::create(NES::SchemaPtr schema) {
    std::unordered_map<std::string, std::unique_ptr<FieldDataGenerator>> generators;
    for (std::size_t i = 0; i < schema->getSize(); ++i) {
        auto attr = schema->get(i);
        auto dataType = attr->getDataType();
        auto physicalType = NES::DefaultPhysicalTypeFactory().getPhysicalType(dataType);

        if (physicalType->isBasicType()) {
            auto basicPhysicalType = std::dynamic_pointer_cast<NES::BasicPhysicalType>(physicalType);
            switch (basicPhysicalType->nativeType) {
                case NES::BasicPhysicalType::NativeType::INT_8: {
                    generators[attr->getName()] = std::make_unique<ConstantSequence<int8_t>>(42);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_16: {
                    generators[attr->getName()] = std::make_unique<ConstantSequence<int16_t>>(420);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_32: {
                    generators[attr->getName()] = std::make_unique<ConstantSequence<int32_t>>(4200);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_64: {
                    generators[attr->getName()] = std::make_unique<ConstantSequence<int64_t>>(42000);
                    break;
                }

                case NES::BasicPhysicalType::NativeType::UINT_8: {
                    generators[attr->getName()] = std::make_unique<IncreasingSequence<uint8_t>>();
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_16: {
                    generators[attr->getName()] = std::make_unique<IncreasingSequence<uint16_t>>();
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_32: {
                    generators[attr->getName()] = std::make_unique<IncreasingSequence<uint32_t>>();
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_64: {
                    generators[attr->getName()] = std::make_unique<IncreasingSequence<uint64_t>>();
                    break;
                }
                case NES::BasicPhysicalType::NativeType::FLOAT: {
                    generators[attr->getName()] = std::make_unique<RandomValues<float>>(0.0f, 100.0f);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::DOUBLE: {
                    generators[attr->getName()] = std::make_unique<RandomValues<double>>(0.0, 100.0);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::CHAR:
                case NES::BasicPhysicalType::NativeType::TEXT: {
                    generators[attr->getName()] = std::make_unique<RandomText>(1, 100);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UNDEFINED:
                case NES::BasicPhysicalType::NativeType::BOOLEAN: NES_THROW_RUNTIME_ERROR("Not Implemented");
            }
        }
    }
    return std::make_unique<AutomaticDataGenerator>(std::move(schema), std::move(generators));
}
NES::Configurations::SchemaTypePtr AutomaticDataGenerator::getSchemaType() { NES_NOT_IMPLEMENTED(); }
