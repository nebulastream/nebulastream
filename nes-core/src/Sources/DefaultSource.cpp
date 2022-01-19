/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/MemoryLayoutTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <stdint.h>
#include <utility>

namespace NES {

DefaultSource::DefaultSource(SchemaPtr schema,
                             Runtime::BufferManagerPtr bufferManager,
                             Runtime::QueryManagerPtr queryManager,
                             const uint64_t numbersOfBufferToProduce,
                             uint64_t frequency,
                             OperatorId operatorId,
                             size_t numSourceLocalBuffers,
                             std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      numbersOfBufferToProduce,
                      operatorId,
                      numSourceLocalBuffers,
                      GatheringMode::FREQUENCY_MODE,
                      std::move(successors)) {
    NES_DEBUG("DefaultSource:" << this << " creating");
    this->gatheringInterval = std::chrono::milliseconds(frequency);
}

std::optional<Runtime::TupleBuffer> DefaultSource::receiveData() {
    // 10 tuples of size one
    NES_DEBUG("Source:" << this << " requesting buffer");

    NES_DEBUG("Source:" << this << " got buffer");
    uint64_t tupleCnt = 10;

    auto value = 1;
    auto fields = schema->fields;

    auto buffer = allocateBuffer();
    if (tupleCnt >= buffer.getCapacity()) {
        NES_THROW_RUNTIME_ERROR("DefaultSource: tupleCnt >= capacity!!!");
    }

    for (uint64_t fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
        for (uint64_t recordIndex = 0; recordIndex < tupleCnt; recordIndex++) {
            auto dataType = fields[fieldIndex]->getDataType();
            auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
            if (physicalType->isBasicType()) {
                auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                if (basicPhysicalType->nativeType == BasicPhysicalType::CHAR) {
                    buffer[recordIndex][fieldIndex].write<char>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_8) {
                    buffer[recordIndex][fieldIndex].write<uint8_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_16) {
                    buffer[recordIndex][fieldIndex].write<uint16_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_32) {
                    buffer[recordIndex][fieldIndex].write<uint32_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_64) {
                    buffer[recordIndex][fieldIndex].write<uint64_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_8) {
                    buffer[recordIndex][fieldIndex].write<int8_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_16) {
                    buffer[recordIndex][fieldIndex].write<int16_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_32) {
                    buffer[recordIndex][fieldIndex].write<int32_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_64) {
                    buffer[recordIndex][fieldIndex].write<int64_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::FLOAT) {
                    buffer[recordIndex][fieldIndex].write<float>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::DOUBLE) {
                    buffer[recordIndex][fieldIndex].write<double>(value);
                } else {
                    NES_DEBUG("This data source only generates data for numeric fields");
                }
            } else {
                NES_DEBUG("This data source only generates data for numeric fields");
            }
        }
    }
    buffer.setNumberOfTuples(tupleCnt);
    // TODO move this to trace
    NES_DEBUG("Source: id=" << operatorId << " Generated buffer with " << buffer.getNumberOfTuples() << "/"
                            << schema->getSchemaSizeInBytes() << "\n"
                            << buffer);
    return buffer.getBuffer();
}

SourceType DefaultSource::getType() const { return DEFAULT_SOURCE; }

std::vector<Schema::MemoryLayoutType> DefaultSource::getSupportedLayouts() {
    return {Schema::MemoryLayoutType::ROW_LAYOUT, Schema::MemoryLayoutType::COLUMNAR_LAYOUT};
}

}// namespace NES
