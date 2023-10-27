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
#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <chrono>
#include <utility>

namespace NES {

DefaultSource::DefaultSource(SchemaPtr schema,
                             Runtime::BufferManagerPtr bufferManager,
                             Runtime::QueryManagerPtr queryManager,
                             const uint64_t numberOfBufferToProduce,
                             uint64_t gatheringInterval,
                             OperatorId operatorId,
                             OriginId originId,
                             size_t numSourceLocalBuffers,
                             std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
                             const std::string& physicalSourceName)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      numberOfBufferToProduce,
                      operatorId,
                      originId,
                      numSourceLocalBuffers,
                      GatheringMode::INTERVAL_MODE,
                      std::move(successors),
                      physicalSourceName) {
    this->gatheringInterval = std::chrono::milliseconds(gatheringInterval);
}

std::optional<Runtime::TupleBuffer> DefaultSource::receiveData() {
    // 10 tuples of size one
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
                if (basicPhysicalType->nativeType == BasicPhysicalType::NativeType::CHAR) {
                    buffer[recordIndex][fieldIndex].write<char>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::NativeType::UINT_8) {
                    buffer[recordIndex][fieldIndex].write<uint8_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::NativeType::UINT_16) {
                    buffer[recordIndex][fieldIndex].write<uint16_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::NativeType::UINT_32) {
                    buffer[recordIndex][fieldIndex].write<uint32_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::NativeType::UINT_64) {
                    buffer[recordIndex][fieldIndex].write<uint64_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::NativeType::INT_8) {
                    buffer[recordIndex][fieldIndex].write<int8_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::NativeType::INT_16) {
                    buffer[recordIndex][fieldIndex].write<int16_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::NativeType::INT_32) {
                    buffer[recordIndex][fieldIndex].write<int32_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::NativeType::INT_64) {
                    buffer[recordIndex][fieldIndex].write<int64_t>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::NativeType::FLOAT) {
                    buffer[recordIndex][fieldIndex].write<float>(value);
                } else if (basicPhysicalType->nativeType == BasicPhysicalType::NativeType::DOUBLE) {
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
    NES_TRACE("Source: id={} Generated buffer with {} / {}",
              operatorId,
              buffer.getNumberOfTuples(),
              schema->getSchemaSizeInBytes());
    return buffer.getBuffer();
}

SourceType DefaultSource::getType() const { return SourceType::DEFAULT_SOURCE; }

std::vector<Schema::MemoryLayoutType> DefaultSource::getSupportedLayouts() {
    return {Schema::MemoryLayoutType::ROW_LAYOUT, Schema::MemoryLayoutType::COLUMNAR_LAYOUT};
}

}// namespace NES
