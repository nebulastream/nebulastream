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
#include <Runtime/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayout.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutField.hpp>
#include <Runtime/MemoryLayout/DynamicColumnLayout.hpp>
#include <Runtime/MemoryLayout/DynamicColumnLayoutField.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <utility>
#include <stdint.h>

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
                      DataSource::GatheringMode::FREQUENCY_MODE,
                      std::move(successors)) {
    NES_DEBUG("DefaultSource:" << this << " creating");
    this->gatheringInterval = std::chrono::milliseconds(frequency);
}

std::optional<Runtime::TupleBuffer> DefaultSource::receiveData() {
    // 10 tuples of size one
    NES_DEBUG("Source:" << this << " requesting buffer");

    auto buf = this->bufferManager->getBufferBlocking();
    NES_DEBUG("Source:" << this << " got buffer");
    uint64_t tupleCnt = 10;

    auto value = 1;
    auto fields = schema->fields;

    if (schema->getLayoutType() == Schema::COL_LAYOUT) {
        auto layout = Runtime::DynamicMemoryLayout::DynamicColumnLayout::create(std::make_shared<Schema>(schema), true);
        Runtime::DynamicMemoryLayout::DynamicColumnLayoutBufferPtr bindedColLayout = layout->bind(buf);

        if (tupleCnt >= bindedColLayout->getCapacity()) {
            NES_THROW_RUNTIME_ERROR("DefaultSource: tupleCnt >= capacity!!!");
        }

        for (uint64_t fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            for (uint64_t recordIndex = 0; recordIndex < tupleCnt; recordIndex++) {
                auto dataType = fields[fieldIndex]->getDataType();
                auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
                if (physicalType->isBasicType()) {
                    auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                    if (basicPhysicalType->nativeType == BasicPhysicalType::CHAR) {
                        ColLayoutField(char,fieldIndex,bindedColLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_8) {
                        ColLayoutField(uint8_t, fieldIndex, bindedColLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_16) {
                        ColLayoutField(uint16_t, fieldIndex, bindedColLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_32) {
                        ColLayoutField(uint32_t, fieldIndex, bindedColLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_64) {
                        ColLayoutField(uint64_t, fieldIndex, bindedColLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_8) {
                        ColLayoutField(int8_t, fieldIndex, bindedColLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_16) {
                        ColLayoutField(int16_t, fieldIndex, bindedColLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_32) {
                        ColLayoutField(int32_t, fieldIndex, bindedColLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_64) {
                        ColLayoutField(int64_t, fieldIndex, bindedColLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::FLOAT) {
                        ColLayoutField(float, fieldIndex, bindedColLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::DOUBLE) {
                        ColLayoutField(double, fieldIndex, bindedColLayout)[recordIndex] = value;
                    } else {
                        NES_DEBUG("This data source only generates data for numeric fields");
                    }
                } else {
                    NES_DEBUG("This data source only generates data for numeric fields");
                }
            }
        }



    } else if (schema->getLayoutType() == Schema::ROW_LAYOUT) {
        auto layout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(std::make_shared<Schema>(schema), true);
        Runtime::DynamicMemoryLayout::DynamicRowLayoutBufferPtr bindedRowLayout = layout->bind(buf);

        if (tupleCnt >= bindedRowLayout->getCapacity()) {
            NES_THROW_RUNTIME_ERROR("DefaultSource: tupleCnt >= capacity!!!");
        }

        for (uint64_t recordIndex = 0; recordIndex < tupleCnt; recordIndex++) {
            for (uint64_t fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                auto dataType = fields[fieldIndex]->getDataType();
                auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
                if (physicalType->isBasicType()) {
                    auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                    if (basicPhysicalType->nativeType == BasicPhysicalType::CHAR) {
                        RowLayoutField(char,fieldIndex,bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_8) {
                        RowLayoutField(uint8_t, fieldIndex, bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_16) {
                        RowLayoutField(uint16_t, fieldIndex, bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_32) {
                        RowLayoutField(uint32_t, fieldIndex, bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::UINT_64) {
                        RowLayoutField(uint64_t, fieldIndex, bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_8) {
                        RowLayoutField(int8_t, fieldIndex, bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_16) {
                        RowLayoutField(int16_t, fieldIndex, bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_32) {
                        RowLayoutField(int32_t, fieldIndex, bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::INT_64) {
                        RowLayoutField(int64_t, fieldIndex, bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::FLOAT) {
                        RowLayoutField(float, fieldIndex, bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->nativeType == BasicPhysicalType::DOUBLE) {
                        RowLayoutField(double, fieldIndex, bindedRowLayout)[recordIndex] = value;
                    } else {
                        NES_DEBUG("This data source only generates data for numeric fields");
                    }
                } else {
                    NES_DEBUG("This data source only generates data for numeric fields");
                }
            }
        }

    } else {
        NES_THROW_RUNTIME_ERROR("DefaultSource: layoutType of schema was neither col nor row!!!");
    }

    buf.setNumberOfTuples(tupleCnt);
    // TODO move this to trace
    NES_DEBUG("Source: id=" << operatorId << " Generated buffer with " << buf.getNumberOfTuples() << "/"
                            << schema->getSchemaSizeInBytes() << "\n"
                            << Util::prettyPrintTupleBuffer(buf, schema));
    return buf;
}

SourceType DefaultSource::getType() const { return DEFAULT_SOURCE; }

std::vector<Schema::MemoryLayoutType> DefaultSource::getSupportedLayouts() {
    return {Schema::MemoryLayoutType::ROW_LAYOUT, Schema::MemoryLayoutType::COL_LAYOUT};
}

}// namespace NES
