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
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::Execution::Util {

    uint64_t murmurHash(uint64_t key) {
        uint64_t hash = key;

        hash ^= hash >> 33;
        hash *= UINT64_C(0xff51afd7ed558ccd);
        hash ^= hash >> 33;
        hash *= UINT64_C(0xc4ceb9fe1a85ec53);
        hash ^= hash >> 33;

        return hash;
    }

    Runtime::TupleBuffer getBufferFromPointer(uint8_t* recordPtr, SchemaPtr schema, BufferManagerPtr bufferManager) {
        auto buffer = bufferManager->getBufferBlocking();
        uint8_t* bufferPtr = buffer.getBuffer();

        auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
        for (auto& field : schema->fields) {
            auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
            std::memcpy(bufferPtr, recordPtr, fieldType->size());
            bufferPtr += fieldType->size();
            recordPtr += fieldType->size();
        }
        buffer.setNumberOfTuples(1);
        return buffer;
    }

    void writeNautilusRecord(size_t recordIndex, int8_t* baseBufferPtr, Nautilus::Record nautilusRecord, SchemaPtr schema, BufferManagerPtr bufferManager) {
        Nautilus::Value<Nautilus::UInt64> nautilusRecordIndex(recordIndex);
        Nautilus::Value<Nautilus::MemRef> nautilusBufferPtr(baseBufferPtr);
        if (schema->getLayoutType() == Schema::ROW_LAYOUT) {
            auto rowMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
            auto memoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(rowMemoryLayout);

            memoryProviderPtr->write(nautilusRecordIndex, nautilusBufferPtr, nautilusRecord);

        } else if (schema->getLayoutType() == Schema::COLUMNAR_LAYOUT) {
            auto columnMemoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bufferManager->getBufferSize());
            auto memoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(columnMemoryLayout);

            memoryProviderPtr->write(nautilusRecordIndex, nautilusBufferPtr, nautilusRecord);

        } else {
            NES_THROW_RUNTIME_ERROR("Schema Layout not supported!");
        }
    }

    std::vector<Runtime::TupleBuffer> mergeBuffersSameWindow(std::vector<Runtime::TupleBuffer>& buffers, SchemaPtr schema,
                                                             const std::string& timeStampFieldName,
                                                             BufferManagerPtr bufferManager,
                                                             uint64_t windowSize) {
        if (buffers.size() == 0) {
            return std::vector<Runtime::TupleBuffer>();
        }

        if (schema->getLayoutType() == Schema::COLUMNAR_LAYOUT) {
            NES_FATAL_ERROR("Column layout is not support for this function currently!");
        }

        NES_INFO("Merging buffers together!");

        std::vector<Runtime::TupleBuffer> retVector;

        auto curBuffer = bufferManager->getBufferBlocking();
        auto numberOfTuplesInBuffer = 0UL;
        auto lastTimeStamp = windowSize - 1;
        for (auto buf : buffers) {
            auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
            auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buf);

            for (auto curTuple = 0UL; curTuple < dynamicTupleBuffer.getNumberOfTuples(); ++curTuple) {
                if (dynamicTupleBuffer[curTuple][timeStampFieldName].read<uint64_t>() > lastTimeStamp ||
                    numberOfTuplesInBuffer >= memoryLayout->getCapacity()) {

                    if (dynamicTupleBuffer[curTuple][timeStampFieldName].read<uint64_t>() > lastTimeStamp) {
                        lastTimeStamp += windowSize;
                    }


                    curBuffer.setNumberOfTuples(numberOfTuplesInBuffer);
                    retVector.emplace_back(std::move(curBuffer));

                    curBuffer = bufferManager->getBufferBlocking();
                    numberOfTuplesInBuffer = 0;
                }

                memcpy(curBuffer.getBuffer() + schema->getSchemaSizeInBytes() * numberOfTuplesInBuffer,
                       buf.getBuffer() + schema->getSchemaSizeInBytes() * curTuple, schema->getSchemaSizeInBytes());
                numberOfTuplesInBuffer += 1;
                curBuffer.setNumberOfTuples(numberOfTuplesInBuffer);
            }
        }

        if (numberOfTuplesInBuffer > 0) {
            curBuffer.setNumberOfTuples(numberOfTuplesInBuffer);
            retVector.emplace_back(std::move(curBuffer));
        }

        return retVector;
    }


    std::vector<Runtime::TupleBuffer> sortBuffersInTupleBuffer(std::vector<Runtime::TupleBuffer>& buffersToSort, SchemaPtr schema,
                                                               const std::string& sortFieldName, BufferManagerPtr bufferManager) {
        if (buffersToSort.size() == 0) {
            return std::vector<Runtime::TupleBuffer>();
        }
        if (schema->getLayoutType() == Schema::COLUMNAR_LAYOUT) {
            NES_FATAL_ERROR("Column layout is not support for this function currently!");
        }


        std::vector<Runtime::TupleBuffer> retVector;
        for (auto bufRead : buffersToSort) {
            std::vector<size_t> indexAlreadyInNewBuffer;
            auto memLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
            auto dynamicTupleBuf = Runtime::MemoryLayouts::DynamicTupleBuffer(memLayout, bufRead);

            auto bufRet = bufferManager->getBufferBlocking();

            for (auto outer = 0UL; outer < bufRead.getNumberOfTuples(); ++outer) {
                auto smallestIndex = bufRead.getNumberOfTuples() + 1;
                for (auto inner = 0UL; inner < bufRead.getNumberOfTuples(); ++inner) {
                    if (std::find(indexAlreadyInNewBuffer.begin(), indexAlreadyInNewBuffer.end(), inner)
                        != indexAlreadyInNewBuffer.end()) {
                        // If we have already moved this index into the
                        continue;
                    }

                    auto sortValueCur = dynamicTupleBuf[inner][sortFieldName].read<uint64_t>();
                    auto sortValueOld = dynamicTupleBuf[smallestIndex][sortFieldName].read<uint64_t>();

                    if (smallestIndex == bufRead.getNumberOfTuples() + 1) {
                        smallestIndex = inner;
                        continue;
                    } else if (sortValueCur < sortValueOld) {
                        smallestIndex = inner;
                    }
                }
                indexAlreadyInNewBuffer.emplace_back(smallestIndex);
                auto posRet = bufRet.getNumberOfTuples();
                memcpy(bufRet.getBuffer() + posRet * schema->getSchemaSizeInBytes(),
                       bufRead.getBuffer() + smallestIndex * schema->getSchemaSizeInBytes(),
                       schema->getSchemaSizeInBytes());
                bufRet.setNumberOfTuples(posRet + 1);
            }
            retVector.emplace_back(bufRet);
            bufRet = bufferManager->getBufferBlocking();
        }

        return retVector;
    }

    Runtime::TupleBuffer getBufferFromRecord(Nautilus::Record nautilusRecord, SchemaPtr schema, BufferManagerPtr bufferManager) {
        auto buffer = bufferManager->getBufferBlocking();
        int8_t* bufferPtr = (int8_t *)buffer.getBuffer();

        writeNautilusRecord(0, bufferPtr, nautilusRecord, schema, bufferManager);

        buffer.setNumberOfTuples(1);
        return buffer;
    }

    std::string printTupleBufferAsCSV(Runtime::TupleBuffer tbuffer, const SchemaPtr& schema) {
        std::stringstream ss;
        auto numberOfTuples = tbuffer.getNumberOfTuples();
        auto* buffer = tbuffer.getBuffer<char>();
        auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
        for (uint64_t i = 0; i < numberOfTuples; i++) {
            uint64_t offset = 0;
            for (uint64_t j = 0; j < schema->getSize(); j++) {
                auto field = schema->get(j);
                auto ptr = field->getDataType();
                auto physicalType = physicalDataTypeFactory.getPhysicalType(ptr);
                auto fieldSize = physicalType->size();
                auto str = physicalType->convertRawToString(buffer + offset + i * schema->getSchemaSizeInBytes());
                ss << str.c_str();
                if (j < schema->getSize() - 1) {
                    ss << ",";
                }
                offset += fieldSize;
            }
            ss << std::endl;
        }
        return ss.str();
    }

    SchemaPtr createJoinSchema(SchemaPtr leftSchema, SchemaPtr rightSchema, const std::string& keyFieldName) {
        NES_ASSERT(leftSchema->getLayoutType() == rightSchema->getLayoutType(), "Left and right schema do not have the same layout type");
        NES_ASSERT(leftSchema->contains(keyFieldName) || rightSchema->contains(keyFieldName),
                   "KeyFieldName = " << keyFieldName << " is not in either left or right schema");

        auto retSchema = Schema::create(leftSchema->getLayoutType());
        if (leftSchema->contains(keyFieldName)) {
            retSchema->addField(leftSchema->get(keyFieldName));
        } else {
            retSchema->addField(rightSchema->get(keyFieldName));
        }

        retSchema->copyFields(leftSchema);
        retSchema->copyFields(rightSchema);

        return retSchema;
    }
}