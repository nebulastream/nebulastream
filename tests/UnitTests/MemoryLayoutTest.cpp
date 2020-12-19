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

#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <cstdlib>
#include <gtest/gtest.h>
#include <iostream>
#include <map>
#include <thread>
#include <vector>

//#define DEBUG_OUTPUT

namespace NES {
using namespace NodeEngine;
class MemoryLayoutTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;

    void SetUp() {
        NES::setupLogging("MemoryLayoutTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MemoryLayout test class.");
        bufferManager = std::make_shared<BufferManager>(4096, 10);
    }
    static void TearDownTestCase() { std::cout << "Tear down MemoryLayout test class." << std::endl; }
};

TEST_F(MemoryLayoutTest, rowLayoutTestInt) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);
    auto buf = bufferManager->getBufferBlocking();
    auto layout = createRowLayout(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/ 0)->write(buf, recordIndex);
        layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/ 1)->write(buf, recordIndex);
        layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/ 2)->write(buf, recordIndex);
    }

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        auto value = layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/ 0)->read(buf);
        ASSERT_EQ(value, recordIndex);

        value = layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/ 1)->read(buf);
        ASSERT_EQ(value, recordIndex);

        value = layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/ 2)->read(buf);
        ASSERT_EQ(value, recordIndex);
    }
}

TEST_F(MemoryLayoutTest, rowLayoutTestArray) {
    SchemaPtr schema = Schema::create()
                           ->addField("t1", DataTypeFactory::createArray(10, DataTypeFactory::createInt64()))
                           ->addField("t2", BasicType::UINT8)
                           ->addField("t3", BasicType::UINT8);

    TupleBuffer buf = bufferManager->getBufferBlocking();
    auto layout = createRowLayout(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        auto arrayField = layout->getArrayField(recordIndex, /*fieldIndex*/ 0);
        arrayField[0]->asValueField<int64_t>()->write(buf, 10);
        layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/ 1)->write(buf, recordIndex);
        layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/ 2)->write(buf, recordIndex);
    }

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        auto arrayField = layout->getArrayField(recordIndex, /*fieldIndex*/ 0);
        auto value = arrayField[0]->asValueField<int64_t>()->read(buf);
        ASSERT_EQ(value, 10);

        value = layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/ 1)->read(buf);
        ASSERT_EQ(value, recordIndex);

        value = layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/ 2)->read(buf);
        ASSERT_EQ(value, recordIndex);
    }
}

TEST_F(MemoryLayoutTest, rowLayoutTestArrayAsPointerField) {
    SchemaPtr schema = Schema::create()->addField("t1", DataTypeFactory::createArray(10, DataTypeFactory::createInt64()));

    TupleBuffer buf = bufferManager->getBufferBlocking();
    auto layout = createRowLayout(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        auto arrayField = layout->getArrayField(recordIndex, /*fieldIndex*/ 0);
        for (uint64_t arrayIndex = 0; arrayIndex < 10; arrayIndex++) {
            (arrayField)[arrayIndex]->asValueField<int64_t>()->write(buf, arrayIndex);
        }
    }

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        auto array = layout->getFieldPointer<int64_t>(buf, recordIndex, /*fieldIndex*/ 0);
        for (uint64_t arrayIndex = 0; arrayIndex < 10; arrayIndex++) {
            ASSERT_EQ(array[arrayIndex], arrayIndex);
        }
    }
}

}// namespace NES
