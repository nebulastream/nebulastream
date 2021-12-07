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
#include <Common/ExecutableType/Array.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutTupleBuffer.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>
#include <vector>

namespace NES::Runtime::MemoryLayouts {
class DynamicMemoryLayoutTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;

    void SetUp() override {
        NESLogger::getInstance()->removeAllAppenders();
        NES::setupLogging("DynamicMemoryLayoutTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup DynamicMemoryLayoutTest test class.");
        bufferManager = std::make_shared<BufferManager>(4096, 10);
    }
    static void TearDownTestCase() {
        std::cout << "Tear down DynamicMemoryLayoutTest class." << std::endl;
        NESLogger::getInstance()->removeAllAppenders();
    }
};

TEST_F(DynamicMemoryLayoutTest, accessDynamicBufferExceptionTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = DynamicTupleBuffer(columnLayout, tupleBuffer);
    // Check for buffer index exception
    ASSERT_ANY_THROW(buffer[-1]);
    ASSERT_ANY_THROW(buffer[10000000L]);
    // Check for field out of bound exception
    ASSERT_ANY_THROW(buffer[0][-1]);
    ASSERT_ANY_THROW(buffer[0][5]);
    // Check for wrong field type exception
    ASSERT_NO_THROW(buffer[0][0].read<uint8_t>());
    ASSERT_ANY_THROW(buffer[0][0].read<uint16_t>());
    ASSERT_ANY_THROW(buffer[0][0].read<uint32_t>());
    ASSERT_ANY_THROW(buffer[0][0].read<uint64_t>());
    ASSERT_ANY_THROW(buffer[0][0].read<int8_t>());
    ASSERT_ANY_THROW(buffer[0][0].read<float>());
    ASSERT_ANY_THROW(buffer[0][0].read<uint8_t*>());
}

TEST_F(DynamicMemoryLayoutTest, readWriteColumnarDynamicBufferTest) {
    SchemaPtr schema = Schema::create()->addField("t1", UINT16)->addField("t2", BOOLEAN)->addField("t3", FLOAT64);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = DynamicTupleBuffer(columnLayout, tupleBuffer);

    for (int i = 0; i < 10; i++) {
        buffer[i][0].write<uint16_t>(i);
        buffer[i]["t2"].write<bool>(true);
        buffer[i][2].write<double>(i * 2.0);
    }

    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(buffer[i][0].read<uint16_t>(), i);
        ASSERT_EQ(buffer[i]["t2"].read<bool>(), true);
        ASSERT_EQ(buffer[i]["t3"].read<double>(), i * 2.0);
    }
}

TEST_F(DynamicMemoryLayoutTest, readWriteRowDynamicBufferTest) {
    SchemaPtr schema = Schema::create()->addField("t1", UINT16)->addField("t2", BOOLEAN)->addField("t3", FLOAT64);

    RowLayoutPtr layout;
    ASSERT_NO_THROW(layout = RowLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(layout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = DynamicTupleBuffer(layout, tupleBuffer);

    for (int i = 0; i < 10; i++) {
        buffer[i][0].write<uint16_t>(i);
        buffer[i]["t2"].write<bool>(true);
        buffer[i][2].write<double>(i * 2.0);
    }

    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(buffer[i][0].read<uint16_t>(), i);
        ASSERT_EQ(buffer[i]["t2"].read<bool>(), true);
        ASSERT_EQ(buffer[i]["t3"].read<double>(), i * 2.0);
    }
}

TEST_F(DynamicMemoryLayoutTest, iterateDynamicBufferTest) {
    SchemaPtr schema = Schema::create()->addField("t1", UINT16)->addField("t2", BOOLEAN)->addField("t3", FLOAT64);

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = DynamicTupleBuffer(columnLayout, tupleBuffer);

    for (int i = 0; i < 10; i++) {
        buffer[i][0].write<uint16_t>(42);
        buffer[i]["t2"].write<bool>(true);
        buffer[i][2].write<double>(42 * 2.0);
    }

    for (auto tuple : buffer) {
        ASSERT_EQ(tuple[0].read<uint16_t>(), 42);
        ASSERT_EQ(tuple["t2"].read<bool>(), true);
        ASSERT_EQ(tuple["t3"].read<double>(), 42 * 2.0);
    }
}

TEST_F(DynamicMemoryLayoutTest, accessArrayDynamicBufferTest) {
    SchemaPtr schema = Schema::create()->addField("t1", DataTypeFactory::createArray(10, DataTypeFactory::createInt64()));

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = DynamicTupleBuffer(columnLayout, tupleBuffer);
    auto array = NES::ExecutableTypes::Array<int64_t, 10>();
    for (int i = 0; i < 10; i++) {
        array[i] = i * 2;
    }
    buffer[0][0].write<NES::ExecutableTypes::Array<int64_t, 10>>(array);
    auto value = buffer[0][0].read<NES::ExecutableTypes::Array<int64_t, 10>>();
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(value[i], i * 2);
    }
}

TEST_F(DynamicMemoryLayoutTest, accessArrayPointerDynamicBufferTest) {
    SchemaPtr schema = Schema::create()->addField("t1", DataTypeFactory::createArray(10, DataTypeFactory::createInt64()));

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = DynamicTupleBuffer(columnLayout, tupleBuffer);

    for (uint64_t t = 0; t < buffer.getCapacity(); t++) {
        auto* value = buffer[t][0].read<int64_t*>();
        for (int i = 0; i < 10; i++) {
            value[i] = i;
        }
    }
    buffer.setNumberOfTuples(buffer.getCapacity());

    for (auto tuple : buffer) {
        auto* arrayPtr = tuple[0].read<int64_t*>();
        for (int i = 0; i < 10; i++) {
            ASSERT_EQ(arrayPtr[i], i);
        }
    }
}

TEST_F(DynamicMemoryLayoutTest, accessFixedCharDynamicBufferTest) {
    SchemaPtr schema = Schema::create()->addField("t1", DataTypeFactory::createFixedChar(10));

    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto buffer = DynamicTupleBuffer(columnLayout, tupleBuffer);

    for (uint64_t t = 0; t < buffer.getCapacity(); t++) {
        auto* value = buffer[t][0].read<char*>();
        std::string str("Test");
        str.copy(value, 4, 0);
    }
    buffer.setNumberOfTuples(buffer.getCapacity());

    for (auto tuple : buffer) {
        auto* arrayPtr = tuple[0].read<char*>();
        std::string str("Test");
        str.compare(arrayPtr);
    }
}

}// namespace NES::Runtime::MemoryLayouts
