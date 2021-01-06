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
#include <NodeEngine/MemoryLayout/DynamicColumnLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicColumnLayoutBuffer.hpp>

#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>
#include <vector>


namespace NES::NodeEngine {
class DynamicMemoryLayoutTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;

    void SetUp() {
        NES::setupLogging("DynamicMemoryLayoutTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup DynamicMemoryLayoutTest test class.");
        bufferManager = std::make_shared<BufferManager>(4096, 10);
    }
    static void TearDownTestCase() { std::cout << "Tear down DynamicMemoryLayoutTest class." << std::endl; }
};

TEST_F(DynamicMemoryLayoutTest, rowLayoutCreateTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_EQ(rowLayout->isCheckBoundaryFieldChecks(), true);
    ASSERT_NE(rowLayout, nullptr);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutCreateTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_EQ(columnLayout->isCheckBoundaryFieldChecks(), true);
    ASSERT_NE(columnLayout, nullptr);
}

TEST_F(DynamicMemoryLayoutTest, rowLayoutMapCalcOffsetTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, true));
    ASSERT_NE(rowLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicRowLayoutBufferPtr mappedRowLayout;
    ASSERT_NO_THROW(mappedRowLayout = std::unique_ptr<DynamicRowLayoutBuffer>(static_cast<DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedRowLayout, nullptr);

    ASSERT_EQ(mappedRowLayout->getCapacity(), tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());
    ASSERT_EQ(mappedRowLayout->getNumberOfRecords(), 0);
    ASSERT_EQ(mappedRowLayout->calcOffset(1, 2), schema->getSchemaSizeInBytes() * 1 + (1 + 2));
    ASSERT_EQ(mappedRowLayout->calcOffset(4, 0), schema->getSchemaSizeInBytes() * 4 + 0);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutMapCalcOffsetTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT16)->addField("t3", BasicType::UINT32);

    DynamicColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = DynamicColumnLayout::create(schema, true));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    DynamicColumnLayoutBufferPtr mappedColumnLayout;
    ASSERT_NO_THROW(mappedColumnLayout = std::unique_ptr<DynamicColumnLayoutBuffer>(static_cast<DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release())));
    ASSERT_NE(mappedColumnLayout, nullptr);

    auto capacity = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();
    ASSERT_EQ(mappedColumnLayout->getCapacity(), capacity);
    ASSERT_EQ(mappedColumnLayout->getNumberOfRecords(), 0);
    ASSERT_EQ(mappedColumnLayout->calcOffset(1, 2), capacity * 1 + capacity * 2 + 1 * 4);
    ASSERT_EQ(mappedColumnLayout->calcOffset(5, 1), capacity * 1 + 5 * 2);
    ASSERT_EQ(mappedColumnLayout->calcOffset(4, 0), capacity * 0 + 4);
}

}// namespace NES
