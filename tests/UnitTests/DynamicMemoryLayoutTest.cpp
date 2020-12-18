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
#include <cstdlib>
#include <gtest/gtest.h>
#include <iostream>
#include <map>
#include <thread>
#include <vector>

//#define DEBUG_OUTPUT

namespace NES {
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

    auto bufferSize = bufferManager->getBufferSize();
    DynamicRowLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = DynamicRowLayout::create(schema, bufferSize, true));
    ASSERT_NE(rowLayout, nullptr);
}

TEST_F(DynamicMemoryLayoutTest, columnLayoutCreateTest) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", BasicType::UINT8)->addField("t3", BasicType::UINT8);

    auto bufferSize = bufferManager->getBufferSize();
    DynamicColumnLayoutPtr rowLayout;
    ASSERT_NO_THROW(rowLayout = DynamicColumnLayout::create(schema, bufferSize, true));
    ASSERT_NE(rowLayout, nullptr);
}

}// namespace NES
