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

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <cstdlib>
#include <gtest/gtest.h>
#include <iostream>

#include <vector>

namespace NES::Runtime::MemoryLayouts {
class JsonFormatTest : public Testing::BaseUnitTest {
  public:
    BufferManagerPtr bufferManager;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ColumnarMemoryLayoutTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ColumnarMemoryLayoutTest test class.");
    }

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        bufferManager = std::make_shared<BufferManager>(4096, 10);
    }
};

/**
 * @brief Tests that we can construct a column layout.
 */
TEST_F(JsonFormatTest, createJsonIterator) {
    // JsonFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager);
    SchemaPtr schema = Schema::create()->addField("t1", BasicType::UINT8)->addField("t2", DataTypeFactory::createText());
    auto jsonIterator = std::make_unique<JsonFormat>(schema, bufferManager);
    ASSERT_NE(jsonIterator, nullptr);

    // Todo create TestTupleBuffer with some content.
    ColumnLayoutPtr columnLayout;
    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
    ASSERT_NE(columnLayout, nullptr);

    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto testBuffer = std::make_unique<Runtime::MemoryLayouts::TestTupleBuffer>(columnLayout, tupleBuffer);

    std::tuple<uint8_t, std::string> writeRecord(13, "37 is correct");
    testBuffer->pushRecordToBuffer(writeRecord, bufferManager.get());
    std::tuple<uint8_t, std::string> readRecord = testBuffer->readRecordFromBuffer<uint8_t, std::string>(0);

    std::cout << static_cast<uint32_t>(std::get<0>(readRecord)) << std::endl;
    std::cout << static_cast<std::string>(std::get<1>(readRecord)) << std::endl;
    NES::Runtime::TupleBuffer buffer = testBuffer->getBuffer();
    auto tupleIterator = jsonIterator->getTupleIterator(buffer);
    std::cout << "First iterator result: " << *tupleIterator.begin() << std::endl;
//
//    ColumnLayoutPtr columnLayout;
//    ASSERT_NO_THROW(columnLayout = ColumnLayout::create(schema, bufferManager->getBufferSize()));
//    ASSERT_NE(columnLayout, nullptr);
}

}// namespace NES::Runtime::MemoryLayouts
