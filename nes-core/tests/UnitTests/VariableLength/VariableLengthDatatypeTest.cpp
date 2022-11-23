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
#include <Common/ExecutableType/Array.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutTupleBuffer.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <algorithm>
#include <cstdlib>
#include <gtest/gtest.h>
#include <iostream>
#include <log4cxx/helpers/exception.h>
#include <vector>

static std::string charset(...);
namespace NES::Runtime::MemoryLayouts {
class VariableLengthDatatypeTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    BufferManagerPtr bufferManager;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ColumnarMemoryLayoutTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ColumnarMemoryLayoutTest test class.");
    }

    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        bufferManager = std::make_shared<BufferManager>();
    }

    std::string randomString(size_t size) {
        auto randomCharacter = []() -> char {
            const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
            return charset[rand() % (sizeof(charset) - 1)];
        };
        std::string str(size, 0);
        std::generate_n(str.begin(), size, randomCharacter);
        return str;
    }
};

TEST_F(VariableLengthDatatypeTest, testvarchar) {
    SchemaPtr schema =
        Schema::create()->addField("t1", BasicType::UINT64)->addField("t2", BasicType::TEXT);


    auto tupleBuffer = bufferManager->getBufferBlocking();

    auto bindedColumnLayout = columnLayout->bind(tupleBuffer);

    auto capacity = tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes();
    ASSERT_EQ(bindedColumnLayout->getCapacity(), capacity);
    ASSERT_EQ(bindedColumnLayout->getNumberOfRecords(), 0u);
    ASSERT_EQ(columnLayout->getFieldOffset(1, 2), capacity * 1 + capacity * 2 + 1 * 4);
    ASSERT_EQ(columnLayout->getFieldOffset(5, 1), capacity * 1 + 5 * 2);
    ASSERT_EQ(columnLayout->getFieldOffset(4, 0), capacity * 0 + 4);
}

}// namespace NES::Runtime::MemoryLayouts