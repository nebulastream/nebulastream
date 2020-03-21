#include <map>
#include <vector>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <API/Schema.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>

//#define DEBUG_OUTPUT

namespace NES {
class MemoryLayoutTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("MemoryLayoutTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MemoryLayout test class.");
    }
    static void TearDownTestCase() {
        std::cout << "Tear down MemoryLayout test class." << std::endl;
    }
};

TEST_F(MemoryLayoutTest, rowLayoutTestInt) {
    Schema schema = Schema().addField("t1", BasicType::UINT8).addField(
        "t2", BasicType::UINT8).addField("t3", BasicType::UINT8);
    TupleBufferPtr buf = BufferManager::instance().getBuffer();
    auto layout = createRowLayout(std::make_shared<Schema>(schema));
    for (int i = 0; i < 10; i++) {
        layout->getValueField<uint8_t>(i, 0)->write(buf, i);
        layout->getValueField<uint8_t>(i, 1)->write(buf, i);
        layout->getValueField<uint8_t>(i, 2)->write(buf, i);
    }

    for (int i = 0; i < 10; i++) {
        auto value = layout->getValueField<uint8_t>(i, 0)->read(buf);
        ASSERT_EQ(value, i);

        value = layout->getValueField<uint8_t>(i, 1)->read(buf);
        ASSERT_EQ(value, i);

        value = layout->getValueField<uint8_t>(i, 2)->read(buf);
        ASSERT_EQ(value, i);
    }
}

TEST_F(MemoryLayoutTest, rowLayoutTestArray) {
    Schema schema = Schema()
        .addField("t1", createArrayDataType(BasicType::INT64, 10))
        .addField("t2", BasicType::UINT8)
        .addField("t3", BasicType::UINT8);

    TupleBufferPtr buf = BufferManager::instance().getBuffer();
    auto layout = createRowLayout(std::make_shared<Schema>(schema));
    for (int i = 0; i < 10; i++) {
        auto arrayField = layout->getArrayField(i, 0);
        arrayField[0]->asValueField<int64_t>()->write(buf, 10);
        layout->getValueField<uint8_t>(i, 1)->write(buf, i);
        layout->getValueField<uint8_t>(i, 2)->write(buf, i);
    }

    for (int i = 0; i < 10; i++) {
        auto arrayField = layout->getArrayField(i, 0);
        auto value = arrayField[0]->asValueField<int64_t>()->read(buf);
        ASSERT_EQ(value, 10);

        value = layout->getValueField<uint8_t>(i, 1)->read(buf);
        ASSERT_EQ(value, i);

        value = layout->getValueField<uint8_t>(i, 2)->read(buf);
        ASSERT_EQ(value, i);
    }
}

TEST_F(MemoryLayoutTest, rowLayoutTestArrayAsPointerField) {
    Schema schema = Schema()
        .addField("t1", createArrayDataType(BasicType::INT64, 10));

    TupleBufferPtr buf = BufferManager::instance().getBuffer();
    auto layout = createRowLayout(std::make_shared<Schema>(schema));
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        auto arrayField = layout->getArrayField(recordIndex, 0);
        for (uint64_t arrayIndex = 0; arrayIndex < 10; arrayIndex++) {
            arrayField[arrayIndex]->asValueField<int64_t>()->write(buf, arrayIndex);
        }
    }

    for (int i = 0; i < 10; i++) {
        auto array = layout->getFieldPointer<int64_t>(buf, i, 0);
        for (uint64_t arrayIndex = 0; arrayIndex < 10; arrayIndex++) {
            ASSERT_EQ(array[arrayIndex], arrayIndex);
        }
    }
}

}
