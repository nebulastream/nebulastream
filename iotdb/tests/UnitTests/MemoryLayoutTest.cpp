#include <map>
#include <vector>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <gtest/gtest.h>
//#include <Util/Logger.hpp>
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
    TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
    auto layout = createRowLayout(std::make_shared<Schema>(schema));
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/0)->write(buf, recordIndex);
        layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/1)->write(buf, recordIndex);
        layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/2)->write(buf, recordIndex);
    }

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        auto value = layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/0)->read(buf);
        ASSERT_EQ(value, recordIndex);

        value = layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/1)->read(buf);
        ASSERT_EQ(value, recordIndex);

        value = layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/2)->read(buf);
        ASSERT_EQ(value, recordIndex);
    }
    BufferManager::instance().releaseBuffer(buf);
}

TEST_F(MemoryLayoutTest, rowLayoutTestArray) {
    Schema schema = Schema()
        .addField("t1", createArrayDataType(BasicType::INT64, 10))
        .addField("t2", BasicType::UINT8)
        .addField("t3", BasicType::UINT8);

    TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
    auto layout = createRowLayout(std::make_shared<Schema>(schema));
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        auto arrayField = layout->getArrayField(recordIndex,  /*fieldIndex*/0);
        arrayField[0]->asValueField<int64_t>()->write(buf, 10);
        layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/ 1)->write(buf, recordIndex);
        layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/ 2)->write(buf, recordIndex);
    }

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        auto arrayField = layout->getArrayField(recordIndex, /*fieldIndex*/0);
        auto value = arrayField[0]->asValueField<int64_t>()->read(buf);
        ASSERT_EQ(value, 10);

        value = layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/1)->read(buf);
        ASSERT_EQ(value, recordIndex);

        value = layout->getValueField<uint8_t>(recordIndex, /*fieldIndex*/2)->read(buf);
        ASSERT_EQ(value, recordIndex);
    }
}

TEST_F(MemoryLayoutTest, rowLayoutTestArrayAsPointerField) {
    Schema schema = Schema()
        .addField("t1", createArrayDataType(BasicType::INT64, 10));

    TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
    auto layout = createRowLayout(std::make_shared<Schema>(schema));
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        auto arrayField = layout->getArrayField(recordIndex,  /*fieldIndex*/0);
        for (uint64_t arrayIndex = 0; arrayIndex < 10; arrayIndex++) {
            arrayField[arrayIndex]->asValueField<int64_t>()->write(buf, arrayIndex);
        }
    }

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        auto array = layout->getFieldPointer<int64_t>(buf, recordIndex, /*fieldIndex*/0);
        for (uint64_t arrayIndex = 0; arrayIndex < 10; arrayIndex++) {
            ASSERT_EQ(array[arrayIndex], arrayIndex);
        }
    }
}

}
