#include <map>
#include <vector>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <log4cxx/appender.h>
#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <random>
#include <API/Schema.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>


//#define DEBUG_OUTPUT

namespace iotdb {
class MemoryLayoutTest : public testing::Test {
 public:
  static void SetUpTestCase() {
#ifdef DEBUG_OUTPUT
    setupLogging();
#endif
    IOTDB_INFO("Setup MemoryLayout test class.");
  }
  static void TearDownTestCase() { std::cout << "Tear down MemoryLayout test class." << std::endl; }

 protected:
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));
  }

};

TEST_F(MemoryLayoutTest, row_layout_test) {
  Schema schema = Schema()
      .addField("t1", BasicType::UINT8)
      .addField("t2", BasicType::UINT8)
      .addField("t3", BasicType::UINT8);
  TupleBufferPtr buf = BufferManager::instance().getBuffer();
  auto layout = createRowLayout(std::make_shared<Schema>(schema));
  for(int i = 0 ; i< 10; i++){
    layout->writeField<uint8_t>(buf, i, 0,i);
    layout->writeField<uint8_t>(buf, i, 1,i);
    layout->writeField<uint8_t>(buf, i, 2,i);
  }


  for(int i = 0 ; i< 10; i++){
    auto value = layout->readField<uint8_t>(buf, i, 0);
    ASSERT_EQ(value, i);

    value = layout->readField<uint8_t>(buf, i, 1);
    ASSERT_EQ(value, i);

    value = layout->readField<uint8_t>(buf, i, 2);
    ASSERT_EQ(value, i);
  }




}



}
