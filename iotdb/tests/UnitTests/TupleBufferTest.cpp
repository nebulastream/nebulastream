#include <iostream>
#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <API/Schema.hpp>
#include <API/Types/DataTypes.hpp>
#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <SourceSink/GeneratorSource.hpp>

using namespace std;
namespace NES {

class TupleBufferTest : public testing::Test {
 public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    std::cout << "Setup TupleBufferTest test class." << std::endl;
  }

  /* Will be called before a test is executed. */
  void SetUp() {
    NES::setupLogging("TupleBufferTest.log", NES::LOG_DEBUG);
    std::cout << "Setup TupleBufferTest test case." << std::endl;
  }

  /* Will be called before a test is executed. */
  void TearDown() {
    std::cout << "Tear down TupleBufferTest test case." << std::endl;
  }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() {
    std::cout << "Tear down TupleBufferTest test class." << std::endl;
  }
};

TEST_F(TupleBufferTest, testPrintingOfTupleBuffer) {

  struct __attribute__((packed)) MyTuple {
    uint64_t i64;
    float f;
    double d;
    uint32_t i32;
    char s[12];
  };

  MyTuple* my_array = (MyTuple*) malloc(5 * sizeof(MyTuple));
  for (unsigned int i = 0; i < 5; ++i) {
    my_array[i] =
    MyTuple {i, float(0.5f * i), double(i * 0.2), i * 2, "1234"};
    std::cout << my_array[i].i64 << "|" << my_array[i].f << "|" << my_array[i].d
    << "|" << my_array[i].i32 << "|" << std::string(my_array[i].s, 12)
    << std::endl;
  }

  TupleBuffer buf {my_array, 5 * sizeof(MyTuple), sizeof(MyTuple), 5};
  Schema s = Schema::create().addField("i64", UINT64).addField("f", FLOAT32)
  .addField("d", FLOAT64).addField("i32", UINT32).addField("s", 12);

  std::string reference =
  "+----------------------------------------------------+\n"
  "|i64:UINT64|f:FLOAT32|d:FLOAT64|i32:UINT32|s:CHAR|\n"
  "+----------------------------------------------------+\n"
  "|0|0.000000|0.000000|0|1234|\n"
  "|1|0.500000|0.200000|2|1234|\n"
  "|2|1.000000|0.400000|4|1234|\n"
  "|3|1.500000|0.600000|6|1234|\n"
  "|4|2.000000|0.800000|8|1234|\n"
  "+----------------------------------------------------+";

  std::string result = NES::toString(buf, s);
  std::cout << "'" << reference << "'" << reference.size() << std::endl;
  std::cout << "'" << result << "'" << result.size() << std::endl;

  EXPECT_EQ(reference.size(), result.size());
  EXPECT_EQ(reference, result);

  free(my_array);
}
TEST_F(TupleBufferTest, testEndianess) {

  struct __attribute__((packed)) TestStruct {
    u_int8_t v1;
    u_int16_t v2;
    u_int32_t v3;
    u_int64_t v4;
    int8_t v5;
    int16_t v6;
    int32_t v7;
    int64_t v8;
    float v9;
    double v10;
  };

  TestStruct ts;
  ts.v1 = 1;
  ts.v2 = 1;
  ts.v3 = 1;
  ts.v4 = 1;
  ts.v5 = 1;
  ts.v6 = 1;
  ts.v7 = 1;
  ts.v8 = 1;
  ts.v9 = 1.1;
  ts.v10 = 1.2;

  TupleBuffer testBuf { &ts, sizeof(TestStruct), sizeof(TestStruct), 1 };
  Schema s = Schema::create().addField("v1", UINT8).addField("v2", UINT16)
      .addField("v3", UINT32).addField("v4", UINT64).addField("v5", INT8)
      .addField("v6", INT16).addField("v7", INT32).addField("v8", INT64)
      .addField("v9", FLOAT32).addField("v10", FLOAT64);

  cout << "to string=" << endl;
  std::string result = NES::toString(testBuf, s);

  cout << "to printTupleBuffer=" << testBuf.printTupleBuffer(s) << endl;

  testBuf.revertEndianness(s);
  cout << "after reverse1=" << testBuf.printTupleBuffer(s) << endl;

  testBuf.revertEndianness(s);
  cout << "after reverse2=" << testBuf.printTupleBuffer(s) << endl;

  string expected = "1,1,1,1,1,1,1,1,1.100000,1.200000\n";
  EXPECT_EQ(expected, testBuf.printTupleBuffer(s));
}

}

