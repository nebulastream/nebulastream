#include <gtest/gtest.h>
#include <Util/SerializationTools.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/PrintSink.hpp>

using namespace iotdb;

class SerializationToolsTest : public testing::Test {
 public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup SerializationToolsTest test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() {
    std::cout << "Setup SerializationToolsTest test case." << std::endl;
  }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup SerializationToolsTest test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down SerializationToolsTest test class." << std::endl; }

  Schema schema = Schema::create()
      .addField("id", BasicType::UINT32)
      .addField("value", BasicType::UINT64);

  Stream stream = Stream("cars", schema);
};

/* Test serialization for Schema  */
TEST_F(SerializationToolsTest, serialize_deserialize_schema) {
  string sschema = SerializationTools::ser_schema(schema);
  Schema dschema = SerializationTools::parse_schema(sschema);
  EXPECT_EQ(schema, dschema);
}
