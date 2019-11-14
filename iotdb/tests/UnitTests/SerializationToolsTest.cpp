#include <gtest/gtest.h>
#include <Util/SerializationTools.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>

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

  Stream cars = Stream("cars", schema);

  InputQuery &query = InputQuery::from(cars)
      .filter(cars["value"] > 42)
      .print(std::cout);
};

/* Test serialization for Schema  */
TEST_F(SerializationToolsTest, serialize_deserialize_schema) {
  string sschema = SerializationTools::ser_schema(schema);
  Schema dschema = SerializationTools::parse_schema(sschema);

  EXPECT_TRUE(!sschema.empty());
}

/* Test serialization for operators  */
TEST_F(SerializationToolsTest, serialize_deserialize_operators) {
  OperatorPtr queryOp = query.getRoot();
  string serOp = SerializationTools::ser_operator(queryOp);
  OperatorPtr deserOp = SerializationTools::parse_operator(serOp);

  EXPECT_TRUE(!serOp.empty());
}

/* Test serialization for zmqSource  */
TEST_F(SerializationToolsTest, serialize_deserialize_zmqSource) {
  DataSourcePtr zmqSource = createZmqSource(schema, "", 0);
  string serSource = SerializationTools::ser_source(zmqSource);
  DataSourcePtr deserZmq = SerializationTools::parse_source(serSource);

  EXPECT_TRUE(!serSource.empty());
}

/* Test serialization for zmqSink  */
TEST_F(SerializationToolsTest, serialize_deserialize_zmqSink) {
  DataSinkPtr zmqSink = createZmqSink(schema, "", 0);
  string serSource = SerializationTools::ser_sink(zmqSink);
  DataSinkPtr deserZmq = SerializationTools::parse_sink(serSource);

  EXPECT_TRUE(!serSource.empty());
}