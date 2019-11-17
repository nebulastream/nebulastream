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

  EXPECT_TRUE(!sschema.empty());
}

/* Test serialization for predicate  */
TEST_F(SerializationToolsTest, serialize_deserialize_predicate) {
  PredicatePtr pred = createPredicate(stream["value"] > 42);
  string serPred = SerializationTools::ser_predicate(pred);
  PredicatePtr deserPred = SerializationTools::parse_predicate(serPred);
  EXPECT_TRUE(!serPred.empty());
}

/* Test serialization for predicate  */
TEST_F(SerializationToolsTest, serialize_deserialize_filter_op) {
  PredicatePtr pred = createPredicate(stream["value"] > 42);
  OperatorPtr op = createFilterOperator(pred);

  string serOp = SerializationTools::ser_operator(op);
  OperatorPtr deserOp = SerializationTools::parse_operator(serOp);
  EXPECT_TRUE(!serOp.empty());
}

/* Test serialization for predicate  */
TEST_F(SerializationToolsTest, serialize_deserialize_source_op) {
  OperatorPtr op = createSourceOperator(createTestDataSourceWithSchema(stream.getSchema()));
  string serOp = SerializationTools::ser_operator(op);
  OperatorPtr deserOp = SerializationTools::parse_operator(serOp);
  EXPECT_TRUE(!serOp.empty());
}

/* Test serialization for predicate  */
TEST_F(SerializationToolsTest, serialize_deserialize_sink_op) {
  OperatorPtr op = createSinkOperator(createPrintSinkWithoutSchema(std::cout));
  string serOp = SerializationTools::ser_operator(op);
  OperatorPtr deserOp = SerializationTools::parse_operator(serOp);
  EXPECT_TRUE(!serOp.empty());
}


/* Test serialization for operators  */
TEST_F(SerializationToolsTest, serialize_deserialize_operators) {
  InputQuery &query = InputQuery::from(stream)
      .filter(stream["value"] > 42)
      .print(std::cout);

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
  string serSink = SerializationTools::ser_sink(zmqSink);
  DataSinkPtr deserZmq = SerializationTools::parse_sink(serSink);

  EXPECT_TRUE(!serSink.empty());
}

/* Test serialization for zmqSink  */
TEST_F(SerializationToolsTest, serialize_deserialize_printSink) {
  DataSinkPtr sink = std::make_shared<PrintSink>(std::cout);
  string serSink = SerializationTools::ser_sink(sink);
  DataSinkPtr deserZmq = SerializationTools::parse_sink(serSink);
  EXPECT_TRUE(!serSink.empty());
}