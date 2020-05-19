#include <GRPC/ExecutableTransferObject.hpp>
#include <SourceSink/PrintSink.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/SerializationTools.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <NodeEngine/QueryManager.hpp>

using namespace NES;

class SerializationToolsTest : public testing::Test {
  public:
    BufferManagerPtr bPtr;
    QueryManagerPtr dPtr;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup SerializationToolsTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp()
    {
        NES::setupLogging("SerializationToolsTest.log", NES::LOG_DEBUG);
        std::cout << "Setup SerializationToolsTest test case." << std::endl;
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
        stream = std::make_shared<Stream>("default", schema);

        bPtr = std::make_shared<BufferManager>(4096, 1024);
        dPtr = std::make_shared<QueryManager>();
    }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Setup SerializationToolsTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down SerializationToolsTest test class." << std::endl; }

    SchemaPtr schema;
    StreamPtr stream;
};

/* Test serialization for Schema  */
TEST_F(SerializationToolsTest, testSerializeDeserializeSchema)
{
    string sschema = SerializationTools::ser_schema(schema);
    SchemaPtr dschema = SerializationTools::parse_schema(sschema);
    EXPECT_TRUE(dschema->equals(schema));
}

/* Test serialization for predicate  */
TEST_F(SerializationToolsTest, testSerializeDeserializePredicate)
{
    PredicatePtr pred = createPredicate(stream->getField("value") < 42);
    string serPred = SerializationTools::ser_predicate(pred);
    PredicatePtr deserPred = SerializationTools::parse_predicate(serPred);
    EXPECT_TRUE(pred->equals(*deserPred.get()));
}

TEST_F(SerializationToolsTest, testSerializeDeserializeFilterOp)
{
    PredicatePtr pred = createPredicate(stream->getField("value") < 42);
    OperatorPtr op = createFilterOperator(pred);
    string serOp = SerializationTools::ser_operator(op);
    OperatorPtr deserOp = SerializationTools::parse_operator(serOp);
    EXPECT_TRUE(op->equals(*op.get()));
}

TEST_F(SerializationToolsTest, testSerializeDeserializeSourceOp)
{
    // TODO: implement equals method for SourceOperator

    OperatorPtr op = createSourceOperator(createDefaultDataSourceWithSchemaForOneBuffer(stream->getSchema(), bPtr, dPtr));
    string serOp = SerializationTools::ser_operator(op);
    OperatorPtr deserOp = SerializationTools::parse_operator(serOp);
    EXPECT_TRUE(!serOp.empty());
}

TEST_F(SerializationToolsTest, testSerializeDeserializeSinkOp)
{
    OperatorPtr op = createSinkOperator(createPrintSinkWithoutSchema(std::cout));
    string serOp = SerializationTools::ser_operator(op);
    OperatorPtr deserOp = SerializationTools::parse_operator(serOp);
    EXPECT_TRUE(!serOp.empty());
}

/* Test serialization for operators  */

TEST_F(SerializationToolsTest, testSerializeDeserializeQueryOperators)
{
    // TODO: implement equals method for all operators
    InputQuery& query = InputQuery::from(*stream).filter(stream->getField("value") < 42).print(std::cout);

    OperatorPtr queryOp = query.getRoot();
    string serOp = SerializationTools::ser_operator(queryOp);
    OperatorPtr deserOp = SerializationTools::parse_operator(serOp);
    EXPECT_TRUE(!serOp.empty());
}

/* Test serialization for zmqSource  */

TEST_F(SerializationToolsTest, testSerializeDeserializeZmqSource)
{
    // TODO: implement equals method for DataSources
    DataSourcePtr zmqSource = createZmqSource(schema, bPtr, dPtr, "", 0);
    string serSource = SerializationTools::ser_source(zmqSource);
    DataSourcePtr deserZmq = SerializationTools::parse_source(serSource);
    EXPECT_TRUE(!serSource.empty());
}

/* Test serialization for zmqSink  */
TEST_F(SerializationToolsTest, testSerializeDeserializeZmqSink)
{
    // TODO: implement equals method for DataSinks
    DataSinkPtr zmqSink = createZmqSink(schema, "", 0);
    string serSink = SerializationTools::ser_sink(zmqSink);
    DataSinkPtr deserZmq = SerializationTools::parse_sink(serSink);
    EXPECT_TRUE(!serSink.empty());
}
/* Test serialization for zmqSourceOperator  */
TEST_F(SerializationToolsTest, testSerializeDeserializeZmqSourceOperator)
{
    // TODO: implement equals method for DataSources
    OperatorPtr zmqOp = createSourceOperator(createZmqSource(schema, bPtr, dPtr, "", 0));
    string serOp = SerializationTools::ser_operator(zmqOp);
    OperatorPtr deserOp = SerializationTools::parse_operator(serOp);
    EXPECT_TRUE(!serOp.empty());
}

/* Test serialization for zmqSink Operator  */
TEST_F(SerializationToolsTest, testSerializeDeserializeZmqSinkOperator)
{
    // TODO: implement equals method for DataSinks
    OperatorPtr zmqOp = createSinkOperator(createZmqSink(schema, "", 0));
    string serOp = SerializationTools::ser_operator(zmqOp);
    OperatorPtr deserOp = SerializationTools::parse_operator(serOp);
    EXPECT_TRUE(!serOp.empty());
}

/* Test serialization for printSink  */
TEST_F(SerializationToolsTest, testSerializeDeserializePrintSink)
{
    DataSinkPtr sink = std::make_shared<PrintSink>(std::cout);
    string serSink = SerializationTools::ser_sink(sink);
    DataSinkPtr deserZmq = SerializationTools::parse_sink(serSink);
    EXPECT_TRUE(!serSink.empty());
}
/* Test serialization for printSink  */
TEST_F(SerializationToolsTest, testSerializeDeserializeExecutabletransferobject)
{
    InputQuery& query = InputQuery::from(*stream).filter(stream->getField("value") < 42).print(std::cout);
    OperatorPtr op = query.getRoot();

    DataSourcePtr zmqSource = createZmqSource(schema, bPtr, dPtr, "test", 4711);
    DataSinkPtr sink = std::make_shared<PrintSink>(std::cout);
    vector<DataSourcePtr> sources{zmqSource};
    vector<DataSinkPtr> destinations{sink};

    ExecutableTransferObject eto = ExecutableTransferObject("example-desc", schema, sources, destinations, op);

    string serEto = SerializationTools::ser_eto(eto);
    ExecutableTransferObject deserEto = SerializationTools::parse_eto(serEto);
    EXPECT_TRUE(!serEto.empty());
}

TEST_F(SerializationToolsTest, testSerializeDeserializeExecutabletransferobjectExdraSchema)
{
    InputQuery& query = InputQuery::from(*stream).filter(stream->getField("value") < 42).print(std::cout);
    OperatorPtr op = query.getRoot();

    DataSourcePtr zmqSource = createZmqSource(schema, bPtr, dPtr, "test", 4711);
    DataSinkPtr sink = std::make_shared<PrintSink>(std::cout);
    vector<DataSourcePtr> sources{zmqSource};
    vector<DataSinkPtr> destinations{sink};

    SchemaPtr schemaExdra = Schema::create()
                                ->addField("type", createArrayDataType(BasicType::CHAR, 30))
                                ->addField("metadata.generated", BasicType::UINT64)
                                ->addField("metadata.title", createArrayDataType(BasicType::CHAR, 50))
                                ->addField("metadata.id", createArrayDataType(BasicType::CHAR, 50))
                                ->addField("features.type", createArrayDataType(BasicType::CHAR, 50))
                                ->addField("features.properties.capacity", BasicType::UINT64)
                                ->addField("features.properties.efficiency", BasicType::FLOAT32)
                                ->addField("features.properties.mag", BasicType::FLOAT32)
                                ->addField("features.properties.time", BasicType::FLOAT32)
                                ->addField("features.properties.updated", BasicType::UINT64)
                                ->addField("features.properties.type", createArrayDataType(BasicType::CHAR, 50))
                                ->addField("features.geometry.type", createArrayDataType(BasicType::CHAR, 50))
                                ->addField("features.geometry.coordinates.longitude", BasicType::FLOAT32)
                                ->addField("features.geometry.coordinates.latitude", BasicType::FLOAT32)
                                ->addField("features.eventId ", createArrayDataType(BasicType::CHAR, 50));

    ExecutableTransferObject eto = ExecutableTransferObject("example-desc", schemaExdra, sources, destinations, op);

    string serEto = SerializationTools::ser_eto(eto);
    ExecutableTransferObject deserEto = SerializationTools::parse_eto(serEto);
    EXPECT_TRUE(!serEto.empty());
}