#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <API/InputQuery.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/DefaultSource.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Network/InputGate.hpp>
#include <Network/PacketHeader.hpp>
#include <Util/SerializationTools.hpp>
#include <QueryCompiler/QueryCompiler.hpp>

using namespace NES;
using namespace std;

class InternalDataTransmissionTest : public testing::Test {
 public:
  string host = "localhost";
  size_t port = 4788;

  static void SetUpTestCase() {
    NES::setupLogging("InternalDataTransmissionTest.log", NES::LOG_DEBUG);
    NES_DEBUG("Setup InternalDataTransmissionTest test class.");
  }

  static void TearDownTestCase() {
    std::cout << "Tear down InternalDataTransmissionTest test class."
              << std::endl;
  }
};

TEST_F(InternalDataTransmissionTest, testInternalTransmission) {
  string queryString =
      "InputQuery::from(default_logical).filter(default_logical[\"id\"] < 42).print(std::cout); ";

  InputQueryPtr query = UtilityFunctions::createQueryFromCodeString(
      queryString);
  OperatorPtr operatorTree = query->getRoot();

  QueryCompilerPtr compiler = createDefaultQueryCompiler();

  QueryExecutionPlanPtr qep1 = compiler->compile(operatorTree);

  DataSourcePtr source1 = createDefaultDataSourceWithSchemaForOneBuffer(
      query->getSourceStream()->getSchema());
  Schema sch1 = Schema::create().addField("sum", BasicType::UINT32);
  //DataSinkPtr sink1 = std::make_shared<ForwardSink>(query->source_stream->getSchema(), host, port);
  DataSinkPtr sink1 = createPrintSinkWithSchema(
      query->getSourceStream()->getSchema(), cout);
  qep1->addDataSource(source1);
  qep1->addDataSink(sink1);

  NodeEngine *ptr = new NodeEngine();
  ptr->deployQueryWithoutStart(qep1);
  ptr->start();
  source1->start();

  sleep(1);
  ptr->stop();
}

TEST_F(InternalDataTransmissionTest, testInputGate) {
  // create InputGate on localhost with ephemeral port to receive data
  InputGate inputGate("*", 0);
  inputGate.setup();
  NES_INFO(
      "InputGate published with host:" << inputGate.getHost() << " port:" << inputGate.getPort())

  // Open Publisher
  auto address = std::string("tcp://") + inputGate.getHost() + std::string(":")
      + std::to_string(inputGate.getPort());
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_PUSH);
  socket.connect(address);

  // thread for receiving the data.
  bool receivingFinished = false;
  auto inputGateThread = std::thread([&]() {
    // Receive data1
    tuple<std::string, TupleBufferPtr> rec1 = inputGate.receiveData();
    std::string sourceId1 = std::get<0>(rec1);
    TupleBufferPtr buffer1 = std::get<1>(rec1);

    // Test received data.
    size_t sum1 = 0;
    uint32_t *tuple1 = (uint32_t*) buffer1->getBuffer();
    for (size_t i = 0; i != 8; ++i) {
      sum1 += *(tuple1++);
    }
    size_t expected1 = 400;
    EXPECT_EQ(sourceId1, "testId1");
    EXPECT_EQ(sum1, expected1);

    BufferManager::instance().releaseFixedSizeBuffer(buffer1);

    // Receive data2
    tuple<std::string, TupleBufferPtr> rec2 = inputGate.receiveData();
    std::string sourceId2 = std::get<0>(rec2);
    TupleBufferPtr buffer2 = std::get<1>(rec2);

    // Test received data.
    size_t sum2 = 0;
    uint32_t *tuple2 = (uint32_t*) buffer2->getBuffer();
    for (size_t i = 0; i != 8; ++i) {
      sum2 += *(tuple2++);
    }
    size_t expected2 = 300;
    EXPECT_EQ(sourceId2, "testId2");
    EXPECT_EQ(sum2, expected2);

    BufferManager::instance().releaseFixedSizeBuffer(buffer2);

    receivingFinished = true;
  });

  std::array<uint32_t, 8> testData1 = { { 0, 100, 1, 99, 2, 98, 3, 97 } };
  size_t testDataSize1 = testData1.size() * sizeof(uint32_t);

  std::array<uint32_t, 8> testData2 = { { 0, 100, 1, 99, 2, 98 } };
  size_t testDataSize2 = testData2.size() * sizeof(uint32_t);

  int timeoutCounter = 0;
  int timeoutMax = 10000000;
  while (!receivingFinished && timeoutCounter <= timeoutMax) {
    timeoutCounter++;
    // Send data from here with one version of a packet header
    PacketHeader pH1(testData1.size(), testDataSize1, "testId1");
    std::string serPH1 = SerializationTools::serPacketHeader(pH1);

    zmq::message_t headerMessage1(serPH1.size());
    memcpy(headerMessage1.data(), serPH1.data(), serPH1.size());
    socket.send(headerMessage1, ZMQ_SNDMORE);

    zmq::message_t messageData1(testDataSize1);
    memcpy(messageData1.data(), testData1.data(), testDataSize1);
    socket.send(messageData1);

    // Send data from here with a different PacketHeader
    PacketHeader pH2(testData2.size(), testDataSize2, "testId2");
    std::string serPH2 = SerializationTools::serPacketHeader(pH2);

    zmq::message_t headerMessage2(serPH2.size());
    memcpy(headerMessage2.data(), serPH2.data(), serPH2.size());
    socket.send(headerMessage2, ZMQ_SNDMORE);

    zmq::message_t messageData2(testDataSize2);
    memcpy(messageData2.data(), testData2.data(), testDataSize2);
    socket.send(messageData2);
  }
  inputGateThread.join();

  if (timeoutCounter >= timeoutMax) {
    NES_ERROR("TestInputGate: Error timeout reached!")
  }
  EXPECT_TRUE(timeoutCounter < timeoutMax);
}
