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

using namespace NES;
using namespace std;

class InternalDataTransmissionTest : public testing::Test {
  public:
    string host = "localhost";
    size_t port = 4788;

    static void SetUpTestCase() {
        setupLogging();
        NES_DEBUG("Setup InternalDataTransmissionTest test class.");
    }

    static void TearDownTestCase() { std::cout << "Tear down InternalDataTransmissionTest test class." << std::endl; }
  protected:
    static void setupLogging() {
        // create PatternLayout
        log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

        // create FileAppender
        LOG4CXX_DECODE_CHAR(fileName, "WindowManager.log");
        log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

        // create ConsoleAppender
        log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

        // set log level
        NESLogger->setLevel(log4cxx::Level::getDebug());
        //logger->setLevel(log4cxx::Level::getInfo());

        // add appenders and other will inherit the settings
        NESLogger->addAppender(file);
        NESLogger->addAppender(console);
    }
};

TEST_F(InternalDataTransmissionTest, testInternalTransmission) {
    string queryString = "InputQuery::from(default_logical).filter(default_logical[\"id\"] < 42).print(std::cout); ";

    InputQueryPtr query = UtilityFunctions::createQueryFromCodeString(queryString);
    OperatorPtr operatorTree = query->getRoot();

    CodeGeneratorPtr code_gen = createCodeGenerator();
    PipelineContextPtr context = createPipelineContext();

    // Parse operators
    operatorTree->produce(code_gen, context, std::cout);
    PipelineStagePtr stage = code_gen->compile(CompilerArgs());
    QueryExecutionPlanPtr qep1(new GeneratedQueryExecutionPlan(stage));

    DataSourcePtr source1 = createDefaultDataSourceWithSchemaForOneBuffer(query->getSourceStream()->getSchema());
    Schema sch1 = Schema::create().addField("sum", BasicType::UINT32);
    //DataSinkPtr sink1 = std::make_shared<ForwardSink>(query->source_stream->getSchema(), host, port);
    DataSinkPtr sink1 = createPrintSinkWithSchema(query->getSourceStream()->getSchema(), cout);
    qep1->addDataSource(source1);
    qep1->addDataSink(sink1);

    NodeEngine* ptr = new NodeEngine();
    ptr->deployQueryWithoutStart(qep1);
    ptr->start();
    source1->start();

    sleep(1);
    ptr->stop();
}

TEST_F(InternalDataTransmissionTest, testInputGate) {
    // the thread for the InputGate to receive data
    InputGate inputGate("*", 0);
    inputGate.setup();

    NES_INFO( "InputGate published with host:" << inputGate.getHost() << " port:" << inputGate.getPort())

    /**
    // Start thread for receiving the data.
    bool receivingFinished = false;
    auto inputGateThread = std::thread([&]() {
      // Receive data.
      tuple<std::string, TupleBufferPtr> rec = inputGate.receiveData();
      std::string sourceId = std::get<0>(rec);
      TupleBufferPtr buffer = std::get<1>(rec);

      // Test received data.
      size_t sum = 0;
      uint32_t* tuple = (uint32_t*)buffer->getBuffer();
      for (size_t i = 0; i != 8; ++i) {
          sum += *(tuple++);
      }
      size_t expected = 400;
      EXPECT_EQ(sum, expected);

      BufferManager::instance().releaseBuffer(buffer);
      receivingFinished = true;
    });
    size_t tupCnt = 8;

    // Open Publisher
    auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);
    zmq::context_t context(1);
    zmq::socket_t socket(context, ZMQ_PUSH);
    socket.connect(address);

    while (!receivingFinished) {
        // Send data from here.
        zmq::message_t message_tupleCnt(8);
        memcpy(message_tupleCnt.data(), &tupCnt, 8);
        socket.send(message_tupleCnt, ZMQ_SNDMORE);

        zmq::message_t message_data(test_data_size);
        memcpy(message_data.data(), test_data.data(), test_data_size);
        socket.send(message_data);
    }
    receiving_thread.join();
     **/
}
