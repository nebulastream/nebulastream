#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <API/InputQuery.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <Network/ZmqReceiver.hpp>
#include <Util/UtilityFunctions.hpp>


using namespace NES;
using namespace std;

class InternalDataTransmissionTest : public testing::Test {
 public:
  string host = "localhost";
  size_t port = 4788;

  static void SetUpTestCase() {
    setupLogging();
    NES_INFO("Setup InternalDataTransmissionTest test class.");
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

  DataSourcePtr source1 = createTestDataSourceWithSchema(query->source_stream->getSchema());
  Schema sch1 = Schema::create().addField("sum", BasicType::UINT32);
  //DataSinkPtr sink1 = std::make_shared<ForwardSink>(query->source_stream->getSchema(), host, port);
  DataSinkPtr sink1 = createPrintSinkWithSchema(query->source_stream->getSchema(), cout);
  qep1->addDataSource(source1);
  qep1->addDataSink(sink1);

  NodeEngine *ptr = new NodeEngine();
  ptr->deployQueryWithoutStart(qep1);
  ptr->start();
  source1->start();

  sleep(5);
  ptr->stop();
}