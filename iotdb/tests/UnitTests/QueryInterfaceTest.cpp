#include "gtest/gtest.h"

#include <cassert>
#include <iostream>
#include <Util/Logger.hpp>
#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>
#include <NodeEngine/Dispatcher.hpp>

#include <API/InputQuery.hpp>
#include <API/UserAPIExpression.hpp>
#include <API/Environment.hpp>
#include <API/Types/DataTypes.hpp>
#include <Util/UtilityFunctions.hpp>
namespace NES {

class SelectionDataGenFunctor {
 public:
  SelectionDataGenFunctor() {
  }

  struct __attribute__((packed)) InputTuple {
    uint32_t id;
    uint32_t value;
  };

  TupleBufferPtr operator()() {
    // 10 tuples of size one
    TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
    uint64_t tupleCnt = buf->getNumberOfTuples();

    assert(buf->getBuffer() != NULL);

    InputTuple *tuples = (InputTuple*) buf->getBuffer();
    for (uint32_t i = 0; i < tupleCnt; i++) {
      tuples[i].id = i;
      tuples[i].value = i * 2;
    }
    buf->setTupleSizeInBytes(sizeof(InputTuple));
    buf->setNumberOfTuples(tupleCnt);
    return buf;
  }
};

class QueryInterfaceTest : public testing::Test {
 public:

  static void SetUpTestCase() {
    NES::setupLogging("QueryInterfaceTest.log", NES::LOG_DEBUG);
    NES_INFO("Setup QueryInterfaceTest test class.");
  }

  static void TearDownTestCase() {
    std::cout << "Tear down QueryInterfaceTest test class." << std::endl;
  }

  void TearDown() {
  }
};

TEST_F(QueryInterfaceTest, testQueryFilter) {
  // define config
  Config config = Config::create();

  Environment env = Environment::create(config);

  SchemaPtr schema = SchemaTemp::create()->addField("id", BasicType::UINT32)->addField(
      "value", BasicType::UINT64);

  Stream def = Stream("default_logical", schema);

  InputQuery &query = InputQuery::from(def).filter(def["value"] > 42)
      .windowByKey(
      def["value"].getAttributeField(),
      TumblingWindow::of(TimeCharacteristic::ProcessingTime, Seconds(10)),
      Sum::on(def["value"])).print(std::cout);

  env.printInputQueryPlan(query);
  env.executeQuery(query);
}

TEST_F(QueryInterfaceTest, testQueryMap) {
  // define config
  Config config = Config::create();

  Environment env = Environment::create(config);

//    Config::create().withParallelism(1).withPreloading().withBufferSize(1000).withNumberOfPassesOverInput(1);
  SchemaPtr schema = SchemaTemp::create()->addField("id", BasicType::UINT32)->addField(
      "value", BasicType::UINT64);

  Stream def = Stream("default", schema);

  AttributeField mappedField("id", BasicType::UINT64);

  InputQuery &query = InputQuery::from(def).map(*schema->get(0),
                                                def["value"] + schema->get(1)).print(
      std::cout);
  env.printInputQueryPlan(query);
  env.executeQuery(query);
}

TEST_F(QueryInterfaceTest, testQueryString) {

  std::stringstream code;

  code << "Schema schema = Schema::create()->addField(\"test\",INT32);"
       << std::endl;
  code << "Stream testStream = Stream(\"test-stream\",schema);" << std::endl;
  code
      << "InputQuery::from(default_stream).map(default_stream[\"value\"],2).filter(default_stream[\"test\"]==5)"
      << std::endl << "" << std::endl << ";" << std::endl;

  try{
    InputQueryPtr inputQuery = UtilityFunctions::createQueryFromCodeString(
          code.str());
  }
  catch(...)
  {
    SUCCEED();
  }

}

}  // namespace NES

