

#include <iostream>
#include <cassert>

#include <Core/DataTypes.hpp>

#include <Core/TupleBuffer.hpp>
#include <CodeGen/HandCodedQueryExecutionPlan.hpp>

#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/GeneratorSource.hpp>

#include <API/InputQuery.hpp>


namespace iotdb{



void createQuery()
{ 
  // define config
  Config config = Config::create().
                  withParallelism(1).
                  withPreloading().
                  withBufferSize(1000).
                  withNumberOfPassesOverInput(1);

  Schema schema = Schema::create().addField("",INT32);

  /** \brief create a source using the following functions:
  * const DataSourcePtr createTestSource();
  * const DataSourcePtr createBinaryFileSource(const Schema& schema, const std::string& path_to_file);
  * const DataSourcePtr createRemoteTCPSource(const Schema& schema, const std::string& server_ip, int port);
  */
  DataSourcePtr source = createTestSource();

  InputQuery::create(config, source)
      .filter(PredicatePtr())
      .orderBy(Sort(SortAttr{schema[0],ASCENDING}))
      .map(MapperPtr())
      //.join(InputQuery::create(config, createTestSource()), JoinPredicatePtr())
      .print(std::cout)
//      .window(createTumblingWindow())
      //.keyBy(Attributes())
      .printInputQueryPlan();

  //AttributeFieldPtr attr = schema[0];

}

void createQueryString(){

  std::stringstream code;
  code << "Config config = Config::create()."
          "        withParallelism(1)."
          "        withPreloading()."
          "        withBufferSize(1000)."
          "        withNumberOfPassesOverInput(1);" << std::endl;

  code << "Schema schema = Schema::create().addField(\"\",INT32);" << std::endl;

  code << "DataSourcePtr source = createTestSource();" << std::endl;

  code << "return InputQuery::create(config, source)" << std::endl
       << ".filter(PredicatePtr())" << std::endl
       <<   ".printInputQueryPlan();" << std::endl;

  InputQuery q(createQueryFromCodeString(code.str()));
}


}

int main(int argc, const char *argv[]) {

	iotdb::Dispatcher::instance();

	iotdb::createQuery();
	iotdb::createQueryString();

	return 0;

}
