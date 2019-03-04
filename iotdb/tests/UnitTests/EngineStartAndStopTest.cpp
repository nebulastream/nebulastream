

#include <cassert>
#include <iostream>

#include <Core/DataTypes.hpp>

#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <Core/TupleBuffer.hpp>

#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/compiledTestPlan.hpp>

namespace iotdb {

int test() {
  CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source = createTestSource();
  qep->setDataSource(source);

  Dispatcher::instance().registerQuery(qep);

  ThreadPool thread_pool;

  thread_pool.start();

  std::cout << "Waiting 2 seconds " << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(3));

//  while(true)
//    {
//  	  std::this_thread::sleep_for(std::chrono::seconds(2));
//  	  std::cout << "waiting to finish" << std::endl;
//    }

  Dispatcher::instance().deregisterQuery(qep);

  thread_pool.stop();

  if (qep->sum == 512 && qep->count == 512) {
    std::cout << "Result Correct" << std::endl;
    return 0;
  } else {
    std::cerr << "Wrong Result: sum=" << qep->sum << ", count=" << qep->count << std::endl;
    return -1;
  }

}
} // namespace iotdb


int main(int argc, const char *argv[]) {

  iotdb::Dispatcher::instance();

  iotdb::test();


  return 0;
}
