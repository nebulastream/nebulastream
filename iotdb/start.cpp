#include <core/DataSource.hpp>
#include <core/TupleBuffer.hpp>
#include <core/Dispatcher.hpp>
#include <core/QueryExecutionPlan.hpp>
#include <core/HandCodedQueryExecutionPlan.hpp>
#include <core/GeneratorSource.hpp>
#include <string>
#include <assert.h>
#include <thread>
#include <core/ThreadPool.hpp>
#include <iostream>
#include <cstdint>

using namespace std;

class Functor {
public:
  Functor() : last_number(0) {}
  TupleBuffer operator()(uint64_t generated_tuples, uint64_t num_tuples_to_process) {
    TupleBuffer buf = Dispatcher::instance().getBuffer();
    assert(buf.buffer != NULL);
    uint64_t generated_tuples_this_pass = buf.buffer_size / sizeof(uint64_t);
    std::cout << generated_tuples << ", " << num_tuples_to_process << std::endl;
    generated_tuples_this_pass = std::min(num_tuples_to_process - generated_tuples, generated_tuples_this_pass);

    uint64_t *tuples = (uint64_t *)buf.buffer;
    for (uint64_t i = 0; i < generated_tuples_this_pass; i++) {
      tuples[i] = last_number++;
    }
    buf.tuple_size_bytes = sizeof(uint64_t);
    buf.num_tuples = generated_tuples_this_pass;
    return buf;
  }

  uint64_t last_number;
};

class CompiledTestQueryExecutionPlan : public HandCodedQueryExecutionPlan {
public:
  uint64_t count;
  uint64_t sum;
  CompiledTestQueryExecutionPlan() : HandCodedQueryExecutionPlan(), count(0), sum(0) {

    DataSourcePtr source(new GeneratorSource<Functor>(100));
    sources.push_back(source);
  }

  bool firstPipelineStage(const TupleBuffer &) { return false; }

  bool executeStage(uint32_t pipeline_stage_id, const TupleBuffer &buf) {
    assert(pipeline_stage_id == 1);
    uint64_t *tuples = static_cast<uint64_t *>(buf.buffer);

    for (uint64_t i = 0; i < buf.num_tuples; ++i) {
      count++;
      sum += tuples[i];
    }
    std::cout << "Processed Block:" << buf.num_tuples << " count: " << count << "sum: " << sum << std::endl;
    return true;
  }
};

void test() {
  QueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());

  Dispatcher::instance().registerQuery(qep);

  ThreadPool thread_pool;

  thread_pool.start();

  std::cout << "Waiting 2 seconds " << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Dispatcher::instance().deregisterQuery(qep);
}

int main(int argc, const char *argv[]) {

  Dispatcher::instance();

  test();

}
