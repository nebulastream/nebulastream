#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <CodeGen/QueryExecutionPlan.hpp>
#include <Core/DataTypes.hpp>
#include <Core/TupleBuffer.hpp>
#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/ThreadPool.hpp>
#include <assert.h>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>

#include <API/Config.hpp>
#include <Core/Schema.hpp>

#include "include/API/InputQuery.hpp"
#include <design.hpp>

using namespace std;

namespace iotdb {

void generate_LLVM_AST();

class Functor {
  public:
    Functor() : last_number(0) {}
    TupleBuffer operator()(uint64_t generated_tuples, uint64_t num_tuples_to_process)
    {
        TupleBuffer buf = Dispatcher::instance().getBuffer();
        assert(buf.buffer != NULL);
        uint64_t generated_tuples_this_pass = buf.buffer_size / sizeof(uint64_t);
        std::cout << generated_tuples << ", " << num_tuples_to_process << std::endl;
        generated_tuples_this_pass = std::min(num_tuples_to_process - generated_tuples, generated_tuples_this_pass);

        uint64_t* tuples = (uint64_t*)buf.buffer;
        for (uint64_t i = 0; i < generated_tuples_this_pass; i++) {
            tuples[i] = last_number++;
        }
        buf.tuple_size_bytes = sizeof(uint64_t);
        buf.num_tuples = generated_tuples_this_pass;
        return buf;
    }

    uint64_t last_number;
};

class CompiledYSBTestQueryExecutionPlan : public HandCodedQueryExecutionPlan {
  public:
    uint64_t count;
    uint64_t sum;
    CompiledYSBTestQueryExecutionPlan() : HandCodedQueryExecutionPlan(), count(0), sum(0)
    {
        Schema s = Schema::create().addField(createField("val", UINT64));
        DataSourcePtr source(new GeneratorSource<Functor>(s, 100));
        sources.push_back(source);
    }

    bool firstPipelineStage(const TupleBuffer&) { return false; }

    bool executeStage(uint32_t pipeline_stage_id, const TupleBuffer& buf)
    {
        assert(pipeline_stage_id == 1);
        uint64_t* tuples = static_cast<uint64_t*>(buf.buffer);

        for (uint64_t i = 0; i < buf.num_tuples; ++i) {
            count++;
            sum += tuples[i];
        }
        std::cout << "Processed Block:" << buf.num_tuples << " count: " << count << "sum: " << sum << std::endl;
        return true;
    }
};

DataSourcePtr createGeneratorDataSource()
{
    Schema s = Schema::create().addField(createField("val", UINT64));
    return std::make_shared<GeneratorSource<Functor>>(s, 100);
}

void test()
{

    iotdb::InputQuery::create(iotdb::Config::create(), iotdb::Schema::create().addField(createField("val", UINT64)),
                              createGeneratorDataSource())
        .filter(PredicatePtr())
        .execute();

    QueryExecutionPlanPtr qep(new CompiledYSBTestQueryExecutionPlan());

    Dispatcher::instance().registerQuery(qep);

    ThreadPool thread_pool;

    thread_pool.start();

    std::cout << "Waiting 2 seconds " << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Dispatcher::instance().deregisterQuery(qep);
}
} // namespace iotdb

int main(int argc, const char* argv[])
{

    iotdb::Dispatcher::instance();

    iotdb::test();

    iotdb::test_design();
}
