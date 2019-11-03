

#include <cassert>
#include <iostream>

#include <Core/DataTypes.hpp>

#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <Core/TupleBuffer.hpp>

#include "../../include/Runtime/CompiledDummyPlan.hpp"
#include <Runtime/DataSource.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <NodeEngine/ThreadPool.hpp>

namespace iotdb {

class CompiledTestQueryExecutionPlan : public HandCodedQueryExecutionPlan {
  public:
    uint64_t count;
    uint64_t sum;
    CompiledTestQueryExecutionPlan() : HandCodedQueryExecutionPlan(), count(0), sum(0) {}

    void setDataSource(DataSourcePtr source) { sources.push_back(source); }
    bool firstPipelineStage(const TupleBuffer&) { return false; }

    bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf)
    {
        uint64_t* tuples = (uint64_t*)buf->buffer;

        for (size_t i = 0; i < buf->num_tuples; ++i) {
            count++;
            sum += tuples[i];
        }
        std::cout << "Processed Block:" << buf->num_tuples << " count: " << count << "sum: " << sum << std::endl;
        return true;
    }
};
typedef std::shared_ptr<CompiledTestQueryExecutionPlan> CompiledTestQueryExecutionPlanPtr;

int test()
{
    CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
    DataSourcePtr source = createTestSource();
    qep->setDataSource(source);

    Dispatcher::instance().registerQuery(qep);

    ThreadPool::instance().start(1);

    std::cout << "Waiting 2 seconds " << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(3));

    //  while(true)
    //    {
    //  	  std::this_thread::sleep_for(std::chrono::seconds(2));
    //  	  std::cout << "waiting to finish" << std::endl;
    //    }

    Dispatcher::instance().deregisterQuery(qep);

    ThreadPool::instance().stop();

    if (qep->sum == 512 && qep->count == 512) {
        std::cout << "Result Correct" << std::endl;
        return 0;
    }
    else {
        std::cerr << "Wrong Result: sum=" << qep->sum << ", count=" << qep->count << std::endl;
        return -1;
    }
}
} // namespace iotdb

int main(int argc, const char* argv[])
{

    iotdb::Dispatcher::instance();

    iotdb::test();

    return 0;
}
