/*
 * compiledTestPlan.hpp
 *
 *  Created on: Feb 19, 2019
 *      Author: zeuchste
 */

#ifndef TESTS_TESTPLANS_COMPILEDTESTPLAN_HPP_
#define TESTS_TESTPLANS_COMPILEDTESTPLAN_HPP_
#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <API/InputQuery.hpp>

namespace iotdb{


class CompiledTestQueryExecutionPlan : public HandCodedQueryExecutionPlan{
public:
    uint64_t count;
    uint64_t sum;
    CompiledTestQueryExecutionPlan()
        : HandCodedQueryExecutionPlan(), count(0), sum(0){


    	DataSourcePtr source = createTestSource();

        sources.push_back(source);
    }

    bool firstPipelineStage(const TupleBuffer&){
        return false;
    }

    bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf){
        uint64_t* tuples = (uint64_t*) buf->buffer;

        for(size_t i=0;i<buf->num_tuples;++i){
            count++;
            sum += tuples[i];
        }
        std::cout << "Processed Block:" << buf->num_tuples << " count: " << count << "sum: " << sum << std::endl;
        return true;
    }
};
typedef std::shared_ptr<CompiledTestQueryExecutionPlan> CompiledTestQueryExecutionPlanPtr;

}
#endif /* TESTS_TESTPLANS_COMPILEDTESTPLAN_HPP_ */
