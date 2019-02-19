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
class Functor{
public:
    Functor() : last_number(0){

    }

    TupleBuffer operator ()(uint64_t generated_tuples, uint64_t num_tuples_to_process){
        TupleBuffer buf = Dispatcher::instance().getBuffer();
        assert(buf.buffer!=NULL);
        uint64_t generated_tuples_this_pass=buf.buffer_size/sizeof(uint64_t);
        std::cout << generated_tuples << ", " << num_tuples_to_process << std::endl;
        generated_tuples_this_pass=std::min(num_tuples_to_process-generated_tuples,generated_tuples_this_pass);

        uint64_t* tuples = (uint64_t*) buf.buffer;
        for(uint64_t i=0;i<generated_tuples_this_pass;i++){
            tuples[i] = last_number++;
        }
        buf.tuple_size_bytes=sizeof(uint64_t);
        buf.num_tuples=generated_tuples_this_pass;
        return buf;
    }

    uint64_t last_number;
};

class CompiledTestQueryExecutionPlan : public HandCodedQueryExecutionPlan{
public:
    uint64_t count;
    uint64_t sum;
    CompiledTestQueryExecutionPlan()
        : HandCodedQueryExecutionPlan(), count(0), sum(0){

        DataSourcePtr source (
              new GeneratorSource<Functor>(
                Schema::create().addField(createField("id", UINT32)),
                100));
        sources.push_back(source);
    }

    bool firstPipelineStage(const TupleBuffer&){
        return false;
    }

    bool executeStage(uint32_t pipeline_stage_id, const TupleBuffer& buf){
        assert(pipeline_stage_id==1);
        uint64_t* tuples = (uint64_t*) buf.buffer;

        for(size_t i=0;i<buf.num_tuples;++i){
            count++;
            sum += tuples[i];
        }
        std::cout << "Processed Block:" << buf.num_tuples << " count: " << count << "sum: " << sum << std::endl;
        return true;
    }
};
typedef std::shared_ptr<CompiledTestQueryExecutionPlan> CompiledTestQueryExecutionPlanPtr;

}
#endif /* TESTS_TESTPLANS_COMPILEDTESTPLAN_HPP_ */
