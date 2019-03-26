/*
 * compiledTestPlan.hpp
 *
 *  Created on: Feb 19, 2019
 *      Author: zeuchste
 */

#ifndef TESTS_TESTPLANS_COMPILEDDUMMYPLAN_HPP_
#define TESTS_TESTPLANS_COMPILEDDUMMYPLAN_HPP_
#include <API/InputQuery.hpp>
#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <memory>

namespace iotdb {

class CompiledDummyPlan : public HandCodedQueryExecutionPlan {
  public:
    uint64_t count;
    uint64_t sum;
    CompiledDummyPlan() : HandCodedQueryExecutionPlan(), count(0), sum(0) {}

    bool firstPipelineStage(const TupleBuffer&) { return false; }

    bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf) { return true; }
};
typedef std::shared_ptr<CompiledDummyPlan> CompiledDummyPlanPtr;

};     // namespace iotdb
#endif /* TESTS_TESTPLANS_COMPILEDTESTPLAN_HPP_ */
