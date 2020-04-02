#ifndef INCLUDE_HANDCODEDQUERYEXECUTIONPLAN_H_
#define INCLUDE_HANDCODEDQUERYEXECUTIONPLAN_H_

#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <stdint.h>
namespace NES {

class HandCodedQueryExecutionPlan : public QueryExecutionPlan {
  public:
    HandCodedQueryExecutionPlan();
    virtual ~HandCodedQueryExecutionPlan();
    virtual bool executeStage(uint32_t pipeline_stage_id, TupleBuffer& buf) = 0;
};
}
#endif /* INCLUDE_HANDCODEDQUERYEXECUTIONPLAN_H_ */
