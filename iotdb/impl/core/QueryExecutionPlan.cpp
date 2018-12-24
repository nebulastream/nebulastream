/*
 * QueryExecutionPlan.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <core/QueryExecutionPlan.hpp>

QueryExecutionPlan::QueryExecutionPlan() : sources(), stages() {}

QueryExecutionPlan::QueryExecutionPlan(const std::vector<DataSourcePtr> &_sources,
                                       const std::vector<PipelineStagePtr> &_stages)
    : sources(_sources), stages(_stages) {}

QueryExecutionPlan::~QueryExecutionPlan(){

}

const std::vector<DataSourcePtr> QueryExecutionPlan::getSources() const { return sources; }

bool QueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, const TupleBuffer &buf) {
  bool ret = stages[pipeline_stage_id]->execute(buf);
  return ret;
}
