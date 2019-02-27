/*
 * QueryExecutionPlan.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <CodeGen/QueryExecutionPlan.hpp>

namespace iotdb {

QueryExecutionPlan::QueryExecutionPlan() : sources(), stages() {}

QueryExecutionPlan::QueryExecutionPlan(const std::vector<DataSourcePtr> &_sources,
                                       const std::vector<PipelineStagePtr> &_stages,
                                       const std::map<DataSource *, uint32_t> &_source_to_stage,
                                       const std::map<uint32_t , uint32_t> &_stage_to_dest)
    : sources(_sources), stages(_stages), source_to_stage(_source_to_stage), stage_to_dest(_stage_to_dest) {}

QueryExecutionPlan::~QueryExecutionPlan() {}

uint32_t QueryExecutionPlan::stageIdFromSource(DataSource * source) { return source_to_stage[source]; } ;

const std::vector<DataSourcePtr> QueryExecutionPlan::getSources() const { return sources; }

bool QueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, const TupleBuffer &buf) {
  WindowState* state;


  void *buffer = malloc(buf.buffer_size);
  TupleBuffer result_buf{buffer,0,0,0};


  std::vector<TupleBuffer*> v;
  v.push_back((TupleBuffer*)&buf);

  bool ret = stages[pipeline_stage_id]->execute(v,state, &result_buf);


  free(result_buf.buffer);

  return ret;
}
}
