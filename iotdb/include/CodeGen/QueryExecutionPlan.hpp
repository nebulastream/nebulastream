/*
 * QueryExecutionPlan.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_QUERYEXECUTIONPLAN_H_
#define INCLUDE_QUERYEXECUTIONPLAN_H_
#include <Runtime/DataSource.hpp>
#include <CodeGen/PipelineStage.hpp>
#include <vector>
#include <Runtime/DataSink.hpp>
#include <map>

namespace iotdb {

class QueryExecutionPlan {
public:
  virtual bool executeStage(uint32_t pipeline_stage_id, const TupleBuffer &buf);
  const std::vector<DataSourcePtr> getSources() const;
  virtual ~QueryExecutionPlan();

protected:
  QueryExecutionPlan();
  QueryExecutionPlan(const std::vector<DataSourcePtr> &_sources, 
          const std::vector<PipelineStagePtr> &_stages, 
          const std::map<uint32_t, DataSinkPtr> &_stage_to_sink);

  std::vector<DataSourcePtr> sources;
  std::vector<PipelineStagePtr> stages;
  std::map<uint32_t, DataSinkPtr> stage_to_sink;
};
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;
}

#endif /* INCLUDE_QUERYEXECUTIONPLAN_H_ */
