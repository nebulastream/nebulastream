#ifndef INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_
#define INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_

#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <QueryCompiler/PipelineStage.hpp>
#include <API/InputQuery.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <stdint.h>
namespace NES {

class GeneratedQueryExecutionPlan : public QueryExecutionPlan {
 public:
  GeneratedQueryExecutionPlan();

  GeneratedQueryExecutionPlan(const std::string& queryId, std::vector<PipelineStagePtr> ptr);

  /**
 * @brief Executes a pipeline state for a given input buffer.
 * @todo currently we assume that the pipeline is never generating more output data then input data.
 * @param query
 * @param ptr
 */
  bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf) override;
 protected:
  std::vector<PipelineStagePtr> pipelineStages;
};

typedef std::shared_ptr<GeneratedQueryExecutionPlan> GeneratedQueryExecutionPlanPtr;

}
#endif /* INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_ */
