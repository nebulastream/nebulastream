#ifndef INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_
#define INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_

#include <API/InputQuery.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <QueryCompiler/PipelineStage.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>

namespace NES {

class GeneratedQueryExecutionPlan : public QueryExecutionPlan {
  public:
    GeneratedQueryExecutionPlan();

    GeneratedQueryExecutionPlan(const std::string& queryId);

    /**
 * @brief Executes a pipeline state for a given input buffer.
 * @todo currently we assume that the pipeline is never generating more output data then input data.
 * @param query
 * @param ptr
 */
    bool executeStage(uint32_t pipelineStageId, TupleBuffer& inputBuffer) override;
};

typedef std::shared_ptr<GeneratedQueryExecutionPlan> GeneratedQueryExecutionPlanPtr;

}// namespace NES
#endif /* INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_ */
