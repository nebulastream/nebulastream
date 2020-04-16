#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/Task.hpp>

namespace NES {

Task::Task(QueryExecutionPlanPtr _qep, uint32_t _pipeline_stage_id, TupleBuffer& buffer)
    : qep(_qep),
      pipeline_stage_id(_pipeline_stage_id),
      buf(buffer) {
}

bool Task::execute() {
    return qep->executeStage(pipeline_stage_id, buf);
}

size_t Task::getNumberOfTuples() {
    return buf.getNumberOfTuples();
}

}  // namespace NES
