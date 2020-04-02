#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/Task.hpp>

#include "../../include/NodeEngine/TupleBuffer.hpp"
#include "../../include/SourceSink/DataSource.hpp"

namespace NES {

Task::Task(QueryExecutionPlanPtr _qep, uint32_t _pipeline_stage_id,
           const TupleBufferPtr pBuf)
    : qep(_qep),
      pipeline_stage_id(_pipeline_stage_id),
      buf(pBuf) {
}

bool Task::execute() {
  return qep->executeStage(pipeline_stage_id, buf);
}

void Task::releaseInputBuffer() {
  NES_DEBUG("Task::releaseInputBuffer buf=" << buf)
  BufferManager::instance().releaseFixedSizeBuffer(buf);
}

size_t Task::getNumberOfTuples() {
    return buf->getNumberOfTuples();
  }

}  // namespace NES
