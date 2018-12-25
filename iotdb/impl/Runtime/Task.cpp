/*
 * Task.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <Runtime/DataSource.hpp>
#include <Runtime/Task.hpp>
#include <CodeGen/QueryExecutionPlan.hpp>
#include <Core/TupleBuffer.hpp>

namespace iotdb {

Task::Task(QueryExecutionPlanPtr _qep, uint32_t _pipeline_stage_id, DataSource *_source, const TupleBuffer &_buf)
    : qep(_qep), pipeline_stage_id(_pipeline_stage_id), source(_source), buf(_buf) {}

bool Task::execute() { return qep->executeStage(pipeline_stage_id, buf); }
}
