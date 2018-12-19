/*
 * Task.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_TASK_H_
#define INCLUDE_TASK_H_


class Task{
public:
    Task(QueryExecutionPlanPtr _qep, uint32_t _pipeline_stage_id, DataSource* _source, const TupleBuffer& _buf);
    bool execute();
private:
    uint32_t pipeline_stage_id;
    QueryExecutionPlanPtr qep;
    DataSource* source;
    const TupleBuffer buf;
};

Task::Task(QueryExecutionPlanPtr _qep,
           uint32_t _pipeline_stage_id,
           DataSource* _source,
           const TupleBuffer& _buf)
    : qep(_qep),
      pipeline_stage_id(_pipeline_stage_id),
      source(_source),
      buf(_buf){

}


#endif /* INCLUDE_TASK_H_ */
