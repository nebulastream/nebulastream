/*
     Licensed under the Apache License, Version 2.0 (the "License");
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at

         https://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
     limitations under the License.
*/

#ifndef NES_NOOPQUERYMANAGER_H
#define NES_NOOPQUERYMANAGER_H
#include <Runtime/QueryManager.hpp>

namespace NES::Runtime {
class NoOpQueryManager : public AbstractQueryManager {
  public:
    ExecutionResult processNextTask(bool running, WorkerContext& workerContext) override;
    void
    addWorkForNextPipeline(TupleBuffer& buffer, Execution::SuccessorExecutablePipeline executable, uint32_t queueId) override;
    void poisonWorkers() override;
    bool addReconfigurationMessage(QueryId queryId,
                                   QuerySubPlanId queryExecutionPlanId,
                                   const ReconfigurationMessage& reconfigurationMessage,
                                   bool blocking) override;
    uint64_t getNumberOfTasksInWorkerQueues() const override;

  protected:
    ExecutionResult terminateLoop(WorkerContext& context) override;

  public:
    bool registerQuery(const Execution::ExecutableQueryPlanPtr& qep) override;
    void postReconfigurationCallback(ReconfigurationMessage& task) override;
    void reconfigure(ReconfigurationMessage& message, WorkerContext& context) override;
    void destroy() override;

  protected:
    void updateStatistics(const Task& task,
                          QueryId queryId,
                          QuerySubPlanId subPlanId,
                          PipelineId pipelineId,
                          WorkerContext& workerContext) override;

  private:
    bool
    addReconfigurationMessage(QueryId queryId, QuerySubPlanId queryExecutionPlanId, TupleBuffer&& buffer, bool blocking) override;
    bool startThreadPool(uint64_t numberOfBuffersPerWorker) override;
};
}// namespace NES::Runtime
#endif//NES_NOOPQUERYMANAGER_H
