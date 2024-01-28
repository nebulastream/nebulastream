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

#include <NoOp/NoOpQueryManager.h>
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
bool NES::Runtime::AbstractQueryManager::registerQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

bool NES::Runtime::AbstractQueryManager::startQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

bool NES::Runtime::AbstractQueryManager::deregisterQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

bool NES::Runtime::AbstractQueryManager::canTriggerEndOfStream(DataSourcePtr source,
                                                               Runtime::QueryTerminationType terminationType) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

bool NES::Runtime::AbstractQueryManager::failQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

bool NES::Runtime::AbstractQueryManager::stopQuery(const Execution::ExecutableQueryPlanPtr& qep,
                                                   Runtime::QueryTerminationType type) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

bool NES::Runtime::AbstractQueryManager::addSoftEndOfStream(DataSourcePtr source) { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

bool NES::Runtime::AbstractQueryManager::addHardEndOfStream(DataSourcePtr source) { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

bool NES::Runtime::AbstractQueryManager::addFailureEndOfStream(DataSourcePtr source) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

bool NES::Runtime::AbstractQueryManager::addEpochPropagation(DataSourcePtr source, uint64_t queryId, uint64_t epochBarrier) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

bool NES::Runtime::AbstractQueryManager::addEndOfStream(DataSourcePtr source, Runtime::QueryTerminationType terminationType) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}
void NES::Runtime::AbstractQueryManager::notifyQueryStatusChange(const Execution::ExecutableQueryPlanPtr& qep,
                                                                 Execution::ExecutableQueryPlanStatus status) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}
void NES::Runtime::AbstractQueryManager::notifySourceFailure(DataSourcePtr failedSource, const std::string reason) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::AbstractQueryManager::notifyTaskFailure(Execution::SuccessorExecutablePipeline pipelineOrSink,
                                                           const std::string& errorMessage) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::AbstractQueryManager::notifySourceCompletion(DataSourcePtr source, QueryTerminationType terminationType) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::AbstractQueryManager::notifyPipelineCompletion(QuerySubPlanId subPlanId,
                                                                  Execution::ExecutablePipelinePtr pipeline,
                                                                  QueryTerminationType terminationType) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}
uint64_t NES::Runtime::AbstractQueryManager::getNumberOfWorkerThreads() { NES_THROW_RUNTIME_ERROR("Not Implemented"); }
void NES::Runtime::AbstractQueryManager::notifySinkCompletion(QuerySubPlanId subPlanId,
                                                              DataSinkPtr sink,
                                                              QueryTerminationType terminationType) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}
uint64_t NES::Runtime::AbstractQueryManager::getQueryId(uint64_t querySubPlanId) const {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}
uint64_t NES::Runtime::AbstractQueryManager::getCurrentTaskSum() { NES_THROW_RUNTIME_ERROR("Not Implemented"); }
uint64_t NES::Runtime::AbstractQueryManager::getNumberOfBuffersPerEpoch() const { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

NES::ExecutionResult NES::Runtime::AbstractQueryManager::processNextTask(bool, NES::Runtime::WorkerContext&) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::AbstractQueryManager::addWorkForNextPipeline(NES::Runtime::TupleBuffer&,
                                                                NES::Runtime::Execution::SuccessorExecutablePipeline,
                                                                uint32_t) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::AbstractQueryManager::postReconfigurationCallback(NES::Runtime::ReconfigurationMessage&) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::AbstractQueryManager::reconfigure(NES::Runtime::ReconfigurationMessage&, NES::Runtime::WorkerContext&) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::AbstractQueryManager::poisonWorkers() { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

void NES::Runtime::AbstractQueryManager::destroy() { /*NoOp*/
}

bool NES::Runtime::AbstractQueryManager::addReconfigurationMessage(NES::QueryId,
                                                                   NES::QuerySubPlanId,
                                                                   const NES::Runtime::ReconfigurationMessage&,
                                                                   bool) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

uint64_t NES::Runtime::AbstractQueryManager::getNumberOfTasksInWorkerQueues() const {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::AbstractQueryManager::updateStatistics(const NES::Runtime::Task&,
                                                          NES::QueryId,
                                                          NES::QuerySubPlanId,
                                                          NES::PipelineId,
                                                          NES::Runtime::WorkerContext&) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

NES::ExecutionResult NES::Runtime::AbstractQueryManager::terminateLoop(NES::Runtime::WorkerContext&) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

bool NES::Runtime::AbstractQueryManager::addReconfigurationMessage(NES::QueryId,
                                                                   NES::QuerySubPlanId,
                                                                   NES::Runtime::TupleBuffer&&,
                                                                   bool) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}
bool NES::Runtime::NoOpQueryManager::registerQuery(const NES::Runtime::Execution::ExecutableQueryPlanPtr&) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

NES::ExecutionResult NES::Runtime::NoOpQueryManager::processNextTask(bool, NES::Runtime::WorkerContext&) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::NoOpQueryManager::addWorkForNextPipeline(NES::Runtime::TupleBuffer&,
                                                            NES::Runtime::Execution::SuccessorExecutablePipeline,
                                                            uint32_t) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::NoOpQueryManager::postReconfigurationCallback(NES::Runtime::ReconfigurationMessage&) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::NoOpQueryManager::reconfigure(NES::Runtime::ReconfigurationMessage&, NES::Runtime::WorkerContext&) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::NoOpQueryManager::poisonWorkers() { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

void NES::Runtime::NoOpQueryManager::destroy() { /*NoOp*/
}

bool NES::Runtime::NoOpQueryManager::addReconfigurationMessage(NES::QueryId,
                                                               NES::QuerySubPlanId,
                                                               const NES::Runtime::ReconfigurationMessage&,
                                                               bool) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

uint64_t NES::Runtime::NoOpQueryManager::getNumberOfTasksInWorkerQueues() const { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

void NES::Runtime::NoOpQueryManager::updateStatistics(const NES::Runtime::Task&,
                                                      NES::QueryId,
                                                      NES::QuerySubPlanId,
                                                      NES::PipelineId,
                                                      NES::Runtime::WorkerContext&) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

NES::ExecutionResult NES::Runtime::NoOpQueryManager::terminateLoop(NES::Runtime::WorkerContext&) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

bool NES::Runtime::NoOpQueryManager::addReconfigurationMessage(NES::QueryId,
                                                               NES::QuerySubPlanId,
                                                               NES::Runtime::TupleBuffer&&,
                                                               bool) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}
bool NES::Runtime::NoOpQueryManager::startThreadPool(uint64_t numberOfBuffersPerWorker) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}
#pragma clang diagnostic pop