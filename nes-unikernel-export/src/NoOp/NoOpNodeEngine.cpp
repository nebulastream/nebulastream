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
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <vector>
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
bool NES::Runtime::NodeEngine::deployQueryInNodeEngine(const Execution::ExecutableQueryPlanPtr&) {
    NES_THROW_RUNTIME_ERROR("deployQueryInNodeEngine not implemented in NoOpNodeEngine");
}

bool NES::Runtime::NodeEngine::registerQueryInNodeEngine(const QueryPlanPtr&) {
    NES_THROW_RUNTIME_ERROR("registerQueryInNodeEngine not implemented in NoOpNodeEngine");
}

bool NES::Runtime::NodeEngine::registerQueryInNodeEngine(const Execution::ExecutableQueryPlanPtr&) {
    NES_THROW_RUNTIME_ERROR("registerQueryInNodeEngine not implemented in NoOpNodeEngine");
}

bool NES::Runtime::NodeEngine::startQuery(QueryId) { NES_THROW_RUNTIME_ERROR("startQuery not implemented in NoOpNodeEngine"); }

bool NES::Runtime::NodeEngine::undeployQuery(QueryId) {
    NES_THROW_RUNTIME_ERROR("undeployQuery not implemented in NoOpNodeEngine");
}

bool NES::Runtime::NodeEngine::unregisterQuery(QueryId) {
    NES_THROW_RUNTIME_ERROR("unregisterQuery not implemented in NoOpNodeEngine");
}

bool NES::Runtime::NodeEngine::stopQuery(QueryId, NES::Runtime::QueryTerminationType) {
    NES_THROW_RUNTIME_ERROR("stopQuery not implemented in NoOpNodeEngine");
}

NES::Runtime::QueryManagerPtr NES::Runtime::NodeEngine::getQueryManager() {
    return std::make_shared<NES::Runtime::NoOpQueryManager>();
}

bool NES::Runtime::NodeEngine::stop(bool) { NES_THROW_RUNTIME_ERROR("stop not implemented in NoOpNodeEngine"); }

NES::Runtime::BufferManagerPtr NES::Runtime::NodeEngine::getBufferManager(uint32_t bufferManagerIndex) const {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void NES::Runtime::NodeEngine::injectEpochBarrier(uint64_t, uint64_t) const {
    NES_THROW_RUNTIME_ERROR("injectEpochBarrier not implemented in NoOpNodeEngine");
}

uint64_t NES::Runtime::NodeEngine::getNodeEngineId() { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

NES::Network::NetworkManagerPtr NES::Runtime::NodeEngine::getNetworkManager() { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

NES::AbstractQueryStatusListenerPtr NES::Runtime::NodeEngine::getQueryStatusListener() {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

NES::Runtime::HardwareManagerPtr NES::Runtime::NodeEngine::getHardwareManager() const {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

NES::Runtime::Execution::ExecutableQueryPlanStatus NES::Runtime::NodeEngine::getQueryStatus(QueryId) {
    NES_THROW_RUNTIME_ERROR("getQueryStatus not implemented in NoOpNodeEngine");
}

void NES::Runtime::NodeEngine::onDataBuffer(Network::NesPartition, TupleBuffer&) {
    // nop :: kept as legacy
}

void NES::Runtime::NodeEngine::onEvent(NES::Network::NesPartition, NES::Runtime::EventPtr) {
    // nop :: kept as legacy
}

void NES::Runtime::NodeEngine::onEndOfStream(Network::Messages::EndOfStreamMessage) {
    // nop :: kept as legacy
}

void NES::Runtime::NodeEngine::onServerError(NES::Network::Messages::ErrorMessage) {
    NES_THROW_RUNTIME_ERROR("onServerError not implemented in NoOpNodeEngine");
}

void NES::Runtime::NodeEngine::onChannelError(Network::Messages::ErrorMessage) {
    NES_THROW_RUNTIME_ERROR("onChannelError not implemented in NoOpNodeEngine");
}

std::vector<NES::Runtime::QueryStatisticsPtr> NES::Runtime::NodeEngine::getQueryStatistics(QueryId) {
    NES_THROW_RUNTIME_ERROR("getQueryStatistics not implemented in NoOpNodeEngine");
}

std::vector<NES::Runtime::QueryStatistics> NES::Runtime::NodeEngine::getQueryStatistics(bool) {
    NES_THROW_RUNTIME_ERROR("getQueryStatistics not implemented in NoOpNodeEngine");
}

NES::Network::PartitionManagerPtr NES::Runtime::NodeEngine::getPartitionManager() { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

std::vector<NES::QuerySubPlanId> NES::Runtime::NodeEngine::getSubQueryIds(uint64_t) {
    NES_THROW_RUNTIME_ERROR("getSubQueryIds not implemented in NoOpNodeEngine");
}

void NES::Runtime::NodeEngine::onFatalError(int signalNumber, std::string callstack) {
    NES_ERROR("onFatalError: signal [{}] error [{}] callstack {}", signalNumber, strerror(errno), callstack);
    std::cerr << "Runtime failed fatally" << std::endl;// it's necessary for testing and it wont harm us to write to stderr
    std::cerr << "Error: " << strerror(errno) << std::endl;
    std::cerr << "Signal: " << std::to_string(signalNumber) << std::endl;
    std::cerr << "Callstack:\n " << callstack << std::endl;
#ifdef ENABLE_CORE_DUMPER
    detail::createCoreDump();
#endif
}

void NES::Runtime::NodeEngine::onFatalException(const std::shared_ptr<std::exception> exception, std::string callstack) {
    NES_ERROR("onFatalException: exception=[{}] callstack={}", exception->what(), callstack);
    std::cerr << "Runtime failed fatally" << std::endl;
    std::cerr << "Error: " << strerror(errno) << std::endl;
    std::cerr << "Exception: " << exception->what() << std::endl;
    std::cerr << "Callstack:\n " << callstack << std::endl;
#ifdef ENABLE_CORE_DUMPER
    detail::createCoreDump();
#endif
}

std::shared_ptr<const NES::Runtime::Execution::ExecutableQueryPlan>
NES::Runtime::NodeEngine::getExecutableQueryPlan(uint64_t) const {
    NES_THROW_RUNTIME_ERROR("getExecutableQueryPlan not implemented in NoOpNodeEngine");
}

bool NES::Runtime::NodeEngine::bufferData(QuerySubPlanId, uint64_t) {
    NES_THROW_RUNTIME_ERROR("bufferData not implemented in NoOpNodeEngine");
}

bool NES::Runtime::NodeEngine::bufferAllData() { NES_THROW_RUNTIME_ERROR("bufferAllData not implemented in NoOpNodeEngine"); }

bool NES::Runtime::NodeEngine::stopBufferingAllData() {
    NES_THROW_RUNTIME_ERROR("stopBufferingAllData not implemented in NoOpNodeEngine");
}

bool NES::Runtime::NodeEngine::updateNetworkSink(uint64_t, const std::string&, uint32_t, QuerySubPlanId, uint64_t) {
    NES_THROW_RUNTIME_ERROR("updateNetworkSink not implemented in NoOpNodeEngine");
}

NES::Monitoring::MetricStorePtr NES::Runtime::NodeEngine::getMetricStore() { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

void NES::Runtime::NodeEngine::setMetricStore(Monitoring::MetricStorePtr) {
    NES_THROW_RUNTIME_ERROR("setMetricStore not implemented in NoOpNodeEngine");
}

NES::TopologyNodeId NES::Runtime::NodeEngine::getNodeId() const { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

void NES::Runtime::NodeEngine::setNodeId(const TopologyNodeId) {
    NES_THROW_RUNTIME_ERROR("setNodeId not implemented in NoOpNodeEngine");
}

#pragma clang diagnostic pop