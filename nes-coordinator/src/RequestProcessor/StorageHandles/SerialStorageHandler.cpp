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
#include <Catalogs/Topology/Topology.hpp>
#include <RequestProcessor/StorageHandles/SerialStorageHandler.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <memory>
#include <utility>

namespace NES::RequestProcessor {

SerialStorageHandler::SerialStorageHandler(StorageDataStructures& storageDataStructures)
    : coordinatorConfiguration(std::move(storageDataStructures.coordinatorConfiguration)),
      topology(std::move(storageDataStructures.topology)),
      globalExecutionPlan(std::move(storageDataStructures.globalExecutionPlan)),
      globalQueryPlan(std::move(storageDataStructures.globalQueryPlan)),
      queryCatalog(std::move(storageDataStructures.queryCatalog)), sourceCatalog(std::move(storageDataStructures.sourceCatalog)),
      udfCatalog(std::move(storageDataStructures.udfCatalog)), amendmentQueue(std::move(storageDataStructures.amendmentQueue)),
      statisticProbeHandler(std::move(storageDataStructures.statisticProbeHandler)) {}

StorageHandlerPtr SerialStorageHandler::create(StorageDataStructures storageDataStructures) {
    return std::make_shared<SerialStorageHandler>(storageDataStructures);
}

Optimizer::GlobalExecutionPlanPtr SerialStorageHandler::getGlobalExecutionPlanHandle(const RequestId) {
    return {&*globalExecutionPlan, UnlockDeleter()};
}

TopologyHandle SerialStorageHandler::getTopologyHandle(const RequestId) { return {&*topology, UnlockDeleter()}; }

QueryCatalogHandle SerialStorageHandler::getQueryCatalogHandle(const RequestId) { return {&*queryCatalog, UnlockDeleter()}; }

GlobalQueryPlanHandle SerialStorageHandler::getGlobalQueryPlanHandle(const RequestId) {
    return {&*globalQueryPlan, UnlockDeleter()};
}

Catalogs::Source::SourceCatalogPtr SerialStorageHandler::getSourceCatalogHandle(const RequestId) {
    return {&*sourceCatalog, UnlockDeleter()};
}

Catalogs::UDF::UDFCatalogPtr SerialStorageHandler::getUDFCatalogHandle(const RequestId) {
    return {&*udfCatalog, UnlockDeleter()};
}

Configurations::CoordinatorConfigurationPtr SerialStorageHandler::getCoordinatorConfiguration(const RequestId) {
    return {&*coordinatorConfiguration, UnlockDeleter()};
}

Optimizer::UMPMCAmendmentQueuePtr SerialStorageHandler::getAmendmentQueue() { return {&*amendmentQueue, UnlockDeleter()}; }

Statistic::StatisticProbeHandlerPtr SerialStorageHandler::getStatisticProbeHandler(RequestId) {
    return {&*statisticProbeHandler, UnlockDeleter()};
}
}// namespace NES::RequestProcessor
