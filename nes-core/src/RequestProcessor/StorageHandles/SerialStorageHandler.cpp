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
#include <RequestProcessor/StorageHandles/SerialStorageHandler.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <Topology/Topology.hpp>
#include <memory>
#include <utility>

namespace NES::RequestProcessor::Experimental {

SerialStorageHandler::SerialStorageHandler(StorageDataStructures storageDataStructures)
    : coordinatorConfiguration(std::move(storageDataStructures.coordinatorConfiguration)),
      topology(std::move(storageDataStructures.topology)),
      globalExecutionPlan(std::move(storageDataStructures.globalExecutionPlan)),
      queryCatalogService(std::move(storageDataStructures.queryCatalogService)),
      globalQueryPlan(std::move(storageDataStructures.globalQueryPlan)),
      sourceCatalog(std::move(storageDataStructures.sourceCatalog)), udfCatalog(std::move(storageDataStructures.udfCatalog)) {}

StorageHandlerPtr SerialStorageHandler::create(const StorageDataStructures& storageDataStructures) {
    return std::make_shared<SerialStorageHandler>(SerialStorageHandler(storageDataStructures));
}

GlobalExecutionPlanHandle SerialStorageHandler::getGlobalExecutionPlanHandle(const RequestId) {
    return {&*globalExecutionPlan, UnlockDeleter()};
}

TopologyHandle SerialStorageHandler::getTopologyHandle(const RequestId) { return {&*topology, UnlockDeleter()}; }

QueryCatalogServiceHandle SerialStorageHandler::getQueryCatalogServiceHandle(const RequestId) {
    return {&*queryCatalogService, UnlockDeleter()};
}

GlobalQueryPlanHandle SerialStorageHandler::getGlobalQueryPlanHandle(const RequestId) {
    return {&*globalQueryPlan, UnlockDeleter()};
}

SourceCatalogHandle SerialStorageHandler::getSourceCatalogHandle(const RequestId) { return {&*sourceCatalog, UnlockDeleter()}; }

UDFCatalogHandle SerialStorageHandler::getUDFCatalogHandle(const RequestId) { return {&*udfCatalog, UnlockDeleter()}; }

CoordinatorConfigurationHandle SerialStorageHandler::getCoordinatorConfiguration(const RequestId) {
    return {&*coordinatorConfiguration, UnlockDeleter()};
}
}// namespace NES::RequestProcessor::Experimental
