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
#include <WorkQueues/SerialStorageAccessHandle.hpp>
#include <memory>
#include <Topology/Topology.hpp>
namespace NES {

SerialStorageAccessHandle::SerialStorageAccessHandle(TopologyPtr topology) : StorageAccessHandle(topology) {}

std::shared_ptr<SerialStorageAccessHandle> SerialStorageAccessHandle::create(const TopologyPtr& topology) {
    return std::make_shared<SerialStorageAccessHandle>(topology);
}


bool SerialStorageAccessHandle::requiresRollback() {
    return true;
}

TopologyHandle SerialStorageAccessHandle::getTopologyHandle() {
    return {&*topology, UnlockDeleter()};
}

QueryCatalogHandle SerialStorageAccessHandle::getQueryCatalogHandle() {
    return {};
}

SourceCatalogHandle SerialStorageAccessHandle::getSourceCatalogHandle() {
    return NES::SourceCatalogHandle();
}

GlobalExecutionPlanHandle SerialStorageAccessHandle::getGlobalExecutionPlanHandle() {
    return NES::GlobalExecutionPlanHandle();
}

GlobalQueryPlanHandle SerialStorageAccessHandle::getGlobalQueryPlanHandle() {
    return NES::GlobalQueryPlanHandle();
}
}
