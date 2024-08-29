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
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/RemoveAllPhysicalSourcesByWorkerEvent.hpp>

NES::RequestProcessor::RemoveAllPhysicalSourcesByWorkerEventPtr
NES::RequestProcessor::RemoveAllPhysicalSourcesByWorkerEvent::create(WorkerId workerId) {
    return std::make_shared<RemoveAllPhysicalSourcesByWorkerEvent>(workerId);
}

NES::RequestProcessor::RemoveAllPhysicalSourcesByWorkerEvent::RemoveAllPhysicalSourcesByWorkerEvent(WorkerId workerId)
    : workerId(workerId) {}

NES::WorkerId NES::RequestProcessor::RemoveAllPhysicalSourcesByWorkerEvent::getWorkerId() const { return workerId; }
