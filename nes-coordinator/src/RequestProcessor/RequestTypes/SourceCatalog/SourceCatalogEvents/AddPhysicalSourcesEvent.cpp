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
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddPhysicalSourcesEvent.hpp>
namespace NES::RequestProcessor {

bool PhysicalSourceDefinition::operator==(const PhysicalSourceDefinition& other) const {
    return logicalSourceName == other.logicalSourceName && physicalSourceName == other.physicalSourceName;
}
AddPhysicalSourcesResponse::AddPhysicalSourcesResponse(bool success, std::vector<std::string> succesfulAdditions)
    : AddPhysicalSourcesResponse(success, succesfulAdditions, std::nullopt) {}

std::vector<std::string> AddPhysicalSourcesResponse::getSuccesfulAdditions() const {
    return succesfulAdditions;
}

std::optional<std::string> AddPhysicalSourcesResponse::getFailedAddition() const {
    return failed;
}

AddPhysicalSourcesResponse::AddPhysicalSourcesResponse(bool success, std::vector<std::string> successfulAdditions, std::optional<std::string> failed)
    : SourceCatalogResponse(success), succesfulAdditions(successfulAdditions), failed(failed) {}

AddPhysicalSourcesEventPtr AddPhysicalSourcesEvent::create(std::vector<PhysicalSourceDefinition> physicalSources,
                                                           WorkerId workerId) {
    return std::make_shared<AddPhysicalSourcesEvent>(physicalSources, workerId);
}
AddPhysicalSourcesEvent::AddPhysicalSourcesEvent(
    std::vector<PhysicalSourceDefinition> physicalSources,
    WorkerId workerId)
    : physicalSources(physicalSources), workerId(workerId) {}

std::vector<PhysicalSourceDefinition>
AddPhysicalSourcesEvent::getPhysicalSources() const {
    return physicalSources;
}

WorkerId AddPhysicalSourcesEvent::getWorkerId() const {
    return workerId;
}
}// namespace NES::RequestProcessor