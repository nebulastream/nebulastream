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
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/RemovePhysicalSourceEvent.hpp>
NES::RequestProcessor::RemovePhysicalSourceEventPtr
NES::RequestProcessor::RemovePhysicalSourceEvent::create(std::string logicalSourceName,
                                                         std::string physicalSourceName,
                                                         WorkerId workerId) {
    return std::make_shared<RemovePhysicalSourceEvent>(logicalSourceName, physicalSourceName, workerId);
}
NES::RequestProcessor::RemovePhysicalSourceEvent::RemovePhysicalSourceEvent(std::string logicalSourceName,
                                                                            std::string physicalSourceName,
                                                                            WorkerId workerId)
    : logicalSourceName(logicalSourceName), physicalSourceName(physicalSourceName), workerId(workerId) {}

NES::WorkerId NES::RequestProcessor::RemovePhysicalSourceEvent::getWorkerId() const {
    return workerId;
}
std::string NES::RequestProcessor::RemovePhysicalSourceEvent::getLogicalSourceName() const {
    return logicalSourceName;
}
std::string NES::RequestProcessor::RemovePhysicalSourceEvent::getPhysicalSourceName() const {
    return physicalSourceName;
}