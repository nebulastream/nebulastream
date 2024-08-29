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

#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddKeyDistributionEntryEvent.hpp>

namespace NES::RequestProcessor {
AddKeyDistributionEntryResponse::AddKeyDistributionEntryResponse(bool success) : BaseUpdateSourceCatalogResponse(success) {}

NES::RequestProcessor::AddKeyDistributionEntryEventPtr
NES::RequestProcessor::AddKeyDistributionEntryEvent::create(std::string logicalSourceName,
                                                            std::string physicalSourceName,
                                                            WorkerId workerId,
                                                            std::string value) {
    return std::make_shared<AddKeyDistributionEntryEvent>(logicalSourceName, physicalSourceName, workerId, value);
}

NES::RequestProcessor::AddKeyDistributionEntryEvent::AddKeyDistributionEntryEvent(std::string logicalSourceName,
                                                                                  std::string physicalSourceName,
                                                                                  WorkerId workerId,
                                                                                  std::string value)
    : logicalSourceName(logicalSourceName), physicalSourceName(physicalSourceName), workerId(workerId), value(value) {}

std::string NES::RequestProcessor::AddKeyDistributionEntryEvent::getLogicalSourceName() const { return logicalSourceName; }
std::string NES::RequestProcessor::AddKeyDistributionEntryEvent::getPhysicalSourceName() const { return physicalSourceName; }
NES::WorkerId NES::RequestProcessor::AddKeyDistributionEntryEvent::getWorkerId() const { return workerId; }
std::string NES::RequestProcessor::AddKeyDistributionEntryEvent::getValue() const { return value; }
}// namespace NES::RequestProcessor
