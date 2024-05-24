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
#include <Util/TestHarness/TestHarnessWorkerConfiguration.hpp>

namespace NES {

TestHarnessWorkerConfigurationPtr TestHarnessWorkerConfiguration::create(WorkerConfigurationPtr workerConfiguration,
                                                                         WorkerId workerId) {
    return std::make_shared<TestHarnessWorkerConfiguration>(
        TestHarnessWorkerConfiguration(std::move(workerConfiguration), TestHarnessWorkerSourceType::NonSource, workerId));
}

TestHarnessWorkerConfigurationPtr TestHarnessWorkerConfiguration::create(WorkerConfigurationPtr workerConfiguration,
                                                                         std::string logicalSourceName,
                                                                         std::string physicalSourceName,
                                                                         TestHarnessWorkerSourceType sourceType,
                                                                         WorkerId workerId) {
    return std::make_shared<TestHarnessWorkerConfiguration>(TestHarnessWorkerConfiguration(std::move(workerConfiguration),
                                                                                           std::move(logicalSourceName),
                                                                                           std::move(physicalSourceName),
                                                                                           sourceType,
                                                                                           workerId));
}

TestHarnessWorkerConfiguration::TestHarnessWorkerConfiguration(WorkerConfigurationPtr workerConfiguration,
                                                               TestHarnessWorkerSourceType sourceType,
                                                               WorkerId workerId)
    : workerConfiguration(std::move(workerConfiguration)), sourceType(sourceType), workerId(workerId){};

TestHarnessWorkerConfiguration::TestHarnessWorkerConfiguration(WorkerConfigurationPtr workerConfiguration,
                                                               std::string logicalSourceName,
                                                               std::string physicalSourceName,
                                                               TestHarnessWorkerSourceType sourceType,
                                                               WorkerId workerId)
    : workerConfiguration(std::move(workerConfiguration)), logicalSourceName(std::move(logicalSourceName)),
      physicalSourceName(std::move(physicalSourceName)), sourceType(sourceType), workerId(workerId){};

void TestHarnessWorkerConfiguration::setQueryStatusListener(const NesWorkerPtr& nesWorker) { this->nesWorker = nesWorker; }

PhysicalSourceTypePtr TestHarnessWorkerConfiguration::getPhysicalSourceType() const { return physicalSource; }

const WorkerConfigurationPtr& TestHarnessWorkerConfiguration::getWorkerConfiguration() const { return workerConfiguration; }

TestHarnessWorkerConfiguration::TestHarnessWorkerSourceType TestHarnessWorkerConfiguration::getSourceType() const {
    return sourceType;
}
void TestHarnessWorkerConfiguration::setPhysicalSourceType(PhysicalSourceTypePtr physicalSource) {
    this->physicalSource = physicalSource;
}
const std::vector<uint8_t*>& TestHarnessWorkerConfiguration::getRecords() const { return records; }

void TestHarnessWorkerConfiguration::addRecord(uint8_t* record) { records.push_back(record); }

WorkerId TestHarnessWorkerConfiguration::getWorkerId() const { return workerId; }

const std::string& TestHarnessWorkerConfiguration::getLogicalSourceName() const { return logicalSourceName; }

const std::string& TestHarnessWorkerConfiguration::getPhysicalSourceName() const { return physicalSourceName; }
const NesWorkerPtr& TestHarnessWorkerConfiguration::getNesWorker() const { return nesWorker; }
}// namespace NES
