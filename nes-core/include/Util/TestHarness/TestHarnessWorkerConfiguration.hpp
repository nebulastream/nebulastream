/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_UTIL_TESTHARNESS_TESTHARNESSWORKERCONFIGURATION_HPP_
#define NES_INCLUDE_UTIL_TESTHARNESS_TESTHARNESSWORKERCONFIGURATION_HPP_

#include <API/Schema.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Nodes/Node.hpp>
#include <utility>

namespace NES {

class TestHarnessWorkerConfiguration;
using TestHarnessWorkerConfigurationPtr = std::shared_ptr<TestHarnessWorkerConfiguration>;

/**
 * @brief A class to keep Configurations of different nodes in the topology
 */
class TestHarnessWorkerConfiguration {

  public:
    enum TestHarnessWorkerSourceType { CSVSource, MemorySource, NonSource };

    static TestHarnessWorkerConfigurationPtr create(WorkerConfigurationPtr workerConfiguration, uint32_t workerId) {
        return std::make_shared<TestHarnessWorkerConfiguration>(
            TestHarnessWorkerConfiguration(std::move(workerConfiguration), NonSource, workerId));
    }

    static TestHarnessWorkerConfigurationPtr create(WorkerConfigurationPtr workerConfiguration,
                                                    std::string logicalSourceName,
                                                    std::string physicalSourceName,
                                                    TestHarnessWorkerSourceType sourceType,
                                                    uint32_t workerId) {
        return std::make_shared<TestHarnessWorkerConfiguration>(TestHarnessWorkerConfiguration(std::move(workerConfiguration),
                                                                                               std::move(logicalSourceName),
                                                                                               std::move(physicalSourceName),
                                                                                               sourceType,
                                                                                               workerId));
    }

    void setNesWorker(const NesWorkerPtr& nesWorker) { this->nesWorker = nesWorker; }

    const WorkerConfigurationPtr& getWorkerConfiguration() const { return workerConfiguration; }
    TestHarnessWorkerSourceType getSourceType() const { return sourceType; }
    const std::vector<uint8_t*>& getRecords() const { return records; }
    void addRecord(uint8_t* record) { records.push_back(record); }
    uint32_t getWorkerId() const { return workerId; }
    const std::string& getLogicalSourceName() const { return logicalSourceName; }
    const std::string& getPhysicalSourceName() const { return physicalSourceName; }
    const NesWorkerPtr& getNesWorker() const { return nesWorker; }

  private:
    TestHarnessWorkerConfiguration(WorkerConfigurationPtr workerConfiguration,
                                   std::string logicalSourceName,
                                   std::string physicalSourceName,
                                   TestHarnessWorkerSourceType sourceType,
                                   uint32_t workerId)
        : workerConfiguration(std::move(workerConfiguration)), logicalSourceName(std::move(logicalSourceName)),
          physicalSourceName(std::move(physicalSourceName)), sourceType(sourceType), workerId(workerId){};

    TestHarnessWorkerConfiguration(WorkerConfigurationPtr workerConfiguration,
                                   TestHarnessWorkerSourceType sourceType,
                                   uint32_t workerId)
        : workerConfiguration(std::move(workerConfiguration)), sourceType(sourceType), workerId(workerId){};

    WorkerConfigurationPtr workerConfiguration;
    std::string logicalSourceName;
    std::string physicalSourceName;
    TestHarnessWorkerSourceType sourceType;
    std::vector<uint8_t*> records;
    uint32_t workerId;
    NesWorkerPtr nesWorker;
};

}// namespace NES

#endif  // NES_INCLUDE_UTIL_TESTHARNESS_TESTHARNESSWORKERCONFIGURATION_HPP_
