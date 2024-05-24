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

#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTHARNESS_TESTHARNESSWORKERCONFIGURATION_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTHARNESS_TESTHARNESSWORKERCONFIGURATION_HPP_

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
    enum class TestHarnessWorkerSourceType : int8_t { CSVSource, MemorySource, LambdaSource, NonSource };

    static TestHarnessWorkerConfigurationPtr create(WorkerConfigurationPtr workerConfiguration, WorkerId workerId);

    static TestHarnessWorkerConfigurationPtr create(WorkerConfigurationPtr workerConfiguration,
                                                    std::string logicalSourceName,
                                                    std::string physicalSourceName,
                                                    TestHarnessWorkerSourceType sourceType,
                                                    WorkerId workerId);

    void setQueryStatusListener(const NesWorkerPtr& nesWorker);

    const WorkerConfigurationPtr& getWorkerConfiguration() const;
    TestHarnessWorkerSourceType getSourceType() const;
    PhysicalSourceTypePtr getPhysicalSourceType() const;
    void setPhysicalSourceType(PhysicalSourceTypePtr physicalSource);
    const std::vector<uint8_t*>& getRecords() const;
    void addRecord(uint8_t* record);
    WorkerId getWorkerId() const;
    const std::string& getLogicalSourceName() const;
    const std::string& getPhysicalSourceName() const;
    const NesWorkerPtr& getNesWorker() const;

  private:
    TestHarnessWorkerConfiguration(WorkerConfigurationPtr workerConfiguration,
                                   std::string logicalSourceName,
                                   std::string physicalSourceName,
                                   TestHarnessWorkerSourceType sourceType,
                                   WorkerId workerId);
    TestHarnessWorkerConfiguration(WorkerConfigurationPtr workerConfiguration,
                                   TestHarnessWorkerSourceType sourceType,
                                   WorkerId workerId);
    WorkerConfigurationPtr workerConfiguration;
    std::string logicalSourceName;
    std::string physicalSourceName;
    TestHarnessWorkerSourceType sourceType;
    std::vector<uint8_t*> records;
    WorkerId workerId;
    NesWorkerPtr nesWorker;
    PhysicalSourceTypePtr physicalSource;
};

}// namespace NES

#endif// NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTHARNESS_TESTHARNESSWORKERCONFIGURATION_HPP_
