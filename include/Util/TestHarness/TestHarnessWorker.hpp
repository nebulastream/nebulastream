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

#ifndef NES_INCLUDE_UTIL_TEST_HARNESS_TEST_HARNESS_WORKER_HPP_
#define NES_INCLUDE_UTIL_TEST_HARNESS_TEST_HARNESS_WORKER_HPP_

#include <API/Schema.hpp>
#include <Catalogs/MemorySourceStreamConfig.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>

namespace NES {

/**
 * @brief A class to keep each worker and its source configuration together
 */
class TestHarnessWorker {
  public:
    enum TestHarnessWorkerType { CSVSource, MemorySource, NonSource };

    NesWorkerPtr wrk;
    TestHarnessWorkerType type;
    std::string logicalStreamName;
    std::string physicalStreamName;
    SchemaPtr schema;
    std::vector<uint8_t*> record;
    PhysicalStreamConfigPtr csvSourceConfig;
};

}// namespace NES

#endif  // NES_INCLUDE_UTIL_TEST_HARNESS_TEST_HARNESS_WORKER_HPP_
