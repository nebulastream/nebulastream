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

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/ConfigurationValidation.hpp>

namespace NES
{
class QueryEngineConfiguration final : public BaseConfiguration
{
    /// validators to prevent nonsensical values for the number of threads and task queue size
    static std::shared_ptr<ConfigurationValidation> numberOfThreadsValidator(std::string threadType);
    static std::shared_ptr<ConfigurationValidation> queueSizeValidator();
    static std::shared_ptr<ConfigurationValidation> pinThreadsValidator();

public:
    QueryEngineConfiguration() = default;

    QueryEngineConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { }

    BoolOption pinThreads
        = {"pin_threads", "false", "Whether to pin worker and io threads", {pinThreadsValidator()}};
    UIntOption numberOfWorkerThreads
        = {"number_of_worker_threads", "4", "Number of worker threads used within the QueryEngine", {numberOfThreadsValidator("Worker")}};
    UIntOption numberOfIOThreads
        = {"number_of_io_threads", "1", "Number of io threads used within the async source runtime", {numberOfThreadsValidator("IO")}};
    UIntOption admissionQueueSize
        = {"admission_queue_size", "1000", "Size of the bounded admission queue used within the QueryEngine", {queueSizeValidator()}};

protected:
    std::vector<BaseOption*> getOptions() override { return {&pinThreads, &numberOfWorkerThreads, &numberOfIOThreads, &admissionQueueSize}; }
};
}
