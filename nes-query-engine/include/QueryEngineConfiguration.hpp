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

namespace NES::Runtime
{
class QueryEngineConfiguration final : public Configurations::BaseConfiguration
{
    /// validators to prevent nonsensical values for the number of threads and task queue size
    static std::shared_ptr<Configurations::ConfigurationValidation> numberOfThreadsValidator();
    static std::shared_ptr<Configurations::ConfigurationValidation> taskQueueSizeValidator();

public:
    QueryEngineConfiguration() = default;

    QueryEngineConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { }

    Configurations::UIntOption numberOfWorkerThreads
        = {"numberOfWorkerThreads", "4", "Number of worker threads used within the QueryEngine", {numberOfThreadsValidator()}};
    Configurations::UIntOption taskQueueSize
        = {"taskQueueSize", "10000", "Size of the bounded task queue used within the QueryEngine", {taskQueueSizeValidator()}};
    Configurations::UIntOption admissionQueueSize
        = {"admissionQueueSize", "1000", "Size of the bounded admission queue used within the QueryEngine", {taskQueueSizeValidator()}};

protected:
    std::vector<BaseOption*> getOptions() override { return {&numberOfWorkerThreads, &taskQueueSize, &admissionQueueSize}; }
};
}
