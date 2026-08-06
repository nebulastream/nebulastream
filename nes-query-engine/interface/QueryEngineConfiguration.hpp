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
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/ConfigurationValidation.hpp>
#include <fmt/format.h>
#include <BufferExhaustionPolicy.hpp>

namespace NES
{
class QueryEngineConfiguration final : public BaseConfiguration
{
    /// validators to prevent nonsensical values for the number of threads and task queue size
    static std::shared_ptr<ConfigurationValidation> numberOfThreadsValidator();
    static std::shared_ptr<ConfigurationValidation> queueSizeValidator();

public:
    QueryEngineConfiguration() = default;

    QueryEngineConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { }

    UIntOption numberOfWorkerThreads
        = {"number_of_worker_threads", "4", "Number of worker threads used within the QueryEngine", {numberOfThreadsValidator()}};
    UIntOption admissionQueueSize
        = {"admission_queue_size", "1000", "Size of the bounded admission queue used within the QueryEngine", {queueSizeValidator()}};

    /// When the global buffer pool is exhausted, a victim query is terminated to free buffers (instead of deadlocking).
    /// This selects which query.
    EnumOption<BufferExhaustionPolicy> bufferExhaustionPolicy
        = {"buffer_exhaustion_policy",
           BufferExhaustionPolicy::TERMINATE_LARGEST,
           fmt::format("Policy for choosing a victim query on buffer-pool exhaustion: {}", enumPipeList<BufferExhaustionPolicy>())};

    /// Number of buffers kept free for the recovery/teardown path when the pool is exhausted (the "don't take the last
    /// buffer" margin). 0 means: use the number of worker threads (so all workers can detect exhaustion concurrently).
    UIntOption bufferRecoveryMargin
        = {"buffer_recovery_margin", "0", "Buffers held back for recovery when the pool is exhausted (0 = number of worker threads).", {}};

protected:
    std::vector<BaseOption*> getOptions() override
    {
        return {&numberOfWorkerThreads, &admissionQueueSize, &bufferExhaustionPolicy, &bufferRecoveryMargin};
    }
};
}
