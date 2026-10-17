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
#include <Configurations/Validation/NumberValidation.hpp>

namespace NES
{

/// Settings of asynchronously executed stages. The plan decomposer passes them to the Async source and the Handoff sink
/// of every stage it splits; an option that is not set leaves the default of the source and the sink in place.
class QueryOptimizerAsyncConfiguration : public BaseConfiguration
{
public:
    QueryOptimizerAsyncConfiguration() = default;
    QueryOptimizerAsyncConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    /// Calls of one async stage in flight at the same time, each for one batch of rows. A function of the stage may lower it.
    UIntOption maxConcurrency
        = {"max_concurrency",
           "16",
           "Calls of one async stage in flight at the same time, each for one batch of rows",
           {std::make_shared<NumberValidation>()}};

    /// Buffers that wait between the producer half of an async stage and its calls before the producer is held back.
    UIntOption channelCapacity
        = {"channel_capacity",
           "64",
           "Buffers that wait between the producer half of an async stage and its calls before the producer is held back",
           {std::make_shared<NumberValidation>()}};

    /// Further attempts of a failed async call, the n-th after 100 ms * 2^n.
    UIntOption maxRetries = {
        "max_retries", "0", "Further attempts of a failed async call, the n-th after 100 ms * 2^n", {std::make_shared<NumberValidation>()}};

private:
    std::vector<BaseOption*> getOptions() override { return {&maxConcurrency, &channelCapacity, &maxRetries}; }
};

}
