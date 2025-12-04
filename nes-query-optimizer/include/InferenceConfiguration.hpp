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

#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/NonZeroValidation.hpp>
#include <fmt/ranges.h>

namespace NES::Configurations
{

enum class PredictionCacheType : uint8_t
{
    NONE,
    TWO_QUEUES,
    FIFO,
    LFU,
    LRU,
    SECOND_CHANCE
};

struct PredictionCacheOptions
{
    PredictionCacheType predictionCacheType;
    uint64_t numberOfEntries;
};

class InferenceConfiguration final : public BaseConfiguration
{
public:
    InferenceConfiguration() = default;
    InferenceConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    UIntOption batchSize
        = {"batch_size", "1", "Batch size", {std::make_shared<NonZeroValidation>()}};
    EnumOption<PredictionCacheType> predictionCacheType
        = {"prediction_cache_type",
           PredictionCacheType::NONE,
           fmt::format("Type of prediction cache {}", fmt::join(magic_enum::enum_names<PredictionCacheType>(), ", "))};
    UIntOption numberOfEntriesPredictionCache
        = {"number_of_entries_prediction_cache", "1", "Size of the prediction cache", {std::make_shared<NonZeroValidation>()}};

private:
    std::vector<BaseOption*> getOptions() override { return {&batchSize, &predictionCacheType, &numberOfEntriesPredictionCache}; }
};

}
