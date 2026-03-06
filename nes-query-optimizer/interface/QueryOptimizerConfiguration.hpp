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

#include <cstdint>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>

namespace NES
{

enum class StreamJoinStrategy : uint8_t
{
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    OPTIMIZER_CHOOSES
};

class QueryOptimizerConfiguration : public BaseConfiguration
{
public:
    QueryOptimizerConfiguration() = default;
    QueryOptimizerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    EnumOption<StreamJoinStrategy> joinStrategy
        = {"join_strategy",
           StreamJoinStrategy::OPTIMIZER_CHOOSES,
           "Join Strategy"
           "[NESTED_LOOP_JOIN|HASH_JOIN|OPTIMIZER_CHOOSES]."};

    BoolOption enableRecordingSelection
        = {"enable_recording_selection", "false", "Enable replay-aware recording selection in the distributed optimizer."};

    FloatOption recordingMaintenanceCostWeight
        = {"recording_maintenance_cost_weight", "1.0", "Weight used for recording maintenance cost estimation."};

    FloatOption recordingReplayCostWeight = {"recording_replay_cost_weight", "1.0", "Weight used for replay scan cost estimation."};

    FloatOption recordingRecomputeCostWeight
        = {"recording_recompute_cost_weight", "1.0", "Weight used for replay recomputation cost estimation."};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &joinStrategy,
            &enableRecordingSelection,
            &recordingMaintenanceCostWeight,
            &recordingReplayCostWeight,
            &recordingRecomputeCostWeight};
    }
};

}
