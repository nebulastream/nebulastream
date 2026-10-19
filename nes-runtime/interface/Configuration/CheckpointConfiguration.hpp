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

#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>

namespace NES
{
class CheckpointConfiguration final : public BaseConfiguration
{
public:
    CheckpointConfiguration() = default;

    CheckpointConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { }

    StringOption checkpointDirectory
        = {"checkpoint_directory", "/tmp/nes-checkpoints", "Directory that stores the checkpoints of this worker."};
    UIntOption checkpointIntervalMs
        = {"checkpoint_interval_ms", "0", "Interval in milliseconds between two checkpoints. 0 disables checkpointing."};
    BoolOption recoverFromCheckpoint
        = {"recover_from_checkpoint", "false", "Recover the queries found in the checkpoint directory when the worker starts."};

private:
    std::vector<BaseOption*> getOptions() override { return {&checkpointDirectory, &checkpointIntervalMs, &recoverFromCheckpoint}; }
};
}
