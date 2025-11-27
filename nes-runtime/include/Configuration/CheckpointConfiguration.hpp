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
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <string>
#include <vector>

namespace NES
{

class CheckpointConfiguration final : public BaseConfiguration
{
public:
    CheckpointConfiguration() = default;
    CheckpointConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { }

    ScalarOption<std::string> checkpointDirectory
        = {"checkpoint_directory",
           "/tmp/nebulastream",
           "Directory that should be used to store runtime checkpoint files."};

    UIntOption checkpointIntervalMs
        = {"checkpoint_interval_ms",
           "60000",
           "Interval in milliseconds for periodic checkpointing. Set to 0 to disable automatic checkpoints."};

    BoolOption recoverFromCheckpoint
        = {"recover_from_checkpoint",
           "false",
           "If enabled, the worker attempts to recover the latest checkpoint at startup."};

private:
    std::vector<BaseOption*> getOptions() override { return {&checkpointDirectory, &checkpointIntervalMs, &recoverFromCheckpoint}; }
};

}
