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

#include <chrono>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

namespace NES
{

class CheckpointManager final
{
public:
    using Callback = std::function<void()>;

    static void initialize(
        std::filesystem::path checkpointDirectory,
        std::chrono::milliseconds checkpointInterval,
        bool recoverFromCheckpoint);
    static void shutdown();

    [[nodiscard]] static std::filesystem::path getCheckpointDirectory();
    [[nodiscard]] static std::filesystem::path getCheckpointPath(std::string_view fileName);
    static void persistFile(std::string_view fileName, std::string_view contents);
    [[nodiscard]] static std::optional<std::string> loadFile(const std::filesystem::path& absolutePath);
    [[nodiscard]] static bool isCheckpointingEnabled();
    [[nodiscard]] static bool shouldRecoverFromCheckpoint();

    static void registerCallback(const std::string& identifier, Callback callback);
    static void unregisterCallback(const std::string& identifier);
};

}
