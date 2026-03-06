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

#include <cstddef>
#include <string>
#include <string_view>

namespace NES::Replay
{

inline constexpr std::string_view DEFAULT_RECORDING_DIRECTORY = "/tmp/REPLAY-NebulaStream/recordings";
inline constexpr std::string_view DEFAULT_RECORDING_FILE_PATH = "/tmp/REPLAY-NebulaStream/store_read_out.bin";
inline constexpr size_t DEFAULT_ESTIMATED_RECORDING_ROWS = 4096;
inline constexpr size_t MIN_RECORDING_SIZE_BYTES = 4096;

[[nodiscard]] std::string getRecordingFilePath(std::string_view recordingId);
[[nodiscard]] std::string getTimeTravelReadAliasPath();
[[nodiscard]] std::string resolveTimeTravelReadProbePath();
void updateTimeTravelReadAlias(const std::string& recordingFilePath);
void clearTimeTravelReadAlias();

}
