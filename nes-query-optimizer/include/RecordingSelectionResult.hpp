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
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>

namespace NES
{

using RecordingId = NESStrongStringType<struct RecordingId_, "invalid">;

struct RecordingSelection
{
    RecordingId recordingId{RecordingId::INVALID};
    Host node{Host::INVALID};
    std::string filePath;

    [[nodiscard]] bool operator==(const RecordingSelection& other) const = default;
};

struct RecordingSelectionResult
{
    std::vector<RecordingSelection> selectedRecordings;

    [[nodiscard]] bool empty() const { return selectedRecordings.empty(); }
    [[nodiscard]] bool operator==(const RecordingSelectionResult& other) const = default;
};

}
