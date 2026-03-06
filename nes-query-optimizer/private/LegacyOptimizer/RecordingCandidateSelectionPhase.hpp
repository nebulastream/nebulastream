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

#include <optional>

#include <LegacyOptimizer/RecordingSelectionTypes.hpp>
#include <RecordingCatalog.hpp>
#include <Replay/ReplaySpecification.hpp>
#include <Util/Pointers.hpp>

namespace NES
{
class WorkerCatalog;
class LogicalPlan;

class RecordingCandidateSelectionPhase
{
public:
    explicit RecordingCandidateSelectionPhase(SharedPtr<const WorkerCatalog> workerCatalog) : workerCatalog(std::move(workerCatalog)) { }

    [[nodiscard]] RecordingCandidateSet apply(
        const LogicalPlan& placedPlan,
        const std::optional<ReplaySpecification>& replaySpecification,
        const RecordingCatalog& recordingCatalog) const;

private:
    SharedPtr<const WorkerCatalog> workerCatalog;
};
}
