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

#include <LegacyOptimizer/RecordingSelectionTypes.hpp>
#include <Util/Pointers.hpp>

#include <cstddef>

namespace NES
{
class WorkerCatalog;

struct ShadowPriceTuning
{
    double replayInitialPriceScale = 1.0;
    double replayStepScale = 1.0;
    double storageStepScale = 1.0;
    size_t maxIterations = 32;
};

struct ShadowPriceIterationStats
{
    size_t iteration = 0;
    double replayTimePrice = 0.0;
    double selectedReplayTimeMs = 0.0;
    double replayConstraintSatisfactionPercent = 100.0;
    double storageBudgetConstraintSatisfactionPercent = 100.0;
    double maxStorageUtilizationPercent = 0.0;
    bool replayConstraintSatisfied = true;
    bool storageBudgetConstraintSatisfied = true;
};

class RecordingBoundarySolver
{
public:
    explicit RecordingBoundarySolver(
        SharedPtr<const WorkerCatalog> workerCatalog,
        ShadowPriceTuning shadowPriceTuning = {})
        : workerCatalog(std::move(workerCatalog)), shadowPriceTuning(shadowPriceTuning)
    {
    }

    [[nodiscard]] RecordingBoundarySelection solve(
        const RecordingCandidateSet& candidateSet,
        std::vector<ShadowPriceIterationStats>* iterationTrace = nullptr) const;

private:
    SharedPtr<const WorkerCatalog> workerCatalog;
    ShadowPriceTuning shadowPriceTuning;
};
}
