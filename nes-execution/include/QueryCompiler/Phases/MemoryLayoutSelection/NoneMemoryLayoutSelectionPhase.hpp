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
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/MemoryLayoutSelection/MemoryLayoutSelectionPhase.hpp>

namespace NES::QueryCompilation
{

/// This phase does not change the memory layout type of any operator / pipeline. It simply returns the pipeline as is.
class NoneMemoryLayoutSelectionPhase final : public MemoryLayoutSelectionPhase
{
public:
    static std::shared_ptr<MemoryLayoutSelectionPhase> create() { return std::make_shared<NoneMemoryLayoutSelectionPhase>(); }
    std::shared_ptr<PipelineQueryPlan> apply(std::shared_ptr<PipelineQueryPlan> pipeline) override { return pipeline; }
};
}
