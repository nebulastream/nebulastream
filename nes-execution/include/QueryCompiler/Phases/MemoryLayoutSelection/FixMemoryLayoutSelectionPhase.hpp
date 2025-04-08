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
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/MemoryLayoutSelection/MemoryLayoutSelectionPhase.hpp>
namespace NES::QueryCompilation
{

/// This phase sets the passed layout type for each pipeline, except for the source and sink pipeline.
/// If the layout type of the source and sink pipeline is different from the passed layout type, we insert a PhysicalMemoryLayoutSwapOperator.
class FixMemoryLayoutSelectionPhase final : public MemoryLayoutSelectionPhase
{
public:
    explicit FixMemoryLayoutSelectionPhase(Schema::MemoryLayoutType layoutType);
    static std::shared_ptr<MemoryLayoutSelectionPhase> create(Schema::MemoryLayoutType layoutType);
    std::shared_ptr<PipelineQueryPlan> apply(std::shared_ptr<PipelineQueryPlan> pipeline) override;

private:
    void process(std::shared_ptr<OperatorPipeline> pipeline);
    void processOperator(std::shared_ptr<Operator> op);
    void insertBetweenThisAndSuccessor(const std::shared_ptr<OperatorPipeline>& pipeline, std::shared_ptr<OperatorPipeline>& newPipeline);
    void insertBetweenThisAndPredecessor(const std::shared_ptr<OperatorPipeline>& pipeline, std::shared_ptr<OperatorPipeline>& newPipeline);
    std::shared_ptr<Schema> getInputSchema(const std::shared_ptr<Operator> op);
    Schema::MemoryLayoutType layoutType;
};
}