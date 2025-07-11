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

#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <InferModelLogicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include "../include/IREEInferenceOperator.hpp"
#include "../include/IREEInferenceOperatorHandler.hpp"
#include "../include/BatchingPhysicalOperator.hpp"

struct LowerToPhysicalIREEInferenceOperator : NES::AbstractRewriteRule
{
    explicit LowerToPhysicalIREEInferenceOperator(NES::Configurations::QueryOptimizerConfiguration conf) : conf(std::move(conf)) { }
    NES::RewriteRuleResultSubgraph apply(NES::LogicalOperator logicalOperator) override
    {
        NES_INFO("Lower infer model operator to IREE operator");

        auto inferModelOperator = logicalOperator.get<NES::InferModel::InferModelLogicalOperator>();
        auto outputOriginId = inferModelOperator.getOutputOriginIds()[0];
        const auto pageSize = conf.pageSize.getValue();

        auto nested = logicalOperator.getInputOriginIds();
        auto flatView = nested | std::views::join;
        const std::vector inputOriginIds(flatView.begin(), flatView.end());

        const auto& model = inferModelOperator.getModel();
        auto handlerId = NES::getNextOperatorHandlerId();

        auto inputSchema = logicalOperator.getInputSchemas().at(0);
        auto memoryProvider = NES::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
            pageSize, inputSchema);
        auto batchingOperator = NES::BatchingPhysicalOperator(handlerId, memoryProvider);

        auto inputFunctions = std::views::transform(
                                  inferModelOperator.getInputFields(),
                                  [](const auto& function) { return NES::QueryCompilation::FunctionProvider::lowerFunction(function); })
            | std::ranges::to<std::vector>();
        auto outputNames = model.getOutputs() | std::views::keys | std::ranges::to<std::vector>();

        auto ireeOperator = NES::IREEInferenceOperator(handlerId, inputFunctions, outputNames, memoryProvider);

        auto handler = std::make_shared<NES::IREEInferenceOperatorHandler>(inputOriginIds, outputOriginId, model);

        if (inferModelOperator.getInputFields().size() == 1
            && inferModelOperator.getInputFields().at(0).getDataType().type != NES::DataType::Type::VARSIZED)
        {
            ireeOperator.isVarSizedInput = true;
        }

        if (model.getOutputs().size() == 1 && model.getOutputs().at(0).second.type == NES::DataType::Type::VARSIZED)
        {
            ireeOperator.isVarSizedOutput = true;
        }
        ireeOperator.outputSize = model.outputSize();
        ireeOperator.inputSize = model.inputSize();

        auto batchingWrapper = std::make_shared<NES::PhysicalOperatorWrapper>(
            batchingOperator,
            inputSchema,
            inputSchema,
            handlerId,
            handler,
            NES::PhysicalOperatorWrapper::PipelineLocation::EMIT);

        auto ireeWrapper = std::make_shared<NES::PhysicalOperatorWrapper>(
            ireeOperator,
            inputSchema,
            logicalOperator.getOutputSchema(),
            handlerId,
            handler,
            NES::PhysicalOperatorWrapper::PipelineLocation::SCAN,
            std::vector{batchingWrapper});

        return {ireeWrapper, {batchingWrapper}};
    }

private:
    NES::Configurations::QueryOptimizerConfiguration conf;
};

std::unique_ptr<NES::AbstractRewriteRule>
NES::RewriteRuleGeneratedRegistrar::RegisterInferenceModelRewriteRule(RewriteRuleRegistryArguments arguments)
{
    return std::make_unique<LowerToPhysicalIREEInferenceOperator>(arguments.conf);
}
