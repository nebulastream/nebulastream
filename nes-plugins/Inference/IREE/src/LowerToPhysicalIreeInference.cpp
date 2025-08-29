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

#include <QueryExecutionConfiguration.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <InferModelLogicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <BatchingPhysicalOperator.hpp>
#include <IREEBatchInferenceOperator.hpp>
#include <IREEBatchInferenceOperatorHandler.hpp>
#include <IREEInferenceOperator.hpp>
#include <IREEInferenceOperatorHandler.hpp>

struct LowerToPhysicalIREEInferenceOperator : NES::AbstractRewriteRule
{
    explicit LowerToPhysicalIREEInferenceOperator(NES::QueryExecutionConfiguration conf) : conf(std::move(conf)) { }
    NES::RewriteRuleResultSubgraph apply(NES::LogicalOperator logicalOperator) override
    {

        auto inferModelOperator = logicalOperator.get<NES::InferModel::InferModelLogicalOperator>();

        const auto& model = inferModelOperator.getModel();
        auto handlerId = NES::getNextOperatorHandlerId();

        auto inputSchema = logicalOperator.getInputSchemas().at(0);

        auto inputFunctions = std::views::transform(
                                  inferModelOperator.getInputFields(),
                                  [](const auto& function) { return NES::QueryCompilation::FunctionProvider::lowerFunction(function); })
            | std::ranges::to<std::vector>();
        auto outputNames = model.getOutputs() | std::views::keys | std::ranges::to<std::vector>();

        /// if the batch size is 1, then we simply use the inference operator with PipelineLocation::INTERMEDIATE
        /// else, add the batching operator (custom emit) and batch inference operator (custom scan)
        if (model.getInputShape().front() == 1 && model.getOutputShape().front() == 1)
        {
            NES_INFO("Lower InferModel operator to IREEInferenceOperator");
            auto ireeOperator = NES::IREEInferenceOperator(handlerId, inputFunctions, outputNames);

            if (inferModelOperator.getInputFields().size() == 1
                && inferModelOperator.getInputFields().at(0).getDataType().type == NES::DataType::Type::VARSIZED)
            {
                ireeOperator.isVarSizedInput = true;
            }

            if (model.getOutputs().size() == 1 && model.getOutputs().at(0).second.type == NES::DataType::Type::VARSIZED)
            {
                ireeOperator.isVarSizedOutput = true;
            }
            ireeOperator.outputSize = model.outputSize();
            ireeOperator.inputSize = model.inputSize();

            auto handler = std::make_shared<NES::IREEInferenceOperatorHandler>(model);

            auto wrapper = std::make_shared<NES::PhysicalOperatorWrapper>(
                ireeOperator,
                logicalOperator.getInputSchemas().at(0),
                logicalOperator.getOutputSchema(),
                handlerId,
                std::move(handler),
                NES::PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

            return {wrapper, {wrapper}};
        }
        else
        {
            NES_INFO("Lower InferModel operator to IREEBatchInferenceOperator");
            auto nested = logicalOperator.getInputOriginIds();
            auto flatView = nested | std::views::join;
            const std::vector inputOriginIds(flatView.begin(), flatView.end());
            auto outputOriginId = inferModelOperator.getOutputOriginIds()[0];
            const auto pageSize = conf.pageSize.getValue();

            auto memoryProvider = NES::Interface::MemoryProvider::TupleBufferMemoryProvider::create(pageSize, inputSchema);
            auto batchingOperator = NES::BatchingPhysicalOperator(handlerId, memoryProvider);

            auto ireeOperator = NES::IREEBatchInferenceOperator(handlerId, inputFunctions, outputNames, memoryProvider);

            auto handler = std::make_shared<NES::IREEBatchInferenceOperatorHandler>(inputOriginIds, outputOriginId, model);

            if (inferModelOperator.getInputFields().size() == 1
                && inferModelOperator.getInputFields().at(0).getDataType().type == NES::DataType::Type::VARSIZED)
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
                batchingOperator, inputSchema, inputSchema, handlerId, handler, NES::PhysicalOperatorWrapper::PipelineLocation::EMIT);

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
    }

private:
    NES::QueryExecutionConfiguration conf;
};

std::unique_ptr<NES::AbstractRewriteRule>
NES::RewriteRuleGeneratedRegistrar::RegisterInferenceModelRewriteRule(RewriteRuleRegistryArguments arguments)
{
    return std::make_unique<LowerToPhysicalIREEInferenceOperator>(arguments.conf);
}
