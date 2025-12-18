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

#include <DataTypes/DataTypeProvider.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <InferModelLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRuleRegistry.hpp>
#include <BatchingPhysicalOperator.hpp>
#include <IREEBatchInferenceOperator.hpp>
#include <IREEBatchCacheInferenceOperator.hpp>
#include <IREEBatchInferenceOperatorHandler.hpp>
#include <IREEInferenceOperator.hpp>
#include <IREECacheInferenceOperator.hpp>
#include <IREEInferenceOperatorHandler.hpp>
#include <iree/runtime/api.h>

struct LowerToPhysicalIREEInferenceOperator : NES::AbstractRewriteRule
{
    explicit LowerToPhysicalIREEInferenceOperator(NES::QueryExecutionConfiguration conf) : conf(std::move(conf)) { }
    NES::RewriteRuleResultSubgraph apply(NES::LogicalOperator logicalOperator) override
    {
        auto inferModelOperator = logicalOperator.getAs<NES::InferModel::InferModelLogicalOperator>();

        const auto& model = inferModelOperator->getModel();
        auto handlerId = NES::getNextOperatorHandlerId();

        auto inputSchema = logicalOperator->getInputSchemas().at(0);

        auto inputFunctions = std::views::transform(
                                  inferModelOperator->getInputFields(),
                                  [](const auto& function) { return NES::QueryCompilation::FunctionProvider::lowerFunction(function); })
            | std::ranges::to<std::vector>();
        auto outputNames = model.getOutputs() | std::views::keys | std::ranges::to<std::vector>();

        const auto batchSize = conf.inferenceConfiguration.batchSize;
        const auto predictionCacheType = conf.inferenceConfiguration.predictionCacheType;
        const auto predictionCacheSize = conf.inferenceConfiguration.numberOfEntriesPredictionCache;

        const auto inputDtype = model.getInputDtype();
        const auto outputDtype = model.getOutputDtype();

        /// if the batch size is 1, then we simply use the inference operator with PipelineLocation::INTERMEDIATE
        /// else, add the batching operator (custom emit) and batch inference operator (custom scan)
        if (batchSize.getValue() == 1)
        {
            std::shared_ptr<NES::PhysicalOperatorWrapper> wrapper = nullptr;
            auto handler = std::make_shared<NES::IREEInferenceOperatorHandler>(model);

            switch (predictionCacheType.getValue())
            {
                case NES::Configurations::PredictionCacheType::NONE: {
                    NES_DEBUG("Lower InferModel operator to IREEInferenceOperator");
                    auto ireeOperator = NES::IREEInferenceOperator(handlerId, inputFunctions, outputNames, inputDtype, outputDtype);
                    if (inferModelOperator->getInputFields().size() == 1
                        && inferModelOperator->getInputFields().at(0).getDataType().type == NES::DataType::Type::VARSIZED)
                    {
                        ireeOperator.isVarSizedInput = true;
                    }

                    if (model.getOutputs().size() == 1 && model.getOutputs().at(0).second.type == NES::DataType::Type::VARSIZED)
                    {
                        ireeOperator.isVarSizedOutput = true;
                    }
                    ireeOperator.outputSize = model.outputSize();
                    ireeOperator.inputSize = model.inputSize();

                    auto wrapper = std::make_shared<NES::PhysicalOperatorWrapper>(
                        ireeOperator,
                        logicalOperator->getInputSchemas().at(0),
                        logicalOperator->getOutputSchema(),
                        handlerId,
                        std::move(handler),
                        NES::PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
                    return {wrapper, {wrapper}};
                }

                case NES::Configurations::PredictionCacheType::TWO_QUEUES:
                case NES::Configurations::PredictionCacheType::FIFO:
                case NES::Configurations::PredictionCacheType::LFU:
                case NES::Configurations::PredictionCacheType::LRU:
                case NES::Configurations::PredictionCacheType::SECOND_CHANCE: {
                    NES_DEBUG("Lower InferModel operator to IREECacheInferenceOperator");
                    NES::Configurations::PredictionCacheOptions predictionCacheOptions{
                        predictionCacheType.getValue(),
                        predictionCacheSize.getValue()};
                    auto ireeOperator = NES::IREECacheInferenceOperator(handlerId, inputFunctions, outputNames, predictionCacheOptions, inputDtype, outputDtype);

                    if (inferModelOperator->getInputFields().size() == 1
                        && inferModelOperator->getInputFields().at(0).getDataType().type == NES::DataType::Type::VARSIZED)
                    {
                        ireeOperator.isVarSizedInput = true;
                    }

                    if (model.getOutputs().size() == 1 && model.getOutputs().at(0).second.type == NES::DataType::Type::VARSIZED)
                    {
                        ireeOperator.isVarSizedOutput = true;
                    }
                    ireeOperator.outputSize = model.outputSize();
                    ireeOperator.inputSize = model.inputSize();

                    auto wrapper = std::make_shared<NES::PhysicalOperatorWrapper>(
                        ireeOperator,
                        logicalOperator->getInputSchemas().at(0),
                        logicalOperator->getOutputSchema(),
                        handlerId,
                        std::move(handler),
                        NES::PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
                    return {wrapper, {wrapper}};
                }
            }
        }
        else
        {
            auto outputOriginIdsOpt = getTrait<NES::OutputOriginIdsTrait>(logicalOperator->getTraitSet());
            auto inputOriginIdsOpt = getTrait<NES::OutputOriginIdsTrait>(logicalOperator->getChildren().at(0).getTraitSet());
            PRECONDITION(outputOriginIdsOpt.has_value(), "Expected the outputOriginIds trait to be set");
            PRECONDITION(inputOriginIdsOpt.has_value(), "Expected the inputOriginIds trait to be set");

            auto& outputOriginIds = outputOriginIdsOpt.value();
            auto outputOriginId = outputOriginIds[0];
            auto inputOriginIds = inputOriginIdsOpt.value();

            const auto pageSize = conf.pageSize.getValue();

            auto memoryProvider = NES::TupleBufferRef::create(pageSize, inputSchema);
            auto handler = std::make_shared<NES::IREEBatchInferenceOperatorHandler>(
                inputOriginIds | std::ranges::to<std::vector>(), outputOriginId, model, batchSize.getValue());

            auto batchingOperator = NES::BatchingPhysicalOperator(handlerId, memoryProvider);
            auto batchingWrapper = std::make_shared<NES::PhysicalOperatorWrapper>(
                batchingOperator, inputSchema, inputSchema, handlerId, handler, NES::PhysicalOperatorWrapper::PipelineLocation::EMIT);

            std::shared_ptr<NES::PhysicalOperatorWrapper> ireeWrapper = nullptr;
            switch (predictionCacheType.getValue())
            {
                case NES::Configurations::PredictionCacheType::NONE: {
                    NES_DEBUG("Lower InferModel operator to IREEBatchInferenceOperator");
                    auto ireeOperator = NES::IREEBatchInferenceOperator(handlerId, inputFunctions, outputNames, memoryProvider, inputDtype, outputDtype);

                    if (inferModelOperator->getInputFields().size() == 1
                        && inferModelOperator->getInputFields().at(0).getDataType().type == NES::DataType::Type::VARSIZED)
                    {
                        ireeOperator.isVarSizedInput = true;
                    }

                    if (model.getOutputs().size() == 1 && model.getOutputs().at(0).second.type == NES::DataType::Type::VARSIZED)
                    {
                        ireeOperator.isVarSizedOutput = true;
                    }
                    ireeOperator.outputSize = model.outputSize();
                    ireeOperator.inputSize = model.inputSize();

                    auto ireeWrapper = std::make_shared<NES::PhysicalOperatorWrapper>(
                        ireeOperator,
                        inputSchema,
                        logicalOperator->getOutputSchema(),
                        handlerId,
                        handler,
                        NES::PhysicalOperatorWrapper::PipelineLocation::SCAN,
                        std::vector{batchingWrapper});
                    return {ireeWrapper, {batchingWrapper}};
                }
                case NES::Configurations::PredictionCacheType::TWO_QUEUES:
                case NES::Configurations::PredictionCacheType::FIFO:
                case NES::Configurations::PredictionCacheType::LFU:
                case NES::Configurations::PredictionCacheType::LRU:
                case NES::Configurations::PredictionCacheType::SECOND_CHANCE: {
                    NES_DEBUG("Lower InferModel operator to IREEBatchCacheInferenceOperator");
                    NES::Configurations::PredictionCacheOptions predictionCacheOptions{
                        predictionCacheType.getValue(),
                        predictionCacheSize.getValue()};
                    auto ireeOperator = NES::IREEBatchCacheInferenceOperator(handlerId, inputFunctions, outputNames, memoryProvider, predictionCacheOptions, inputDtype, outputDtype);

                    if (inferModelOperator->getInputFields().size() == 1
                        && inferModelOperator->getInputFields().at(0).getDataType().type == NES::DataType::Type::VARSIZED)
                    {
                        ireeOperator.isVarSizedInput = true;
                    }

                    if (model.getOutputs().size() == 1 && model.getOutputs().at(0).second.type == NES::DataType::Type::VARSIZED)
                    {
                        ireeOperator.isVarSizedOutput = true;
                    }
                    ireeOperator.outputSize = model.outputSize();
                    ireeOperator.inputSize = model.inputSize();

                    auto ireeWrapper = std::make_shared<NES::PhysicalOperatorWrapper>(
                        ireeOperator,
                        inputSchema,
                        logicalOperator->getOutputSchema(),
                        handlerId,
                        handler,
                        NES::PhysicalOperatorWrapper::PipelineLocation::SCAN,
                        std::vector{batchingWrapper});
                    return {ireeWrapper, {batchingWrapper}};
                }
            }
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
