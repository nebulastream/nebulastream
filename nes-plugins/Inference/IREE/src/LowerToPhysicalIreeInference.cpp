//
// Created by ls on 6/23/25.
//

#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Functions/FunctionProvider.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include "../include/IREEInferenceOperator.hpp"
#include "../include/IREEInferenceOperatorHandler.hpp"

#include <LogicalInferModelOperator.hpp>
#include <RewriteRuleRegistry.hpp>

struct LowerToPhysicalIREEInferenceOperator : NES::AbstractRewriteRule
{
    explicit LowerToPhysicalIREEInferenceOperator(NES::Configurations::QueryOptimizerConfiguration conf) : conf(std::move(conf)) { }
    NES::RewriteRuleResultSubgraph apply(NES::LogicalOperator logicalOperator) override
    {
        auto inferModelOperator = logicalOperator.get<NES::InferModel::LogicalInferModelOperator>();

        NES_INFO("Lower infer model operator to IREE operator");
        const auto& model = inferModelOperator.getModel();

        auto handlerId = NES::getNextOperatorHandlerId();
        auto handler = std::make_shared<NES::IREEInferenceOperatorHandler>(model);

        auto inputFunctions = std::views::transform(
                                  inferModelOperator.getInputFields(),
                                  [](const auto& function) { return NES::QueryCompilation::FunctionProvider::lowerFunction(function); })
            | std::ranges::to<std::vector>();
        auto outputNames = model.getOutputs() | std::views::keys | std::ranges::to<std::vector>();
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

        auto wrapper = std::make_shared<NES::PhysicalOperatorWrapper>(
            ireeOperator,
            logicalOperator.getInputSchemas().at(0),
            logicalOperator.getOutputSchema(),
            handlerId,
            std::move(handler),
            NES::PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

        return {wrapper, {wrapper}};
    }

private:
    NES::Configurations::QueryOptimizerConfiguration conf;
};

std::unique_ptr<NES::AbstractRewriteRule>
NES::RewriteRuleGeneratedRegistrar::RegisterInferenceModelRewriteRule(RewriteRuleRegistryArguments arguments)
{
    return std::make_unique<LowerToPhysicalIREEInferenceOperator>(arguments.conf);
}