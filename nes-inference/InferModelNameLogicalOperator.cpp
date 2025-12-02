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

#include <filesystem>
#include <memory>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>

#include <InferModelNameLogicalOperator.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES::InferModel
{


std::string InferModelNameLogicalOperator::explain(ExplainVerbosity, OperatorId opId) const
{
    return fmt::format("INFER_MODEL_NAME(opId: {}, modelName: {})", opId, modelName);
}
}

NES::LogicalOperatorRegistryReturnType
NES::LogicalOperatorGeneratedRegistrar::RegisterInferenceModelNameLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    auto functions = std::get<FunctionList>(arguments.config.at("inputFields")).functions()
        | std::views::transform([](const auto& serializedFunction)
                                { return FunctionSerializationUtil::deserializeFunction(serializedFunction); })
        | std::ranges::to<std::vector>();

    auto logicalOperator = InferModel::InferModelNameLogicalOperator(
        std::get<std::string>(arguments.config.at(InferModel::InferModelNameLogicalOperator::ConfigParameters::MODEL_NAME)), functions);
    return logicalOperator;
}
