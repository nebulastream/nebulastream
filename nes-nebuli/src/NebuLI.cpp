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

#include <NebuLI.hpp>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <istream>
#include <memory>
#include <optional>
#include <ranges>
#include <regex>
#include <stdexcept>
#include <string>
#include <utility>
#include <API/Schema.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES::CLI
{

std::shared_ptr<DecomposedQueryPlan> Optimizer::optimize(std::shared_ptr<QueryPlan>& plan)
{
    const auto semanticQueryValidation = NES::Optimizer::SemanticQueryValidation::create(sourceCatalog);
    auto logicalSourceExpansionRule = NES::Optimizer::LogicalSourceExpansionRule(sourceCatalog);
    const auto typeInference = NES::Optimizer::TypeInferencePhase::create(sourceCatalog);
    const auto originIdInferencePhase = NES::Optimizer::OriginIdInferencePhase::create();
    const auto queryRewritePhase = NES::Optimizer::QueryRewritePhase::create();

    semanticQueryValidation->validate(plan); /// performs the first type inference
    logicalSourceExpansionRule.apply(plan);
    typeInference->performTypeInferenceQuery(plan);

    originIdInferencePhase->execute(plan);
    queryRewritePhase->execute(plan);
    typeInference->performTypeInferenceQuery(plan);

    NES_INFO("QEP:\n {}", plan->toString());
    NES_INFO("Sink Schema: {}", *plan->getRootOperators()[0]->getOutputSchema());
    return std::make_shared<DecomposedQueryPlan>(INITIAL<QueryId>, INITIAL<WorkerId>, plan->getRootOperators());
}
QueryId Nebuli::registerQuery(const std::shared_ptr<DecomposedQueryPlan>& plan)
{
    return QueryId{grpcClient->registerQuery(*plan)};
}
void Nebuli::startQuery(const QueryId queryId)
{
    grpcClient->start(queryId.getRawValue());
}
void Nebuli::stopQuery(const QueryId queryId)
{
    grpcClient->stop(queryId.getRawValue());
}
void Nebuli::unregisterQuery(QueryId queryId)
{
    grpcClient->unregister(queryId.getRawValue());
}
}
