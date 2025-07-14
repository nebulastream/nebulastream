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

#include <filesystem>
#include <fstream>
#include <istream>
#include <memory>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <utility>

#include <Configurations/ConfigurationsNames.hpp>
#include <Identifiers/Identifiers.hpp>
#include <LegacyOptimizer/LogicalSourceExpansionRule.hpp>
#include <LegacyOptimizer/OriginIdInferencePhase.hpp>
#include <LegacyOptimizer/SourceInferencePhase.hpp>
#include <LegacyOptimizer/TypeInferencePhase.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>

namespace NES::CLI
{

QueryId Nebuli::registerQuery(const LogicalPlan& plan)
{
    return QueryId{grpcClient->registerQuery(plan)};
}
void Nebuli::startQuery(const QueryId queryId)
{
    grpcClient->start(queryId);
}
void Nebuli::stopQuery(const QueryId queryId)
{
    grpcClient->stop(queryId);
}
void Nebuli::unregisterQuery(const QueryId queryId)
{
    grpcClient->unregister(queryId);
}
}
