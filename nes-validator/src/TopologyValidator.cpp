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

#include <Validator/TopologyValidator.hpp>
#include "YamlBinding.hpp"

#include <exception>
#include <memory>
#include <string>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sources/SourceCatalog.hpp>

namespace NES::Validator
{

std::string validateTopology(const std::string& yamlString)
{
    try
    {
        // 1. Parse YAML into TopologyConfig
        auto config = YAML::Load(yamlString).as<TopologyConfig>();

        // 2. Populate source catalog from parsed config
        auto sourceCatalog = std::make_shared<SourceCatalog>();

        for (const auto& logicalSource : config.logical)
        {
            Schema schema;
            for (const auto& field : logicalSource.schema)
            {
                schema.addField(field.name, field.type);
            }
            auto addResult = sourceCatalog->addLogicalSource(logicalSource.name, schema);
            if (!addResult)
            {
                return "Duplicate logical source: " + logicalSource.name;
            }
        }

        for (const auto& physicalSource : config.physical)
        {
            auto logicalOpt = sourceCatalog->getLogicalSource(physicalSource.logical);
            if (!logicalOpt)
            {
                return "Unknown logical source: " + physicalSource.logical;
            }
            auto result = sourceCatalog->addPhysicalSource(
                *logicalOpt, physicalSource.type, Host(physicalSource.host), physicalSource.sourceConfig, physicalSource.parserConfig);
            if (!result)
            {
                return result.error().what();
            }
        }

        // 3. Validate SQL queries
        for (const auto& query : config.query)
        {
            // Parse SQL and create logical query plan — throws on invalid SQL syntax.
            // Uses the same code path as nes-cli (createLogicalQueryPlanFromSQLString).
            auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);

            // Validate that all source names referenced in the query exist in the catalog.
            // The ANTLR parser creates SourceNameLogicalOperator nodes with names but does
            // not check the catalog — that is done later by the optimizer in the real engine.
            auto sourceOps = getOperatorByType<SourceNameLogicalOperator>(plan);
            for (const auto& sourceOp : sourceOps)
            {
                const auto& sourceName = sourceOp->getLogicalSourceName();
                if (!sourceCatalog->containsLogicalSource(sourceName))
                {
                    return "Unknown source in query: " + sourceName;
                }
            }
        }

        return ""; // success
    }
    catch (const std::exception& e)
    {
        return e.what();
    }
}

} // namespace NES::Validator
