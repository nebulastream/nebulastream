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

#include <memory>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <LegacyOptimizer/Phases/TypeInferencePhase.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <Common/DataTypes/Undefined.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Plans/LogicalPlan.hpp>

using namespace std::string_literals;

namespace NES::LegacyOptimizer
{

void SemanticQueryValidation::validate(QueryPlan& queryPlan, const Catalogs::Source::SourceCatalog& sourceCatalog) const
{
    /// check if we have valid root operators, i.e., sinks
    sinkOperatorValidityCheck(queryPlan);
    /// Checking if the logical source can be found in the source catalog
    logicalSourceValidityCheck(queryPlan, sourceCatalog);
    /// Checking if the physical source can be found in the source catalog
    physicalSourceValidityCheck(queryPlan, sourceCatalog);

    try
    {
        TypeInferencePhase::apply(queryPlan.getOperatorByType<SourceNameLogicalOperator>(), sourceCatalog);
        TypeInferencePhase::apply(queryPlan);
    }
    catch (std::exception& e)
    {
        std::string errorMessage = e.what();
        throw QueryInvalid(errorMessage);
    }
}

void eraseAllSubStr(std::string& mainStr, const std::string& toErase)
{
    size_t pos = std::string::npos;
    while ((pos = mainStr.find(toErase)) != std::string::npos)
    {
        mainStr.erase(pos, toErase.length());
    }
}

void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr)
{
    size_t pos = data.find(toSearch);
    while (pos != std::string::npos)
    {
        data.replace(pos, toSearch.size(), replaceStr);
        pos = data.find(toSearch, pos + replaceStr.size());
    }
}

void createExceptionForPredicate(std::string& predicateString)
{
    /// Removing unnecessary data from the error messages for better readability
    eraseAllSubStr(predicateString, "[INTEGER]");
    eraseAllSubStr(predicateString, "(");
    eraseAllSubStr(predicateString, ")");
    eraseAllSubStr(predicateString, "FieldAccessNode");
    eraseAllSubStr(predicateString, "ConstantValue");
    eraseAllSubStr(predicateString, "BasicValue");

    /// Adding whitespace between bool operators for better readability
    findAndReplaceAll(predicateString, "&&", " && ");
    findAndReplaceAll(predicateString, "||", " || ");

    throw QueryInvalid("Unsatisfiable Query due to filter condition:\n" + predicateString + "\n");
}

void SemanticQueryValidation::logicalSourceValidityCheck(const QueryPlan& queryPlan, Catalogs::Source::SourceCatalog& sourceCatalog) const
{
    /// Getting the source operators from the query plan
    auto sourceOperators = queryPlan.getOperatorByType<SourceNameLogicalOperator>();

    for (const auto& source : sourceOperators)
    {
        /// Making sure that all logical sources are present in the source catalog
        if (!sourceCatalog.containsLogicalSource(source.getLogicalSourceName()))
        {
            throw QueryInvalid("The logical source '" + source.getLogicalSourceName() + "' can not be found in the SourceCatalog\n");
        }
    }
}

void SemanticQueryValidation::physicalSourceValidityCheck(
    const QueryPlan& queryPlan, Catalogs::Source::SourceCatalog& sourceCatalog) const
{
    /// Identify the source operators
    auto sourceOperators = queryPlan.getOperatorByType<SourceNameLogicalOperator>();
    std::vector<std::string> invalidLogicalSourceNames;
    for (auto sourceOperator : sourceOperators)
    {
        if (sourceCatalog.getPhysicalSources(sourceOperator.getLogicalSourceName()).empty())
        {
            invalidLogicalSourceNames.emplace_back(sourceOperator.getLogicalSourceName());
        }
    }

    /// If there are invalid logical sources then report them
    if (!invalidLogicalSourceNames.empty())
    {
        std::stringstream invalidSources;
        invalidSources << "[";
        std::copy(
            invalidLogicalSourceNames.begin(),
            invalidLogicalSourceNames.end() - 1,
            std::ostream_iterator<std::string>(invalidSources, ","));
        invalidSources << invalidLogicalSourceNames.back();
        invalidSources << "]";
        throw QueryInvalid("Logical source(s) " + invalidSources.str() + " are found to have no physical source(s) defined.");
    }
}

void SemanticQueryValidation::sinkOperatorValidityCheck(const QueryPlan& queryPlan) const
{
    auto rootOperators = queryPlan.getRootOperators();
    /// Check if root operator exists ina query plan
    if (rootOperators.empty())
    {
        throw QueryInvalid("Query "s + queryPlan.toString() + " does not contain any root operator");
    }

    /// Check if all root operators of type sink
    for (auto& root : rootOperators)
    {
        if (not root.tryGet<SinkLogicalOperator>())
        {
            throw QueryInvalid("Query "s + queryPlan.toString() + " does not contain a valid sink operator as root");
        }
    }
}

}