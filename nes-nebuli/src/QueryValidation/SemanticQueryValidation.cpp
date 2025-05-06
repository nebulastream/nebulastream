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
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
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

using namespace std::string_literals;

namespace NES::Optimizer
{
SemanticQueryValidation::SemanticQueryValidation(const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog)
    : sourceCatalog(sourceCatalog)
{
}

std::shared_ptr<SemanticQueryValidation>
SemanticQueryValidation::create(const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog)
{
    return std::make_shared<SemanticQueryValidation>(sourceCatalog);
}

void SemanticQueryValidation::validate(const std::shared_ptr<QueryPlan>& queryPlan) const
{
    /// check if we have valid root operators, i.e., sinks
    sinkOperatorValidityCheck(queryPlan);
    /// Checking if the logical source can be found in the source catalog
    logicalSourceValidityCheck(queryPlan, sourceCatalog);
    /// Checking if the physical source can be found in the source catalog
    physicalSourceValidityCheck(queryPlan, sourceCatalog);

    try
    {
        const auto typeInferencePhase = TypeInferencePhase::create(sourceCatalog);
        typeInferencePhase->performTypeInferenceSources(queryPlan->getSourceOperators<SourceNameLogicalOperator>());
        TypeInferencePhase::performTypeInferenceQuery(queryPlan);
    }
    catch (std::exception& e)
    {
        std::string errorMessage = e.what();
        throw QueryInvalid(errorMessage);
    }

    /// check if infer model is correctly defined
    inferModelValidityCheck(queryPlan);
}

void SemanticQueryValidation::createExceptionForPredicate(std::string& predicateString)
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

void SemanticQueryValidation::eraseAllSubStr(std::string& mainStr, const std::string& toErase)
{
    size_t pos = std::string::npos;
    while ((pos = mainStr.find(toErase)) != std::string::npos)
    {
        mainStr.erase(pos, toErase.length());
    }
}

void SemanticQueryValidation::findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr)
{
    size_t pos = data.find(toSearch);
    while (pos != std::string::npos)
    {
        data.replace(pos, toSearch.size(), replaceStr);
        pos = data.find(toSearch, pos + replaceStr.size());
    }
}

void SemanticQueryValidation::logicalSourceValidityCheck(
    const std::shared_ptr<QueryPlan>& queryPlan, const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog)
{
    /// Getting the source operators from the query plan
    auto sourceOperators = queryPlan->getSourceOperators<SourceNameLogicalOperator>();

    for (const auto& source : sourceOperators)
    {
        /// Making sure that all logical sources are present in the source catalog
        if (!sourceCatalog->containsLogicalSource(source->getLogicalSourceName()))
        {
            throw QueryInvalid("The logical source '" + source->getLogicalSourceName() + "' can not be found in the SourceCatalog\n");
        }
    }
}

void SemanticQueryValidation::physicalSourceValidityCheck(
    const std::shared_ptr<QueryPlan>& queryPlan, const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog)
{
    /// Identify the source operators
    auto sourceOperators = queryPlan->getSourceOperators<SourceNameLogicalOperator>();
    std::vector<std::string> invalidLogicalSourceNames;
    for (auto sourceOperator : sourceOperators)
    {
        if (sourceCatalog->getPhysicalSources(sourceOperator->getLogicalSourceName()).empty())
        {
            invalidLogicalSourceNames.emplace_back(sourceOperator->getLogicalSourceName());
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

void SemanticQueryValidation::sinkOperatorValidityCheck(const std::shared_ptr<QueryPlan>& queryPlan)
{
    auto rootOperators = queryPlan->getRootOperators();
    /// Check if root operator exists ina query plan
    if (rootOperators.empty())
    {
        throw QueryInvalid("Query "s + queryPlan->toString() + " does not contain any root operator");
    }

    /// Check if all root operators of type sink
    for (auto& root : rootOperators)
    {
        if (!NES::Util::instanceOf<SinkLogicalOperator>(root))
        {
            throw QueryInvalid("Query "s + queryPlan->toString() + " does not contain a valid sink operator as root");
        }
    }
}

void SemanticQueryValidation::inferModelValidityCheck(const std::shared_ptr<QueryPlan>& queryPlan)
{
    auto inferModelOperators = queryPlan->getOperatorByType<InferModel::LogicalInferModelOperator>();
    if (!inferModelOperators.empty())
    {
        std::shared_ptr<DataType> commonStamp;
        for (const auto& inferModelOperator : inferModelOperators)
        {
            for (const auto& inputField : inferModelOperator->getInputFields())
            {
                auto field = NES::Util::as<NodeFunctionFieldAccess>(inputField);
                if (!NES::Util::instanceOf<Numeric>(field->getStamp()) && !NES::Util::instanceOf<Boolean>(field->getStamp())
                    && !NES::Util::instanceOf<VariableSizedDataType>(field->getStamp()))
                {
                    throw QueryInvalid(
                        "SemanticQueryValidation::advanceSemanticQueryValidation: Inputted data type for infer model not supported: "
                        + field->getStamp()->toString());
                }
                if (!commonStamp)
                {
                    commonStamp = field->getStamp();
                }
                else
                {
                    commonStamp = commonStamp->join(field->getStamp());
                }
            }
        }
        NES_DEBUG("SemanticQueryValidation::advanceSemanticQueryValidation: Common stamp is: {}", commonStamp->toString());
        if (NES::Util::instanceOf<Undefined>(commonStamp))
        {
            throw QueryInvalid("SemanticQueryValidation::advanceSemanticQueryValidation: Boolean and Numeric data types cannot be mixed as "
                               "input to infer model.");
        }
    }
}
}
