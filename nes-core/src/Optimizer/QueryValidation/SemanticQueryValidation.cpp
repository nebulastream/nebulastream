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

#include <API/Query.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/SignatureComputationException.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/QuerySignatureUtil.hpp>
#include <Optimizer/QueryValidation/SemanticQueryValidation.hpp>
#include <cstring>
#include <iterator>
#include <utility>
#include <z3++.h>

namespace NES::Optimizer {

SemanticQueryValidation::SemanticQueryValidation(SourceCatalogPtr sourceCatalog) : streamCatalog(std::move(sourceCatalog)) {}

SemanticQueryValidationPtr SemanticQueryValidation::create(const SourceCatalogPtr& sourceCatalog) {
    return std::make_shared<SemanticQueryValidation>(sourceCatalog);
}

void SemanticQueryValidation::validate(const QueryPtr& inputQuery) {

    auto queryPlan = inputQuery->getQueryPlan();

    // Checking if the logical source can be found in the source catalog
    logicalSourceValidityCheck(queryPlan, streamCatalog);
    // Checking if the physical source can be found in the source catalog
    physicalSourceValidityCheck(queryPlan, streamCatalog);

    try {
        auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
        typeInferencePhase->execute(queryPlan);
    } catch (std::exception& e) {
        std::string errorMessage = e.what();

        // Handling nonexistend field
        if (errorMessage.find("FieldAccessExpression:") != std::string::npos) {
            throw InvalidQueryException(errorMessage + "\n");
        }
        throw InvalidQueryException(errorMessage + "\n");
    }

    try {
        // Creating a z3 context for the signature inference and the z3 solver
        z3::ContextPtr context = std::make_shared<z3::context>();
        auto signatureInferencePhase =
            Optimizer::SignatureInferencePhase::create(context, QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
        z3::solver solver(*context);
        signatureInferencePhase->execute(queryPlan);
        auto filterOperators = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();

        // Looping through all filter operators of the Query, checking for contradicting conditions.
        for (const auto& filterOp : filterOperators) {
            Optimizer::QuerySignaturePtr qsp = filterOp->getZ3Signature();

            // Adding the conditions to the z3 SMT solver
            z3::ExprPtr conditions = qsp->getConditions();
            solver.add(*(qsp->getConditions()));

            // If the filter conditions are unsatisfiable, we report the one that broke satisfiability
            if (solver.check() == z3::unsat) {
                auto predicateStr = filterOp->getPredicate()->toString();
                createExceptionForPredicate(predicateStr);
            }
        }
    } catch (SignatureComputationException& ex) {
        NES_WARNING("Unable to compute signature due to " << ex.what());
    } catch (std::exception& e) {
        std::string errorMessage = e.what();
        throw InvalidQueryException(errorMessage + "\n");
    }
}

void SemanticQueryValidation::createExceptionForPredicate(std::string& predicateString) {

    // Removing unnecessary data from the error messages for better readability
    eraseAllSubStr(predicateString, "[INTEGER]");
    eraseAllSubStr(predicateString, "(");
    eraseAllSubStr(predicateString, ")");
    eraseAllSubStr(predicateString, "FieldAccessNode");
    eraseAllSubStr(predicateString, "ConstantValue");
    eraseAllSubStr(predicateString, "BasicValue");
    eraseAllSubStr(predicateString, "$");

    // Adding whitespace between bool operators for better readability
    findAndReplaceAll(predicateString, "&&", " && ");
    findAndReplaceAll(predicateString, "||", " || ");

    // Removing logical stream names for better readability
    auto allLogicalStreams = streamCatalog->getAllLogicalStreamAsString();
    for (const auto& logicalStream : allLogicalStreams) {
        eraseAllSubStr(predicateString, logicalStream.first);
    }

    throw InvalidQueryException("Unsatisfiable Query due to filter condition:\n" + predicateString + "\n");
}

void SemanticQueryValidation::eraseAllSubStr(std::string& mainStr, const std::string& toErase) {
    size_t pos = std::string::npos;
    while ((pos = mainStr.find(toErase)) != std::string::npos) {
        mainStr.erase(pos, toErase.length());
    }
}

void SemanticQueryValidation::findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr) {
    size_t pos = data.find(toSearch);
    while (pos != std::string::npos) {
        data.replace(pos, toSearch.size(), replaceStr);
        pos = data.find(toSearch, pos + replaceStr.size());
    }
}

void SemanticQueryValidation::logicalSourceValidityCheck(const NES::QueryPlanPtr& queryPlan,
                                                         const SourceCatalogPtr& sourceCatalog) {

    // Getting the source operators from the query plan
    auto sourceOperators = queryPlan->getSourceOperators();

    for (const auto& source : sourceOperators) {
        auto sourceDescriptor = source->getSourceDescriptor();

        // Filtering for logical stream sources
        if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
            auto streamName = sourceDescriptor->getLogicalSourceName();

            // Making sure that all logical stream sources are present in the stream catalog
            if (!sourceCatalog->containsLogicalSource(streamName)) {
                throw InvalidQueryException("The logical source '" + streamName + "' can not be found in the SourceCatalog\n");
            }
        }
    }
}

void SemanticQueryValidation::physicalSourceValidityCheck(const QueryPlanPtr& queryPlan, const SourceCatalogPtr& sourceCatalog) {
    //Identify the source operators
    auto sourceOperators = queryPlan->getSourceOperators();
    std::vector<std::string> invalidLogicalSourceNames;
    for (auto sourceOperator : sourceOperators) {
        auto logicalSourceName = sourceOperator->getSourceDescriptor()->getLogicalSourceName();
        if (sourceCatalog->getPhysicalSources(logicalSourceName).empty()) {
            invalidLogicalSourceNames.emplace_back(logicalSourceName);
        }
    }

    //If there are invalid logical sources then report them
    if (!invalidLogicalSourceNames.empty()) {
        std::stringstream invalidSources;
        invalidSources << "[";
        std::copy(invalidLogicalSourceNames.begin(),
                  invalidLogicalSourceNames.end() - 1,
                  std::ostream_iterator<std::string>(invalidSources, ","));
        invalidSources << invalidLogicalSourceNames.back();
        invalidSources << "]";
        throw InvalidQueryException("Logical source(s) " + invalidSources.str()
                                    + " are found to have no physical source(s) defined.");
    }
}

}// namespace NES::Optimizer