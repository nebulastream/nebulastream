/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Catalogs/SourceCatalog.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Optimizer/QueryValidation/SemanticQueryValidation.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <cstring>
#include <utility>
#include <z3++.h>

namespace NES::Optimizer {

SemanticQueryValidation::SemanticQueryValidation(SourceCatalogPtr streamCatalog) : streamCatalog(std::move(streamCatalog)) {}

SemanticQueryValidationPtr SemanticQueryValidation::create(const SourceCatalogPtr& scp) {
    return std::make_shared<SemanticQueryValidation>(scp);
}

void SemanticQueryValidation::checkSatisfiability(const QueryPtr& inputQuery) {

    // Creating a z3 context for the signature inference and the z3 solver
    z3::ContextPtr context = std::make_shared<z3::context>();

    auto queryPlan = inputQuery->getQueryPlan();
    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(context, QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);

    // Checking if the stream source can be found in the stream catalog
    sourceValidityCheck(queryPlan, streamCatalog);

    try {
        typeInferencePhase->execute(queryPlan);
        signatureInferencePhase->execute(queryPlan);
    } catch (std::exception& e) {
        std::string errorMessage = e.what();

        // Handling nonexistend field
        if (errorMessage.find("FieldAccessExpression:") != std::string::npos) {
            throw InvalidQueryException("SemanticQueryValidation: " + errorMessage + "\n");
        }
        throw InvalidQueryException("SemanticQueryValidation: " + errorMessage + "\n");
    }

    z3::solver solver(*context);
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
            handleException(predicateStr);
        }
    }
}

void SemanticQueryValidation::handleException(std::string& predicateString) {

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

    throw InvalidQueryException("SemanticQueryValidation: Unsatisfiable Query due to filter condition:\n" + predicateString
                                + "\n");
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

void SemanticQueryValidation::sourceValidityCheck(const NES::QueryPlanPtr& queryPlan, const SourceCatalogPtr& streamCatalog) {

    // Getting the source operators from the query plan
    auto sourceOperators = queryPlan->getSourceOperators();

    for (const auto& source : sourceOperators) {
        auto sourceDescriptor = source->getSourceDescriptor();

        // Filtering for logical stream sources
        if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
            auto streamName = sourceDescriptor->getStreamName();

            // Making sure that all logical stream sources are present in the stream catalog
            if (!streamCatalog->testIfLogicalStreamExistsInSchemaMapping(streamName)) {
                throw InvalidQueryException("SemanticQueryValidation: The logical stream " + streamName
                                            + " could not be found in the SourceCatalog\n");
            }
        }
    }
}

}// namespace NES::Optimizer