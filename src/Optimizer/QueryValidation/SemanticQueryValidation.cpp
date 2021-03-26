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


#include <Optimizer/QueryValidation/SemanticQueryValidation.hpp>
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Util/Logger.hpp>
#include <string.h>
#include <z3++.h>
#include <Exceptions/InvalidQueryException.hpp>

namespace NES {

NES::SemanticQueryValidation::SemanticQueryValidation(StreamCatalogPtr scp) {
    streamCatalog = scp;
}

SemanticQueryValidationPtr NES::SemanticQueryValidation::create(StreamCatalogPtr scp){
    return std::make_shared<SemanticQueryValidation>(scp);
}

void NES::SemanticQueryValidation::checkSatisfiability(QueryPtr inputQuery) {

    z3::ContextPtr context = std::make_shared<z3::context>();
    
    auto queryPlan = inputQuery->getQueryPlan();
    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    auto signatureInferencePhase = Optimizer::SignatureInferencePhase::create(context);
    
    sourceValidityCheck(queryPlan, streamCatalog);

    try {
        typeInferencePhase->execute(queryPlan);
        signatureInferencePhase->execute(queryPlan);
    } catch(std::exception& e) {
        std::string errorMessage = e.what();
        if(errorMessage.find("FieldAccessExpression:") != std::string::npos){
            // handle nonexistend field
            throw InvalidQueryException("SemanticQueryValidation: " + errorMessage + "\n");
        }

        throw InvalidQueryException("SemanticQueryValidation: " + errorMessage + "\n");
    }
    z3::solver solver(*context);
    auto filterOperators = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();

    for (auto filterOp : filterOperators) {
        Optimizer::QuerySignaturePtr qsp = filterOp->getSignature();

        z3::ExprPtr conditions = qsp->getConditions();
        solver.add(*(qsp->getConditions()));

        if(solver.check() == z3::unsat){
            auto predicateStr = filterOp->getPredicate()->toString();
            handleException(predicateStr);
        }
    }
}

void NES::SemanticQueryValidation::handleException(std::string & predicateString) {
    eraseAllSubStr(predicateString, "[INTEGER]");
    eraseAllSubStr(predicateString, "(");
    eraseAllSubStr(predicateString, ")");
    eraseAllSubStr(predicateString, "FieldAccessNode");
    eraseAllSubStr(predicateString, "ConstantValue");
    eraseAllSubStr(predicateString, "BasicValue");
    eraseAllSubStr(predicateString, "$");

    findAndReplaceAll(predicateString, "&&", " && ");
    findAndReplaceAll(predicateString, "||", " || ");

    auto allLogicalStreams = streamCatalog->getAllLogicalStreamAsString();
    for (auto logicalStream: allLogicalStreams){
        eraseAllSubStr(predicateString, logicalStream.first);
    } 
    
    throw InvalidQueryException("SemanticQueryValidation: Unsatisfiable Query due to filter condition:\n" 
                                + predicateString + "\n");
}

void NES::SemanticQueryValidation::eraseAllSubStr(std::string & mainStr, const std::string & toErase) {
    size_t pos = std::string::npos;
    while ((pos  = mainStr.find(toErase) )!= std::string::npos)
    {
        mainStr.erase(pos, toErase.length());
    }
}

void NES::SemanticQueryValidation::findAndReplaceAll(std::string & data, std::string toSearch, std::string replaceStr) {
    size_t pos = data.find(toSearch);
    while( pos != std::string::npos)
    {
        data.replace(pos, toSearch.size(), replaceStr);
        pos = data.find(toSearch, pos + replaceStr.size());
    }
}

void NES::SemanticQueryValidation::sourceValidityCheck(NES::QueryPlanPtr queryPlan, StreamCatalogPtr streamCatalog){

    auto sourceOperators = queryPlan->getSourceOperators();

    for (auto source : sourceOperators) {
        auto sourceDescriptor = source->getSourceDescriptor();

        if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
            auto streamName = sourceDescriptor->getStreamName();

            if(!streamCatalog->testIfLogicalStreamExistsInSchemaMapping(streamName)){
                throw InvalidQueryException("SemanticQueryValidation: The logical stream " + streamName
                                        + " could not be found in the StreamCatalog\n");
            }
        }
    }
}

}// namespace NES