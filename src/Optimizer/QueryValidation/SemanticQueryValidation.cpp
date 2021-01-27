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

namespace NES {

bool NES::SemanticQueryValidation::isSatisfiable(QueryPtr inputQuery) {

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    z3::ContextPtr context = std::make_shared<z3::context>();
    
    auto queryPlan = inputQuery->getQueryPlan();
    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    auto signatureInferencePhase = Optimizer::SignatureInferencePhase::create(context);
    
    sourceValidityCheck(queryPlan, streamCatalog);

    try {
        typeInferencePhase->execute(queryPlan);
        signatureInferencePhase->execute(queryPlan);
    } catch(std::runtime_error& e) {
        std::string errorMessage = e.what();
        if(errorMessage.find("FieldAccessExpression:") != std::string::npos){
            // handle nonexistend field
            NES_THROW_RUNTIME_ERROR("SemanticQueryValidation: " + errorMessage + "\n");
        }

        NES_THROW_RUNTIME_ERROR("SemanticQueryValidation: " + errorMessage + "\n");
    }
    z3::solver solver(*context);
    auto filterOperators = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();

    for (int i = 0; i < filterOperators.size(); i++) {
        Optimizer::QuerySignaturePtr qsp = filterOperators[i]->getSignature();
        solver.add(*(qsp->getConditions()));
    }

    if(solver.check() == z3::unsat){
        NES_THROW_RUNTIME_ERROR("SemanticQueryValidation: Unsatisfiable Query\n");
    }

    return solver.check() != z3::unsat;
}

void NES::SemanticQueryValidation::sourceValidityCheck(NES::QueryPlanPtr queryPlan, StreamCatalogPtr streamCatalog){

    auto allLogicalStreams = streamCatalog->getAllLogicalStreamAsString();
    auto sourceOperators = queryPlan->getSourceOperators();

    for (auto source : sourceOperators) {
        auto sourceDescriptor = source->getSourceDescriptor();

        if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
            auto streamName = sourceDescriptor->getStreamName();

            if(!isLogicalStreamInCatalog(streamName, allLogicalStreams)){
                NES_THROW_RUNTIME_ERROR("SemanticQueryValidation: The logical stream " + streamName
                                        + " could not be found in the StreamCatalog\n");
            }
        }
    }
}

bool NES::SemanticQueryValidation::isLogicalStreamInCatalog(std::string streamname, std::map<std::string, std::string> allLogicalStreams) {
    for (auto logicalStream : allLogicalStreams){
        if(logicalStream.first == streamname){
            return true;
        }
    }
    return false;
}

}// namespace NES