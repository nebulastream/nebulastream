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

#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/DataTypes/ArrayType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Exceptions/SignatureComputationException.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QuerySignatures/QuerySignature.hpp>
#include <Util/QuerySignatures/QuerySignatureUtil.hpp>
#include <iterator>
#include <utility>
#include <z3++.h>

using namespace std::string_literals;

namespace NES::Optimizer {
SemanticQueryValidation::SemanticQueryValidation(const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                 const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                                                 bool advanceChecks)
    : sourceCatalog(sourceCatalog), performAdvanceChecks(advanceChecks), udfCatalog(udfCatalog) {}

SemanticQueryValidationPtr SemanticQueryValidation::create(const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                           const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                                                           bool advanceChecks) {
    return std::make_shared<SemanticQueryValidation>(sourceCatalog, udfCatalog, advanceChecks);
}

void SemanticQueryValidation::validate(const QueryPlanPtr& queryPlan) {
    // check if we have valid root operators, i.e., sinks
    sinkOperatorValidityCheck(queryPlan);
    // Checking if the logical source can be found in the source catalog
    logicalSourceValidityCheck(queryPlan, sourceCatalog);
    // Checking if the physical source can be found in the source catalog
    physicalSourceValidityCheck(queryPlan, sourceCatalog);

    try {
        auto typeInferencePhase = TypeInferencePhase::create(sourceCatalog, udfCatalog);
        typeInferencePhase->execute(queryPlan, queryPlan->getFaultTolerance());
    } catch (std::exception& e) {
        std::string errorMessage = e.what();
        // Handling nonexistend field
        if (errorMessage.find("FieldAccessExpression:") != std::string::npos) {
            throw InvalidQueryException(errorMessage + "\n");
        }
        throw InvalidQueryException(errorMessage + "\n");
    }

    // check if infer model is correctly defined
    inferModelValidityCheck(queryPlan);

    if (performAdvanceChecks) {
        advanceSemanticQueryValidation(queryPlan);
    }
}

void SemanticQueryValidation::advanceSemanticQueryValidation(const QueryPlanPtr& queryPlan) {
    try {
        // Creating a z3 context for the signature inference and the z3 solver
        z3::ContextPtr context = std::make_shared<z3::context>();
        auto signatureInferencePhase =
            SignatureInferencePhase::create(context, QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
        z3::solver solver(*context);
        signatureInferencePhase->execute(queryPlan);
        auto filterOperators = queryPlan->getOperatorByType<LogicalFilterOperator>();

        // Looping through all filter operators of the Query, checking for contradicting conditions.
        for (const auto& filterOp : filterOperators) {
            QuerySignaturePtr qsp = filterOp->getZ3Signature();

            // Adding the conditions to the z3 SMT solver
            z3::ExprPtr conditions = qsp->getConditions();
            solver.add(*(qsp->getConditions()));
            std::stringstream s;
            s << solver;
            NES_INFO("{}", s.str());
            // If the filter conditions are unsatisfiable, we report the one that broke satisfiability
            if (solver.check() == z3::unsat) {
                auto predicateStr = filterOp->getPredicate()->toString();
                createExceptionForPredicate(predicateStr);
            }
        }
    } catch (SignatureComputationException& ex) {
        NES_WARNING("Unable to compute signature due to {}", ex.what());
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

    // Adding whitespace between bool operators for better readability
    findAndReplaceAll(predicateString, "&&", " && ");
    findAndReplaceAll(predicateString, "||", " || ");

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
                                                         const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {

    // Getting the source operators from the query plan
    auto sourceOperators = queryPlan->getSourceOperators();

    for (const auto& source : sourceOperators) {
        auto sourceDescriptor = source->getSourceDescriptor();

        // Filtering for logical sources
        if (sourceDescriptor->instanceOf<LogicalSourceDescriptor>()) {
            auto sourceName = sourceDescriptor->getLogicalSourceName();

            // Making sure that all logical sources are present in the source catalog
            if (!sourceCatalog->containsLogicalSource(sourceName)) {
                throw InvalidQueryException("The logical source '" + sourceName + "' can not be found in the SourceCatalog\n");
            }
        }
    }
}

void SemanticQueryValidation::physicalSourceValidityCheck(const QueryPlanPtr& queryPlan,
                                                          const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
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

void SemanticQueryValidation::sinkOperatorValidityCheck(const QueryPlanPtr& queryPlan) {
    auto rootOperators = queryPlan->getRootOperators();
    //Check if root operator exists ina query plan
    if (rootOperators.empty()) {
        throw InvalidQueryException("Query "s + queryPlan->toString() + " does not contain any root operator");
    }

    //Check if all root operators of type sink
    for (auto& root : rootOperators) {
        if (!root->instanceOf<SinkLogicalOperator>()) {
            throw InvalidQueryException("Query "s + queryPlan->toString() + " does not contain a valid sink operator as root");
        }
    }
}

void SemanticQueryValidation::inferModelValidityCheck(const QueryPlanPtr& queryPlan) {

    auto inferModelOperators = queryPlan->getOperatorByType<InferModel::LogicalInferModelOperator>();
    if (!inferModelOperators.empty()) {
        DataTypePtr commonStamp;
        for (const auto& inferModelOperator : inferModelOperators) {
            for (const auto& inputField : inferModelOperator->getInputFields()) {
                auto field = inputField->as<FieldAccessExpressionNode>();
                if (!field->getStamp()->isNumeric() && !field->getStamp()->isBoolean() && !field->getStamp()->isText()) {
                    throw InvalidQueryException("SemanticQueryValidation::advanceSemanticQueryValidation: Inputted data type for "
                                                "infer model not supported: "
                                                + field->getStamp()->toString());
                }
                if (!commonStamp) {
                    commonStamp = field->getStamp();
                } else {
                    commonStamp = commonStamp->join(field->getStamp());
                }
            }
        }
        NES_DEBUG("SemanticQueryValidation::advanceSemanticQueryValidation: Common stamp is: {}", commonStamp->toString());
        if (commonStamp->isUndefined()) {
            throw InvalidQueryException("SemanticQueryValidation::advanceSemanticQueryValidation: Boolean and Numeric data types "
                                        "cannot be mixed as input to infer model.");
        }
    }
}

}// namespace NES::Optimizer
