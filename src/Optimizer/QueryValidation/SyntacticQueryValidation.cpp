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

#include <API/Expressions/Expressions.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryParsingService.hpp>

namespace NES::Optimizer {

SyntacticQueryValidation::SyntacticQueryValidation(QueryParsingServicePtr queryParsingService)
    : queryParsingService(queryParsingService) {}

SyntacticQueryValidationPtr SyntacticQueryValidation::create(QueryParsingServicePtr queryParsingService) {
    return std::make_shared<SyntacticQueryValidation>(queryParsingService);
}

bool SyntacticQueryValidation::checkIfFieldRenameExpressionExist(NodePtr start) {
    bool isFieldRenameExpressionExist = false;
    if (!start->getChildren().empty()) {
        for (auto child : start->getChildren()) {
            if (child->instanceOf<FieldRenameExpressionNode>()) {
                isFieldRenameExpressionExist = true;
            }

            if (!isFieldRenameExpressionExist) {
                isFieldRenameExpressionExist = checkIfFieldRenameExpressionExist(child);
            } else {
                break;
            }
        }
    }
    return isFieldRenameExpressionExist;
}

void SyntacticQueryValidation::checkValidity(const std::string& inputQuery) {
    try {
        // Compiling the query string to an object
        // If it's unsuccessful, the validity check fails
        QueryPtr query = queryParsingService->createQueryFromCodeString(inputQuery);

        // check for attribute rename operation outside projection
        auto queryPlanIterator = QueryPlanIterator(query->getQueryPlan());
        for (auto qPlanItr = queryPlanIterator.begin(); qPlanItr != QueryPlanIterator::end(); ++qPlanItr) {
            // set data modification factor for map operator
            if ((*qPlanItr)->instanceOf<MapLogicalOperatorNode>()) {
                const auto mapExp = (*qPlanItr)->as<MapLogicalOperatorNode>()->getMapExpression();
                auto isFieldRenameExists = checkIfFieldRenameExpressionExist(mapExp);
                if (isFieldRenameExists) {
                    throw InvalidQueryException("Attribute rename used outside projection operator.");
                }
            } else if ((*qPlanItr)->instanceOf<FilterLogicalOperatorNode>()) {
                const auto predicateExp = (*qPlanItr)->as<FilterLogicalOperatorNode>()->getPredicate();
                auto isFieldRenameExists = checkIfFieldRenameExpressionExist(predicateExp);
                if (isFieldRenameExists) {
                    throw InvalidQueryException("Attribute rename used outside projection operator.");
                }
            } else if((*qPlanItr)->instanceOf<WindowOperatorNode>()) {
                // TODO #1489: Add checks here
            } else if((*qPlanItr)->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
                // TODO #1489: Add checks here
            } else if((*qPlanItr)->instanceOf<JoinLogicalOperatorNode>()) {
                // TODO #1489: Add checks here
            }
        }

    } catch (const std::exception& ex) {
        handleException(ex);
    }
}

NES::QueryPtr SyntacticQueryValidation::checkValidityAndGetQuery(const std::string& inputQuery) {
    NES::QueryPtr query;
    try {
        // Compiling the query string to an object
        // If it's unsuccessful, the validity check fails
        // If it's successful, we return the created object
        query = queryParsingService->createQueryFromCodeString(inputQuery);
        return query;
    } catch (const std::exception& ex) {
        handleException(ex);
    }
    return query;
}

void SyntacticQueryValidation::handleException(const std::exception& ex) {

    // We only keep the meaningful part of the exception message for better readability
    std::string error_message = ex.what();
    std::string start_str = "error: ";
    std::string end_str = "^";// arrow pointing to the syntax error (from gcc)

    int start_idx = error_message.find(start_str) + start_str.length();
    int end_idx = error_message.find(end_str) + end_str.length();
    std::string clean_error_message;

    // If "error:" and "^" are present, we only keep the part of the message that's in between them
    if (start_idx == -1 || end_idx == -1) {
        clean_error_message = error_message;
    } else {
        clean_error_message = error_message.substr(start_idx, end_idx - start_idx);
    }
    clean_error_message = "SyntacticQueryValidation:\n" + clean_error_message;
    throw InvalidQueryException(clean_error_message + "\n");
}

}// namespace NES::Optimizer