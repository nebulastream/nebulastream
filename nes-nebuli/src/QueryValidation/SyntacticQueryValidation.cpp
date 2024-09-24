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

#include <string>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Optimizer
{
SyntacticQueryValidation::SyntacticQueryValidation(QueryParsingServicePtr queryParsingService) : queryParsingService(queryParsingService)
{
}

SyntacticQueryValidationPtr SyntacticQueryValidation::create(QueryParsingServicePtr queryParsingService)
{
    return std::make_shared<SyntacticQueryValidation>(queryParsingService);
}

NES::QueryPlanPtr SyntacticQueryValidation::validate(const std::string& inputQuery)
{
    NES::QueryPlanPtr queryPlan = nullptr;
    try
    {
        NES_DEBUG("SyntacticQueryValidation: parse C++ query from query string.");
        queryPlan = queryParsingService->createQueryFromCodeString(inputQuery);
        /// If it's unsuccessful, the validity check fails
    }
    catch (const std::exception& ex)
    {
        handleException(ex);
    }
    /// If it's successful, we return the created object
    return queryPlan;
}

void SyntacticQueryValidation::handleException(const std::exception& ex)
{
    /// We only keep the meaningful part of the exception message for better readability
    std::string error_message = ex.what();
    std::string start_str = "error: ";
    std::string end_str = "^"; /// arrow pointing to the syntax error (from gcc)

    int start_idx = error_message.find(start_str) + start_str.length();
    int end_idx = error_message.find(end_str) + end_str.length();
    std::string clean_error_message;

    /// If "error:" and "^" are present, we only keep the part of the message that's in between them
    if (start_idx == -1 || end_idx == -1)
    {
        clean_error_message = error_message;
    }
    else
    {
        clean_error_message = error_message.substr(start_idx, end_idx - start_idx);
    }
    throw QueryInvalid(": SyntacticQueryValidation:\n{}", clean_error_message);
}
}
