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

#ifndef NES_INCLUDE_OPTIMIZER_QUERYVALIDATION_SYNTACTICQUERYVALIDATION_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYVALIDATION_SYNTACTICQUERYVALIDATION_HPP_

#include <memory>

namespace NES {

class Query;
using QueryPtr = std::shared_ptr<Query>;

class Pattern;
using PatternPtr = std::shared_ptr<Pattern>;

class QueryParsingService;
using QueryParsingServicePtr = std::shared_ptr<QueryParsingService>;

}// namespace NES

namespace NES::Optimizer {

class SyntacticQueryValidation;
using SyntacticQueryValidationPtr = std::shared_ptr<SyntacticQueryValidation>;

/**
 * @brief This class is responsible for Syntactic Query Validation
 */
class SyntacticQueryValidation {
  public:
    SyntacticQueryValidation(QueryParsingServicePtr queryParsingService);

    static SyntacticQueryValidationPtr create(QueryParsingServicePtr queryParsingService);

    /**
     * @brief Checks the syntactic validity of a Query string
     */
    void checkValidity(const std::string& inputQuery);

    /**
     * @brief Checks the syntactic validity of a Query string and returns the created Query object
     */
    QueryPtr validate(const std::string& inputQuery);

  private:
    QueryParsingServicePtr queryParsingService;
    /**
     * @brief Throws InvalidQueryException with formatted exception message
     */
    static void handleException(const std::exception& ex);
};

}// namespace NES::Optimizer

#endif  // NES_INCLUDE_OPTIMIZER_QUERYVALIDATION_SYNTACTICQUERYVALIDATION_HPP_
