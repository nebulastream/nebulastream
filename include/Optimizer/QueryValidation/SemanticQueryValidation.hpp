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

#ifndef NES_OPTIMIZE_SEMANTIC_QUERY_VALIDATION_HPP
#define NES_OPTIMIZE_SEMANTIC_QUERY_VALIDATION_HPP

#include <memory>

namespace NES {
class schema;
using SchemaPtr = std::shared_ptr<Schema>;

class Query;
using QueryPtr = std::shared_ptr<Query>;

}// namespace NES

namespace NES::Optimizer {

class SemanticQueryValidation;
using SemanticQueryValidationPtr = std::shared_ptr<SemanticQueryValidation>;

/**
 * @brief This class is responsible for Semantic Query Validation
 */
class SemanticQueryValidation {
  private:
    StreamCatalogPtr streamCatalog;

    /**
     * @brief Checks if the stream source in the provided QueryPlan is valid
     */
    void sourceValidityCheck(NES::QueryPlanPtr queryPlan, StreamCatalogPtr streamCatalog);

    /**
     * @brief Throws InvalidQueryException with formatted exception message
     */
    void handleException(std::string& predicateString);

    /**
     * @brief Deletes a substring from a string
     */
    void eraseAllSubStr(std::string& mainStr, const std::string& toErase);

    /**
     * @brief Replaces all occurances of a substring in a string
     */
    void findAndReplaceAll(std::string& data, std::string toSearch, std::string replaceStr);

  public:
    /**
     * @brief Checks the semantic validity of a Query object
     */
    void checkSatisfiability(QueryPtr inputQuery);

    /**
     * @brief Constructor for the SemanticQueryValidation class
     */
    explicit SemanticQueryValidation(StreamCatalogPtr streamCatalog);

    /**
     * @brief Creates an instance of SemanticQueryValidation
     */
    static SemanticQueryValidationPtr create(StreamCatalogPtr scp);
};

using SemanticQueryValidationPtr = std::shared_ptr<SemanticQueryValidation>;

}// namespace NES::Optimizer

#endif//NES_OPTIMIZE_SEMANTIC_QUERY_VALIDATION_HPP