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

#ifndef NES_INCLUDE_OPTIMIZER_QUERY_VALIDATION_SEMANTIC_QUERY_VALIDATION_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERY_VALIDATION_SEMANTIC_QUERY_VALIDATION_HPP_

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
    SourceCatalogPtr streamCatalog;

    /**
     * @brief Checks if the stream source in the provided QueryPlan is valid
     */
    static void sourceValidityCheck(const NES::QueryPlanPtr& queryPlan, const SourceCatalogPtr& streamCatalog);

    /**
     * @brief Throws InvalidQueryException with formatted exception message
     */
    void handleException(std::string& predicateString);

    /**
     * @brief Deletes a substring from a string
     */
    static void eraseAllSubStr(std::string& mainStr, const std::string& toErase);

    /**
     * @brief Replaces all occurances of a substring in a string
     */
    static void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr);

  public:
    /**
     * @brief Checks the semantic validity of a Query object
     */
    void checkSatisfiability(const QueryPtr& inputQuery);

    /**
     * @brief Constructor for the SemanticQueryValidation class
     */
    explicit SemanticQueryValidation(SourceCatalogPtr streamCatalog);

    /**
     * @brief Creates an instance of SemanticQueryValidation
     */
    static SemanticQueryValidationPtr create(const SourceCatalogPtr& scp);
};

using SemanticQueryValidationPtr = std::shared_ptr<SemanticQueryValidation>;

}// namespace NES::Optimizer

#endif// NES_INCLUDE_OPTIMIZER_QUERY_VALIDATION_SEMANTIC_QUERY_VALIDATION_HPP_
