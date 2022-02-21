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

#ifndef NES_INCLUDE_OPTIMIZER_QUERYVALIDATION_SEMANTICQUERYVALIDATION_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYVALIDATION_SEMANTICQUERYVALIDATION_HPP_

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

  public:
    /**
     * @brief Checks the semantic validity of a Query object
     * @param inputQuery: query to check
     */
    void validate(const QueryPtr& inputQuery);

    /**
     * @brief Constructor for the SemanticQueryValidation class
     * @param sourceCatalog: source catalog
     * @param advanceChecks: perform advance check
     */
    explicit SemanticQueryValidation(SourceCatalogPtr sourceCatalog, bool advanceChecks);

    /**
     * @brief Creates an instance of SemanticQueryValidation
     * @param sourceCatalog: source catalog
     * @param advanceChecks: perform advance check
     */
    static SemanticQueryValidationPtr create(const SourceCatalogPtr& sourceCatalog, bool advanceChecks);

    /**
     * Performs advance semantic validation of the queries. For example, checking if the filters in the query are semantically valid.
     * @param queryPlan: query plan on which the semantic validation is to be applied
     */
    void advanceSemanticQueryValidation(QueryPlanPtr& queryPlan);

  private:

    /**
     * @brief Checks if the logical source in the provided QueryPlan is valid
     * @param queryPlan: query plan to check
     * @param sourceCatalog: source catalog
     */
    static void logicalSourceValidityCheck(const NES::QueryPlanPtr& queryPlan, const SourceCatalogPtr& sourceCatalog);

    /**
     * @brief Checks if the physical source for the provided QueryPlan is present
     * @param queryPlan: query plan to check
     * @param sourceCatalog: source catalog
     */
    static void physicalSourceValidityCheck(const NES::QueryPlanPtr& queryPlan, const SourceCatalogPtr& sourceCatalog);

    /**
     * @brief Throws InvalidQueryException with formatted exception message
     * @param predicateString: predicate string
     */
    void createExceptionForPredicate(std::string& predicateString);

    /**
     * @brief Deletes a substring from a string
     * @param mainStr: main string
     * @param toErase: string to erase
     */
    static void eraseAllSubStr(std::string& mainStr, const std::string& toErase);

    /**
     * @brief Replaces all occurrences of a substring in a string
     * @param data: data to search in
     * @param toSearch: data to search
     * @param replaceStr: replace the string
     */
    static void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr);

    SourceCatalogPtr streamCatalog;
    bool performAdvanceChecks;
};

using SemanticQueryValidationPtr = std::shared_ptr<SemanticQueryValidation>;

}// namespace NES::Optimizer

#endif// NES_INCLUDE_OPTIMIZER_QUERYVALIDATION_SEMANTICQUERYVALIDATION_HPP_
