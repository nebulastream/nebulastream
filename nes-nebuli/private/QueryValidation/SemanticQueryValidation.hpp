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

#pragma once

#include <memory>
#include <Plans/Query/QueryPlan.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>

namespace NES
{

namespace Catalogs::UDF
{
}

}

namespace NES::Optimizer
{

/**
 * @brief This class is responsible for Semantic Query Validation
 */
class SemanticQueryValidation
{
public:
    /**
     * @brief Checks the semantic validity of a Query object
     * @param queryPlan: query to check
     */
    void validate(const std::shared_ptr<QueryPlan>& queryPlan) const;

    /**
     * @brief Constructor for the SemanticQueryValidation class
     * @param sourceCatalog: source catalog
     * @param udfCatalog: udf catalog
     * @param advanceChecks: perform advance check
     */
    explicit SemanticQueryValidation(const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog);

    /**
     * @brief Creates an instance of SemanticQueryValidation
     * @param sourceCatalog: source catalog
     * @param udfCatalog: udf catalog
     * @param advanceChecks: perform advance check
     */
    static std::shared_ptr<SemanticQueryValidation> create(const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog);

private:
    /**
     * Check if infer model operator is correctly defined or not
     * @param queryPlan: query plan to check
     */
    static void inferModelValidityCheck(const std::shared_ptr<QueryPlan>& queryPlan);

    /**
     * Performs semantic validation if the query plan has sink as it root operator.
     * @param queryPlan: query plan on which the semantic validation is to be applied
     */
    static void sinkOperatorValidityCheck(const std::shared_ptr<QueryPlan>& queryPlan);

    /**
     * @brief Checks if the logical source in the provided QueryPlan is valid
     * @param queryPlan: query plan to check
     * @param sourceCatalog: source catalog
     */
    static void logicalSourceValidityCheck(
        const std::shared_ptr<QueryPlan>& queryPlan, const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog);

    /**
     * @brief Checks if the physical source for the provided QueryPlan is present
     * @param queryPlan: query plan to check
     * @param sourceCatalog: source catalog
     */
    static void physicalSourceValidityCheck(
        const std::shared_ptr<QueryPlan>& queryPlan, const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog);

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

    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;
};
}
