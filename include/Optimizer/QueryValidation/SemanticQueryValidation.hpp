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

#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <memory>

namespace NES {

class SemanticQueryValidation;
typedef std::shared_ptr<SemanticQueryValidation> SemanticQueryValidationPtr;

/**
 * @brief This class is responsible for Semantic Query Validation
 */
class SemanticQueryValidation {
    private:
        StreamCatalogPtr streamCatalog;
        bool isLogicalStreamInCatalog(std::string streamname, std::map<std::string, std::string> allLogicalStreams);
        void sourceValidityCheck(NES::QueryPlanPtr queryPlan, StreamCatalogPtr streamCatalog);
        void handleException(std::string & predicateString);
        void eraseAllSubStr(std::string & mainStr, const std::string & toErase);
        void findAndReplaceAll(std::string & data, std::string toSearch, std::string replaceStr);
    public:
        void checkSatisfiability(QueryPtr inputQuery);
        SemanticQueryValidation(StreamCatalogPtr scp);
        static SemanticQueryValidationPtr create(StreamCatalogPtr scp);
};

typedef std::shared_ptr<SemanticQueryValidation> SemanticQueryValidationPtr;

}// namespace NES

#endif//NES_OPTIMIZE_SEMANTIC_QUERY_VALIDATION_HPP