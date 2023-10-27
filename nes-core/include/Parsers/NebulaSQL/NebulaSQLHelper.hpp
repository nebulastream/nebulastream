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

#ifndef NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLHELPER_HPP_
#define NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLHELPER_HPP_

#include <API/QueryAPI.hpp>
#include <Parsers/NebulaSQL/NebulaSQLOperatorNode.hpp>
#include <list>
#include <map>
#include <string>

namespace NES::Parsers {

        /**
* @brief This class represents the results from parsing the ANTLR AST tree
* Attributes of this class represent the different clauses and a merge into a query after parsing the AST
*/
        class NebulaSQLHelper {
          private:
            std::vector<ExpressionNodePtr> projectionFields;
            std::vector<std::string> sources;
            SinkDescriptorPtr sinkDescriptor;
            std::vector<ExpressionNodePtr> whereClauses;


          public:
            void setProjectionFields(const std::vector<ExpressionNodePtr>& projectionFields);
            void addProjectionField(ExpressionNodePtr expressionNode);

            const std::vector<std::string>& getSources() const;
            void addSource(const std::string& source);

            const SinkDescriptorPtr getSinkDescriptor() const;
            void setSink(SinkDescriptorPtr sink);

            const std::vector<ExpressionNodePtr>& getWhereClauses() const;
            void addWhereClause(ExpressionNodePtr expression);

            const std::vector<ExpressionNodePtr>& getProjectionFields() const;

            uint64_t getLimit() const;
            const std::string& getNewName() const;
            const FieldAssignmentExpressionNodePtr& getMapExpression() const;
            const WatermarkStrategyDescriptorPtr& getWatermarkStrategieDescriptor() const;
            const NES::Windowing::WindowTypePtr getWindowType() const;
        };
    }// namespace NES::Parsers

#endif// NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLHELPER_HPP_
