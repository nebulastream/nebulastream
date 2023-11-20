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
enum NebulaSQLWindowType{
    NO_WINDOW,
    WINDOW_SLIDING,
    WINDOW_TUMBLING,
    WINDOW_COUNT,
};
        class NebulaSQLHelper {
          private:
            std::vector<ExpressionNodePtr> projectionFields;
            std::vector<ExpressionNodePtr> whereClauses;
            std::string source;
            std::list<ExpressionNodePtr> expressionList;







          public:
            //Constructors
            NebulaSQLHelper() = default;

            std::vector<QueryPlanPtr> queryPlans;
            bool isSelect = false;
            bool isWhere = false;
            bool isFrom = false;
            bool isArithmeticBinary = false;
            bool isJoinRelation = false;
            bool isFunctionCall = false;
            bool isSimpleCondition = true;
            bool hasMultipleAttributes = false;


            std::vector<ExpressionNodePtr> projections;
            std::vector<SinkDescriptorPtr> sinkDescriptor;

            std::vector<ExpressionNodePtr> expressionBuilder;

            std::vector<FieldAssignmentExpressionNodePtr> mapBuilder;

            std::string opBoolean;
            std::string opValue;

            std::string newSourceName;

            std::string timestamp = "timestamp";
            int size = -1;
            std::string timeUnit;
            NebulaSQLWindowType windowType = NO_WINDOW;



            // Getter and Setter

            const std::map<int32_t, NebulaSQLOperatorNode>& getOperatorList() const;
            const std::list<ExpressionNodePtr>& getExpressions() const;
            const std::vector<ExpressionNodePtr>& getProjectionFields() const;
            void addExpression(ExpressionNodePtr expressionNode);
            void addProjectionField(ExpressionNodePtr expressionNode);
            const NES::Windowing::WindowTypePtr getWindowType() const;
            void addSource(std::string sourceName);
            const std::string getSource() const;
            void addMapExpression(FieldAssignmentExpressionNodePtr expressionNode);
            std::vector<FieldAssignmentExpressionNodePtr> getMapExpressions() const;
            void setMapExpressions(std::vector<FieldAssignmentExpressionNodePtr> expressions);
        };
    }// namespace NES::Parsers

#endif// NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLHELPER_HPP_
