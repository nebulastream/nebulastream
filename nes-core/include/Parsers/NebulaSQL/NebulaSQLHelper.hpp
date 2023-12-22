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
    WINDOW_THRESHOLD,
};
        class NebulaSQLHelper {
          private:
            std::vector<ExpressionNodePtr> projectionFields;
            std::list<ExpressionNodePtr> whereClauses;
            std::list<ExpressionNodePtr> havingClauses;
            std::string source;







          public:
            //Constructors
            NebulaSQLHelper() = default;

            std::vector<QueryPlanPtr> queryPlans;
            bool isSelect = false;
            bool isWhereOrHaving = false;
            bool isFrom = false;
            bool isWindow = false;
            bool isArithmeticBinary = false;
            bool isJoinRelation = false;
            bool isFunctionCall = false;
            bool isSimpleCondition = true;
            bool hasMultipleAttributes = false;
            std::shared_ptr<Windowing::WindowType> windowType;
            bool isTimeBasedWindow = true;
            bool isSetOperation = false;
            bool isGroupBy;
            std::vector<WindowAggregationDescriptorPtr> windowAggs;

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
            int minimumCount = -1;

            int identCountHelper = 0;
            int implicitMapCountHelper = 0;
            std::vector<FieldAccessExpressionNodePtr> groupByFields;
            std::vector<std::pair<ExpressionNodePtr, ExpressionNodePtr>> joinKeys;
            std::vector<std::string> joinSources;
            std::vector<ExpressionNodePtr> joinKeyRelationHelper;
            std::vector<std::string> joinSourceRenames;



            // Getter and Setter

            const std::list<ExpressionNodePtr>& getWhereClauses() const;
            const std::list<ExpressionNodePtr>& getHavingClauses() const;
            const std::vector<ExpressionNodePtr>& getProjectionFields() const;
            void addWhereClause(ExpressionNodePtr expressionNode);
            void addHavingClause(ExpressionNodePtr expressionNode);
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
