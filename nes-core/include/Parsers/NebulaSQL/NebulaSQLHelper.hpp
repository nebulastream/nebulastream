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
            SinkDescriptorPtr sinkDescriptor;
            std::vector<ExpressionNodePtr> whereClauses;
            std::map<int32_t, std::string> sourceList;
            std::map<int32_t, NebulaSQLOperatorNode> operatorList;// contains the operators from the PATTERN clause
            std::list<ExpressionNodePtr> expressionList;
            std::list<SinkDescriptorPtr> sinkList; // INTO
            std::pair<std::string, int32_t> window;// WITHIN




            /*
            std::map<std::string, std::string> queryMap;

            std::string selectClause;
            std::string whereClause;
            std::string fromClause;
            std::string mapClause;
            std::string unionClause;




            std::string timeUnit;
            std::string timestampParameter;
            std::string sizeParameter;
            std::string advancebyParameter;
            std::string watermarkParameter;
            std::string countWindow;
            std::string globalWindow;
            std::string tumblingWindow;
            std::string slidingWindow;

            std::vector<std::string> aggregateFunctionList;
            std::string aggregateFunction;
            std::string groupByAttribute;

            bool isWindowTumbling = false;
            bool isWindowSliding = false;
            bool isFunctionCall = false;

            int childCountForAlias = 0; // child count for aggregate function alias
            std::string aggregateFuncForAlias;

            bool isJoinRelation = false;
            std::string joinCriteria;
            std::vector<std::string> joinRelationList;
            std::map<std::string, std::string> joinCriteriaMap;
            std::string secondRelation;

            bool isJoinCriteriaDereference = false;
            bool isHavingClause = false;
            std::string havingClause = "";
            bool isSinkClause = false;
            std::string sinkType;
            bool isWatermarkClause = false;
            std::string watermarkClause;

             */

          public:
            //Constructors
            NebulaSQLHelper() = default;
            // Getter and Setter
            const std::map<int32_t, std::string>& getSources() const;
            void setSources(const std::map<int32_t, std::string>& sources);
            const std::map<int32_t, NebulaSQLOperatorNode>& getOperatorList() const;
            void setOperatorList(const std::map<int32_t, NebulaSQLOperatorNode>& operatorList);
            const std::list<ExpressionNodePtr>& getExpressions() const;
            void setExpressions(const std::list<ExpressionNodePtr>& expressions);
            const std::vector<ExpressionNodePtr>& getProjectionFields() const;
            void setProjectionFields(const std::vector<ExpressionNodePtr>& projectionFields);
            const std::list<SinkDescriptorPtr>& getSinks() const;
            void setSinks(const std::list<SinkDescriptorPtr>& sinks);
            const std::pair<std::string, int>& getWindow() const;
            void setWindow(const std::pair<std::string, int>& window);
            void addSource(std::pair<int32_t, std::basic_string<char>> sourcePair);
            void updateSource(const int32_t key, std::string sourceName);
            void addExpression(ExpressionNodePtr expressionNode);
            void addSink(SinkDescriptorPtr sinkDescriptor);
            void addProjectionField(ExpressionNodePtr expressionNode);
            void addOperatorNode(NebulaSQLOperatorNode operatorNode);
            uint64_t getLimit() const;
            /*
            const std::string& getNewName() const;
            const FieldAssignmentExpressionNodePtr& getMapExpression() const;
            const WatermarkStrategyDescriptorPtr& getWatermarkStrategieDescriptor() const;
             */
            const NES::Windowing::WindowTypePtr getWindowType() const;
            std::vector<QueryPlanPtr> queryPlans;
            bool isSelect = false;
            bool isWhere = false;
            bool isFrom = false;
            bool isArithmeticBinary = false;
            bool isJoinRelation = false;
            bool isFunctionCall = false;

            bool hasMultipleAttributes = false;
             /*
            const std::map<std::string, std::string> getQueryMap() const;
            void setQueryMap(std::map<std::string, std::string> queryMap);
            */

            std::vector<ExpressionNodePtr> projections;

        };
    }// namespace NES::Parsers

#endif// NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLHELPER_HPP_
