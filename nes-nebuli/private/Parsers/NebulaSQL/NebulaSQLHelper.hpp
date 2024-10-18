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

#include <list>
#include <map>
#include <string>
#include <API/QueryAPI.hpp>
#include <Sinks/SinkDescriptor.hpp>

namespace NES::Parsers
{

/**
* @brief This class represents the results from parsing the ANTLR AST tree
* Attributes of this class represent the different clauses and a merge into a query after parsing the AST
*/
enum NebulaSQLWindowType
{
    NO_WINDOW,
    WINDOW_SLIDING,
    WINDOW_TUMBLING,
    WINDOW_THRESHOLD,
};
class NebulaSQLHelper
{
private:
    std::vector<std::shared_ptr<NES::NodeFunction>> projectionFields;
    std::list<std::shared_ptr<NES::NodeFunction>> whereClauses;
    std::list<std::shared_ptr<NES::NodeFunction>> havingClauses;
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

    std::vector<std::shared_ptr<NES::NodeFunction>> projections;
    std::vector<std::shared_ptr<Sinks::SinkDescriptor>> sinkDescriptor;

    std::vector<std::shared_ptr<NES::NodeFunction>> expressionBuilder;


    std::vector<std::shared_ptr<NES::NodeFunctionFieldAssignment>> mapBuilder;

    std::string opBoolean;
    std::string opValue;

    std::string newSourceName;

    std::string timestamp = "timestamp";
    int size = -1;
    std::string timeUnit;
    int minimumCount = -1;

    int identCountHelper = 0;
    int implicitMapCountHelper = 0;
    std::vector<std::shared_ptr<NES::NodeFunctionFieldAccess>> groupByFields;
    std::vector<std::pair<std::shared_ptr<NES::NodeFunction>, std::shared_ptr<NES::NodeFunction>>> joinKeys;
    std::vector<std::string> joinSources;
    std::vector<std::shared_ptr<NES::NodeFunction>> joinKeyRelationHelper;
    std::vector<std::string> joinSourceRenames;


    // Getter and Setter

    const std::list<std::shared_ptr<NES::NodeFunction>>& getWhereClauses() const;
    const std::list<std::shared_ptr<NES::NodeFunction>>& getHavingClauses() const;
    const std::vector<std::shared_ptr<NES::NodeFunction>>& getProjectionFields() const;
    void addWhereClause(std::shared_ptr<NES::NodeFunction> expressionNode);
    void addHavingClause(std::shared_ptr<NES::NodeFunction> expressionNode);
    void addProjectionField(std::shared_ptr<NES::NodeFunction> expressionNode);
    const NES::Windowing::WindowTypePtr getWindowType() const;
    void addSource(std::string sourceName);
    const std::string getSource() const;
    void addMapExpression(std::shared_ptr<NES::NodeFunctionFieldAssignment> expressionNode);
    std::vector<std::shared_ptr<NES::NodeFunctionFieldAssignment>> getMapExpressions() const;
    void setMapExpressions(std::vector<std::shared_ptr<NES::NodeFunctionFieldAssignment>> expressions);
};
} // namespace NES::Parsers

#endif // NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLHELPER_HPP_
