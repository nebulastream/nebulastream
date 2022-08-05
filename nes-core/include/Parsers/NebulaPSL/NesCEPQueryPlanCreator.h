
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

#ifndef NES_NESCEPQUERYPLANCREATOR_H
#define NES_NESCEPQUERYPLANCREATOR_H

#include "NePSLPatternEventNode.h"
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Parsers/NebulaPSL/gen/NesCEPBaseListener.h>
#include <Plans/Query/QueryPlan.hpp>
#include <string>

namespace NES {
/**
 * @brief This class creates the query plan from ANTLR AST
 * It inherits from the auto-generated ANTLR base listener to walk the AST created from the pattern string.
 * This enables the parsing of declarative patterns into NES queries.
 */

class NesCEPQueryPlanCreator : public NesCEPBaseListener {



  private:
    int sourceCounter = 0;
    std::map<int,std::string> sources;
    std::map<int, NePSLPatternEventNode*> operatorList; // contains the operators from the PATTERN clause
    int currentOperatorPointer = -1;
    std::list<NES::ExpressionNodePtr> expressions;
    std::list<NES::ExpressionItem> projectionFields;
    std::list<std::shared_ptr<NES::SinkDescriptor>> sinks; // INTO
    std::pair<std::string,int> window; // WITHIN
    int lastSeenSourcePtr = -1;


    int nodeId = 0;
    int direction = -1;// 1 for right child, 0 for op -1, for left child
    int currentElementPointer = -1;


    std::map<int, NePSLPatternEventNode*>::iterator it = operatorList.begin();
    NES::Query query = NES::Query(NULL);

    bool inWhere = false;
    bool leftFilter = true;
    std::string currentLeftExp;
    std::string currentRightExp;

  public:
    const std::map<int, NePSLPatternEventNode*>& GetOperatorList() const;
    void SetOperatorList(const std::map<int, NePSLPatternEventNode*>& operator_list);
    const std::list<std::shared_ptr<NES::SinkDescriptor>>& GetSinks() const;
    void SetSinks(const std::list<std::shared_ptr<NES::SinkDescriptor>>& sinks);
    const std::pair<std::string, int>& getWindow() const;
    void setWindow(const std::pair<std::string, int>& window);
    int GetLastSeenSourcePtr() const;
    void SetLastSeenSourcePtr(int last_seen_source_ptr);
    int GetSourceCounter() const;
    void SetSourceCounter(int source_counter);
    int getNodeId() const;
    void setNodeId(int node_id);
    int getDirection() const;
    void setDirection(int direction);
    int GetCurrentElementPointer() const;
    void setCurrentElementPointer(int current_element_pointer);
    int GetCurrentParentPointer() const;
    void SetCurrentParentPointer(int current_parent_pointer);
    const std::map<int, NePSLPatternEventNode*>::iterator& GetIt() const;
    void SetIt(const std::map<int, NePSLPatternEventNode*>::iterator& it);
    const NES::Query& getQuery() const;
    void SetQuery(const NES::Query& query);
    bool IsInWhere() const;
    void SetInWhere(bool in_where);
    bool IsLeftFilter() const;
    void SetLeftFilter(bool left_filter);
    const std::string& GetCurrentLeftExp() const;
    void SetCurrentLeftExp(const std::string& current_left_exp);
    const std::string& GetCurrentRightExp() const;
    void SetCurrentRightExp(const std::string& current_right_exp);
    const std::map<int, std::string>& getSources();
    void setSources(const std::map<int, std::string>& sources);

    /** the following methods read out the AST tree and collect all mined patterns in the global pattern list
     * An example of the AST looks as follows:
     * (query (cepPattern PATTERN test := (compositeEventExpressions (
        (listEvents (eventElem (event A)) (operatorRule AND) (eventElem (event B))) ))
        FROM (inputStreams (inputStream default_logical AS A) , (inputStream default_logical_b AS B))
        WHERE (whereExp (expression (predicate (predicate (expressionAtom (eventAttribute A . (attribute currentSpeed)))) (comparisonOperator <) (predicate (expressionAtom (eventAttribute A . (attribute allowedSpeed)))))))
        WITHIN (timeConstraints [ (interval 3 (intervalType MINUTE)) ])
        INTO (sinkList (sink (sinkType Print) :: testSink))) <EOF>
     * each keyword in the AST has its two functions, enter (when X is visited) and exist (called after all children of X have been visited), contained in the parent class NesCEPBaseListener,
     * auto-generated by the ANTLR dependency (gen-folders)
     */

    //ListEvents
    /** @brief mines pattern from the PATTERN clause
      * @param context
      */
    void enterListEvents(NesCEPParser::ListEventsContext* context) override;

    // EventElement
    /** @brief marks current (event) element as a child of the currentOperatorPointer subPattern and
      * move one step up in the AST hierarchy
      * @param context
      */
    void exitEventElem(NesCEPParser::EventElemContext* context) override;

    // Event
    /**
         * @brief marks the position of the event inside of the currentElementPointer subPattern
         * @param context
         */
    void enterEvent(NesCEPParser::EventContext* context) override;

    // Operators
    /**
      * @brief adds operator to the currentOperatorPointer subPattern
      * @param context
      */
    void enterOperatorRule(NesCEPParser::OperatorRuleContext* context) override;

    /**
      * @brief leaves the current operator element and changes direction to right to mine right branch for binary operators
      * @param context
      */
    void exitOperatorRule(NesCEPParser::OperatorRuleContext* context) override;

    // FROM clause
    /** @brief substitutes the input stream alias by its real name in every subPattern
      * @param context
      */
    void exitInputStream(NesCEPParser::InputStreamContext* context) override;

    // WHERE clause
    /** @brief marks that the tree walker starts parsing a WHERE clause
      * @param context
      */
    void enterWhereExp(NesCEPParser::WhereExpContext* context) override;

    /** @brief marks that the tree walker finished parsing the WHERE clause
      * @param context
      */
    void exitWhereExp(NesCEPParser::WhereExpContext* context) override;

    //INTO clause
    /**
      * @brief add a sink operator to the sink list
      * @param context
      */
    void enterSink(NesCEPParser::SinkContext* context) override;

    /** @brief append the list of sinks to the query plan
      * @param context
      */
    void exitSinkList(NesCEPParser::SinkListContext* context) override;

    // WITHIN clause
    /** @brief
      * @param context
      */
    void exitInterval(NesCEPParser::IntervalContext* cxt) override;



    //TODO
    /**
         * @brief append a map operator to the query plan
         * @param context
         */
    void enterOutAttribute(NesCEPParser::OutAttributeContext* context) override;



    /**
         * @brief add the appropriate iteration to the currentElementPointer subPattern
         * @param context
         */
    void enterQuantifiers(NesCEPParser::QuantifiersContext* context) override;



    /**
         * @brief add a "<" filter to the query plan when needed
         * @param context
         */
    void exitBinaryComparasionPredicate(NesCEPParser::BinaryComparasionPredicateContext* context) override;

    /**
         * @brief if walker is inside of WHERE clause mark current position of the Attribute
         * @param context
         */
    void enterAttribute(NesCEPParser::AttributeContext* context) override;

    /**
     * @brief this class creates the query from the list of patterns
     */
    NES::Query createQueryFromPatternList();
    void addFilters();
    std::pair<Windowing::TimeMeasure, Windowing::TimeMeasure> transformWindowToTimeMeasurements();
    void addProjections();
};

}//end of namespace NES

#endif//NES_NESCEPQUERYPLANCREATOR_H