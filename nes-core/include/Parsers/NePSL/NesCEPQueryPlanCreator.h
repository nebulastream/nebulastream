
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

#include <Parsers/NePSL/gen/NesCEPBaseListener.h>
#include <API/NePSLPattern.h>
#include <Plans/Query/QueryPlan.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

class NesCEPQueryPlanCreator: public NesCEPBaseListener {
      private:
        int direction=-1; // -1 for right 0 for op 1 for left
        int currentPointer=-1;
        int currentParent=-1;
        int id=0;
        std::list<NePSLPattern*> patterns;
        std::list<NePSLPattern*>::iterator it = patterns.begin();
        NES::Query query=NES::Query(NULL);
        std::list<std::shared_ptr<NES::SinkDescriptor>> sinks;
        bool inWhere=false;
        bool leftFilter= true   ;
        std::string currentLeftExp;
        std::string currentRightExp;

      public:
        int getDirection();
        void setDirection(int direction);
        int getCurrentPointer();
        void setCurrentPointer(int currentPointer) ;
        int getCurrentParent() ;
        void setCurrentParent(int currentParent) ;
        int getId();
        void setId(int id);
        const NES::Query& getQuery() ;

        /*----------------------------------------*/
        /**
         * @brief creates a subPattern
         * @param context
         */
        void enterListEvents(NesCEPParser::ListEventsContext* context) override;

        /**
         * @brief creates a subPattern
         * @param context
         */
        void enterEventElem(NesCEPParser::EventElemContext* context) override ;

        /**
         * @brief append the sources and the operators that connect them to the query plan
         * @param context
         */
        void exitInputStreams(NesCEPParser::InputStreamsContext* context) override ;

        /**
         * @brief substitutes the input stream alias by its real name in every subPattern
         * @param context
         */
        void exitInputStream(NesCEPParser::InputStreamContext* context) override ;

        /**
         * @brief marks that the walker is in the WHERE clause
         * @param context
         */
        void enterWhereExp(NesCEPParser::WhereExpContext* context) override ;

        /**
         * @brief marks that the walker is no more in the WHERE clause
        * @param context
        */
        void exitWhereExp(NesCEPParser::WhereExpContext* context) override ;

        /**
         * @brief append a map operator to the query plan
         * @param context
         */
        void enterOutAttribute(NesCEPParser::OutAttributeContext* context) override;

        /**
         * @brief append the sink operators to the query plan
         * @param context
         */
        void exitSinkList(NesCEPParser::SinkListContext* context) override ;

        /**
         * @brief add a sink operator to the sink list
         * @param context
         */
        void enterSink(NesCEPParser::SinkContext* context) override;

        /**
         * @brief mark currentPointer subPattern as a child of the currentParent subPattern and move one step up in the hierarchy
         * @param context
         */
        void exitEventElem(NesCEPParser::EventElemContext* context) override ;

        /**
         * @brief mark the position of the event inside of the currentPointer subPattern
         * @param context
         */
        void enterEvent(NesCEPParser::EventContext* context) override ;

        /**
         * @brief add the appropriate iteration to the currentPointer subPattern
         * @param context
         */
        void enterQuantifiers(NesCEPParser::QuantifiersContext* context) override ;

        /**
         * @brief add the appropriate operator to the currentParent subPattern
         * @param context
         */
        void enterOperatorRule(NesCEPParser::OperatorRuleContext* context) override ;

        /**
         * @brief change direction to right
         * @param context
         */
        void exitOperatorRule(NesCEPParser::OperatorRuleContext* context) override;

        /**
         * @brief add a "<" filter to the query plan when needed
         * @param context
         */
        void exitBinaryComparasionPredicate(NesCEPParser::BinaryComparasionPredicateContext* context) override ;

        /**
         * @brief if walker is inside of WHERE clause mark current position of the Attribute
         * @param context
         */
        void enterAttribute(NesCEPParser::AttributeContext* context) override ;
    };

#endif//NES_NESCEPQUERYPLANCREATOR_H