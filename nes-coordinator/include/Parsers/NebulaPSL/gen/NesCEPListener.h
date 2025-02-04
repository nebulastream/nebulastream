
// Generated from CLionProjects/nebulastream/nes-coordinator/src/Parsers/NebulaPSL/gen/NesCEP.g4 by ANTLR 4.9.2

#ifndef NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPLISTENER_H_
#define NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPLISTENER_H_
#pragma once

#include <Parsers/NebulaPSL/gen/NesCEPParser.h>
#include <antlr4-runtime.h>

namespace NES::Parsers {

/**
 * This interface defines an abstract listener for a parse tree produced by NesCEPParser.
 */
class NesCEPListener : public antlr4::tree::ParseTreeListener {
  public:
    virtual void enterQuery(NesCEPParser::QueryContext* ctx) = 0;
    virtual void exitQuery(NesCEPParser::QueryContext* ctx) = 0;

    virtual void enterCepPattern(NesCEPParser::CepPatternContext* ctx) = 0;
    virtual void exitCepPattern(NesCEPParser::CepPatternContext* ctx) = 0;

    virtual void enterInputStreams(NesCEPParser::InputStreamsContext* ctx) = 0;
    virtual void exitInputStreams(NesCEPParser::InputStreamsContext* ctx) = 0;

    virtual void enterInputStream(NesCEPParser::InputStreamContext* ctx) = 0;
    virtual void exitInputStream(NesCEPParser::InputStreamContext* ctx) = 0;

    virtual void enterCompositeEventExpressions(NesCEPParser::CompositeEventExpressionsContext* ctx) = 0;
    virtual void exitCompositeEventExpressions(NesCEPParser::CompositeEventExpressionsContext* ctx) = 0;

    virtual void enterWhereExp(NesCEPParser::WhereExpContext* ctx) = 0;
    virtual void exitWhereExp(NesCEPParser::WhereExpContext* ctx) = 0;

    virtual void enterWindowConstraints(NesCEPParser::WindowConstraintsContext* ctx) = 0;
    virtual void exitWindowConstraints(NesCEPParser::WindowConstraintsContext* ctx) = 0;

    virtual void enterWindowType(NesCEPParser::WindowTypeContext* ctx) = 0;
    virtual void exitWindowType(NesCEPParser::WindowTypeContext* ctx) = 0;

    virtual void enterTimeConstraints(NesCEPParser::TimeConstraintsContext* ctx) = 0;
    virtual void exitTimeConstraints(NesCEPParser::TimeConstraintsContext* ctx) = 0;

    virtual void enterConstraint(NesCEPParser::ConstraintContext* ctx) = 0;
    virtual void exitConstraint(NesCEPParser::ConstraintContext* ctx) = 0;

    virtual void enterInterval(NesCEPParser::IntervalContext* ctx) = 0;
    virtual void exitInterval(NesCEPParser::IntervalContext* ctx) = 0;

    virtual void enterIntervalType(NesCEPParser::IntervalTypeContext* ctx) = 0;
    virtual void exitIntervalType(NesCEPParser::IntervalTypeContext* ctx) = 0;

    virtual void enterOption(NesCEPParser::OptionContext* ctx) = 0;
    virtual void exitOption(NesCEPParser::OptionContext* ctx) = 0;

    virtual void enterOutputExpression(NesCEPParser::OutputExpressionContext* ctx) = 0;
    virtual void exitOutputExpression(NesCEPParser::OutputExpressionContext* ctx) = 0;

    virtual void enterOutAttribute(NesCEPParser::OutAttributeContext* ctx) = 0;
    virtual void exitOutAttribute(NesCEPParser::OutAttributeContext* ctx) = 0;

    virtual void enterSinkList(NesCEPParser::SinkListContext* ctx) = 0;
    virtual void exitSinkList(NesCEPParser::SinkListContext* ctx) = 0;

    virtual void enterListEvents(NesCEPParser::ListEventsContext* ctx) = 0;
    virtual void exitListEvents(NesCEPParser::ListEventsContext* ctx) = 0;

    virtual void enterEventElem(NesCEPParser::EventElemContext* ctx) = 0;
    virtual void exitEventElem(NesCEPParser::EventElemContext* ctx) = 0;

    virtual void enterEvent(NesCEPParser::EventContext* ctx) = 0;
    virtual void exitEvent(NesCEPParser::EventContext* ctx) = 0;

    virtual void enterQuantifiers(NesCEPParser::QuantifiersContext* ctx) = 0;
    virtual void exitQuantifiers(NesCEPParser::QuantifiersContext* ctx) = 0;

    virtual void enterIterMax(NesCEPParser::IterMaxContext* ctx) = 0;
    virtual void exitIterMax(NesCEPParser::IterMaxContext* ctx) = 0;

    virtual void enterIterMin(NesCEPParser::IterMinContext* ctx) = 0;
    virtual void exitIterMin(NesCEPParser::IterMinContext* ctx) = 0;

    virtual void enterConsecutiveOption(NesCEPParser::ConsecutiveOptionContext* ctx) = 0;
    virtual void exitConsecutiveOption(NesCEPParser::ConsecutiveOptionContext* ctx) = 0;

    virtual void enterParameterName(NesCEPParser::ParameterNameContext* ctx) = 0;
    virtual void exitParameterName(NesCEPParser::ParameterNameContext* ctx) = 0;

    virtual void enterOperatorRule(NesCEPParser::OperatorRuleContext* ctx) = 0;
    virtual void exitOperatorRule(NesCEPParser::OperatorRuleContext* ctx) = 0;

    virtual void enterSequence(NesCEPParser::SequenceContext* ctx) = 0;
    virtual void exitSequence(NesCEPParser::SequenceContext* ctx) = 0;

    virtual void enterContiguity(NesCEPParser::ContiguityContext* ctx) = 0;
    virtual void exitContiguity(NesCEPParser::ContiguityContext* ctx) = 0;

    virtual void enterSinkType(NesCEPParser::SinkTypeContext* ctx) = 0;
    virtual void exitSinkType(NesCEPParser::SinkTypeContext* ctx) = 0;

    virtual void enterNullNotnull(NesCEPParser::NullNotnullContext* ctx) = 0;
    virtual void exitNullNotnull(NesCEPParser::NullNotnullContext* ctx) = 0;

    virtual void enterConstant(NesCEPParser::ConstantContext* ctx) = 0;
    virtual void exitConstant(NesCEPParser::ConstantContext* ctx) = 0;

    virtual void enterExpressions(NesCEPParser::ExpressionsContext* ctx) = 0;
    virtual void exitExpressions(NesCEPParser::ExpressionsContext* ctx) = 0;

    virtual void enterIsExpression(NesCEPParser::IsExpressionContext* ctx) = 0;
    virtual void exitIsExpression(NesCEPParser::IsExpressionContext* ctx) = 0;

    virtual void enterNotExpression(NesCEPParser::NotExpressionContext* ctx) = 0;
    virtual void exitNotExpression(NesCEPParser::NotExpressionContext* ctx) = 0;

    virtual void enterLogicalExpression(NesCEPParser::LogicalExpressionContext* ctx) = 0;
    virtual void exitLogicalExpression(NesCEPParser::LogicalExpressionContext* ctx) = 0;

    virtual void enterPredicateExpression(NesCEPParser::PredicateExpressionContext* ctx) = 0;
    virtual void exitPredicateExpression(NesCEPParser::PredicateExpressionContext* ctx) = 0;

    virtual void enterExpressionAtomPredicate(NesCEPParser::ExpressionAtomPredicateContext* ctx) = 0;
    virtual void exitExpressionAtomPredicate(NesCEPParser::ExpressionAtomPredicateContext* ctx) = 0;

    virtual void enterBinaryComparisonPredicate(NesCEPParser::BinaryComparisonPredicateContext* ctx) = 0;
    virtual void exitBinaryComparisonPredicate(NesCEPParser::BinaryComparisonPredicateContext* ctx) = 0;

    virtual void enterInPredicate(NesCEPParser::InPredicateContext* ctx) = 0;
    virtual void exitInPredicate(NesCEPParser::InPredicateContext* ctx) = 0;

    virtual void enterIsNullPredicate(NesCEPParser::IsNullPredicateContext* ctx) = 0;
    virtual void exitIsNullPredicate(NesCEPParser::IsNullPredicateContext* ctx) = 0;

    virtual void enterUnaryExpressionAtom(NesCEPParser::UnaryExpressionAtomContext* ctx) = 0;
    virtual void exitUnaryExpressionAtom(NesCEPParser::UnaryExpressionAtomContext* ctx) = 0;

    virtual void enterAttributeAtom(NesCEPParser::AttributeAtomContext* ctx) = 0;
    virtual void exitAttributeAtom(NesCEPParser::AttributeAtomContext* ctx) = 0;

    virtual void enterConstantExpressionAtom(NesCEPParser::ConstantExpressionAtomContext* ctx) = 0;
    virtual void exitConstantExpressionAtom(NesCEPParser::ConstantExpressionAtomContext* ctx) = 0;

    virtual void enterBinaryExpressionAtom(NesCEPParser::BinaryExpressionAtomContext* ctx) = 0;
    virtual void exitBinaryExpressionAtom(NesCEPParser::BinaryExpressionAtomContext* ctx) = 0;

    virtual void enterBitExpressionAtom(NesCEPParser::BitExpressionAtomContext* ctx) = 0;
    virtual void exitBitExpressionAtom(NesCEPParser::BitExpressionAtomContext* ctx) = 0;

    virtual void enterNestedExpressionAtom(NesCEPParser::NestedExpressionAtomContext* ctx) = 0;
    virtual void exitNestedExpressionAtom(NesCEPParser::NestedExpressionAtomContext* ctx) = 0;

    virtual void enterMathExpressionAtom(NesCEPParser::MathExpressionAtomContext* ctx) = 0;
    virtual void exitMathExpressionAtom(NesCEPParser::MathExpressionAtomContext* ctx) = 0;

    virtual void enterEventAttribute(NesCEPParser::EventAttributeContext* ctx) = 0;
    virtual void exitEventAttribute(NesCEPParser::EventAttributeContext* ctx) = 0;

    virtual void enterEventIteration(NesCEPParser::EventIterationContext* ctx) = 0;
    virtual void exitEventIteration(NesCEPParser::EventIterationContext* ctx) = 0;

    virtual void enterMathExpression(NesCEPParser::MathExpressionContext* ctx) = 0;
    virtual void exitMathExpression(NesCEPParser::MathExpressionContext* ctx) = 0;

    virtual void enterAggregation(NesCEPParser::AggregationContext* ctx) = 0;
    virtual void exitAggregation(NesCEPParser::AggregationContext* ctx) = 0;

    virtual void enterValue(NesCEPParser::ValueContext* ctx) = 0;
    virtual void exitValue(NesCEPParser::ValueContext* ctx) = 0;

    virtual void enterAttribute(NesCEPParser::AttributeContext* ctx) = 0;
    virtual void exitAttribute(NesCEPParser::AttributeContext* ctx) = 0;

    virtual void enterAttVal(NesCEPParser::AttValContext* ctx) = 0;
    virtual void exitAttVal(NesCEPParser::AttValContext* ctx) = 0;

    virtual void enterBoolRule(NesCEPParser::BoolRuleContext* ctx) = 0;
    virtual void exitBoolRule(NesCEPParser::BoolRuleContext* ctx) = 0;

    virtual void enterCondition(NesCEPParser::ConditionContext* ctx) = 0;
    virtual void exitCondition(NesCEPParser::ConditionContext* ctx) = 0;

    virtual void enterUnaryOperator(NesCEPParser::UnaryOperatorContext* ctx) = 0;
    virtual void exitUnaryOperator(NesCEPParser::UnaryOperatorContext* ctx) = 0;

    virtual void enterComparisonOperator(NesCEPParser::ComparisonOperatorContext* ctx) = 0;
    virtual void exitComparisonOperator(NesCEPParser::ComparisonOperatorContext* ctx) = 0;

    virtual void enterLogicalOperator(NesCEPParser::LogicalOperatorContext* ctx) = 0;
    virtual void exitLogicalOperator(NesCEPParser::LogicalOperatorContext* ctx) = 0;

    virtual void enterBitOperator(NesCEPParser::BitOperatorContext* ctx) = 0;
    virtual void exitBitOperator(NesCEPParser::BitOperatorContext* ctx) = 0;

    virtual void enterMathOperator(NesCEPParser::MathOperatorContext* ctx) = 0;
    virtual void exitMathOperator(NesCEPParser::MathOperatorContext* ctx) = 0;

    virtual void enterSinkWithParameters(NesCEPParser::SinkWithParametersContext* ctx) = 0;
    virtual void exitSinkWithParameters(NesCEPParser::SinkWithParametersContext* ctx) = 0;

    virtual void enterSinkWithoutParameters(NesCEPParser::SinkWithoutParametersContext* ctx) = 0;
    virtual void exitSinkWithoutParameters(NesCEPParser::SinkWithoutParametersContext* ctx) = 0;

    virtual void enterParameters(NesCEPParser::ParametersContext* ctx) = 0;
    virtual void exitParameters(NesCEPParser::ParametersContext* ctx) = 0;

    virtual void enterParameter(NesCEPParser::ParameterContext* ctx) = 0;
    virtual void exitParameter(NesCEPParser::ParameterContext* ctx) = 0;

    virtual void enterFileName(NesCEPParser::FileNameContext* ctx) = 0;
    virtual void exitFileName(NesCEPParser::FileNameContext* ctx) = 0;

    virtual void enterTopic(NesCEPParser::TopicContext* ctx) = 0;
    virtual void exitTopic(NesCEPParser::TopicContext* ctx) = 0;

    virtual void enterAddress(NesCEPParser::AddressContext* ctx) = 0;
    virtual void exitAddress(NesCEPParser::AddressContext* ctx) = 0;
};

}// namespace NES::Parsers
#endif// NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPLISTENER_H_
