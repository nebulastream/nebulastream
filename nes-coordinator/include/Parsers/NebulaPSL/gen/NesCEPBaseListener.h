
// Generated from CLionProjects/nebulastream/nes-coordinator/src/Parsers/NebulaPSL/gen/NesCEP.g4 by ANTLR 4.9.2

#ifndef NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPBASELISTENER_H_
#define NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPBASELISTENER_H_
#pragma once

#include <Parsers/NebulaPSL/gen/NesCEPListener.h>
#include <antlr4-runtime.h>

namespace NES::Parsers {

/**
 * This class provides an empty implementation of NesCEPListener,
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
class NesCEPBaseListener : public NesCEPListener {
  public:
    virtual void enterQuery(NesCEPParser::QueryContext* /*ctx*/) override {}
    virtual void exitQuery(NesCEPParser::QueryContext* /*ctx*/) override {}

    virtual void enterCepPattern(NesCEPParser::CepPatternContext* /*ctx*/) override {}
    virtual void exitCepPattern(NesCEPParser::CepPatternContext* /*ctx*/) override {}

    virtual void enterInputStreams(NesCEPParser::InputStreamsContext* /*ctx*/) override {}
    virtual void exitInputStreams(NesCEPParser::InputStreamsContext* /*ctx*/) override {}

    virtual void enterInputStream(NesCEPParser::InputStreamContext* /*ctx*/) override {}
    virtual void exitInputStream(NesCEPParser::InputStreamContext* /*ctx*/) override {}

    virtual void enterCompositeEventExpressions(NesCEPParser::CompositeEventExpressionsContext* /*ctx*/) override {}
    virtual void exitCompositeEventExpressions(NesCEPParser::CompositeEventExpressionsContext* /*ctx*/) override {}

    virtual void enterWhereExp(NesCEPParser::WhereExpContext* /*ctx*/) override {}
    virtual void exitWhereExp(NesCEPParser::WhereExpContext* /*ctx*/) override {}

    virtual void enterWindowConstraints(NesCEPParser::WindowConstraintsContext* /*ctx*/) override {}
    virtual void exitWindowConstraints(NesCEPParser::WindowConstraintsContext* /*ctx*/) override {}

    virtual void enterWindowType(NesCEPParser::WindowTypeContext* /*ctx*/) override {}
    virtual void exitWindowType(NesCEPParser::WindowTypeContext* /*ctx*/) override {}

    virtual void enterTimeConstraints(NesCEPParser::TimeConstraintsContext* /*ctx*/) override {}
    virtual void exitTimeConstraints(NesCEPParser::TimeConstraintsContext* /*ctx*/) override {}

    virtual void enterConstraint(NesCEPParser::ConstraintContext* /*ctx*/) override {}
    virtual void exitConstraint(NesCEPParser::ConstraintContext* /*ctx*/) override {}

    virtual void enterInterval(NesCEPParser::IntervalContext* /*ctx*/) override {}
    virtual void exitInterval(NesCEPParser::IntervalContext* /*ctx*/) override {}

    virtual void enterIntervalType(NesCEPParser::IntervalTypeContext* /*ctx*/) override {}
    virtual void exitIntervalType(NesCEPParser::IntervalTypeContext* /*ctx*/) override {}

    virtual void enterOption(NesCEPParser::OptionContext* /*ctx*/) override {}
    virtual void exitOption(NesCEPParser::OptionContext* /*ctx*/) override {}

    virtual void enterOutputExpression(NesCEPParser::OutputExpressionContext* /*ctx*/) override {}
    virtual void exitOutputExpression(NesCEPParser::OutputExpressionContext* /*ctx*/) override {}

    virtual void enterOutAttribute(NesCEPParser::OutAttributeContext* /*ctx*/) override {}
    virtual void exitOutAttribute(NesCEPParser::OutAttributeContext* /*ctx*/) override {}

    virtual void enterSinkList(NesCEPParser::SinkListContext* /*ctx*/) override {}
    virtual void exitSinkList(NesCEPParser::SinkListContext* /*ctx*/) override {}

    virtual void enterListEvents(NesCEPParser::ListEventsContext* /*ctx*/) override {}
    virtual void exitListEvents(NesCEPParser::ListEventsContext* /*ctx*/) override {}

    virtual void enterEventElem(NesCEPParser::EventElemContext* /*ctx*/) override {}
    virtual void exitEventElem(NesCEPParser::EventElemContext* /*ctx*/) override {}

    virtual void enterEvent(NesCEPParser::EventContext* /*ctx*/) override {}
    virtual void exitEvent(NesCEPParser::EventContext* /*ctx*/) override {}

    virtual void enterQuantifiers(NesCEPParser::QuantifiersContext* /*ctx*/) override {}
    virtual void exitQuantifiers(NesCEPParser::QuantifiersContext* /*ctx*/) override {}

    virtual void enterIterMax(NesCEPParser::IterMaxContext* /*ctx*/) override {}
    virtual void exitIterMax(NesCEPParser::IterMaxContext* /*ctx*/) override {}

    virtual void enterIterMin(NesCEPParser::IterMinContext* /*ctx*/) override {}
    virtual void exitIterMin(NesCEPParser::IterMinContext* /*ctx*/) override {}

    virtual void enterConsecutiveOption(NesCEPParser::ConsecutiveOptionContext* /*ctx*/) override {}
    virtual void exitConsecutiveOption(NesCEPParser::ConsecutiveOptionContext* /*ctx*/) override {}

    virtual void enterParameterName(NesCEPParser::ParameterNameContext* /*ctx*/) override {}
    virtual void exitParameterName(NesCEPParser::ParameterNameContext* /*ctx*/) override {}

    virtual void enterOperatorRule(NesCEPParser::OperatorRuleContext* /*ctx*/) override {}
    virtual void exitOperatorRule(NesCEPParser::OperatorRuleContext* /*ctx*/) override {}

    virtual void enterSequence(NesCEPParser::SequenceContext* /*ctx*/) override {}
    virtual void exitSequence(NesCEPParser::SequenceContext* /*ctx*/) override {}

    virtual void enterContiguity(NesCEPParser::ContiguityContext* /*ctx*/) override {}
    virtual void exitContiguity(NesCEPParser::ContiguityContext* /*ctx*/) override {}

    virtual void enterSinkType(NesCEPParser::SinkTypeContext* /*ctx*/) override {}
    virtual void exitSinkType(NesCEPParser::SinkTypeContext* /*ctx*/) override {}

    virtual void enterNullNotnull(NesCEPParser::NullNotnullContext* /*ctx*/) override {}
    virtual void exitNullNotnull(NesCEPParser::NullNotnullContext* /*ctx*/) override {}

    virtual void enterConstant(NesCEPParser::ConstantContext* /*ctx*/) override {}
    virtual void exitConstant(NesCEPParser::ConstantContext* /*ctx*/) override {}

    virtual void enterExpressions(NesCEPParser::ExpressionsContext* /*ctx*/) override {}
    virtual void exitExpressions(NesCEPParser::ExpressionsContext* /*ctx*/) override {}

    virtual void enterIsExpression(NesCEPParser::IsExpressionContext* /*ctx*/) override {}
    virtual void exitIsExpression(NesCEPParser::IsExpressionContext* /*ctx*/) override {}

    virtual void enterNotExpression(NesCEPParser::NotExpressionContext* /*ctx*/) override {}
    virtual void exitNotExpression(NesCEPParser::NotExpressionContext* /*ctx*/) override {}

    virtual void enterLogicalExpression(NesCEPParser::LogicalExpressionContext* /*ctx*/) override {}
    virtual void exitLogicalExpression(NesCEPParser::LogicalExpressionContext* /*ctx*/) override {}

    virtual void enterPredicateExpression(NesCEPParser::PredicateExpressionContext* /*ctx*/) override {}
    virtual void exitPredicateExpression(NesCEPParser::PredicateExpressionContext* /*ctx*/) override {}

    virtual void enterExpressionAtomPredicate(NesCEPParser::ExpressionAtomPredicateContext* /*ctx*/) override {}
    virtual void exitExpressionAtomPredicate(NesCEPParser::ExpressionAtomPredicateContext* /*ctx*/) override {}

    virtual void enterBinaryComparisonPredicate(NesCEPParser::BinaryComparisonPredicateContext* /*ctx*/) override {}
    virtual void exitBinaryComparisonPredicate(NesCEPParser::BinaryComparisonPredicateContext* /*ctx*/) override {}

    virtual void enterInPredicate(NesCEPParser::InPredicateContext* /*ctx*/) override {}
    virtual void exitInPredicate(NesCEPParser::InPredicateContext* /*ctx*/) override {}

    virtual void enterIsNullPredicate(NesCEPParser::IsNullPredicateContext* /*ctx*/) override {}
    virtual void exitIsNullPredicate(NesCEPParser::IsNullPredicateContext* /*ctx*/) override {}

    virtual void enterUnaryExpressionAtom(NesCEPParser::UnaryExpressionAtomContext* /*ctx*/) override {}
    virtual void exitUnaryExpressionAtom(NesCEPParser::UnaryExpressionAtomContext* /*ctx*/) override {}

    virtual void enterAttributeAtom(NesCEPParser::AttributeAtomContext* /*ctx*/) override {}
    virtual void exitAttributeAtom(NesCEPParser::AttributeAtomContext* /*ctx*/) override {}

    virtual void enterConstantExpressionAtom(NesCEPParser::ConstantExpressionAtomContext* /*ctx*/) override {}
    virtual void exitConstantExpressionAtom(NesCEPParser::ConstantExpressionAtomContext* /*ctx*/) override {}

    virtual void enterBinaryExpressionAtom(NesCEPParser::BinaryExpressionAtomContext* /*ctx*/) override {}
    virtual void exitBinaryExpressionAtom(NesCEPParser::BinaryExpressionAtomContext* /*ctx*/) override {}

    virtual void enterBitExpressionAtom(NesCEPParser::BitExpressionAtomContext* /*ctx*/) override {}
    virtual void exitBitExpressionAtom(NesCEPParser::BitExpressionAtomContext* /*ctx*/) override {}

    virtual void enterNestedExpressionAtom(NesCEPParser::NestedExpressionAtomContext* /*ctx*/) override {}
    virtual void exitNestedExpressionAtom(NesCEPParser::NestedExpressionAtomContext* /*ctx*/) override {}

    virtual void enterMathExpressionAtom(NesCEPParser::MathExpressionAtomContext* /*ctx*/) override {}
    virtual void exitMathExpressionAtom(NesCEPParser::MathExpressionAtomContext* /*ctx*/) override {}

    virtual void enterEventAttribute(NesCEPParser::EventAttributeContext* /*ctx*/) override {}
    virtual void exitEventAttribute(NesCEPParser::EventAttributeContext* /*ctx*/) override {}

    virtual void enterEventIteration(NesCEPParser::EventIterationContext* /*ctx*/) override {}
    virtual void exitEventIteration(NesCEPParser::EventIterationContext* /*ctx*/) override {}

    virtual void enterMathExpression(NesCEPParser::MathExpressionContext* /*ctx*/) override {}
    virtual void exitMathExpression(NesCEPParser::MathExpressionContext* /*ctx*/) override {}

    virtual void enterAggregation(NesCEPParser::AggregationContext* /*ctx*/) override {}
    virtual void exitAggregation(NesCEPParser::AggregationContext* /*ctx*/) override {}

    virtual void enterValue(NesCEPParser::ValueContext* /*ctx*/) override {}
    virtual void exitValue(NesCEPParser::ValueContext* /*ctx*/) override {}

    virtual void enterAttribute(NesCEPParser::AttributeContext* /*ctx*/) override {}
    virtual void exitAttribute(NesCEPParser::AttributeContext* /*ctx*/) override {}

    virtual void enterAttVal(NesCEPParser::AttValContext* /*ctx*/) override {}
    virtual void exitAttVal(NesCEPParser::AttValContext* /*ctx*/) override {}

    virtual void enterBoolRule(NesCEPParser::BoolRuleContext* /*ctx*/) override {}
    virtual void exitBoolRule(NesCEPParser::BoolRuleContext* /*ctx*/) override {}

    virtual void enterCondition(NesCEPParser::ConditionContext* /*ctx*/) override {}
    virtual void exitCondition(NesCEPParser::ConditionContext* /*ctx*/) override {}

    virtual void enterUnaryOperator(NesCEPParser::UnaryOperatorContext* /*ctx*/) override {}
    virtual void exitUnaryOperator(NesCEPParser::UnaryOperatorContext* /*ctx*/) override {}

    virtual void enterComparisonOperator(NesCEPParser::ComparisonOperatorContext* /*ctx*/) override {}
    virtual void exitComparisonOperator(NesCEPParser::ComparisonOperatorContext* /*ctx*/) override {}

    virtual void enterLogicalOperator(NesCEPParser::LogicalOperatorContext* /*ctx*/) override {}
    virtual void exitLogicalOperator(NesCEPParser::LogicalOperatorContext* /*ctx*/) override {}

    virtual void enterBitOperator(NesCEPParser::BitOperatorContext* /*ctx*/) override {}
    virtual void exitBitOperator(NesCEPParser::BitOperatorContext* /*ctx*/) override {}

    virtual void enterMathOperator(NesCEPParser::MathOperatorContext* /*ctx*/) override {}
    virtual void exitMathOperator(NesCEPParser::MathOperatorContext* /*ctx*/) override {}

    virtual void enterSinkWithParameters(NesCEPParser::SinkWithParametersContext* /*ctx*/) override {}
    virtual void exitSinkWithParameters(NesCEPParser::SinkWithParametersContext* /*ctx*/) override {}

    virtual void enterSinkWithoutParameters(NesCEPParser::SinkWithoutParametersContext* /*ctx*/) override {}
    virtual void exitSinkWithoutParameters(NesCEPParser::SinkWithoutParametersContext* /*ctx*/) override {}

    virtual void enterParameters(NesCEPParser::ParametersContext* /*ctx*/) override {}
    virtual void exitParameters(NesCEPParser::ParametersContext* /*ctx*/) override {}

    virtual void enterParameter(NesCEPParser::ParameterContext* /*ctx*/) override {}
    virtual void exitParameter(NesCEPParser::ParameterContext* /*ctx*/) override {}

    virtual void enterFileName(NesCEPParser::FileNameContext* /*ctx*/) override {}
    virtual void exitFileName(NesCEPParser::FileNameContext* /*ctx*/) override {}

    virtual void enterTopic(NesCEPParser::TopicContext* /*ctx*/) override {}
    virtual void exitTopic(NesCEPParser::TopicContext* /*ctx*/) override {}

    virtual void enterAddress(NesCEPParser::AddressContext* /*ctx*/) override {}
    virtual void exitAddress(NesCEPParser::AddressContext* /*ctx*/) override {}

    virtual void enterEveryRule(antlr4::ParserRuleContext* /*ctx*/) override {}
    virtual void exitEveryRule(antlr4::ParserRuleContext* /*ctx*/) override {}
    virtual void visitTerminal(antlr4::tree::TerminalNode* /*node*/) override {}
    virtual void visitErrorNode(antlr4::tree::ErrorNode* /*node*/) override {}
};

}// namespace NES::Parsers
#endif// NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPBASELISTENER_H_
