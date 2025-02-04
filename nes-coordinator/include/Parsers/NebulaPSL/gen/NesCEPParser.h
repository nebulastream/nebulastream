
// Generated from CLionProjects/nebulastream/nes-coordinator/src/Parsers/NebulaPSL/gen/NesCEP.g4 by ANTLR 4.9.2

#ifndef NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPPARSER_H_
#define NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPPARSER_H_
#pragma once

#include <antlr4-runtime/antlr4-runtime.h>

namespace NES::Parsers {

class NesCEPParser : public antlr4::Parser {
  public:
    enum {
        T__0 = 1,
        T__1 = 2,
        T__2 = 3,
        T__3 = 4,
        T__4 = 5,
        T__5 = 6,
        T__6 = 7,
        T__7 = 8,
        T__8 = 9,
        T__9 = 10,
        T__10 = 11,
        T__11 = 12,
        T__12 = 13,
        T__13 = 14,
        T__14 = 15,
        T__15 = 16,
        T__16 = 17,
        T__17 = 18,
        T__18 = 19,
        T__19 = 20,
        T__20 = 21,
        INT = 22,
        FLOAT = 23,
        PROTOCOL = 24,
        FILETYPE = 25,
        PORT = 26,
        WS = 27,
        FROM = 28,
        PATTERN = 29,
        WHERE = 30,
        WITHIN = 31,
        CONSUMING = 32,
        SELECT = 33,
        INTO = 34,
        ALL = 35,
        ANY = 36,
        SEP = 37,
        COMMA = 38,
        LPARENTHESIS = 39,
        RPARENTHESIS = 40,
        NOT = 41,
        NOT_OP = 42,
        SEQ = 43,
        NEXT = 44,
        AND = 45,
        OR = 46,
        STAR = 47,
        PLUS = 48,
        D_POINTS = 49,
        LBRACKET = 50,
        RBRACKET = 51,
        XOR = 52,
        IN = 53,
        IS = 54,
        NULLTOKEN = 55,
        BETWEEN = 56,
        BINARY = 57,
        TRUE = 58,
        FALSE = 59,
        UNKNOWN = 60,
        QUARTER = 61,
        MONTH = 62,
        DAY = 63,
        HOUR = 64,
        MINUTE = 65,
        WEEK = 66,
        SECOND = 67,
        MICROSECOND = 68,
        AS = 69,
        EQUAL = 70,
        KAFKA = 71,
        FILE = 72,
        MQTT = 73,
        NETWORK = 74,
        NULLOUTPUT = 75,
        OPC = 76,
        PRINT = 77,
        ZMQ = 78,
        POINT = 79,
        QUOTE = 80,
        AVG = 81,
        SUM = 82,
        MIN = 83,
        MAX = 84,
        COUNT = 85,
        IF = 86,
        LOGOR = 87,
        LOGAND = 88,
        LOGXOR = 89,
        NONE = 90,
        URL = 91,
        NAME = 92,
        ID = 93,
        PATH = 94
    };

    enum {
        RuleQuery = 0,
        RuleCepPattern = 1,
        RuleInputStreams = 2,
        RuleInputStream = 3,
        RuleCompositeEventExpressions = 4,
        RuleWhereExp = 5,
        RuleWindowConstraints = 6,
        RuleWindowType = 7,
        RuleTimeConstraints = 8,
        RuleConstraint = 9,
        RuleInterval = 10,
        RuleIntervalType = 11,
        RuleOption = 12,
        RuleOutputExpression = 13,
        RuleOutAttribute = 14,
        RuleSinkList = 15,
        RuleListEvents = 16,
        RuleEventElem = 17,
        RuleEvent = 18,
        RuleQuantifiers = 19,
        RuleIterMax = 20,
        RuleIterMin = 21,
        RuleConsecutiveOption = 22,
        RuleParameterName = 23,
        RuleOperatorRule = 24,
        RuleSequence = 25,
        RuleContiguity = 26,
        RuleSinkType = 27,
        RuleNullNotnull = 28,
        RuleConstant = 29,
        RuleExpressions = 30,
        RuleExpression = 31,
        RulePredicate = 32,
        RuleExpressionAtom = 33,
        RuleEventAttribute = 34,
        RuleEventIteration = 35,
        RuleMathExpression = 36,
        RuleAggregation = 37,
        RuleValue = 38,
        RuleAttribute = 39,
        RuleAttVal = 40,
        RuleBoolRule = 41,
        RuleCondition = 42,
        RuleUnaryOperator = 43,
        RuleComparisonOperator = 44,
        RuleLogicalOperator = 45,
        RuleBitOperator = 46,
        RuleMathOperator = 47,
        RuleSink = 48,
        RuleParameters = 49,
        RuleParameter = 50,
        RuleFileName = 51,
        RuleTopic = 52,
        RuleAddress = 53
    };

    explicit NesCEPParser(antlr4::TokenStream* input);
    ~NesCEPParser();

    virtual std::string getGrammarFileName() const override;
    virtual const antlr4::atn::ATN& getATN() const override { return _atn; };
    virtual const std::vector<std::string>& getTokenNames() const override {
        return _tokenNames;
    };// deprecated: use vocabulary instead.
    virtual const std::vector<std::string>& getRuleNames() const override;
    virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

    class QueryContext;
    class CepPatternContext;
    class InputStreamsContext;
    class InputStreamContext;
    class CompositeEventExpressionsContext;
    class WhereExpContext;
    class WindowConstraintsContext;
    class WindowTypeContext;
    class TimeConstraintsContext;
    class ConstraintContext;
    class IntervalContext;
    class IntervalTypeContext;
    class OptionContext;
    class OutputExpressionContext;
    class OutAttributeContext;
    class SinkListContext;
    class ListEventsContext;
    class EventElemContext;
    class EventContext;
    class QuantifiersContext;
    class IterMaxContext;
    class IterMinContext;
    class ConsecutiveOptionContext;
    class ParameterNameContext;
    class OperatorRuleContext;
    class SequenceContext;
    class ContiguityContext;
    class SinkTypeContext;
    class NullNotnullContext;
    class ConstantContext;
    class ExpressionsContext;
    class ExpressionContext;
    class PredicateContext;
    class ExpressionAtomContext;
    class EventAttributeContext;
    class EventIterationContext;
    class MathExpressionContext;
    class AggregationContext;
    class ValueContext;
    class AttributeContext;
    class AttValContext;
    class BoolRuleContext;
    class ConditionContext;
    class UnaryOperatorContext;
    class ComparisonOperatorContext;
    class LogicalOperatorContext;
    class BitOperatorContext;
    class MathOperatorContext;
    class SinkContext;
    class ParametersContext;
    class ParameterContext;
    class FileNameContext;
    class TopicContext;
    class AddressContext;

    class QueryContext : public antlr4::ParserRuleContext {
      public:
        QueryContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* EOF();
        std::vector<CepPatternContext*> cepPattern();
        CepPatternContext* cepPattern(size_t i);

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    QueryContext* query();

    class CepPatternContext : public antlr4::ParserRuleContext {
      public:
        CepPatternContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* PATTERN();
        antlr4::tree::TerminalNode* NAME();
        antlr4::tree::TerminalNode* SEP();
        CompositeEventExpressionsContext* compositeEventExpressions();
        antlr4::tree::TerminalNode* FROM();
        InputStreamsContext* inputStreams();
        antlr4::tree::TerminalNode* INTO();
        SinkListContext* sinkList();
        antlr4::tree::TerminalNode* WHERE();
        WhereExpContext* whereExp();
        antlr4::tree::TerminalNode* WITHIN();
        WindowConstraintsContext* windowConstraints();
        antlr4::tree::TerminalNode* CONSUMING();
        OptionContext* option();
        antlr4::tree::TerminalNode* SELECT();
        OutputExpressionContext* outputExpression();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    CepPatternContext* cepPattern();

    class InputStreamsContext : public antlr4::ParserRuleContext {
      public:
        InputStreamsContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        std::vector<InputStreamContext*> inputStream();
        InputStreamContext* inputStream(size_t i);
        std::vector<antlr4::tree::TerminalNode*> COMMA();
        antlr4::tree::TerminalNode* COMMA(size_t i);

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    InputStreamsContext* inputStreams();

    class InputStreamContext : public antlr4::ParserRuleContext {
      public:
        InputStreamContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        std::vector<antlr4::tree::TerminalNode*> NAME();
        antlr4::tree::TerminalNode* NAME(size_t i);
        antlr4::tree::TerminalNode* AS();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    InputStreamContext* inputStream();

    class CompositeEventExpressionsContext : public antlr4::ParserRuleContext {
      public:
        CompositeEventExpressionsContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        ListEventsContext* listEvents();
        antlr4::tree::TerminalNode* LPARENTHESIS();
        antlr4::tree::TerminalNode* RPARENTHESIS();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    CompositeEventExpressionsContext* compositeEventExpressions();

    class WhereExpContext : public antlr4::ParserRuleContext {
      public:
        WhereExpContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        ExpressionContext* expression();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    WhereExpContext* whereExp();

    class WindowConstraintsContext : public antlr4::ParserRuleContext {
      public:
        WindowConstraintsContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        WindowTypeContext* windowType();
        antlr4::tree::TerminalNode* LBRACKET();
        TimeConstraintsContext* timeConstraints();
        antlr4::tree::TerminalNode* RBRACKET();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    WindowConstraintsContext* windowConstraints();

    class WindowTypeContext : public antlr4::ParserRuleContext {
      public:
        WindowTypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    WindowTypeContext* windowType();

    class TimeConstraintsContext : public antlr4::ParserRuleContext {
      public:
        TimeConstraintsContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* NAME();
        std::vector<antlr4::tree::TerminalNode*> COMMA();
        antlr4::tree::TerminalNode* COMMA(size_t i);
        std::vector<ConstraintContext*> constraint();
        ConstraintContext* constraint(size_t i);
        std::vector<IntervalContext*> interval();
        IntervalContext* interval(size_t i);

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    TimeConstraintsContext* timeConstraints();

    class ConstraintContext : public antlr4::ParserRuleContext {
      public:
        ConstraintContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ConstraintContext* constraint();

    class IntervalContext : public antlr4::ParserRuleContext {
      public:
        IntervalContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* INT();
        IntervalTypeContext* intervalType();
        antlr4::tree::TerminalNode* WS();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    IntervalContext* interval();

    class IntervalTypeContext : public antlr4::ParserRuleContext {
      public:
        IntervalTypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* QUARTER();
        antlr4::tree::TerminalNode* MONTH();
        antlr4::tree::TerminalNode* DAY();
        antlr4::tree::TerminalNode* HOUR();
        antlr4::tree::TerminalNode* MINUTE();
        antlr4::tree::TerminalNode* WEEK();
        antlr4::tree::TerminalNode* SECOND();
        antlr4::tree::TerminalNode* MICROSECOND();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    IntervalTypeContext* intervalType();

    class OptionContext : public antlr4::ParserRuleContext {
      public:
        OptionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* ALL();
        antlr4::tree::TerminalNode* NONE();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    OptionContext* option();

    class OutputExpressionContext : public antlr4::ParserRuleContext {
      public:
        OutputExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* NAME();
        antlr4::tree::TerminalNode* SEP();
        antlr4::tree::TerminalNode* LBRACKET();
        std::vector<OutAttributeContext*> outAttribute();
        OutAttributeContext* outAttribute(size_t i);
        antlr4::tree::TerminalNode* RBRACKET();
        std::vector<antlr4::tree::TerminalNode*> COMMA();
        antlr4::tree::TerminalNode* COMMA(size_t i);

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    OutputExpressionContext* outputExpression();

    class OutAttributeContext : public antlr4::ParserRuleContext {
      public:
        OutAttributeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* NAME();
        antlr4::tree::TerminalNode* EQUAL();
        AttValContext* attVal();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    OutAttributeContext* outAttribute();

    class SinkListContext : public antlr4::ParserRuleContext {
      public:
        SinkListContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        std::vector<SinkContext*> sink();
        SinkContext* sink(size_t i);
        std::vector<antlr4::tree::TerminalNode*> COMMA();
        antlr4::tree::TerminalNode* COMMA(size_t i);

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    SinkListContext* sinkList();

    class ListEventsContext : public antlr4::ParserRuleContext {
      public:
        ListEventsContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        std::vector<EventElemContext*> eventElem();
        EventElemContext* eventElem(size_t i);
        std::vector<OperatorRuleContext*> operatorRule();
        OperatorRuleContext* operatorRule(size_t i);

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ListEventsContext* listEvents();

    class EventElemContext : public antlr4::ParserRuleContext {
      public:
        EventElemContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        EventContext* event();
        antlr4::tree::TerminalNode* NOT();
        antlr4::tree::TerminalNode* LPARENTHESIS();
        ListEventsContext* listEvents();
        antlr4::tree::TerminalNode* RPARENTHESIS();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    EventElemContext* eventElem();

    class EventContext : public antlr4::ParserRuleContext {
      public:
        EventContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* NAME();
        QuantifiersContext* quantifiers();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    EventContext* event();

    class QuantifiersContext : public antlr4::ParserRuleContext {
      public:
        QuantifiersContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* STAR();
        antlr4::tree::TerminalNode* PLUS();
        antlr4::tree::TerminalNode* LBRACKET();
        antlr4::tree::TerminalNode* INT();
        antlr4::tree::TerminalNode* RBRACKET();
        ConsecutiveOptionContext* consecutiveOption();
        IterMinContext* iterMin();
        antlr4::tree::TerminalNode* D_POINTS();
        IterMaxContext* iterMax();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    QuantifiersContext* quantifiers();

    class IterMaxContext : public antlr4::ParserRuleContext {
      public:
        IterMaxContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* INT();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    IterMaxContext* iterMax();

    class IterMinContext : public antlr4::ParserRuleContext {
      public:
        IterMinContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* INT();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    IterMinContext* iterMin();

    class ConsecutiveOptionContext : public antlr4::ParserRuleContext {
      public:
        ConsecutiveOptionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* NEXT();
        antlr4::tree::TerminalNode* ANY();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ConsecutiveOptionContext* consecutiveOption();

    class ParameterNameContext : public antlr4::ParserRuleContext {
      public:
        ParameterNameContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* NAME();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ParameterNameContext* parameterName();

    class OperatorRuleContext : public antlr4::ParserRuleContext {
      public:
        OperatorRuleContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* AND();
        antlr4::tree::TerminalNode* OR();
        SequenceContext* sequence();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    OperatorRuleContext* operatorRule();

    class SequenceContext : public antlr4::ParserRuleContext {
      public:
        SequenceContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* SEQ();
        ContiguityContext* contiguity();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    SequenceContext* sequence();

    class ContiguityContext : public antlr4::ParserRuleContext {
      public:
        ContiguityContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* NEXT();
        antlr4::tree::TerminalNode* ANY();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ContiguityContext* contiguity();

    class SinkTypeContext : public antlr4::ParserRuleContext {
      public:
        SinkTypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* MQTT();
        antlr4::tree::TerminalNode* OPC();
        antlr4::tree::TerminalNode* ZMQ();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    SinkTypeContext* sinkType();

    class NullNotnullContext : public antlr4::ParserRuleContext {
      public:
        NullNotnullContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* NULLTOKEN();
        antlr4::tree::TerminalNode* NOT();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    NullNotnullContext* nullNotnull();

    class ConstantContext : public antlr4::ParserRuleContext {
      public:
        ConstantContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        std::vector<antlr4::tree::TerminalNode*> QUOTE();
        antlr4::tree::TerminalNode* QUOTE(size_t i);
        antlr4::tree::TerminalNode* NAME();
        antlr4::tree::TerminalNode* FLOAT();
        antlr4::tree::TerminalNode* INT();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ConstantContext* constant();

    class ExpressionsContext : public antlr4::ParserRuleContext {
      public:
        ExpressionsContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        std::vector<ExpressionContext*> expression();
        ExpressionContext* expression(size_t i);
        std::vector<antlr4::tree::TerminalNode*> COMMA();
        antlr4::tree::TerminalNode* COMMA(size_t i);

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ExpressionsContext* expressions();

    class ExpressionContext : public antlr4::ParserRuleContext {
      public:
        ExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);

        ExpressionContext() = default;
        void copyFrom(ExpressionContext* context);
        using antlr4::ParserRuleContext::copyFrom;

        virtual size_t getRuleIndex() const override;
    };

    class IsExpressionContext : public ExpressionContext {
      public:
        IsExpressionContext(ExpressionContext* ctx);

        antlr4::Token* testValue = nullptr;
        PredicateContext* predicate();
        antlr4::tree::TerminalNode* IS();
        antlr4::tree::TerminalNode* TRUE();
        antlr4::tree::TerminalNode* FALSE();
        antlr4::tree::TerminalNode* UNKNOWN();
        antlr4::tree::TerminalNode* NOT();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class NotExpressionContext : public ExpressionContext {
      public:
        NotExpressionContext(ExpressionContext* ctx);

        antlr4::tree::TerminalNode* NOT_OP();
        ExpressionContext* expression();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class LogicalExpressionContext : public ExpressionContext {
      public:
        LogicalExpressionContext(ExpressionContext* ctx);

        std::vector<ExpressionContext*> expression();
        ExpressionContext* expression(size_t i);
        LogicalOperatorContext* logicalOperator();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class PredicateExpressionContext : public ExpressionContext {
      public:
        PredicateExpressionContext(ExpressionContext* ctx);

        PredicateContext* predicate();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ExpressionContext* expression();
    ExpressionContext* expression(int precedence);
    class PredicateContext : public antlr4::ParserRuleContext {
      public:
        PredicateContext(antlr4::ParserRuleContext* parent, size_t invokingState);

        PredicateContext() = default;
        void copyFrom(PredicateContext* context);
        using antlr4::ParserRuleContext::copyFrom;

        virtual size_t getRuleIndex() const override;
    };

    class ExpressionAtomPredicateContext : public PredicateContext {
      public:
        ExpressionAtomPredicateContext(PredicateContext* ctx);

        ExpressionAtomContext* expressionAtom();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class BinaryComparisonPredicateContext : public PredicateContext {
      public:
        BinaryComparisonPredicateContext(PredicateContext* ctx);

        NesCEPParser::PredicateContext* left = nullptr;
        NesCEPParser::PredicateContext* right = nullptr;
        ComparisonOperatorContext* comparisonOperator();
        std::vector<PredicateContext*> predicate();
        PredicateContext* predicate(size_t i);
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class InPredicateContext : public PredicateContext {
      public:
        InPredicateContext(PredicateContext* ctx);

        PredicateContext* predicate();
        antlr4::tree::TerminalNode* IN();
        antlr4::tree::TerminalNode* LPARENTHESIS();
        ExpressionsContext* expressions();
        antlr4::tree::TerminalNode* RPARENTHESIS();
        antlr4::tree::TerminalNode* NOT();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class IsNullPredicateContext : public PredicateContext {
      public:
        IsNullPredicateContext(PredicateContext* ctx);

        PredicateContext* predicate();
        antlr4::tree::TerminalNode* IS();
        NullNotnullContext* nullNotnull();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    PredicateContext* predicate();
    PredicateContext* predicate(int precedence);
    class ExpressionAtomContext : public antlr4::ParserRuleContext {
      public:
        ExpressionAtomContext(antlr4::ParserRuleContext* parent, size_t invokingState);

        ExpressionAtomContext() = default;
        void copyFrom(ExpressionAtomContext* context);
        using antlr4::ParserRuleContext::copyFrom;

        virtual size_t getRuleIndex() const override;
    };

    class UnaryExpressionAtomContext : public ExpressionAtomContext {
      public:
        UnaryExpressionAtomContext(ExpressionAtomContext* ctx);

        UnaryOperatorContext* unaryOperator();
        ExpressionAtomContext* expressionAtom();
        antlr4::tree::TerminalNode* WS();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class AttributeAtomContext : public ExpressionAtomContext {
      public:
        AttributeAtomContext(ExpressionAtomContext* ctx);

        EventAttributeContext* eventAttribute();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class ConstantExpressionAtomContext : public ExpressionAtomContext {
      public:
        ConstantExpressionAtomContext(ExpressionAtomContext* ctx);

        ConstantContext* constant();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class BinaryExpressionAtomContext : public ExpressionAtomContext {
      public:
        BinaryExpressionAtomContext(ExpressionAtomContext* ctx);

        antlr4::tree::TerminalNode* BINARY();
        ExpressionAtomContext* expressionAtom();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class BitExpressionAtomContext : public ExpressionAtomContext {
      public:
        BitExpressionAtomContext(ExpressionAtomContext* ctx);

        NesCEPParser::ExpressionAtomContext* left = nullptr;
        NesCEPParser::ExpressionAtomContext* right = nullptr;
        BitOperatorContext* bitOperator();
        std::vector<ExpressionAtomContext*> expressionAtom();
        ExpressionAtomContext* expressionAtom(size_t i);
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class NestedExpressionAtomContext : public ExpressionAtomContext {
      public:
        NestedExpressionAtomContext(ExpressionAtomContext* ctx);

        antlr4::tree::TerminalNode* LPARENTHESIS();
        std::vector<ExpressionContext*> expression();
        ExpressionContext* expression(size_t i);
        antlr4::tree::TerminalNode* RPARENTHESIS();
        std::vector<antlr4::tree::TerminalNode*> COMMA();
        antlr4::tree::TerminalNode* COMMA(size_t i);
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class MathExpressionAtomContext : public ExpressionAtomContext {
      public:
        MathExpressionAtomContext(ExpressionAtomContext* ctx);

        NesCEPParser::ExpressionAtomContext* left = nullptr;
        NesCEPParser::ExpressionAtomContext* right = nullptr;
        MathOperatorContext* mathOperator();
        std::vector<ExpressionAtomContext*> expressionAtom();
        ExpressionAtomContext* expressionAtom(size_t i);
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ExpressionAtomContext* expressionAtom();
    ExpressionAtomContext* expressionAtom(int precedence);
    class EventAttributeContext : public antlr4::ParserRuleContext {
      public:
        EventAttributeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        AggregationContext* aggregation();
        antlr4::tree::TerminalNode* LPARENTHESIS();
        ExpressionsContext* expressions();
        antlr4::tree::TerminalNode* RPARENTHESIS();
        EventIterationContext* eventIteration();
        antlr4::tree::TerminalNode* POINT();
        AttributeContext* attribute();
        antlr4::tree::TerminalNode* NAME();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    EventAttributeContext* eventAttribute();

    class EventIterationContext : public antlr4::ParserRuleContext {
      public:
        EventIterationContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* NAME();
        antlr4::tree::TerminalNode* LBRACKET();
        antlr4::tree::TerminalNode* RBRACKET();
        MathExpressionContext* mathExpression();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    EventIterationContext* eventIteration();

    class MathExpressionContext : public antlr4::ParserRuleContext {
      public:
        NesCEPParser::ExpressionAtomContext* left = nullptr;
        NesCEPParser::ExpressionAtomContext* right = nullptr;
        MathExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        MathOperatorContext* mathOperator();
        std::vector<ExpressionAtomContext*> expressionAtom();
        ExpressionAtomContext* expressionAtom(size_t i);
        std::vector<ConstantContext*> constant();
        ConstantContext* constant(size_t i);
        antlr4::tree::TerminalNode* D_POINTS();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    MathExpressionContext* mathExpression();

    class AggregationContext : public antlr4::ParserRuleContext {
      public:
        AggregationContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* AVG();
        antlr4::tree::TerminalNode* SUM();
        antlr4::tree::TerminalNode* MIN();
        antlr4::tree::TerminalNode* MAX();
        antlr4::tree::TerminalNode* COUNT();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    AggregationContext* aggregation();

    class ValueContext : public antlr4::ParserRuleContext {
      public:
        ValueContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        AttributeContext* attribute();
        antlr4::tree::TerminalNode* POINT();
        antlr4::tree::TerminalNode* FILETYPE();
        antlr4::tree::TerminalNode* URL();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ValueContext* value();

    class AttributeContext : public antlr4::ParserRuleContext {
      public:
        AttributeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* NAME();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    AttributeContext* attribute();

    class AttValContext : public antlr4::ParserRuleContext {
      public:
        AttValContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* IF();
        ConditionContext* condition();
        EventAttributeContext* eventAttribute();
        EventContext* event();
        ExpressionContext* expression();
        BoolRuleContext* boolRule();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    AttValContext* attVal();

    class BoolRuleContext : public antlr4::ParserRuleContext {
      public:
        BoolRuleContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* TRUE();
        antlr4::tree::TerminalNode* FALSE();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    BoolRuleContext* boolRule();

    class ConditionContext : public antlr4::ParserRuleContext {
      public:
        ConditionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* LPARENTHESIS();
        ExpressionContext* expression();
        std::vector<antlr4::tree::TerminalNode*> COMMA();
        antlr4::tree::TerminalNode* COMMA(size_t i);
        std::vector<AttValContext*> attVal();
        AttValContext* attVal(size_t i);
        antlr4::tree::TerminalNode* RPARENTHESIS();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ConditionContext* condition();

    class UnaryOperatorContext : public antlr4::ParserRuleContext {
      public:
        UnaryOperatorContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* PLUS();
        antlr4::tree::TerminalNode* NOT();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    UnaryOperatorContext* unaryOperator();

    class ComparisonOperatorContext : public antlr4::ParserRuleContext {
      public:
        ComparisonOperatorContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        std::vector<antlr4::tree::TerminalNode*> EQUAL();
        antlr4::tree::TerminalNode* EQUAL(size_t i);
        std::vector<antlr4::tree::TerminalNode*> WS();
        antlr4::tree::TerminalNode* WS(size_t i);
        antlr4::tree::TerminalNode* NOT_OP();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ComparisonOperatorContext* comparisonOperator();

    class LogicalOperatorContext : public antlr4::ParserRuleContext {
      public:
        LogicalOperatorContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* LOGAND();
        antlr4::tree::TerminalNode* LOGXOR();
        antlr4::tree::TerminalNode* LOGOR();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    LogicalOperatorContext* logicalOperator();

    class BitOperatorContext : public antlr4::ParserRuleContext {
      public:
        BitOperatorContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* LOGXOR();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    BitOperatorContext* bitOperator();

    class MathOperatorContext : public antlr4::ParserRuleContext {
      public:
        MathOperatorContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* STAR();
        antlr4::tree::TerminalNode* PLUS();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    MathOperatorContext* mathOperator();

    class SinkContext : public antlr4::ParserRuleContext {
      public:
        SinkContext(antlr4::ParserRuleContext* parent, size_t invokingState);

        SinkContext() = default;
        void copyFrom(SinkContext* context);
        using antlr4::ParserRuleContext::copyFrom;

        virtual size_t getRuleIndex() const override;
    };

    class SinkWithParametersContext : public SinkContext {
      public:
        SinkWithParametersContext(SinkContext* ctx);

        SinkTypeContext* sinkType();
        antlr4::tree::TerminalNode* LPARENTHESIS();
        ParametersContext* parameters();
        antlr4::tree::TerminalNode* RPARENTHESIS();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    class SinkWithoutParametersContext : public SinkContext {
      public:
        SinkWithoutParametersContext(SinkContext* ctx);

        SinkTypeContext* sinkType();
        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    SinkContext* sink();

    class ParametersContext : public antlr4::ParserRuleContext {
      public:
        ParametersContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        std::vector<ParameterContext*> parameter();
        ParameterContext* parameter(size_t i);
        std::vector<ValueContext*> value();
        ValueContext* value(size_t i);
        std::vector<antlr4::tree::TerminalNode*> COMMA();
        antlr4::tree::TerminalNode* COMMA(size_t i);

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ParametersContext* parameters();

    class ParameterContext : public antlr4::ParserRuleContext {
      public:
        ParameterContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    ParameterContext* parameter();

    class FileNameContext : public antlr4::ParserRuleContext {
      public:
        FileNameContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    FileNameContext* fileName();

    class TopicContext : public antlr4::ParserRuleContext {
      public:
        TopicContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    TopicContext* topic();

    class AddressContext : public antlr4::ParserRuleContext {
      public:
        AddressContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    AddressContext* address();

    virtual bool sempred(antlr4::RuleContext* _localctx, size_t ruleIndex, size_t predicateIndex) override;
    bool expressionSempred(ExpressionContext* _localctx, size_t predicateIndex);
    bool predicateSempred(PredicateContext* _localctx, size_t predicateIndex);
    bool expressionAtomSempred(ExpressionAtomContext* _localctx, size_t predicateIndex);

  private:
    static std::vector<antlr4::dfa::DFA> _decisionToDFA;
    static antlr4::atn::PredictionContextCache _sharedContextCache;
    static std::vector<std::string> _ruleNames;
    static std::vector<std::string> _tokenNames;

    static std::vector<std::string> _literalNames;
    static std::vector<std::string> _symbolicNames;
    static antlr4::dfa::Vocabulary _vocabulary;
    static antlr4::atn::ATN _atn;
    static std::vector<uint16_t> _serializedATN;

    struct Initializer {
        Initializer();
    };
    static Initializer _init;
};

}// namespace NES::Parsers
#endif// NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPPARSER_H_
