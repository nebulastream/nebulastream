
// Generated from IoTDB/nes-core/src/Parsers/NebulaPSL/gen/NesCEP.g4 by ANTLR 4.9.2

#ifndef NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPPARSER_H_
#define NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPPARSER_H_

#include <antlr4-runtime.h>

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
        WS = 9,
        FROM = 10,
        PATTERN = 11,
        WHERE = 12,
        WITHIN = 13,
        CONSUMING = 14,
        RETURN = 15,
        INTO = 16,
        ALL = 17,
        ANY = 18,
        SEP = 19,
        COMMA = 20,
        LPARENTHESIS = 21,
        RPARENTHESIS = 22,
        NOT = 23,
        NOT_OP = 24,
        SEQ = 25,
        NEXT = 26,
        AND = 27,
        OR = 28,
        STAR = 29,
        PLUS = 30,
        D_POINTS = 31,
        LBRACKET = 32,
        RBRACKET = 33,
        XOR = 34,
        IN = 35,
        IS = 36,
        NULLTOKEN = 37,
        BETWEEN = 38,
        BINARY = 39,
        TRUE = 40,
        FALSE = 41,
        UNKNOWN = 42,
        QUARTER = 43,
        MONTH = 44,
        DAY = 45,
        HOUR = 46,
        MINUTE = 47,
        WEEK = 48,
        SECOND = 49,
        MICROSECOND = 50,
        AS = 51,
        EQUAL = 52,
        SINKSEP = 53,
        KAFKA = 54,
        FILE = 55,
        MQTT = 56,
        NETWORK = 57,
        NULLOUTPUT = 58,
        OPC = 59,
        PRINT = 60,
        ZMQ = 61,
        POINT = 62,
        QUOTE = 63,
        AVG = 64,
        SUM = 65,
        MIN = 66,
        MAX = 67,
        COUNT = 68,
        IF = 69,
        LOGOR = 70,
        LOGAND = 71,
        LOGXOR = 72,
        NONE = 73,
        INT = 74,
        NAME = 75,
        ID = 76
    };

    enum {
        RuleQuery = 0,
        RuleCepPattern = 1,
        RuleInputStreams = 2,
        RuleInputStream = 3,
        RuleCompositeEventExpressions = 4,
        RuleWhereExp = 5,
        RuleTimeConstraints = 6,
        RuleInterval = 7,
        RuleIntervalType = 8,
        RuleOption = 9,
        RuleOutputExpression = 10,
        RuleOutAttribute = 11,
        RuleSinkList = 12,
        RuleSink = 13,
        RuleListEvents = 14,
        RuleEventElem = 15,
        RuleEvent = 16,
        RuleQuantifiers = 17,
        RuleIterMax = 18,
        RuleIterMin = 19,
        RuleConsecutiveOption = 20,
        RuleOperatorRule = 21,
        RuleSequence = 22,
        RuleContiguity = 23,
        RuleSinkType = 24,
        RuleNullNotnull = 25,
        RuleConstant = 26,
        RuleExpressions = 27,
        RuleExpression = 28,
        RulePredicate = 29,
        RuleExpressionAtom = 30,
        RuleEventAttribute = 31,
        RuleEventIteration = 32,
        RuleMathExpression = 33,
        RuleAggregation = 34,
        RuleAttribute = 35,
        RuleAttVal = 36,
        RuleBoolRule = 37,
        RuleCondition = 38,
        RuleUnaryOperator = 39,
        RuleComparisonOperator = 40,
        RuleLogicalOperator = 41,
        RuleBitOperator = 42,
        RuleMathOperator = 43
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
    class TimeConstraintsContext;
    class IntervalContext;
    class IntervalTypeContext;
    class OptionContext;
    class OutputExpressionContext;
    class OutAttributeContext;
    class SinkListContext;
    class SinkContext;
    class ListEventsContext;
    class EventElemContext;
    class EventContext;
    class QuantifiersContext;
    class IterMaxContext;
    class IterMinContext;
    class ConsecutiveOptionContext;
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
    class AttributeContext;
    class AttValContext;
    class BoolRuleContext;
    class ConditionContext;
    class UnaryOperatorContext;
    class ComparisonOperatorContext;
    class LogicalOperatorContext;
    class BitOperatorContext;
    class MathOperatorContext;

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
        TimeConstraintsContext* timeConstraints();
        antlr4::tree::TerminalNode* CONSUMING();
        OptionContext* option();
        antlr4::tree::TerminalNode* RETURN();
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
        antlr4::tree::TerminalNode* LPARENTHESIS();
        ListEventsContext* listEvents();
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

    class TimeConstraintsContext : public antlr4::ParserRuleContext {
      public:
        TimeConstraintsContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* LBRACKET();
        IntervalContext* interval();
        antlr4::tree::TerminalNode* RBRACKET();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    TimeConstraintsContext* timeConstraints();

    class IntervalContext : public antlr4::ParserRuleContext {
      public:
        IntervalContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        antlr4::tree::TerminalNode* INT();
        IntervalTypeContext* intervalType();

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

    class SinkContext : public antlr4::ParserRuleContext {
      public:
        SinkContext(antlr4::ParserRuleContext* parent, size_t invokingState);
        virtual size_t getRuleIndex() const override;
        SinkTypeContext* sinkType();
        antlr4::tree::TerminalNode* SINKSEP();
        antlr4::tree::TerminalNode* NAME();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    SinkContext* sink();

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
        antlr4::tree::TerminalNode* KAFKA();
        antlr4::tree::TerminalNode* FILE();
        antlr4::tree::TerminalNode* MQTT();
        antlr4::tree::TerminalNode* NETWORK();
        antlr4::tree::TerminalNode* NULLOUTPUT();
        antlr4::tree::TerminalNode* OPC();
        antlr4::tree::TerminalNode* PRINT();
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

    class BinaryComparasionPredicateContext : public PredicateContext {
      public:
        BinaryComparasionPredicateContext(PredicateContext* ctx);

        NesCEPParser::PredicateContext* left = nullptr;
        NesCEPParser::PredicateContext* right = nullptr;
        ComparisonOperatorContext* comparisonOperator();
        std::vector<PredicateContext*> predicate();
        PredicateContext* predicate(size_t i);
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
        antlr4::tree::TerminalNode* AVGNode();
        antlr4::tree::TerminalNode* SUMNode();
        antlr4::tree::TerminalNode* MINNode();
        antlr4::tree::TerminalNode* MAXNode();
        antlr4::tree::TerminalNode* COUNTNode();

        virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
        virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;
    };

    AggregationContext* aggregation();

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
#endif  // NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_GEN_NESCEPPARSER_H_
