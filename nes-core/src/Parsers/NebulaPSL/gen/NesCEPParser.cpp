
// Generated from IoTDB/nes-core/src/Parsers/NebulaPSL/gen/NesCEP.g4 by ANTLR 4.9.2

#include "Util/Logger/Logger.hpp"
#include <Parsers/NebulaPSL/gen/NesCEPListener.h>
#include <Parsers/NebulaPSL/gen/NesCEPParser.h>

using namespace antlrcpp;
using namespace NES::Parsers;
using namespace antlr4;

NesCEPParser::NesCEPParser(TokenStream* input) : Parser(input) {
    _interpreter = new atn::ParserATNSimulator(this, _atn, _decisionToDFA, _sharedContextCache);
}

NesCEPParser::~NesCEPParser() { delete _interpreter; }

std::string NesCEPParser::getGrammarFileName() const { return "NesCEP.g4"; }

const std::vector<std::string>& NesCEPParser::getRuleNames() const { return _ruleNames; }

dfa::Vocabulary& NesCEPParser::getVocabulary() const { return _vocabulary; }

//----------------- QueryContext ------------------------------------------------------------------

NesCEPParser::QueryContext::QueryContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::QueryContext::EOF() { return getToken(NesCEPParser::EOF, 0); }

std::vector<NesCEPParser::CepPatternContext*> NesCEPParser::QueryContext::cepPattern() {
    return getRuleContexts<NesCEPParser::CepPatternContext>();
}

NesCEPParser::CepPatternContext* NesCEPParser::QueryContext::cepPattern(size_t i) {
    return getRuleContext<NesCEPParser::CepPatternContext>(i);
}

size_t NesCEPParser::QueryContext::getRuleIndex() const { return NesCEPParser::RuleQuery; }

void NesCEPParser::QueryContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterQuery(this);
}

void NesCEPParser::QueryContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitQuery(this);
}

NesCEPParser::QueryContext* NesCEPParser::query() {
    QueryContext* _localctx = _tracker.createInstance<QueryContext>(_ctx, getState());
    enterRule(_localctx, 0, NesCEPParser::RuleQuery);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(89);
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
            setState(88);
            cepPattern();
            setState(91);
            _errHandler->sync(this);
            _la = _input->LA(1);
        } while (_la == NesCEPParser::PATTERN);
        setState(93);
        match(NesCEPParser::EOF);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- CepPatternContext ------------------------------------------------------------------

NesCEPParser::CepPatternContext::CepPatternContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::CepPatternContext::PATTERN() { return getToken(NesCEPParser::PATTERN, 0); }

tree::TerminalNode* NesCEPParser::CepPatternContext::NAME() { return getToken(NesCEPParser::NAME, 0); }

tree::TerminalNode* NesCEPParser::CepPatternContext::SEP() { return getToken(NesCEPParser::SEP, 0); }

NesCEPParser::CompositeEventExpressionsContext* NesCEPParser::CepPatternContext::compositeEventExpressions() {
    return getRuleContext<NesCEPParser::CompositeEventExpressionsContext>(0);
}

tree::TerminalNode* NesCEPParser::CepPatternContext::FROM() { return getToken(NesCEPParser::FROM, 0); }

NesCEPParser::InputStreamsContext* NesCEPParser::CepPatternContext::inputStreams() {
    return getRuleContext<NesCEPParser::InputStreamsContext>(0);
}

tree::TerminalNode* NesCEPParser::CepPatternContext::INTO() { return getToken(NesCEPParser::INTO, 0); }

NesCEPParser::SinkListContext* NesCEPParser::CepPatternContext::sinkList() {
    return getRuleContext<NesCEPParser::SinkListContext>(0);
}

tree::TerminalNode* NesCEPParser::CepPatternContext::WHERE() { return getToken(NesCEPParser::WHERE, 0); }

NesCEPParser::WhereExpContext* NesCEPParser::CepPatternContext::whereExp() {
    return getRuleContext<NesCEPParser::WhereExpContext>(0);
}

tree::TerminalNode* NesCEPParser::CepPatternContext::WITHIN() { return getToken(NesCEPParser::WITHIN, 0); }

NesCEPParser::TimeConstraintsContext* NesCEPParser::CepPatternContext::timeConstraints() {
    return getRuleContext<NesCEPParser::TimeConstraintsContext>(0);
}

tree::TerminalNode* NesCEPParser::CepPatternContext::CONSUMING() { return getToken(NesCEPParser::CONSUMING, 0); }

NesCEPParser::OptionContext* NesCEPParser::CepPatternContext::option() { return getRuleContext<NesCEPParser::OptionContext>(0); }

tree::TerminalNode* NesCEPParser::CepPatternContext::RETURN() { return getToken(NesCEPParser::RETURN, 0); }

NesCEPParser::OutputExpressionContext* NesCEPParser::CepPatternContext::outputExpression() {
    return getRuleContext<NesCEPParser::OutputExpressionContext>(0);
}

size_t NesCEPParser::CepPatternContext::getRuleIndex() const { return NesCEPParser::RuleCepPattern; }

void NesCEPParser::CepPatternContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterCepPattern(this);
}

void NesCEPParser::CepPatternContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitCepPattern(this);
}

NesCEPParser::CepPatternContext* NesCEPParser::cepPattern() {
    CepPatternContext* _localctx = _tracker.createInstance<CepPatternContext>(_ctx, getState());
    enterRule(_localctx, 2, NesCEPParser::RuleCepPattern);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(95);
        match(NesCEPParser::PATTERN);
        setState(96);
        match(NesCEPParser::NAME);
        setState(97);
        match(NesCEPParser::SEP);
        setState(98);
        compositeEventExpressions();
        setState(99);
        match(NesCEPParser::FROM);
        setState(100);
        inputStreams();
        setState(103);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::WHERE) {
            setState(101);
            match(NesCEPParser::WHERE);
            setState(102);
            whereExp();
        }
        setState(107);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::WITHIN) {
            setState(105);
            match(NesCEPParser::WITHIN);
            setState(106);
            timeConstraints();
        }
        setState(111);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::CONSUMING) {
            setState(109);
            match(NesCEPParser::CONSUMING);
            setState(110);
            option();
        }
        setState(115);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::RETURN) {
            setState(113);
            match(NesCEPParser::RETURN);
            setState(114);
            outputExpression();
        }
        setState(117);
        match(NesCEPParser::INTO);
        setState(118);
        sinkList();

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- InputStreamsContext ------------------------------------------------------------------

NesCEPParser::InputStreamsContext::InputStreamsContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<NesCEPParser::InputStreamContext*> NesCEPParser::InputStreamsContext::inputStream() {
    return getRuleContexts<NesCEPParser::InputStreamContext>();
}

NesCEPParser::InputStreamContext* NesCEPParser::InputStreamsContext::inputStream(size_t i) {
    return getRuleContext<NesCEPParser::InputStreamContext>(i);
}

std::vector<tree::TerminalNode*> NesCEPParser::InputStreamsContext::COMMA() { return getTokens(NesCEPParser::COMMA); }

tree::TerminalNode* NesCEPParser::InputStreamsContext::COMMA(size_t i) { return getToken(NesCEPParser::COMMA, i); }

size_t NesCEPParser::InputStreamsContext::getRuleIndex() const { return NesCEPParser::RuleInputStreams; }

void NesCEPParser::InputStreamsContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterInputStreams(this);
}

void NesCEPParser::InputStreamsContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitInputStreams(this);
}

NesCEPParser::InputStreamsContext* NesCEPParser::inputStreams() {
    InputStreamsContext* _localctx = _tracker.createInstance<InputStreamsContext>(_ctx, getState());
    enterRule(_localctx, 4, NesCEPParser::RuleInputStreams);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(120);
        inputStream();
        setState(125);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NesCEPParser::COMMA) {
            setState(121);
            match(NesCEPParser::COMMA);
            setState(122);
            inputStream();
            setState(127);
            _errHandler->sync(this);
            _la = _input->LA(1);
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- InputStreamContext ------------------------------------------------------------------

NesCEPParser::InputStreamContext::InputStreamContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode*> NesCEPParser::InputStreamContext::NAME() { return getTokens(NesCEPParser::NAME); }

tree::TerminalNode* NesCEPParser::InputStreamContext::NAME(size_t i) { return getToken(NesCEPParser::NAME, i); }

tree::TerminalNode* NesCEPParser::InputStreamContext::AS() { return getToken(NesCEPParser::AS, 0); }

size_t NesCEPParser::InputStreamContext::getRuleIndex() const { return NesCEPParser::RuleInputStream; }

void NesCEPParser::InputStreamContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterInputStream(this);
}

void NesCEPParser::InputStreamContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitInputStream(this);
}

NesCEPParser::InputStreamContext* NesCEPParser::inputStream() {
    InputStreamContext* _localctx = _tracker.createInstance<InputStreamContext>(_ctx, getState());
    enterRule(_localctx, 6, NesCEPParser::RuleInputStream);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(128);
        match(NesCEPParser::NAME);
        setState(131);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::AS) {
            setState(129);
            match(NesCEPParser::AS);
            setState(130);
            match(NesCEPParser::NAME);
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- CompositeEventExpressionsContext ------------------------------------------------------------------

NesCEPParser::CompositeEventExpressionsContext::CompositeEventExpressionsContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::CompositeEventExpressionsContext::LPARENTHESIS() {
    return getToken(NesCEPParser::LPARENTHESIS, 0);
}

NesCEPParser::ListEventsContext* NesCEPParser::CompositeEventExpressionsContext::listEvents() {
    return getRuleContext<NesCEPParser::ListEventsContext>(0);
}

tree::TerminalNode* NesCEPParser::CompositeEventExpressionsContext::RPARENTHESIS() {
    return getToken(NesCEPParser::RPARENTHESIS, 0);
}

size_t NesCEPParser::CompositeEventExpressionsContext::getRuleIndex() const {
    return NesCEPParser::RuleCompositeEventExpressions;
}

void NesCEPParser::CompositeEventExpressionsContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterCompositeEventExpressions(this);
}

void NesCEPParser::CompositeEventExpressionsContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitCompositeEventExpressions(this);
}

NesCEPParser::CompositeEventExpressionsContext* NesCEPParser::compositeEventExpressions() {
    CompositeEventExpressionsContext* _localctx = _tracker.createInstance<CompositeEventExpressionsContext>(_ctx, getState());
    enterRule(_localctx, 8, NesCEPParser::RuleCompositeEventExpressions);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(133);
        match(NesCEPParser::LPARENTHESIS);
        setState(134);
        listEvents();
        setState(135);
        match(NesCEPParser::RPARENTHESIS);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- WhereExpContext ------------------------------------------------------------------

NesCEPParser::WhereExpContext::WhereExpContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

NesCEPParser::ExpressionContext* NesCEPParser::WhereExpContext::expression() {
    return getRuleContext<NesCEPParser::ExpressionContext>(0);
}

size_t NesCEPParser::WhereExpContext::getRuleIndex() const { return NesCEPParser::RuleWhereExp; }

void NesCEPParser::WhereExpContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterWhereExp(this);
}

void NesCEPParser::WhereExpContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitWhereExp(this);
}

NesCEPParser::WhereExpContext* NesCEPParser::whereExp() {
    WhereExpContext* _localctx = _tracker.createInstance<WhereExpContext>(_ctx, getState());
    enterRule(_localctx, 10, NesCEPParser::RuleWhereExp);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(137);
        expression(0);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- TimeConstraintsContext ------------------------------------------------------------------

NesCEPParser::TimeConstraintsContext::TimeConstraintsContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::TimeConstraintsContext::LBRACKET() { return getToken(NesCEPParser::LBRACKET, 0); }

NesCEPParser::IntervalContext* NesCEPParser::TimeConstraintsContext::interval() {
    return getRuleContext<NesCEPParser::IntervalContext>(0);
}

tree::TerminalNode* NesCEPParser::TimeConstraintsContext::RBRACKET() { return getToken(NesCEPParser::RBRACKET, 0); }

size_t NesCEPParser::TimeConstraintsContext::getRuleIndex() const { return NesCEPParser::RuleTimeConstraints; }

void NesCEPParser::TimeConstraintsContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterTimeConstraints(this);
}

void NesCEPParser::TimeConstraintsContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitTimeConstraints(this);
}

NesCEPParser::TimeConstraintsContext* NesCEPParser::timeConstraints() {
    TimeConstraintsContext* _localctx = _tracker.createInstance<TimeConstraintsContext>(_ctx, getState());
    enterRule(_localctx, 12, NesCEPParser::RuleTimeConstraints);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(139);
        match(NesCEPParser::LBRACKET);
        setState(140);
        interval();
        setState(141);
        match(NesCEPParser::RBRACKET);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- IntervalContext ------------------------------------------------------------------

NesCEPParser::IntervalContext::IntervalContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::IntervalContext::INT() { return getToken(NesCEPParser::INT, 0); }

NesCEPParser::IntervalTypeContext* NesCEPParser::IntervalContext::intervalType() {
    return getRuleContext<NesCEPParser::IntervalTypeContext>(0);
}

size_t NesCEPParser::IntervalContext::getRuleIndex() const { return NesCEPParser::RuleInterval; }

void NesCEPParser::IntervalContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterInterval(this);
}

void NesCEPParser::IntervalContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitInterval(this);
}

NesCEPParser::IntervalContext* NesCEPParser::interval() {
    IntervalContext* _localctx = _tracker.createInstance<IntervalContext>(_ctx, getState());
    enterRule(_localctx, 14, NesCEPParser::RuleInterval);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(143);
        match(NesCEPParser::INT);
        setState(144);
        intervalType();

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- IntervalTypeContext ------------------------------------------------------------------

NesCEPParser::IntervalTypeContext::IntervalTypeContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::IntervalTypeContext::QUARTER() { return getToken(NesCEPParser::QUARTER, 0); }

tree::TerminalNode* NesCEPParser::IntervalTypeContext::MONTH() { return getToken(NesCEPParser::MONTH, 0); }

tree::TerminalNode* NesCEPParser::IntervalTypeContext::DAY() { return getToken(NesCEPParser::DAY, 0); }

tree::TerminalNode* NesCEPParser::IntervalTypeContext::HOUR() { return getToken(NesCEPParser::HOUR, 0); }

tree::TerminalNode* NesCEPParser::IntervalTypeContext::MINUTE() { return getToken(NesCEPParser::MINUTE, 0); }

tree::TerminalNode* NesCEPParser::IntervalTypeContext::WEEK() { return getToken(NesCEPParser::WEEK, 0); }

tree::TerminalNode* NesCEPParser::IntervalTypeContext::SECOND() { return getToken(NesCEPParser::SECOND, 0); }

tree::TerminalNode* NesCEPParser::IntervalTypeContext::MICROSECOND() { return getToken(NesCEPParser::MICROSECOND, 0); }

size_t NesCEPParser::IntervalTypeContext::getRuleIndex() const { return NesCEPParser::RuleIntervalType; }

void NesCEPParser::IntervalTypeContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterIntervalType(this);
}

void NesCEPParser::IntervalTypeContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitIntervalType(this);
}

NesCEPParser::IntervalTypeContext* NesCEPParser::intervalType() {
    IntervalTypeContext* _localctx = _tracker.createInstance<IntervalTypeContext>(_ctx, getState());
    enterRule(_localctx, 16, NesCEPParser::RuleIntervalType);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(146);
        _la = _input->LA(1);
        if (!((((_la & ~0x3fULL) == 0)
               && ((1ULL << _la)
                   & ((1ULL << NesCEPParser::QUARTER) | (1ULL << NesCEPParser::MONTH) | (1ULL << NesCEPParser::DAY)
                      | (1ULL << NesCEPParser::HOUR) | (1ULL << NesCEPParser::MINUTE) | (1ULL << NesCEPParser::WEEK)
                      | (1ULL << NesCEPParser::SECOND) | (1ULL << NesCEPParser::MICROSECOND)))
                   != 0))) {
            _errHandler->recoverInline(this);
        } else {
            _errHandler->reportMatch(this);
            consume();
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- OptionContext ------------------------------------------------------------------

NesCEPParser::OptionContext::OptionContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::OptionContext::ALL() { return getToken(NesCEPParser::ALL, 0); }

tree::TerminalNode* NesCEPParser::OptionContext::NONE() { return getToken(NesCEPParser::NONE, 0); }

size_t NesCEPParser::OptionContext::getRuleIndex() const { return NesCEPParser::RuleOption; }

void NesCEPParser::OptionContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterOption(this);
}

void NesCEPParser::OptionContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitOption(this);
}

NesCEPParser::OptionContext* NesCEPParser::option() {
    OptionContext* _localctx = _tracker.createInstance<OptionContext>(_ctx, getState());
    enterRule(_localctx, 18, NesCEPParser::RuleOption);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(148);
        _la = _input->LA(1);
        if (!(_la == NesCEPParser::ALL

              || _la == NesCEPParser::NONE)) {
            _errHandler->recoverInline(this);
        } else {
            _errHandler->reportMatch(this);
            consume();
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- OutputExpressionContext ------------------------------------------------------------------

NesCEPParser::OutputExpressionContext::OutputExpressionContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::OutputExpressionContext::NAME() { return getToken(NesCEPParser::NAME, 0); }

tree::TerminalNode* NesCEPParser::OutputExpressionContext::SEP() { return getToken(NesCEPParser::SEP, 0); }

tree::TerminalNode* NesCEPParser::OutputExpressionContext::LBRACKET() { return getToken(NesCEPParser::LBRACKET, 0); }

std::vector<NesCEPParser::OutAttributeContext*> NesCEPParser::OutputExpressionContext::outAttribute() {
    return getRuleContexts<NesCEPParser::OutAttributeContext>();
}

NesCEPParser::OutAttributeContext* NesCEPParser::OutputExpressionContext::outAttribute(size_t i) {
    return getRuleContext<NesCEPParser::OutAttributeContext>(i);
}

tree::TerminalNode* NesCEPParser::OutputExpressionContext::RBRACKET() { return getToken(NesCEPParser::RBRACKET, 0); }

std::vector<tree::TerminalNode*> NesCEPParser::OutputExpressionContext::COMMA() { return getTokens(NesCEPParser::COMMA); }

tree::TerminalNode* NesCEPParser::OutputExpressionContext::COMMA(size_t i) { return getToken(NesCEPParser::COMMA, i); }

size_t NesCEPParser::OutputExpressionContext::getRuleIndex() const { return NesCEPParser::RuleOutputExpression; }

void NesCEPParser::OutputExpressionContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterOutputExpression(this);
}

void NesCEPParser::OutputExpressionContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitOutputExpression(this);
}

NesCEPParser::OutputExpressionContext* NesCEPParser::outputExpression() {
    OutputExpressionContext* _localctx = _tracker.createInstance<OutputExpressionContext>(_ctx, getState());
    enterRule(_localctx, 20, NesCEPParser::RuleOutputExpression);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(150);
        match(NesCEPParser::NAME);
        setState(151);
        match(NesCEPParser::SEP);
        setState(152);
        match(NesCEPParser::LBRACKET);
        setState(153);
        outAttribute();
        setState(158);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NesCEPParser::COMMA) {
            setState(154);
            match(NesCEPParser::COMMA);
            setState(155);
            outAttribute();
            setState(160);
            _errHandler->sync(this);
            _la = _input->LA(1);
        }
        setState(161);
        match(NesCEPParser::RBRACKET);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- OutAttributeContext ------------------------------------------------------------------

NesCEPParser::OutAttributeContext::OutAttributeContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::OutAttributeContext::NAME() { return getToken(NesCEPParser::NAME, 0); }

tree::TerminalNode* NesCEPParser::OutAttributeContext::EQUAL() { return getToken(NesCEPParser::EQUAL, 0); }

NesCEPParser::AttValContext* NesCEPParser::OutAttributeContext::attVal() {
    return getRuleContext<NesCEPParser::AttValContext>(0);
}

size_t NesCEPParser::OutAttributeContext::getRuleIndex() const { return NesCEPParser::RuleOutAttribute; }

void NesCEPParser::OutAttributeContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterOutAttribute(this);
}

void NesCEPParser::OutAttributeContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitOutAttribute(this);
}

NesCEPParser::OutAttributeContext* NesCEPParser::outAttribute() {
    OutAttributeContext* _localctx = _tracker.createInstance<OutAttributeContext>(_ctx, getState());
    enterRule(_localctx, 22, NesCEPParser::RuleOutAttribute);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(163);
        match(NesCEPParser::NAME);
        setState(164);
        match(NesCEPParser::EQUAL);
        setState(165);
        attVal();

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- SinkListContext ------------------------------------------------------------------

NesCEPParser::SinkListContext::SinkListContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<NesCEPParser::SinkContext*> NesCEPParser::SinkListContext::sink() {
    return getRuleContexts<NesCEPParser::SinkContext>();
}

NesCEPParser::SinkContext* NesCEPParser::SinkListContext::sink(size_t i) { return getRuleContext<NesCEPParser::SinkContext>(i); }

std::vector<tree::TerminalNode*> NesCEPParser::SinkListContext::COMMA() { return getTokens(NesCEPParser::COMMA); }

tree::TerminalNode* NesCEPParser::SinkListContext::COMMA(size_t i) { return getToken(NesCEPParser::COMMA, i); }

size_t NesCEPParser::SinkListContext::getRuleIndex() const { return NesCEPParser::RuleSinkList; }

void NesCEPParser::SinkListContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterSinkList(this);
}

void NesCEPParser::SinkListContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitSinkList(this);
}

NesCEPParser::SinkListContext* NesCEPParser::sinkList() {
    SinkListContext* _localctx = _tracker.createInstance<SinkListContext>(_ctx, getState());
    enterRule(_localctx, 24, NesCEPParser::RuleSinkList);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(167);
        sink();
        setState(172);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NesCEPParser::COMMA) {
            setState(168);
            match(NesCEPParser::COMMA);
            setState(169);
            sink();
            setState(174);
            _errHandler->sync(this);
            _la = _input->LA(1);
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- SinkContext ------------------------------------------------------------------

NesCEPParser::SinkContext::SinkContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

NesCEPParser::SinkTypeContext* NesCEPParser::SinkContext::sinkType() { return getRuleContext<NesCEPParser::SinkTypeContext>(0); }

tree::TerminalNode* NesCEPParser::SinkContext::SINKSEP() { return getToken(NesCEPParser::SINKSEP, 0); }

tree::TerminalNode* NesCEPParser::SinkContext::NAME() { return getToken(NesCEPParser::NAME, 0); }

size_t NesCEPParser::SinkContext::getRuleIndex() const { return NesCEPParser::RuleSink; }

void NesCEPParser::SinkContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterSink(this);
}

void NesCEPParser::SinkContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitSink(this);
}

NesCEPParser::SinkContext* NesCEPParser::sink() {
    SinkContext* _localctx = _tracker.createInstance<SinkContext>(_ctx, getState());
    enterRule(_localctx, 26, NesCEPParser::RuleSink);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(175);
        sinkType();
        setState(176);
        match(NesCEPParser::SINKSEP);
        setState(177);
        match(NesCEPParser::NAME);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- ListEventsContext ------------------------------------------------------------------

NesCEPParser::ListEventsContext::ListEventsContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<NesCEPParser::EventElemContext*> NesCEPParser::ListEventsContext::eventElem() {
    return getRuleContexts<NesCEPParser::EventElemContext>();
}

NesCEPParser::EventElemContext* NesCEPParser::ListEventsContext::eventElem(size_t i) {
    return getRuleContext<NesCEPParser::EventElemContext>(i);
}

std::vector<NesCEPParser::OperatorRuleContext*> NesCEPParser::ListEventsContext::operatorRule() {
    return getRuleContexts<NesCEPParser::OperatorRuleContext>();
}

NesCEPParser::OperatorRuleContext* NesCEPParser::ListEventsContext::operatorRule(size_t i) {
    return getRuleContext<NesCEPParser::OperatorRuleContext>(i);
}

size_t NesCEPParser::ListEventsContext::getRuleIndex() const { return NesCEPParser::RuleListEvents; }

void NesCEPParser::ListEventsContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterListEvents(this);
}

void NesCEPParser::ListEventsContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitListEvents(this);
}

NesCEPParser::ListEventsContext* NesCEPParser::listEvents() {
    ListEventsContext* _localctx = _tracker.createInstance<ListEventsContext>(_ctx, getState());
    enterRule(_localctx, 28, NesCEPParser::RuleListEvents);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(179);
        eventElem();
        setState(185);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while ((((_la & ~0x3fULL) == 0)
                && ((1ULL << _la)
                    & ((1ULL << NesCEPParser::ANY) | (1ULL << NesCEPParser::SEQ) | (1ULL << NesCEPParser::NEXT)
                       | (1ULL << NesCEPParser::AND) | (1ULL << NesCEPParser::OR)))
                    != 0)) {
            setState(180);
            operatorRule();
            setState(181);
            eventElem();
            setState(187);
            _errHandler->sync(this);
            _la = _input->LA(1);
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- EventElemContext ------------------------------------------------------------------

NesCEPParser::EventElemContext::EventElemContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

NesCEPParser::EventContext* NesCEPParser::EventElemContext::event() { return getRuleContext<NesCEPParser::EventContext>(0); }

tree::TerminalNode* NesCEPParser::EventElemContext::NOT() { return getToken(NesCEPParser::NOT, 0); }

tree::TerminalNode* NesCEPParser::EventElemContext::LPARENTHESIS() { return getToken(NesCEPParser::LPARENTHESIS, 0); }

NesCEPParser::ListEventsContext* NesCEPParser::EventElemContext::listEvents() {
    return getRuleContext<NesCEPParser::ListEventsContext>(0);
}

tree::TerminalNode* NesCEPParser::EventElemContext::RPARENTHESIS() { return getToken(NesCEPParser::RPARENTHESIS, 0); }

size_t NesCEPParser::EventElemContext::getRuleIndex() const { return NesCEPParser::RuleEventElem; }

void NesCEPParser::EventElemContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterEventElem(this);
}

void NesCEPParser::EventElemContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitEventElem(this);
}

NesCEPParser::EventElemContext* NesCEPParser::eventElem() {
    EventElemContext* _localctx = _tracker.createInstance<EventElemContext>(_ctx, getState());
    enterRule(_localctx, 30, NesCEPParser::RuleEventElem);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(199);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 12, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(189);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::NOT) {
                    setState(188);
                    match(NesCEPParser::NOT);
                }
                setState(191);
                event();
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(193);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::NOT) {
                    setState(192);
                    match(NesCEPParser::NOT);
                }
                setState(195);
                match(NesCEPParser::LPARENTHESIS);
                setState(196);
                listEvents();
                setState(197);
                match(NesCEPParser::RPARENTHESIS);
                break;
            }

            default: break;
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- EventContext ------------------------------------------------------------------

NesCEPParser::EventContext::EventContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::EventContext::NAME() { return getToken(NesCEPParser::NAME, 0); }

NesCEPParser::QuantifiersContext* NesCEPParser::EventContext::quantifiers() {
    return getRuleContext<NesCEPParser::QuantifiersContext>(0);
}

size_t NesCEPParser::EventContext::getRuleIndex() const { return NesCEPParser::RuleEvent; }

void NesCEPParser::EventContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterEvent(this);
}

void NesCEPParser::EventContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitEvent(this);
}

NesCEPParser::EventContext* NesCEPParser::event() {
    EventContext* _localctx = _tracker.createInstance<EventContext>(_ctx, getState());
    enterRule(_localctx, 32, NesCEPParser::RuleEvent);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(201);
        match(NesCEPParser::NAME);
        setState(203);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0)
             && ((1ULL << _la) & ((1ULL << NesCEPParser::STAR) | (1ULL << NesCEPParser::PLUS) | (1ULL << NesCEPParser::LBRACKET)))
                 != 0)) {
            setState(202);
            quantifiers();
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- QuantifiersContext ------------------------------------------------------------------

NesCEPParser::QuantifiersContext::QuantifiersContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::QuantifiersContext::STAR() { return getToken(NesCEPParser::STAR, 0); }

tree::TerminalNode* NesCEPParser::QuantifiersContext::PLUS() { return getToken(NesCEPParser::PLUS, 0); }

tree::TerminalNode* NesCEPParser::QuantifiersContext::LBRACKET() { return getToken(NesCEPParser::LBRACKET, 0); }

tree::TerminalNode* NesCEPParser::QuantifiersContext::INT() { return getToken(NesCEPParser::INT, 0); }

tree::TerminalNode* NesCEPParser::QuantifiersContext::RBRACKET() { return getToken(NesCEPParser::RBRACKET, 0); }

NesCEPParser::ConsecutiveOptionContext* NesCEPParser::QuantifiersContext::consecutiveOption() {
    return getRuleContext<NesCEPParser::ConsecutiveOptionContext>(0);
}

NesCEPParser::IterMinContext* NesCEPParser::QuantifiersContext::iterMin() {
    return getRuleContext<NesCEPParser::IterMinContext>(0);
}

tree::TerminalNode* NesCEPParser::QuantifiersContext::D_POINTS() { return getToken(NesCEPParser::D_POINTS, 0); }

NesCEPParser::IterMaxContext* NesCEPParser::QuantifiersContext::iterMax() {
    return getRuleContext<NesCEPParser::IterMaxContext>(0);
}

size_t NesCEPParser::QuantifiersContext::getRuleIndex() const { return NesCEPParser::RuleQuantifiers; }

void NesCEPParser::QuantifiersContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterQuantifiers(this);
}

void NesCEPParser::QuantifiersContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitQuantifiers(this);
}

NesCEPParser::QuantifiersContext* NesCEPParser::quantifiers() {
    QuantifiersContext* _localctx = _tracker.createInstance<QuantifiersContext>(_ctx, getState());
    enterRule(_localctx, 34, NesCEPParser::RuleQuantifiers);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(225);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 17, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(205);
                match(NesCEPParser::STAR);
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(206);
                match(NesCEPParser::PLUS);
                break;
            }

            case 3: {
                enterOuterAlt(_localctx, 3);
                setState(207);
                match(NesCEPParser::LBRACKET);
                setState(209);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::ANY

                    || _la == NesCEPParser::NEXT) {
                    setState(208);
                    consecutiveOption();
                }
                setState(211);
                match(NesCEPParser::INT);
                setState(213);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::PLUS) {
                    setState(212);
                    match(NesCEPParser::PLUS);
                }
                setState(215);
                match(NesCEPParser::RBRACKET);
                break;
            }

            case 4: {
                enterOuterAlt(_localctx, 4);
                setState(216);
                match(NesCEPParser::LBRACKET);
                setState(218);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::ANY

                    || _la == NesCEPParser::NEXT) {
                    setState(217);
                    consecutiveOption();
                }
                setState(220);
                iterMin();
                setState(221);
                match(NesCEPParser::D_POINTS);
                setState(222);
                iterMax();
                setState(223);
                match(NesCEPParser::RBRACKET);
                break;
            }

            default: break;
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- IterMaxContext ------------------------------------------------------------------

NesCEPParser::IterMaxContext::IterMaxContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::IterMaxContext::INT() { return getToken(NesCEPParser::INT, 0); }

size_t NesCEPParser::IterMaxContext::getRuleIndex() const { return NesCEPParser::RuleIterMax; }

void NesCEPParser::IterMaxContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterIterMax(this);
}

void NesCEPParser::IterMaxContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitIterMax(this);
}

NesCEPParser::IterMaxContext* NesCEPParser::iterMax() {
    IterMaxContext* _localctx = _tracker.createInstance<IterMaxContext>(_ctx, getState());
    enterRule(_localctx, 36, NesCEPParser::RuleIterMax);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(227);
        match(NesCEPParser::INT);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- IterMinContext ------------------------------------------------------------------

NesCEPParser::IterMinContext::IterMinContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::IterMinContext::INT() { return getToken(NesCEPParser::INT, 0); }

size_t NesCEPParser::IterMinContext::getRuleIndex() const { return NesCEPParser::RuleIterMin; }

void NesCEPParser::IterMinContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterIterMin(this);
}

void NesCEPParser::IterMinContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitIterMin(this);
}

NesCEPParser::IterMinContext* NesCEPParser::iterMin() {
    IterMinContext* _localctx = _tracker.createInstance<IterMinContext>(_ctx, getState());
    enterRule(_localctx, 38, NesCEPParser::RuleIterMin);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(229);
        match(NesCEPParser::INT);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- ConsecutiveOptionContext ------------------------------------------------------------------

NesCEPParser::ConsecutiveOptionContext::ConsecutiveOptionContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::ConsecutiveOptionContext::NEXT() { return getToken(NesCEPParser::NEXT, 0); }

tree::TerminalNode* NesCEPParser::ConsecutiveOptionContext::ANY() { return getToken(NesCEPParser::ANY, 0); }

size_t NesCEPParser::ConsecutiveOptionContext::getRuleIndex() const { return NesCEPParser::RuleConsecutiveOption; }

void NesCEPParser::ConsecutiveOptionContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterConsecutiveOption(this);
}

void NesCEPParser::ConsecutiveOptionContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitConsecutiveOption(this);
}

NesCEPParser::ConsecutiveOptionContext* NesCEPParser::consecutiveOption() {
    ConsecutiveOptionContext* _localctx = _tracker.createInstance<ConsecutiveOptionContext>(_ctx, getState());
    enterRule(_localctx, 40, NesCEPParser::RuleConsecutiveOption);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(232);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::ANY) {
            setState(231);
            match(NesCEPParser::ANY);
        }
        setState(234);
        match(NesCEPParser::NEXT);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- OperatorRuleContext ------------------------------------------------------------------

NesCEPParser::OperatorRuleContext::OperatorRuleContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::OperatorRuleContext::AND() { return getToken(NesCEPParser::AND, 0); }

tree::TerminalNode* NesCEPParser::OperatorRuleContext::OR() { return getToken(NesCEPParser::OR, 0); }

NesCEPParser::SequenceContext* NesCEPParser::OperatorRuleContext::sequence() {
    return getRuleContext<NesCEPParser::SequenceContext>(0);
}

size_t NesCEPParser::OperatorRuleContext::getRuleIndex() const { return NesCEPParser::RuleOperatorRule; }

void NesCEPParser::OperatorRuleContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterOperatorRule(this);
}

void NesCEPParser::OperatorRuleContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitOperatorRule(this);
}

NesCEPParser::OperatorRuleContext* NesCEPParser::operatorRule() {
    OperatorRuleContext* _localctx = _tracker.createInstance<OperatorRuleContext>(_ctx, getState());
    enterRule(_localctx, 42, NesCEPParser::RuleOperatorRule);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(239);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
            case NesCEPParser::AND: {
                enterOuterAlt(_localctx, 1);
                setState(236);
                match(NesCEPParser::AND);
                break;
            }

            case NesCEPParser::OR: {
                enterOuterAlt(_localctx, 2);
                setState(237);
                match(NesCEPParser::OR);
                break;
            }

            case NesCEPParser::ANY:
            case NesCEPParser::SEQ:
            case NesCEPParser::NEXT: {
                enterOuterAlt(_localctx, 3);
                setState(238);
                sequence();
                break;
            }

            default: throw NoViableAltException(this);
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- SequenceContext ------------------------------------------------------------------

NesCEPParser::SequenceContext::SequenceContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::SequenceContext::SEQ() { return getToken(NesCEPParser::SEQ, 0); }

NesCEPParser::ContiguityContext* NesCEPParser::SequenceContext::contiguity() {
    return getRuleContext<NesCEPParser::ContiguityContext>(0);
}

size_t NesCEPParser::SequenceContext::getRuleIndex() const { return NesCEPParser::RuleSequence; }

void NesCEPParser::SequenceContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterSequence(this);
}

void NesCEPParser::SequenceContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitSequence(this);
}

NesCEPParser::SequenceContext* NesCEPParser::sequence() {
    SequenceContext* _localctx = _tracker.createInstance<SequenceContext>(_ctx, getState());
    enterRule(_localctx, 44, NesCEPParser::RuleSequence);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(243);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
            case NesCEPParser::SEQ: {
                enterOuterAlt(_localctx, 1);
                setState(241);
                match(NesCEPParser::SEQ);
                break;
            }

            case NesCEPParser::ANY:
            case NesCEPParser::NEXT: {
                enterOuterAlt(_localctx, 2);
                setState(242);
                contiguity();
                break;
            }

            default: throw NoViableAltException(this);
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- ContiguityContext ------------------------------------------------------------------

NesCEPParser::ContiguityContext::ContiguityContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::ContiguityContext::NEXT() { return getToken(NesCEPParser::NEXT, 0); }

tree::TerminalNode* NesCEPParser::ContiguityContext::ANY() { return getToken(NesCEPParser::ANY, 0); }

size_t NesCEPParser::ContiguityContext::getRuleIndex() const { return NesCEPParser::RuleContiguity; }

void NesCEPParser::ContiguityContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterContiguity(this);
}

void NesCEPParser::ContiguityContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitContiguity(this);
}

NesCEPParser::ContiguityContext* NesCEPParser::contiguity() {
    ContiguityContext* _localctx = _tracker.createInstance<ContiguityContext>(_ctx, getState());
    enterRule(_localctx, 46, NesCEPParser::RuleContiguity);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(248);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
            case NesCEPParser::NEXT: {
                enterOuterAlt(_localctx, 1);
                setState(245);
                match(NesCEPParser::NEXT);
                break;
            }

            case NesCEPParser::ANY: {
                enterOuterAlt(_localctx, 2);
                setState(246);
                match(NesCEPParser::ANY);
                setState(247);
                match(NesCEPParser::NEXT);
                break;
            }

            default: throw NoViableAltException(this);
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- SinkTypeContext ------------------------------------------------------------------

NesCEPParser::SinkTypeContext::SinkTypeContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::SinkTypeContext::KAFKA() { return getToken(NesCEPParser::KAFKA, 0); }

tree::TerminalNode* NesCEPParser::SinkTypeContext::FILE() { return getToken(NesCEPParser::FILE, 0); }

tree::TerminalNode* NesCEPParser::SinkTypeContext::MQTT() { return getToken(NesCEPParser::MQTT, 0); }

tree::TerminalNode* NesCEPParser::SinkTypeContext::NETWORK() { return getToken(NesCEPParser::NETWORK, 0); }

tree::TerminalNode* NesCEPParser::SinkTypeContext::NULLOUTPUT() { return getToken(NesCEPParser::NULLOUTPUT, 0); }

tree::TerminalNode* NesCEPParser::SinkTypeContext::OPC() { return getToken(NesCEPParser::OPC, 0); }

tree::TerminalNode* NesCEPParser::SinkTypeContext::PRINT() { return getToken(NesCEPParser::PRINT, 0); }

tree::TerminalNode* NesCEPParser::SinkTypeContext::ZMQ() { return getToken(NesCEPParser::ZMQ, 0); }

size_t NesCEPParser::SinkTypeContext::getRuleIndex() const { return NesCEPParser::RuleSinkType; }

void NesCEPParser::SinkTypeContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterSinkType(this);
}

void NesCEPParser::SinkTypeContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitSinkType(this);
}

NesCEPParser::SinkTypeContext* NesCEPParser::sinkType() {
    SinkTypeContext* _localctx = _tracker.createInstance<SinkTypeContext>(_ctx, getState());
    enterRule(_localctx, 48, NesCEPParser::RuleSinkType);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(250);
        _la = _input->LA(1);
        if (!((((_la & ~0x3fULL) == 0)
               && ((1ULL << _la)
                   & ((1ULL << NesCEPParser::KAFKA) | (1ULL << NesCEPParser::FILE) | (1ULL << NesCEPParser::MQTT)
                      | (1ULL << NesCEPParser::NETWORK) | (1ULL << NesCEPParser::NULLOUTPUT) | (1ULL << NesCEPParser::OPC)
                      | (1ULL << NesCEPParser::PRINT) | (1ULL << NesCEPParser::ZMQ)))
                   != 0))) {
            _errHandler->recoverInline(this);
        } else {
            _errHandler->reportMatch(this);
            consume();
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- NullNotnullContext ------------------------------------------------------------------

NesCEPParser::NullNotnullContext::NullNotnullContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::NullNotnullContext::NULLTOKEN() { return getToken(NesCEPParser::NULLTOKEN, 0); }

tree::TerminalNode* NesCEPParser::NullNotnullContext::NOT() { return getToken(NesCEPParser::NOT, 0); }

size_t NesCEPParser::NullNotnullContext::getRuleIndex() const { return NesCEPParser::RuleNullNotnull; }

void NesCEPParser::NullNotnullContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterNullNotnull(this);
}

void NesCEPParser::NullNotnullContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitNullNotnull(this);
}

NesCEPParser::NullNotnullContext* NesCEPParser::nullNotnull() {
    NullNotnullContext* _localctx = _tracker.createInstance<NullNotnullContext>(_ctx, getState());
    enterRule(_localctx, 50, NesCEPParser::RuleNullNotnull);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(253);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::NOT) {
            setState(252);
            match(NesCEPParser::NOT);
        }
        setState(255);
        match(NesCEPParser::NULLTOKEN);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- ConstantContext ------------------------------------------------------------------

NesCEPParser::ConstantContext::ConstantContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode*> NesCEPParser::ConstantContext::QUOTE() { return getTokens(NesCEPParser::QUOTE); }

tree::TerminalNode* NesCEPParser::ConstantContext::QUOTE(size_t i) { return getToken(NesCEPParser::QUOTE, i); }

tree::TerminalNode* NesCEPParser::ConstantContext::NAME() { return getToken(NesCEPParser::NAME, 0); }

tree::TerminalNode* NesCEPParser::ConstantContext::INT() { return getToken(NesCEPParser::INT, 0); }

size_t NesCEPParser::ConstantContext::getRuleIndex() const { return NesCEPParser::RuleConstant; }

void NesCEPParser::ConstantContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterConstant(this);
}

void NesCEPParser::ConstantContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitConstant(this);
}

NesCEPParser::ConstantContext* NesCEPParser::constant() {
    ConstantContext* _localctx = _tracker.createInstance<ConstantContext>(_ctx, getState());
    enterRule(_localctx, 52, NesCEPParser::RuleConstant);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(262);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
            case NesCEPParser::QUOTE: {
                enterOuterAlt(_localctx, 1);
                setState(257);
                match(NesCEPParser::QUOTE);
                setState(258);
                match(NesCEPParser::NAME);
                setState(259);
                match(NesCEPParser::QUOTE);
                break;
            }

            case NesCEPParser::INT: {
                enterOuterAlt(_localctx, 2);
                setState(260);
                match(NesCEPParser::INT);
                break;
            }

            case NesCEPParser::NAME: {
                enterOuterAlt(_localctx, 3);
                setState(261);
                match(NesCEPParser::NAME);
                break;
            }

            default: throw NoViableAltException(this);
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- ExpressionsContext ------------------------------------------------------------------

NesCEPParser::ExpressionsContext::ExpressionsContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<NesCEPParser::ExpressionContext*> NesCEPParser::ExpressionsContext::expression() {
    return getRuleContexts<NesCEPParser::ExpressionContext>();
}

NesCEPParser::ExpressionContext* NesCEPParser::ExpressionsContext::expression(size_t i) {
    return getRuleContext<NesCEPParser::ExpressionContext>(i);
}

std::vector<tree::TerminalNode*> NesCEPParser::ExpressionsContext::COMMA() { return getTokens(NesCEPParser::COMMA); }

tree::TerminalNode* NesCEPParser::ExpressionsContext::COMMA(size_t i) { return getToken(NesCEPParser::COMMA, i); }

size_t NesCEPParser::ExpressionsContext::getRuleIndex() const { return NesCEPParser::RuleExpressions; }

void NesCEPParser::ExpressionsContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterExpressions(this);
}

void NesCEPParser::ExpressionsContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitExpressions(this);
}

NesCEPParser::ExpressionsContext* NesCEPParser::expressions() {
    ExpressionsContext* _localctx = _tracker.createInstance<ExpressionsContext>(_ctx, getState());
    enterRule(_localctx, 54, NesCEPParser::RuleExpressions);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(264);
        expression(0);
        setState(269);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NesCEPParser::COMMA) {
            setState(265);
            match(NesCEPParser::COMMA);
            setState(266);
            expression(0);
            setState(271);
            _errHandler->sync(this);
            _la = _input->LA(1);
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- ExpressionContext ------------------------------------------------------------------

NesCEPParser::ExpressionContext::ExpressionContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t NesCEPParser::ExpressionContext::getRuleIndex() const { return NesCEPParser::RuleExpression; }

void NesCEPParser::ExpressionContext::copyFrom(ExpressionContext* ctx) { ParserRuleContext::copyFrom(ctx); }

//----------------- IsExpressionContext ------------------------------------------------------------------

NesCEPParser::PredicateContext* NesCEPParser::IsExpressionContext::predicate() {
    return getRuleContext<NesCEPParser::PredicateContext>(0);
}

tree::TerminalNode* NesCEPParser::IsExpressionContext::IS() { return getToken(NesCEPParser::IS, 0); }

tree::TerminalNode* NesCEPParser::IsExpressionContext::TRUE() { return getToken(NesCEPParser::TRUE, 0); }

tree::TerminalNode* NesCEPParser::IsExpressionContext::FALSE() { return getToken(NesCEPParser::FALSE, 0); }

tree::TerminalNode* NesCEPParser::IsExpressionContext::UNKNOWN() { return getToken(NesCEPParser::UNKNOWN, 0); }

tree::TerminalNode* NesCEPParser::IsExpressionContext::NOT() { return getToken(NesCEPParser::NOT, 0); }

NesCEPParser::IsExpressionContext::IsExpressionContext(ExpressionContext* ctx) { copyFrom(ctx); }

void NesCEPParser::IsExpressionContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterIsExpression(this);
}
void NesCEPParser::IsExpressionContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitIsExpression(this);
}
//----------------- NotExpressionContext ------------------------------------------------------------------

tree::TerminalNode* NesCEPParser::NotExpressionContext::NOT_OP() { return getToken(NesCEPParser::NOT_OP, 0); }

NesCEPParser::ExpressionContext* NesCEPParser::NotExpressionContext::expression() {
    return getRuleContext<NesCEPParser::ExpressionContext>(0);
}

NesCEPParser::NotExpressionContext::NotExpressionContext(ExpressionContext* ctx) { copyFrom(ctx); }

void NesCEPParser::NotExpressionContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterNotExpression(this);
}
void NesCEPParser::NotExpressionContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitNotExpression(this);
}
//----------------- LogicalExpressionContext ------------------------------------------------------------------

std::vector<NesCEPParser::ExpressionContext*> NesCEPParser::LogicalExpressionContext::expression() {
    return getRuleContexts<NesCEPParser::ExpressionContext>();
}

NesCEPParser::ExpressionContext* NesCEPParser::LogicalExpressionContext::expression(size_t i) {
    return getRuleContext<NesCEPParser::ExpressionContext>(i);
}

NesCEPParser::LogicalOperatorContext* NesCEPParser::LogicalExpressionContext::logicalOperator() {
    return getRuleContext<NesCEPParser::LogicalOperatorContext>(0);
}

NesCEPParser::LogicalExpressionContext::LogicalExpressionContext(ExpressionContext* ctx) { copyFrom(ctx); }

void NesCEPParser::LogicalExpressionContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterLogicalExpression(this);
}
void NesCEPParser::LogicalExpressionContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitLogicalExpression(this);
}
//----------------- PredicateExpressionContext ------------------------------------------------------------------

NesCEPParser::PredicateContext* NesCEPParser::PredicateExpressionContext::predicate() {
    return getRuleContext<NesCEPParser::PredicateContext>(0);
}

NesCEPParser::PredicateExpressionContext::PredicateExpressionContext(ExpressionContext* ctx) { copyFrom(ctx); }

void NesCEPParser::PredicateExpressionContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterPredicateExpression(this);
}
void NesCEPParser::PredicateExpressionContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitPredicateExpression(this);
}

NesCEPParser::ExpressionContext* NesCEPParser::expression() { return expression(0); }

NesCEPParser::ExpressionContext* NesCEPParser::expression(int precedence) {
    ParserRuleContext* parentContext = _ctx;
    size_t parentState = getState();
    NesCEPParser::ExpressionContext* _localctx = _tracker.createInstance<ExpressionContext>(_ctx, parentState);
    NesCEPParser::ExpressionContext* previousContext = _localctx;
    (void) previousContext;// Silence compiler, in case the context is not used by generated code.
    size_t startState = 56;
    enterRecursionRule(_localctx, 56, NesCEPParser::RuleExpression, precedence);

    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        unrollRecursionContexts(parentContext);
    });
    try {
        size_t alt;
        enterOuterAlt(_localctx, 1);
        setState(283);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 26, _ctx)) {
            case 1: {
                _localctx = _tracker.createInstance<NotExpressionContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;

                setState(273);
                match(NesCEPParser::NOT_OP);
                setState(274);
                expression(4);
                break;
            }

            case 2: {
                _localctx = _tracker.createInstance<IsExpressionContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;
                setState(275);
                predicate(0);
                setState(276);
                match(NesCEPParser::IS);
                setState(278);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::NOT) {
                    setState(277);
                    match(NesCEPParser::NOT);
                }
                setState(280);
                dynamic_cast<IsExpressionContext*>(_localctx)->testValue = _input->LT(1);
                _la = _input->LA(1);
                if (!((((_la & ~0x3fULL) == 0)
                       && ((1ULL << _la)
                           & ((1ULL << NesCEPParser::TRUE) | (1ULL << NesCEPParser::FALSE) | (1ULL << NesCEPParser::UNKNOWN)))
                           != 0))) {
                    dynamic_cast<IsExpressionContext*>(_localctx)->testValue = _errHandler->recoverInline(this);
                } else {
                    _errHandler->reportMatch(this);
                    consume();
                }
                break;
            }

            case 3: {
                _localctx = _tracker.createInstance<PredicateExpressionContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;
                setState(282);
                predicate(0);
                break;
            }

            default: break;
        }
        _ctx->stop = _input->LT(-1);
        setState(291);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 27, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
            if (alt == 1) {
                if (!_parseListeners.empty())
                    triggerExitRuleEvent();
                previousContext = _localctx;
                auto newContext = _tracker.createInstance<LogicalExpressionContext>(
                    _tracker.createInstance<ExpressionContext>(parentContext, parentState));
                _localctx = newContext;
                pushNewRecursionContext(newContext, startState, RuleExpression);
                setState(285);

                if (!(precpred(_ctx, 3)))
                    throw FailedPredicateException(this, "precpred(_ctx, 3)");
                setState(286);
                logicalOperator();
                setState(287);
                expression(4);
            }
            setState(293);
            _errHandler->sync(this);
            alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 27, _ctx);
        }
    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }
    return _localctx;
}

//----------------- PredicateContext ------------------------------------------------------------------

NesCEPParser::PredicateContext::PredicateContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t NesCEPParser::PredicateContext::getRuleIndex() const { return NesCEPParser::RulePredicate; }

void NesCEPParser::PredicateContext::copyFrom(PredicateContext* ctx) { ParserRuleContext::copyFrom(ctx); }

//----------------- ExpressionAtomPredicateContext ------------------------------------------------------------------

NesCEPParser::ExpressionAtomContext* NesCEPParser::ExpressionAtomPredicateContext::expressionAtom() {
    return getRuleContext<NesCEPParser::ExpressionAtomContext>(0);
}

NesCEPParser::ExpressionAtomPredicateContext::ExpressionAtomPredicateContext(PredicateContext* ctx) { copyFrom(ctx); }

void NesCEPParser::ExpressionAtomPredicateContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterExpressionAtomPredicate(this);
}
void NesCEPParser::ExpressionAtomPredicateContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitExpressionAtomPredicate(this);
}
//----------------- InPredicateContext ------------------------------------------------------------------

NesCEPParser::PredicateContext* NesCEPParser::InPredicateContext::predicate() {
    return getRuleContext<NesCEPParser::PredicateContext>(0);
}

tree::TerminalNode* NesCEPParser::InPredicateContext::IN() { return getToken(NesCEPParser::IN, 0); }

tree::TerminalNode* NesCEPParser::InPredicateContext::LPARENTHESIS() { return getToken(NesCEPParser::LPARENTHESIS, 0); }

NesCEPParser::ExpressionsContext* NesCEPParser::InPredicateContext::expressions() {
    return getRuleContext<NesCEPParser::ExpressionsContext>(0);
}

tree::TerminalNode* NesCEPParser::InPredicateContext::RPARENTHESIS() { return getToken(NesCEPParser::RPARENTHESIS, 0); }

tree::TerminalNode* NesCEPParser::InPredicateContext::NOT() { return getToken(NesCEPParser::NOT, 0); }

NesCEPParser::InPredicateContext::InPredicateContext(PredicateContext* ctx) { copyFrom(ctx); }

void NesCEPParser::InPredicateContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterInPredicate(this);
}
void NesCEPParser::InPredicateContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitInPredicate(this);
}
//----------------- BinaryComparasionPredicateContext ------------------------------------------------------------------

NesCEPParser::ComparisonOperatorContext* NesCEPParser::BinaryComparasionPredicateContext::comparisonOperator() {
    return getRuleContext<NesCEPParser::ComparisonOperatorContext>(0);
}

std::vector<NesCEPParser::PredicateContext*> NesCEPParser::BinaryComparasionPredicateContext::predicate() {
    return getRuleContexts<NesCEPParser::PredicateContext>();
}

NesCEPParser::PredicateContext* NesCEPParser::BinaryComparasionPredicateContext::predicate(size_t i) {
    return getRuleContext<NesCEPParser::PredicateContext>(i);
}

NesCEPParser::BinaryComparasionPredicateContext::BinaryComparasionPredicateContext(PredicateContext* ctx) { copyFrom(ctx); }

void NesCEPParser::BinaryComparasionPredicateContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterBinaryComparisonPredicate(this);
}
void NesCEPParser::BinaryComparasionPredicateContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitBinaryComparisonPredicate(this);
}
//----------------- IsNullPredicateContext ------------------------------------------------------------------

NesCEPParser::PredicateContext* NesCEPParser::IsNullPredicateContext::predicate() {
    return getRuleContext<NesCEPParser::PredicateContext>(0);
}

tree::TerminalNode* NesCEPParser::IsNullPredicateContext::IS() { return getToken(NesCEPParser::IS, 0); }

NesCEPParser::NullNotnullContext* NesCEPParser::IsNullPredicateContext::nullNotnull() {
    return getRuleContext<NesCEPParser::NullNotnullContext>(0);
}

NesCEPParser::IsNullPredicateContext::IsNullPredicateContext(PredicateContext* ctx) { copyFrom(ctx); }

void NesCEPParser::IsNullPredicateContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterIsNullPredicate(this);
}
void NesCEPParser::IsNullPredicateContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitIsNullPredicate(this);
}

NesCEPParser::PredicateContext* NesCEPParser::predicate() { return predicate(0); }

NesCEPParser::PredicateContext* NesCEPParser::predicate(int precedence) {
    ParserRuleContext* parentContext = _ctx;
    size_t parentState = getState();
    NesCEPParser::PredicateContext* _localctx = _tracker.createInstance<PredicateContext>(_ctx, parentState);
    NesCEPParser::PredicateContext* previousContext = _localctx;
    (void) previousContext;// Silence compiler, in case the context is not used by generated code.
    size_t startState = 58;
    enterRecursionRule(_localctx, 58, NesCEPParser::RulePredicate, precedence);

    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        unrollRecursionContexts(parentContext);
    });
    try {
        size_t alt;
        enterOuterAlt(_localctx, 1);
        _localctx = _tracker.createInstance<ExpressionAtomPredicateContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;

        setState(295);
        expressionAtom(0);
        _ctx->stop = _input->LT(-1);
        setState(315);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
            if (alt == 1) {
                if (!_parseListeners.empty())
                    triggerExitRuleEvent();
                previousContext = _localctx;
                setState(313);
                _errHandler->sync(this);
                switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 29, _ctx)) {
                    case 1: {
                        auto newContext = _tracker.createInstance<BinaryComparasionPredicateContext>(
                            _tracker.createInstance<PredicateContext>(parentContext, parentState));
                        _localctx = newContext;
                        newContext->left = previousContext;
                        pushNewRecursionContext(newContext, startState, RulePredicate);
                        setState(297);

                        if (!(precpred(_ctx, 2)))
                            throw FailedPredicateException(this, "precpred(_ctx, 2)");
                        setState(298);
                        comparisonOperator();
                        setState(299);
                        dynamic_cast<BinaryComparasionPredicateContext*>(_localctx)->right = predicate(3);
                        break;
                    }

                    case 2: {
                        auto newContext = _tracker.createInstance<InPredicateContext>(
                            _tracker.createInstance<PredicateContext>(parentContext, parentState));
                        _localctx = newContext;
                        pushNewRecursionContext(newContext, startState, RulePredicate);
                        setState(301);

                        if (!(precpred(_ctx, 4)))
                            throw FailedPredicateException(this, "precpred(_ctx, 4)");
                        setState(303);
                        _errHandler->sync(this);

                        _la = _input->LA(1);
                        if (_la == NesCEPParser::NOT) {
                            setState(302);
                            match(NesCEPParser::NOT);
                        }
                        setState(305);
                        match(NesCEPParser::IN);
                        setState(306);
                        match(NesCEPParser::LPARENTHESIS);
                        setState(307);
                        expressions();
                        setState(308);
                        match(NesCEPParser::RPARENTHESIS);
                        break;
                    }

                    case 3: {
                        auto newContext = _tracker.createInstance<IsNullPredicateContext>(
                            _tracker.createInstance<PredicateContext>(parentContext, parentState));
                        _localctx = newContext;
                        pushNewRecursionContext(newContext, startState, RulePredicate);
                        setState(310);

                        if (!(precpred(_ctx, 3)))
                            throw FailedPredicateException(this, "precpred(_ctx, 3)");
                        setState(311);
                        match(NesCEPParser::IS);
                        setState(312);
                        nullNotnull();
                        break;
                    }

                    default: break;
                }
            }
            setState(317);
            _errHandler->sync(this);
            alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx);
        }
    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }
    return _localctx;
}

//----------------- ExpressionAtomContext ------------------------------------------------------------------

NesCEPParser::ExpressionAtomContext::ExpressionAtomContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t NesCEPParser::ExpressionAtomContext::getRuleIndex() const { return NesCEPParser::RuleExpressionAtom; }

void NesCEPParser::ExpressionAtomContext::copyFrom(ExpressionAtomContext* ctx) { ParserRuleContext::copyFrom(ctx); }

//----------------- UnaryExpressionAtomContext ------------------------------------------------------------------

NesCEPParser::UnaryOperatorContext* NesCEPParser::UnaryExpressionAtomContext::unaryOperator() {
    return getRuleContext<NesCEPParser::UnaryOperatorContext>(0);
}

NesCEPParser::ExpressionAtomContext* NesCEPParser::UnaryExpressionAtomContext::expressionAtom() {
    return getRuleContext<NesCEPParser::ExpressionAtomContext>(0);
}

NesCEPParser::UnaryExpressionAtomContext::UnaryExpressionAtomContext(ExpressionAtomContext* ctx) { copyFrom(ctx); }

void NesCEPParser::UnaryExpressionAtomContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterUnaryExpressionAtom(this);
}
void NesCEPParser::UnaryExpressionAtomContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitUnaryExpressionAtom(this);
}
//----------------- AttributeAtomContext ------------------------------------------------------------------

NesCEPParser::EventAttributeContext* NesCEPParser::AttributeAtomContext::eventAttribute() {
    return getRuleContext<NesCEPParser::EventAttributeContext>(0);
}

NesCEPParser::AttributeAtomContext::AttributeAtomContext(ExpressionAtomContext* ctx) { copyFrom(ctx); }

void NesCEPParser::AttributeAtomContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterAttributeAtom(this);
}
void NesCEPParser::AttributeAtomContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitAttributeAtom(this);
}
//----------------- ConstantExpressionAtomContext ------------------------------------------------------------------

NesCEPParser::ConstantContext* NesCEPParser::ConstantExpressionAtomContext::constant() {
    return getRuleContext<NesCEPParser::ConstantContext>(0);
}

NesCEPParser::ConstantExpressionAtomContext::ConstantExpressionAtomContext(ExpressionAtomContext* ctx) { copyFrom(ctx); }

void NesCEPParser::ConstantExpressionAtomContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterConstantExpressionAtom(this);
}
void NesCEPParser::ConstantExpressionAtomContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitConstantExpressionAtom(this);
}
//----------------- BinaryExpressionAtomContext ------------------------------------------------------------------

tree::TerminalNode* NesCEPParser::BinaryExpressionAtomContext::BINARY() { return getToken(NesCEPParser::BINARY, 0); }

NesCEPParser::ExpressionAtomContext* NesCEPParser::BinaryExpressionAtomContext::expressionAtom() {
    return getRuleContext<NesCEPParser::ExpressionAtomContext>(0);
}

NesCEPParser::BinaryExpressionAtomContext::BinaryExpressionAtomContext(ExpressionAtomContext* ctx) { copyFrom(ctx); }

void NesCEPParser::BinaryExpressionAtomContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterBinaryExpressionAtom(this);
}
void NesCEPParser::BinaryExpressionAtomContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitBinaryExpressionAtom(this);
}
//----------------- BitExpressionAtomContext ------------------------------------------------------------------

NesCEPParser::BitOperatorContext* NesCEPParser::BitExpressionAtomContext::bitOperator() {
    return getRuleContext<NesCEPParser::BitOperatorContext>(0);
}

std::vector<NesCEPParser::ExpressionAtomContext*> NesCEPParser::BitExpressionAtomContext::expressionAtom() {
    return getRuleContexts<NesCEPParser::ExpressionAtomContext>();
}

NesCEPParser::ExpressionAtomContext* NesCEPParser::BitExpressionAtomContext::expressionAtom(size_t i) {
    return getRuleContext<NesCEPParser::ExpressionAtomContext>(i);
}

NesCEPParser::BitExpressionAtomContext::BitExpressionAtomContext(ExpressionAtomContext* ctx) { copyFrom(ctx); }

void NesCEPParser::BitExpressionAtomContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterBitExpressionAtom(this);
}
void NesCEPParser::BitExpressionAtomContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitBitExpressionAtom(this);
}
//----------------- NestedExpressionAtomContext ------------------------------------------------------------------

tree::TerminalNode* NesCEPParser::NestedExpressionAtomContext::LPARENTHESIS() { return getToken(NesCEPParser::LPARENTHESIS, 0); }

std::vector<NesCEPParser::ExpressionContext*> NesCEPParser::NestedExpressionAtomContext::expression() {
    return getRuleContexts<NesCEPParser::ExpressionContext>();
}

NesCEPParser::ExpressionContext* NesCEPParser::NestedExpressionAtomContext::expression(size_t i) {
    return getRuleContext<NesCEPParser::ExpressionContext>(i);
}

tree::TerminalNode* NesCEPParser::NestedExpressionAtomContext::RPARENTHESIS() { return getToken(NesCEPParser::RPARENTHESIS, 0); }

std::vector<tree::TerminalNode*> NesCEPParser::NestedExpressionAtomContext::COMMA() { return getTokens(NesCEPParser::COMMA); }

tree::TerminalNode* NesCEPParser::NestedExpressionAtomContext::COMMA(size_t i) { return getToken(NesCEPParser::COMMA, i); }

NesCEPParser::NestedExpressionAtomContext::NestedExpressionAtomContext(ExpressionAtomContext* ctx) { copyFrom(ctx); }

void NesCEPParser::NestedExpressionAtomContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterNestedExpressionAtom(this);
}
void NesCEPParser::NestedExpressionAtomContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitNestedExpressionAtom(this);
}
//----------------- MathExpressionAtomContext ------------------------------------------------------------------

NesCEPParser::MathOperatorContext* NesCEPParser::MathExpressionAtomContext::mathOperator() {
    return getRuleContext<NesCEPParser::MathOperatorContext>(0);
}

std::vector<NesCEPParser::ExpressionAtomContext*> NesCEPParser::MathExpressionAtomContext::expressionAtom() {
    return getRuleContexts<NesCEPParser::ExpressionAtomContext>();
}

NesCEPParser::ExpressionAtomContext* NesCEPParser::MathExpressionAtomContext::expressionAtom(size_t i) {
    return getRuleContext<NesCEPParser::ExpressionAtomContext>(i);
}

NesCEPParser::MathExpressionAtomContext::MathExpressionAtomContext(ExpressionAtomContext* ctx) { copyFrom(ctx); }

void NesCEPParser::MathExpressionAtomContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterMathExpressionAtom(this);
}
void NesCEPParser::MathExpressionAtomContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitMathExpressionAtom(this);
}

NesCEPParser::ExpressionAtomContext* NesCEPParser::expressionAtom() { return expressionAtom(0); }

NesCEPParser::ExpressionAtomContext* NesCEPParser::expressionAtom(int precedence) {
    ParserRuleContext* parentContext = _ctx;
    size_t parentState = getState();
    NesCEPParser::ExpressionAtomContext* _localctx = _tracker.createInstance<ExpressionAtomContext>(_ctx, parentState);
    NesCEPParser::ExpressionAtomContext* previousContext = _localctx;
    (void) previousContext;// Silence compiler, in case the context is not used by generated code.
    size_t startState = 60;
    enterRecursionRule(_localctx, 60, NesCEPParser::RuleExpressionAtom, precedence);

    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        unrollRecursionContexts(parentContext);
    });
    try {
        size_t alt;
        enterOuterAlt(_localctx, 1);
        setState(337);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 32, _ctx)) {
            case 1: {
                _localctx = _tracker.createInstance<AttributeAtomContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;

                setState(319);
                eventAttribute();
                break;
            }

            case 2: {
                _localctx = _tracker.createInstance<UnaryExpressionAtomContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;
                setState(320);
                unaryOperator();
                setState(321);
                expressionAtom(6);
                break;
            }

            case 3: {
                _localctx = _tracker.createInstance<BinaryExpressionAtomContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;
                setState(323);
                match(NesCEPParser::BINARY);
                setState(324);
                expressionAtom(5);
                break;
            }

            case 4: {
                _localctx = _tracker.createInstance<NestedExpressionAtomContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;
                setState(325);
                match(NesCEPParser::LPARENTHESIS);
                setState(326);
                expression(0);
                setState(331);
                _errHandler->sync(this);
                _la = _input->LA(1);
                while (_la == NesCEPParser::COMMA) {
                    setState(327);
                    match(NesCEPParser::COMMA);
                    setState(328);
                    expression(0);
                    setState(333);
                    _errHandler->sync(this);
                    _la = _input->LA(1);
                }
                setState(334);
                match(NesCEPParser::RPARENTHESIS);
                break;
            }

            case 5: {
                _localctx = _tracker.createInstance<ConstantExpressionAtomContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;
                setState(336);
                constant();
                break;
            }

            default: break;
        }
        _ctx->stop = _input->LT(-1);
        setState(349);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 34, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
            if (alt == 1) {
                if (!_parseListeners.empty())
                    triggerExitRuleEvent();
                previousContext = _localctx;
                setState(347);
                _errHandler->sync(this);
                switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 33, _ctx)) {
                    case 1: {
                        auto newContext = _tracker.createInstance<BitExpressionAtomContext>(
                            _tracker.createInstance<ExpressionAtomContext>(parentContext, parentState));
                        _localctx = newContext;
                        newContext->left = previousContext;
                        pushNewRecursionContext(newContext, startState, RuleExpressionAtom);
                        setState(339);

                        if (!(precpred(_ctx, 3)))
                            throw FailedPredicateException(this, "precpred(_ctx, 3)");
                        setState(340);
                        bitOperator();
                        setState(341);
                        dynamic_cast<BitExpressionAtomContext*>(_localctx)->right = expressionAtom(4);
                        break;
                    }

                    case 2: {
                        auto newContext = _tracker.createInstance<MathExpressionAtomContext>(
                            _tracker.createInstance<ExpressionAtomContext>(parentContext, parentState));
                        _localctx = newContext;
                        newContext->left = previousContext;
                        pushNewRecursionContext(newContext, startState, RuleExpressionAtom);
                        setState(343);

                        if (!(precpred(_ctx, 2)))
                            throw FailedPredicateException(this, "precpred(_ctx, 2)");
                        setState(344);
                        mathOperator();
                        setState(345);
                        dynamic_cast<MathExpressionAtomContext*>(_localctx)->right = expressionAtom(3);
                        break;
                    }

                    default: break;
                }
            }
            setState(351);
            _errHandler->sync(this);
            alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 34, _ctx);
        }
    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }
    return _localctx;
}

//----------------- EventAttributeContext ------------------------------------------------------------------

NesCEPParser::EventAttributeContext::EventAttributeContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

NesCEPParser::AggregationContext* NesCEPParser::EventAttributeContext::aggregation() {
    return getRuleContext<NesCEPParser::AggregationContext>(0);
}

tree::TerminalNode* NesCEPParser::EventAttributeContext::LPARENTHESIS() { return getToken(NesCEPParser::LPARENTHESIS, 0); }

NesCEPParser::ExpressionsContext* NesCEPParser::EventAttributeContext::expressions() {
    return getRuleContext<NesCEPParser::ExpressionsContext>(0);
}

tree::TerminalNode* NesCEPParser::EventAttributeContext::RPARENTHESIS() { return getToken(NesCEPParser::RPARENTHESIS, 0); }

NesCEPParser::EventIterationContext* NesCEPParser::EventAttributeContext::eventIteration() {
    return getRuleContext<NesCEPParser::EventIterationContext>(0);
}

tree::TerminalNode* NesCEPParser::EventAttributeContext::POINT() { return getToken(NesCEPParser::POINT, 0); }

NesCEPParser::AttributeContext* NesCEPParser::EventAttributeContext::attribute() {
    return getRuleContext<NesCEPParser::AttributeContext>(0);
}

tree::TerminalNode* NesCEPParser::EventAttributeContext::NAME() { return getToken(NesCEPParser::NAME, 0); }

size_t NesCEPParser::EventAttributeContext::getRuleIndex() const { return NesCEPParser::RuleEventAttribute; }

void NesCEPParser::EventAttributeContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterEventAttribute(this);
}

void NesCEPParser::EventAttributeContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitEventAttribute(this);
}

NesCEPParser::EventAttributeContext* NesCEPParser::eventAttribute() {
    EventAttributeContext* _localctx = _tracker.createInstance<EventAttributeContext>(_ctx, getState());
    enterRule(_localctx, 62, NesCEPParser::RuleEventAttribute);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(365);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 36, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(352);
                aggregation();
                setState(353);
                match(NesCEPParser::LPARENTHESIS);
                setState(354);
                expressions();
                setState(355);
                match(NesCEPParser::RPARENTHESIS);
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(357);
                eventIteration();
                setState(360);
                _errHandler->sync(this);

                switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 35, _ctx)) {
                    case 1: {
                        setState(358);
                        match(NesCEPParser::POINT);
                        setState(359);
                        attribute();
                        break;
                    }

                    default: break;
                }
                break;
            }

            case 3: {
                enterOuterAlt(_localctx, 3);
                setState(362);
                match(NesCEPParser::NAME);
                setState(363);
                match(NesCEPParser::POINT);
                setState(364);
                attribute();
                break;
            }

            default: break;
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- EventIterationContext ------------------------------------------------------------------

NesCEPParser::EventIterationContext::EventIterationContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::EventIterationContext::NAME() { return getToken(NesCEPParser::NAME, 0); }

tree::TerminalNode* NesCEPParser::EventIterationContext::LBRACKET() { return getToken(NesCEPParser::LBRACKET, 0); }

tree::TerminalNode* NesCEPParser::EventIterationContext::RBRACKET() { return getToken(NesCEPParser::RBRACKET, 0); }

NesCEPParser::MathExpressionContext* NesCEPParser::EventIterationContext::mathExpression() {
    return getRuleContext<NesCEPParser::MathExpressionContext>(0);
}

size_t NesCEPParser::EventIterationContext::getRuleIndex() const { return NesCEPParser::RuleEventIteration; }

void NesCEPParser::EventIterationContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterEventIteration(this);
}

void NesCEPParser::EventIterationContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitEventIteration(this);
}

NesCEPParser::EventIterationContext* NesCEPParser::eventIteration() {
    EventIterationContext* _localctx = _tracker.createInstance<EventIterationContext>(_ctx, getState());
    enterRule(_localctx, 64, NesCEPParser::RuleEventIteration);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(367);
        match(NesCEPParser::NAME);
        setState(368);
        match(NesCEPParser::LBRACKET);
        setState(370);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0)
             && ((1ULL << _la)
                 & ((1ULL << NesCEPParser::T__0) | (1ULL << NesCEPParser::LPARENTHESIS) | (1ULL << NesCEPParser::NOT)
                    | (1ULL << NesCEPParser::PLUS) | (1ULL << NesCEPParser::BINARY) | (1ULL << NesCEPParser::QUOTE)))
                 != 0)
            || ((((_la - 64) & ~0x3fULL) == 0)
                && ((1ULL << (_la - 64))
                    & ((1ULL << (NesCEPParser::AVG - 64)) | (1ULL << (NesCEPParser::SUM - 64))
                       | (1ULL << (NesCEPParser::MIN - 64)) | (1ULL << (NesCEPParser::MAX - 64))
                       | (1ULL << (NesCEPParser::COUNT - 64)) | (1ULL << (NesCEPParser::INT - 64))
                       | (1ULL << (NesCEPParser::NAME - 64))))
                    != 0)) {
            setState(369);
            mathExpression();
        }
        setState(372);
        match(NesCEPParser::RBRACKET);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- MathExpressionContext ------------------------------------------------------------------

NesCEPParser::MathExpressionContext::MathExpressionContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

NesCEPParser::MathOperatorContext* NesCEPParser::MathExpressionContext::mathOperator() {
    return getRuleContext<NesCEPParser::MathOperatorContext>(0);
}

std::vector<NesCEPParser::ExpressionAtomContext*> NesCEPParser::MathExpressionContext::expressionAtom() {
    return getRuleContexts<NesCEPParser::ExpressionAtomContext>();
}

NesCEPParser::ExpressionAtomContext* NesCEPParser::MathExpressionContext::expressionAtom(size_t i) {
    return getRuleContext<NesCEPParser::ExpressionAtomContext>(i);
}

std::vector<NesCEPParser::ConstantContext*> NesCEPParser::MathExpressionContext::constant() {
    return getRuleContexts<NesCEPParser::ConstantContext>();
}

NesCEPParser::ConstantContext* NesCEPParser::MathExpressionContext::constant(size_t i) {
    return getRuleContext<NesCEPParser::ConstantContext>(i);
}

tree::TerminalNode* NesCEPParser::MathExpressionContext::D_POINTS() { return getToken(NesCEPParser::D_POINTS, 0); }

size_t NesCEPParser::MathExpressionContext::getRuleIndex() const { return NesCEPParser::RuleMathExpression; }

void NesCEPParser::MathExpressionContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterMathExpression(this);
}

void NesCEPParser::MathExpressionContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitMathExpression(this);
}

NesCEPParser::MathExpressionContext* NesCEPParser::mathExpression() {
    MathExpressionContext* _localctx = _tracker.createInstance<MathExpressionContext>(_ctx, getState());
    enterRule(_localctx, 66, NesCEPParser::RuleMathExpression);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(383);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 39, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(374);
                dynamic_cast<MathExpressionContext*>(_localctx)->left = expressionAtom(0);
                setState(375);
                mathOperator();
                setState(376);
                dynamic_cast<MathExpressionContext*>(_localctx)->right = expressionAtom(0);
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(378);
                constant();
                setState(381);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::D_POINTS) {
                    setState(379);
                    match(NesCEPParser::D_POINTS);
                    setState(380);
                    constant();
                }
                break;
            }

            default: break;
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- AggregationContext ------------------------------------------------------------------

NesCEPParser::AggregationContext::AggregationContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::AggregationContext::AVGNode() { return getToken(NesCEPParser::AVG, 0); }

tree::TerminalNode* NesCEPParser::AggregationContext::SUMNode() { return getToken(NesCEPParser::SUM, 0); }

tree::TerminalNode* NesCEPParser::AggregationContext::MINNode() { return getToken(NesCEPParser::MIN, 0); }

tree::TerminalNode* NesCEPParser::AggregationContext::MAXNode() { return getToken(NesCEPParser::MAX, 0); }

tree::TerminalNode* NesCEPParser::AggregationContext::COUNTNode() { return getToken(NesCEPParser::COUNT, 0); }

size_t NesCEPParser::AggregationContext::getRuleIndex() const { return NesCEPParser::RuleAggregation; }

void NesCEPParser::AggregationContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterAggregation(this);
}

void NesCEPParser::AggregationContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitAggregation(this);
}

NesCEPParser::AggregationContext* NesCEPParser::aggregation() {
    AggregationContext* _localctx = _tracker.createInstance<AggregationContext>(_ctx, getState());
    enterRule(_localctx, 68, NesCEPParser::RuleAggregation);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(391);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
            case NesCEPParser::LPARENTHESIS: {
                enterOuterAlt(_localctx, 1);

                break;
            }

            case NesCEPParser::AVG: {
                enterOuterAlt(_localctx, 2);
                setState(386);
                match(NesCEPParser::AVG);
                break;
            }

            case NesCEPParser::SUM: {
                enterOuterAlt(_localctx, 3);
                setState(387);
                match(NesCEPParser::SUM);
                break;
            }

            case NesCEPParser::MIN: {
                enterOuterAlt(_localctx, 4);
                setState(388);
                match(NesCEPParser::MIN);
                break;
            }

            case NesCEPParser::MAX: {
                enterOuterAlt(_localctx, 5);
                setState(389);
                match(NesCEPParser::MAX);
                break;
            }

            case NesCEPParser::COUNT: {
                enterOuterAlt(_localctx, 6);
                setState(390);
                match(NesCEPParser::COUNT);
                break;
            }

            default: throw NoViableAltException(this);
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- AttributeContext ------------------------------------------------------------------

NesCEPParser::AttributeContext::AttributeContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::AttributeContext::NAME() { return getToken(NesCEPParser::NAME, 0); }

size_t NesCEPParser::AttributeContext::getRuleIndex() const { return NesCEPParser::RuleAttribute; }

void NesCEPParser::AttributeContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterAttribute(this);
}

void NesCEPParser::AttributeContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitAttribute(this);
}

NesCEPParser::AttributeContext* NesCEPParser::attribute() {
    AttributeContext* _localctx = _tracker.createInstance<AttributeContext>(_ctx, getState());
    enterRule(_localctx, 70, NesCEPParser::RuleAttribute);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(393);
        match(NesCEPParser::NAME);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- AttValContext ------------------------------------------------------------------

NesCEPParser::AttValContext::AttValContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::AttValContext::IF() { return getToken(NesCEPParser::IF, 0); }

NesCEPParser::ConditionContext* NesCEPParser::AttValContext::condition() {
    return getRuleContext<NesCEPParser::ConditionContext>(0);
}

NesCEPParser::EventAttributeContext* NesCEPParser::AttValContext::eventAttribute() {
    return getRuleContext<NesCEPParser::EventAttributeContext>(0);
}

NesCEPParser::EventContext* NesCEPParser::AttValContext::event() { return getRuleContext<NesCEPParser::EventContext>(0); }

NesCEPParser::ExpressionContext* NesCEPParser::AttValContext::expression() {
    return getRuleContext<NesCEPParser::ExpressionContext>(0);
}

NesCEPParser::BoolRuleContext* NesCEPParser::AttValContext::boolRule() {
    return getRuleContext<NesCEPParser::BoolRuleContext>(0);
}

size_t NesCEPParser::AttValContext::getRuleIndex() const { return NesCEPParser::RuleAttVal; }

void NesCEPParser::AttValContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterAttVal(this);
}

void NesCEPParser::AttValContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitAttVal(this);
}

NesCEPParser::AttValContext* NesCEPParser::attVal() {
    AttValContext* _localctx = _tracker.createInstance<AttValContext>(_ctx, getState());
    enterRule(_localctx, 72, NesCEPParser::RuleAttVal);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(401);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 41, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(395);
                match(NesCEPParser::IF);
                setState(396);
                condition();
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(397);
                eventAttribute();
                break;
            }

            case 3: {
                enterOuterAlt(_localctx, 3);
                setState(398);
                event();
                break;
            }

            case 4: {
                enterOuterAlt(_localctx, 4);
                setState(399);
                expression(0);
                break;
            }

            case 5: {
                enterOuterAlt(_localctx, 5);
                setState(400);
                boolRule();
                break;
            }

            default: break;
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- BoolRuleContext ------------------------------------------------------------------

NesCEPParser::BoolRuleContext::BoolRuleContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::BoolRuleContext::TRUE() { return getToken(NesCEPParser::TRUE, 0); }

tree::TerminalNode* NesCEPParser::BoolRuleContext::FALSE() { return getToken(NesCEPParser::FALSE, 0); }

size_t NesCEPParser::BoolRuleContext::getRuleIndex() const { return NesCEPParser::RuleBoolRule; }

void NesCEPParser::BoolRuleContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterBoolRule(this);
}

void NesCEPParser::BoolRuleContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitBoolRule(this);
}

NesCEPParser::BoolRuleContext* NesCEPParser::boolRule() {
    BoolRuleContext* _localctx = _tracker.createInstance<BoolRuleContext>(_ctx, getState());
    enterRule(_localctx, 74, NesCEPParser::RuleBoolRule);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(403);
        _la = _input->LA(1);
        if (!(_la == NesCEPParser::TRUE

              || _la == NesCEPParser::FALSE)) {
            _errHandler->recoverInline(this);
        } else {
            _errHandler->reportMatch(this);
            consume();
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- ConditionContext ------------------------------------------------------------------

NesCEPParser::ConditionContext::ConditionContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::ConditionContext::LPARENTHESIS() { return getToken(NesCEPParser::LPARENTHESIS, 0); }

NesCEPParser::ExpressionContext* NesCEPParser::ConditionContext::expression() {
    return getRuleContext<NesCEPParser::ExpressionContext>(0);
}

std::vector<tree::TerminalNode*> NesCEPParser::ConditionContext::COMMA() { return getTokens(NesCEPParser::COMMA); }

tree::TerminalNode* NesCEPParser::ConditionContext::COMMA(size_t i) { return getToken(NesCEPParser::COMMA, i); }

std::vector<NesCEPParser::AttValContext*> NesCEPParser::ConditionContext::attVal() {
    return getRuleContexts<NesCEPParser::AttValContext>();
}

NesCEPParser::AttValContext* NesCEPParser::ConditionContext::attVal(size_t i) {
    return getRuleContext<NesCEPParser::AttValContext>(i);
}

tree::TerminalNode* NesCEPParser::ConditionContext::RPARENTHESIS() { return getToken(NesCEPParser::RPARENTHESIS, 0); }

size_t NesCEPParser::ConditionContext::getRuleIndex() const { return NesCEPParser::RuleCondition; }

void NesCEPParser::ConditionContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterCondition(this);
}

void NesCEPParser::ConditionContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitCondition(this);
}

NesCEPParser::ConditionContext* NesCEPParser::condition() {
    ConditionContext* _localctx = _tracker.createInstance<ConditionContext>(_ctx, getState());
    enterRule(_localctx, 76, NesCEPParser::RuleCondition);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(405);
        match(NesCEPParser::LPARENTHESIS);
        setState(406);
        expression(0);
        setState(407);
        match(NesCEPParser::COMMA);
        setState(408);
        attVal();
        setState(409);
        match(NesCEPParser::COMMA);
        setState(410);
        attVal();
        setState(411);
        match(NesCEPParser::RPARENTHESIS);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- UnaryOperatorContext ------------------------------------------------------------------

NesCEPParser::UnaryOperatorContext::UnaryOperatorContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::UnaryOperatorContext::PLUS() { return getToken(NesCEPParser::PLUS, 0); }

tree::TerminalNode* NesCEPParser::UnaryOperatorContext::NOT() { return getToken(NesCEPParser::NOT, 0); }

size_t NesCEPParser::UnaryOperatorContext::getRuleIndex() const { return NesCEPParser::RuleUnaryOperator; }

void NesCEPParser::UnaryOperatorContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterUnaryOperator(this);
}

void NesCEPParser::UnaryOperatorContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitUnaryOperator(this);
}

NesCEPParser::UnaryOperatorContext* NesCEPParser::unaryOperator() {
    UnaryOperatorContext* _localctx = _tracker.createInstance<UnaryOperatorContext>(_ctx, getState());
    enterRule(_localctx, 78, NesCEPParser::RuleUnaryOperator);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(413);
        _la = _input->LA(1);
        if (!((((_la & ~0x3fULL) == 0)
               && ((1ULL << _la) & ((1ULL << NesCEPParser::T__0) | (1ULL << NesCEPParser::NOT) | (1ULL << NesCEPParser::PLUS)))
                   != 0))) {
            _errHandler->recoverInline(this);
        } else {
            _errHandler->reportMatch(this);
            consume();
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- ComparisonOperatorContext ------------------------------------------------------------------

NesCEPParser::ComparisonOperatorContext::ComparisonOperatorContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode*> NesCEPParser::ComparisonOperatorContext::EQUAL() { return getTokens(NesCEPParser::EQUAL); }

tree::TerminalNode* NesCEPParser::ComparisonOperatorContext::EQUAL(size_t i) { return getToken(NesCEPParser::EQUAL, i); }

tree::TerminalNode* NesCEPParser::ComparisonOperatorContext::NOT_OP() { return getToken(NesCEPParser::NOT_OP, 0); }

size_t NesCEPParser::ComparisonOperatorContext::getRuleIndex() const { return NesCEPParser::RuleComparisonOperator; }

void NesCEPParser::ComparisonOperatorContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterComparisonOperator(this);
}

void NesCEPParser::ComparisonOperatorContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitComparisonOperator(this);
}

NesCEPParser::ComparisonOperatorContext* NesCEPParser::comparisonOperator() {
    ComparisonOperatorContext* _localctx = _tracker.createInstance<ComparisonOperatorContext>(_ctx, getState());
    enterRule(_localctx, 80, NesCEPParser::RuleComparisonOperator);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(431);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 42, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(415);
                match(NesCEPParser::EQUAL);
                setState(416);
                match(NesCEPParser::EQUAL);
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(417);
                match(NesCEPParser::T__1);
                break;
            }

            case 3: {
                enterOuterAlt(_localctx, 3);
                setState(418);
                match(NesCEPParser::T__2);
                break;
            }

            case 4: {
                enterOuterAlt(_localctx, 4);
                setState(419);
                match(NesCEPParser::T__2);
                setState(420);
                match(NesCEPParser::EQUAL);
                break;
            }

            case 5: {
                enterOuterAlt(_localctx, 5);
                setState(421);
                match(NesCEPParser::T__1);
                setState(422);
                match(NesCEPParser::EQUAL);
                break;
            }

            case 6: {
                enterOuterAlt(_localctx, 6);
                setState(423);
                match(NesCEPParser::T__2);
                setState(424);
                match(NesCEPParser::T__1);
                break;
            }

            case 7: {
                enterOuterAlt(_localctx, 7);
                setState(425);
                match(NesCEPParser::NOT_OP);
                setState(426);
                match(NesCEPParser::EQUAL);
                break;
            }

            case 8: {
                enterOuterAlt(_localctx, 8);
                setState(427);
                match(NesCEPParser::T__2);
                setState(428);
                match(NesCEPParser::EQUAL);
                setState(429);
                match(NesCEPParser::T__1);
                break;
            }

            case 9: {
                enterOuterAlt(_localctx, 9);
                setState(430);
                match(NesCEPParser::EQUAL);
                break;
            }

            default: break;
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- LogicalOperatorContext ------------------------------------------------------------------

NesCEPParser::LogicalOperatorContext::LogicalOperatorContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::LogicalOperatorContext::LOGAND() { return getToken(NesCEPParser::LOGAND, 0); }

tree::TerminalNode* NesCEPParser::LogicalOperatorContext::LOGXOR() { return getToken(NesCEPParser::LOGXOR, 0); }

tree::TerminalNode* NesCEPParser::LogicalOperatorContext::LOGOR() { return getToken(NesCEPParser::LOGOR, 0); }

size_t NesCEPParser::LogicalOperatorContext::getRuleIndex() const { return NesCEPParser::RuleLogicalOperator; }

void NesCEPParser::LogicalOperatorContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterLogicalOperator(this);
}

void NesCEPParser::LogicalOperatorContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitLogicalOperator(this);
}

NesCEPParser::LogicalOperatorContext* NesCEPParser::logicalOperator() {
    LogicalOperatorContext* _localctx = _tracker.createInstance<LogicalOperatorContext>(_ctx, getState());
    enterRule(_localctx, 82, NesCEPParser::RuleLogicalOperator);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(433);
        _la = _input->LA(1);
        if (!(((((_la - 70) & ~0x3fULL) == 0)
               && ((1ULL << (_la - 70))
                   & ((1ULL << (NesCEPParser::LOGOR - 70)) | (1ULL << (NesCEPParser::LOGAND - 70))
                      | (1ULL << (NesCEPParser::LOGXOR - 70))))
                   != 0))) {
            _errHandler->recoverInline(this);
        } else {
            _errHandler->reportMatch(this);
            consume();
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- BitOperatorContext ------------------------------------------------------------------

NesCEPParser::BitOperatorContext::BitOperatorContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::BitOperatorContext::LOGXOR() { return getToken(NesCEPParser::LOGXOR, 0); }

size_t NesCEPParser::BitOperatorContext::getRuleIndex() const { return NesCEPParser::RuleBitOperator; }

void NesCEPParser::BitOperatorContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterBitOperator(this);
}

void NesCEPParser::BitOperatorContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitBitOperator(this);
}

NesCEPParser::BitOperatorContext* NesCEPParser::bitOperator() {
    BitOperatorContext* _localctx = _tracker.createInstance<BitOperatorContext>(_ctx, getState());
    enterRule(_localctx, 84, NesCEPParser::RuleBitOperator);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(442);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
            case NesCEPParser::T__2: {
                enterOuterAlt(_localctx, 1);
                setState(435);
                match(NesCEPParser::T__2);
                setState(436);
                match(NesCEPParser::T__2);
                break;
            }

            case NesCEPParser::T__1: {
                enterOuterAlt(_localctx, 2);
                setState(437);
                match(NesCEPParser::T__1);
                setState(438);
                match(NesCEPParser::T__1);
                break;
            }

            case NesCEPParser::T__3: {
                enterOuterAlt(_localctx, 3);
                setState(439);
                match(NesCEPParser::T__3);
                break;
            }

            case NesCEPParser::LOGXOR: {
                enterOuterAlt(_localctx, 4);
                setState(440);
                match(NesCEPParser::LOGXOR);
                break;
            }

            case NesCEPParser::T__4: {
                enterOuterAlt(_localctx, 5);
                setState(441);
                match(NesCEPParser::T__4);
                break;
            }

            default: throw NoViableAltException(this);
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- MathOperatorContext ------------------------------------------------------------------

NesCEPParser::MathOperatorContext::MathOperatorContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::MathOperatorContext::STAR() { return getToken(NesCEPParser::STAR, 0); }

tree::TerminalNode* NesCEPParser::MathOperatorContext::PLUS() { return getToken(NesCEPParser::PLUS, 0); }

size_t NesCEPParser::MathOperatorContext::getRuleIndex() const { return NesCEPParser::RuleMathOperator; }

void NesCEPParser::MathOperatorContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterMathOperator(this);
}

void NesCEPParser::MathOperatorContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitMathOperator(this);
}

NesCEPParser::MathOperatorContext* NesCEPParser::mathOperator() {
    MathOperatorContext* _localctx = _tracker.createInstance<MathOperatorContext>(_ctx, getState());
    enterRule(_localctx, 86, NesCEPParser::RuleMathOperator);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(444);
        _la = _input->LA(1);
        if (!((((_la & ~0x3fULL) == 0)
               && ((1ULL << _la)
                   & ((1ULL << NesCEPParser::T__0) | (1ULL << NesCEPParser::T__5) | (1ULL << NesCEPParser::T__6)
                      | (1ULL << NesCEPParser::T__7) | (1ULL << NesCEPParser::STAR) | (1ULL << NesCEPParser::PLUS)))
                   != 0))) {
            _errHandler->recoverInline(this);
        } else {
            _errHandler->reportMatch(this);
            consume();
        }

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

bool NesCEPParser::sempred(RuleContext* context, size_t ruleIndex, size_t predicateIndex) {
    switch (ruleIndex) {
        case 28: return expressionSempred(dynamic_cast<ExpressionContext*>(context), predicateIndex);
        case 29: return predicateSempred(dynamic_cast<PredicateContext*>(context), predicateIndex);
        case 30: return expressionAtomSempred(dynamic_cast<ExpressionAtomContext*>(context), predicateIndex);

        default: break;
    }
    return true;
}

bool NesCEPParser::expressionSempred(ExpressionContext* _localctx, size_t predicateIndex) {
    NES_INFO("NesCEPParser : expressionSempred {}", _localctx->getText());
    switch (predicateIndex) {
        case 0: return precpred(_ctx, 3);

        default: break;
    }
    return true;
}

bool NesCEPParser::predicateSempred(PredicateContext* _localctx, size_t predicateIndex) {
    NES_INFO("NesCEPParser : predicateSempred {}", _localctx->getText());
    switch (predicateIndex) {
        case 1: return precpred(_ctx, 2);
        case 2: return precpred(_ctx, 4);
        case 3: return precpred(_ctx, 3);

        default: break;
    }
    return true;
}

bool NesCEPParser::expressionAtomSempred(ExpressionAtomContext* _localctx, size_t predicateIndex) {
    NES_INFO("NesCEPParser : expressionAtomSempred {}", _localctx->getText());
    switch (predicateIndex) {
        case 4: return precpred(_ctx, 3);
        case 5: return precpred(_ctx, 2);

        default: break;
    }
    return true;
}

// Static vars and initialization.
std::vector<dfa::DFA> NesCEPParser::_decisionToDFA;
atn::PredictionContextCache NesCEPParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN NesCEPParser::_atn;
std::vector<uint16_t> NesCEPParser::_serializedATN;

std::vector<std::string> NesCEPParser::_ruleNames = {"query",
                                                     "cepPattern",
                                                     "inputStreams",
                                                     "inputStream",
                                                     "compositeEventExpressions",
                                                     "whereExp",
                                                     "timeConstraints",
                                                     "interval",
                                                     "intervalType",
                                                     "option",
                                                     "outputExpression",
                                                     "outAttribute",
                                                     "sinkList",
                                                     "sink",
                                                     "listEvents",
                                                     "eventElem",
                                                     "event",
                                                     "quantifiers",
                                                     "iterMax",
                                                     "iterMin",
                                                     "consecutiveOption",
                                                     "operatorRule",
                                                     "sequence",
                                                     "contiguity",
                                                     "sinkType",
                                                     "nullNotnull",
                                                     "constant",
                                                     "expressions",
                                                     "expression",
                                                     "predicate",
                                                     "expressionAtom",
                                                     "eventAttribute",
                                                     "eventIteration",
                                                     "mathExpression",
                                                     "aggregation",
                                                     "attribute",
                                                     "attVal",
                                                     "boolRule",
                                                     "condition",
                                                     "unaryOperator",
                                                     "comparisonOperator",
                                                     "logicalOperator",
                                                     "bitOperator",
                                                     "mathOperator"};

std::vector<std::string> NesCEPParser::_literalNames = {
    "",        "'-'",    "'>'",       "'<'",       "'&'",          "'|'",           "'/'",       "'%'",       "'--'",
    "",        "'FROM'", "'PATTERN'", "'WHERE'",   "'WITHIN'",     "'CONSUMING'",   "'RETURN'",  "'INTO'",    "'ALL'",
    "'ANY'",   "':='",   "','",       "'('",       "')'",          "'NOT'",         "'!'",       "'SEQ'",     "'NEXT'",
    "'AND'",   "'OR'",   "'*'",       "'+'",       "':'",          "'['",           "']'",       "'XOR'",     "'IN'",
    "'IS'",    "'NULL'", "'BETWEEN'", "'BINARY'",  "'TRUE'",       "'FALSE'",       "'UNKNOWN'", "'QUARTER'", "'MONTH'",
    "'DAY'",   "'HOUR'", "'MINUTE'",  "'WEEK'",    "'SECOND'",     "'MICROSECOND'", "'AS'",      "'='",       "'::'",
    "'Kafka'", "'File'", "'MQTT'",    "'Network'", "'NullOutput'", "'OPC'",         "'Print'",   "'ZMQ'",     "'.'",
    "'\"'",    "'AVG'",  "'SUM'",     "'MIN'",     "'MAX'",        "'COUNT'",       "'IF'",      "'||'",      "'&&'",
    "'^'",     "'NONE'"};

std::vector<std::string> NesCEPParser::_symbolicNames = {"",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "WS",
                                                         "FROM",
                                                         "PATTERN",
                                                         "WHERE",
                                                         "WITHIN",
                                                         "CONSUMING",
                                                         "RETURN",
                                                         "INTO",
                                                         "ALL",
                                                         "ANY",
                                                         "SEP",
                                                         "COMMA",
                                                         "LPARENTHESIS",
                                                         "RPARENTHESIS",
                                                         "NOT",
                                                         "NOT_OP",
                                                         "SEQ",
                                                         "NEXT",
                                                         "AND",
                                                         "OR",
                                                         "STAR",
                                                         "PLUS",
                                                         "D_POINTS",
                                                         "LBRACKET",
                                                         "RBRACKET",
                                                         "XOR",
                                                         "IN",
                                                         "IS",
                                                         "NULLTOKEN",
                                                         "BETWEEN",
                                                         "BINARY",
                                                         "TRUE",
                                                         "FALSE",
                                                         "UNKNOWN",
                                                         "QUARTER",
                                                         "MONTH",
                                                         "DAY",
                                                         "HOUR",
                                                         "MINUTE",
                                                         "WEEK",
                                                         "SECOND",
                                                         "MICROSECOND",
                                                         "AS",
                                                         "EQUAL",
                                                         "SINKSEP",
                                                         "KAFKA",
                                                         "FILE",
                                                         "MQTT",
                                                         "NETWORK",
                                                         "NULLOUTPUT",
                                                         "OPC",
                                                         "PRINT",
                                                         "ZMQ",
                                                         "POINT",
                                                         "QUOTE",
                                                         "AVG",
                                                         "SUM",
                                                         "MIN",
                                                         "MAX",
                                                         "COUNT",
                                                         "IF",
                                                         "LOGOR",
                                                         "LOGAND",
                                                         "LOGXOR",
                                                         "NONE",
                                                         "INT",
                                                         "NAME",
                                                         "ID"};

dfa::Vocabulary NesCEPParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> NesCEPParser::_tokenNames;

NesCEPParser::Initializer::Initializer() {
    for (size_t i = 0; i < _symbolicNames.size(); ++i) {
        std::string name = _vocabulary.getLiteralName(i);
        if (name.empty()) {
            name = _vocabulary.getSymbolicName(i);
        }

        if (name.empty()) {
            _tokenNames.push_back("<INVALID>");
        } else {
            _tokenNames.push_back(name);
        }
    }

    static const uint16_t serializedATNSegment0[] = {
        0x3,   0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964, 0x3,   0x4e,  0x1c1, 0x4,   0x2,   0x9,   0x2,
        0x4,   0x3,    0x9,    0x3,    0x4,    0x4,    0x9,    0x4,    0x4,    0x5,   0x9,   0x5,   0x4,   0x6,   0x9,   0x6,
        0x4,   0x7,    0x9,    0x7,    0x4,    0x8,    0x9,    0x8,    0x4,    0x9,   0x9,   0x9,   0x4,   0xa,   0x9,   0xa,
        0x4,   0xb,    0x9,    0xb,    0x4,    0xc,    0x9,    0xc,    0x4,    0xd,   0x9,   0xd,   0x4,   0xe,   0x9,   0xe,
        0x4,   0xf,    0x9,    0xf,    0x4,    0x10,   0x9,    0x10,   0x4,    0x11,  0x9,   0x11,  0x4,   0x12,  0x9,   0x12,
        0x4,   0x13,   0x9,    0x13,   0x4,    0x14,   0x9,    0x14,   0x4,    0x15,  0x9,   0x15,  0x4,   0x16,  0x9,   0x16,
        0x4,   0x17,   0x9,    0x17,   0x4,    0x18,   0x9,    0x18,   0x4,    0x19,  0x9,   0x19,  0x4,   0x1a,  0x9,   0x1a,
        0x4,   0x1b,   0x9,    0x1b,   0x4,    0x1c,   0x9,    0x1c,   0x4,    0x1d,  0x9,   0x1d,  0x4,   0x1e,  0x9,   0x1e,
        0x4,   0x1f,   0x9,    0x1f,   0x4,    0x20,   0x9,    0x20,   0x4,    0x21,  0x9,   0x21,  0x4,   0x22,  0x9,   0x22,
        0x4,   0x23,   0x9,    0x23,   0x4,    0x24,   0x9,    0x24,   0x4,    0x25,  0x9,   0x25,  0x4,   0x26,  0x9,   0x26,
        0x4,   0x27,   0x9,    0x27,   0x4,    0x28,   0x9,    0x28,   0x4,    0x29,  0x9,   0x29,  0x4,   0x2a,  0x9,   0x2a,
        0x4,   0x2b,   0x9,    0x2b,   0x4,    0x2c,   0x9,    0x2c,   0x4,    0x2d,  0x9,   0x2d,  0x3,   0x2,   0x6,   0x2,
        0x5c,  0xa,    0x2,    0xd,    0x2,    0xe,    0x2,    0x5d,   0x3,    0x2,   0x3,   0x2,   0x3,   0x3,   0x3,   0x3,
        0x3,   0x3,    0x3,    0x3,    0x3,    0x3,    0x3,    0x3,    0x3,    0x3,   0x3,   0x3,   0x5,   0x3,   0x6a,  0xa,
        0x3,   0x3,    0x3,    0x3,    0x3,    0x5,    0x3,    0x6e,   0xa,    0x3,   0x3,   0x3,   0x3,   0x3,   0x5,   0x3,
        0x72,  0xa,    0x3,    0x3,    0x3,    0x3,    0x3,    0x5,    0x3,    0x76,  0xa,   0x3,   0x3,   0x3,   0x3,   0x3,
        0x3,   0x3,    0x3,    0x4,    0x3,    0x4,    0x3,    0x4,    0x7,    0x4,   0x7e,  0xa,   0x4,   0xc,   0x4,   0xe,
        0x4,   0x81,   0xb,    0x4,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,   0x5,   0x5,   0x86,  0xa,   0x5,   0x3,
        0x6,   0x3,    0x6,    0x3,    0x6,    0x3,    0x6,    0x3,    0x7,    0x3,   0x7,   0x3,   0x8,   0x3,   0x8,   0x3,
        0x8,   0x3,    0x8,    0x3,    0x9,    0x3,    0x9,    0x3,    0x9,    0x3,   0xa,   0x3,   0xa,   0x3,   0xb,   0x3,
        0xb,   0x3,    0xc,    0x3,    0xc,    0x3,    0xc,    0x3,    0xc,    0x3,   0xc,   0x3,   0xc,   0x7,   0xc,   0x9f,
        0xa,   0xc,    0xc,    0xc,    0xe,    0xc,    0xa2,   0xb,    0xc,    0x3,   0xc,   0x3,   0xc,   0x3,   0xd,   0x3,
        0xd,   0x3,    0xd,    0x3,    0xd,    0x3,    0xe,    0x3,    0xe,    0x3,   0xe,   0x7,   0xe,   0xad,  0xa,   0xe,
        0xc,   0xe,    0xe,    0xe,    0xb0,   0xb,    0xe,    0x3,    0xf,    0x3,   0xf,   0x3,   0xf,   0x3,   0xf,   0x3,
        0x10,  0x3,    0x10,   0x3,    0x10,   0x3,    0x10,   0x7,    0x10,   0xba,  0xa,   0x10,  0xc,   0x10,  0xe,   0x10,
        0xbd,  0xb,    0x10,   0x3,    0x11,   0x5,    0x11,   0xc0,   0xa,    0x11,  0x3,   0x11,  0x3,   0x11,  0x5,   0x11,
        0xc4,  0xa,    0x11,   0x3,    0x11,   0x3,    0x11,   0x3,    0x11,   0x3,   0x11,  0x5,   0x11,  0xca,  0xa,   0x11,
        0x3,   0x12,   0x3,    0x12,   0x5,    0x12,   0xce,   0xa,    0x12,   0x3,   0x13,  0x3,   0x13,  0x3,   0x13,  0x3,
        0x13,  0x5,    0x13,   0xd4,   0xa,    0x13,   0x3,    0x13,   0x3,    0x13,  0x5,   0x13,  0xd8,  0xa,   0x13,  0x3,
        0x13,  0x3,    0x13,   0x3,    0x13,   0x5,    0x13,   0xdd,   0xa,    0x13,  0x3,   0x13,  0x3,   0x13,  0x3,   0x13,
        0x3,   0x13,   0x3,    0x13,   0x5,    0x13,   0xe4,   0xa,    0x13,   0x3,   0x14,  0x3,   0x14,  0x3,   0x15,  0x3,
        0x15,  0x3,    0x16,   0x5,    0x16,   0xeb,   0xa,    0x16,   0x3,    0x16,  0x3,   0x16,  0x3,   0x17,  0x3,   0x17,
        0x3,   0x17,   0x5,    0x17,   0xf2,   0xa,    0x17,   0x3,    0x18,   0x3,   0x18,  0x5,   0x18,  0xf6,  0xa,   0x18,
        0x3,   0x19,   0x3,    0x19,   0x3,    0x19,   0x5,    0x19,   0xfb,   0xa,   0x19,  0x3,   0x1a,  0x3,   0x1a,  0x3,
        0x1b,  0x5,    0x1b,   0x100,  0xa,    0x1b,   0x3,    0x1b,   0x3,    0x1b,  0x3,   0x1c,  0x3,   0x1c,  0x3,   0x1c,
        0x3,   0x1c,   0x3,    0x1c,   0x5,    0x1c,   0x109,  0xa,    0x1c,   0x3,   0x1d,  0x3,   0x1d,  0x3,   0x1d,  0x7,
        0x1d,  0x10e,  0xa,    0x1d,   0xc,    0x1d,   0xe,    0x1d,   0x111,  0xb,   0x1d,  0x3,   0x1e,  0x3,   0x1e,  0x3,
        0x1e,  0x3,    0x1e,   0x3,    0x1e,   0x3,    0x1e,   0x5,    0x1e,   0x119, 0xa,   0x1e,  0x3,   0x1e,  0x3,   0x1e,
        0x3,   0x1e,   0x5,    0x1e,   0x11e,  0xa,    0x1e,   0x3,    0x1e,   0x3,   0x1e,  0x3,   0x1e,  0x3,   0x1e,  0x7,
        0x1e,  0x124,  0xa,    0x1e,   0xc,    0x1e,   0xe,    0x1e,   0x127,  0xb,   0x1e,  0x3,   0x1f,  0x3,   0x1f,  0x3,
        0x1f,  0x3,    0x1f,   0x3,    0x1f,   0x3,    0x1f,   0x3,    0x1f,   0x3,   0x1f,  0x3,   0x1f,  0x5,   0x1f,  0x132,
        0xa,   0x1f,   0x3,    0x1f,   0x3,    0x1f,   0x3,    0x1f,   0x3,    0x1f,  0x3,   0x1f,  0x3,   0x1f,  0x3,   0x1f,
        0x3,   0x1f,   0x7,    0x1f,   0x13c,  0xa,    0x1f,   0xc,    0x1f,   0xe,   0x1f,  0x13f, 0xb,   0x1f,  0x3,   0x20,
        0x3,   0x20,   0x3,    0x20,   0x3,    0x20,   0x3,    0x20,   0x3,    0x20,  0x3,   0x20,  0x3,   0x20,  0x3,   0x20,
        0x3,   0x20,   0x3,    0x20,   0x7,    0x20,   0x14c,  0xa,    0x20,   0xc,   0x20,  0xe,   0x20,  0x14f, 0xb,   0x20,
        0x3,   0x20,   0x3,    0x20,   0x3,    0x20,   0x5,    0x20,   0x154,  0xa,   0x20,  0x3,   0x20,  0x3,   0x20,  0x3,
        0x20,  0x3,    0x20,   0x3,    0x20,   0x3,    0x20,   0x3,    0x20,   0x3,   0x20,  0x7,   0x20,  0x15e, 0xa,   0x20,
        0xc,   0x20,   0xe,    0x20,   0x161,  0xb,    0x20,   0x3,    0x21,   0x3,   0x21,  0x3,   0x21,  0x3,   0x21,  0x3,
        0x21,  0x3,    0x21,   0x3,    0x21,   0x3,    0x21,   0x5,    0x21,   0x16b, 0xa,   0x21,  0x3,   0x21,  0x3,   0x21,
        0x3,   0x21,   0x5,    0x21,   0x170,  0xa,    0x21,   0x3,    0x22,   0x3,   0x22,  0x3,   0x22,  0x5,   0x22,  0x175,
        0xa,   0x22,   0x3,    0x22,   0x3,    0x22,   0x3,    0x23,   0x3,    0x23,  0x3,   0x23,  0x3,   0x23,  0x3,   0x23,
        0x3,   0x23,   0x3,    0x23,   0x5,    0x23,   0x180,  0xa,    0x23,   0x5,   0x23,  0x182, 0xa,   0x23,  0x3,   0x24,
        0x3,   0x24,   0x3,    0x24,   0x3,    0x24,   0x3,    0x24,   0x3,    0x24,  0x5,   0x24,  0x18a, 0xa,   0x24,  0x3,
        0x25,  0x3,    0x25,   0x3,    0x26,   0x3,    0x26,   0x3,    0x26,   0x3,   0x26,  0x3,   0x26,  0x3,   0x26,  0x5,
        0x26,  0x194,  0xa,    0x26,   0x3,    0x27,   0x3,    0x27,   0x3,    0x28,  0x3,   0x28,  0x3,   0x28,  0x3,   0x28,
        0x3,   0x28,   0x3,    0x28,   0x3,    0x28,   0x3,    0x28,   0x3,    0x29,  0x3,   0x29,  0x3,   0x2a,  0x3,   0x2a,
        0x3,   0x2a,   0x3,    0x2a,   0x3,    0x2a,   0x3,    0x2a,   0x3,    0x2a,  0x3,   0x2a,  0x3,   0x2a,  0x3,   0x2a,
        0x3,   0x2a,   0x3,    0x2a,   0x3,    0x2a,   0x3,    0x2a,   0x3,    0x2a,  0x3,   0x2a,  0x5,   0x2a,  0x1b2, 0xa,
        0x2a,  0x3,    0x2b,   0x3,    0x2b,   0x3,    0x2c,   0x3,    0x2c,   0x3,   0x2c,  0x3,   0x2c,  0x3,   0x2c,  0x3,
        0x2c,  0x3,    0x2c,   0x5,    0x2c,   0x1bd,  0xa,    0x2c,   0x3,    0x2d,  0x3,   0x2d,  0x3,   0x2d,  0x2,   0x5,
        0x3a,  0x3c,   0x3e,   0x2e,   0x2,    0x4,    0x6,    0x8,    0xa,    0xc,   0xe,   0x10,  0x12,  0x14,  0x16,  0x18,
        0x1a,  0x1c,   0x1e,   0x20,   0x22,   0x24,   0x26,   0x28,   0x2a,   0x2c,  0x2e,  0x30,  0x32,  0x34,  0x36,  0x38,
        0x3a,  0x3c,   0x3e,   0x40,   0x42,   0x44,   0x46,   0x48,   0x4a,   0x4c,  0x4e,  0x50,  0x52,  0x54,  0x56,  0x58,
        0x2,   0xa,    0x3,    0x2,    0x2d,   0x34,   0x4,    0x2,    0x13,   0x13,  0x4b,  0x4b,  0x3,   0x2,   0x38,  0x3f,
        0x3,   0x2,    0x2a,   0x2c,   0x3,    0x2,    0x2a,   0x2b,   0x5,    0x2,   0x3,   0x3,   0x19,  0x19,  0x20,  0x20,
        0x3,   0x2,    0x48,   0x4a,   0x5,    0x2,    0x3,    0x3,    0x8,    0xa,   0x1f,  0x20,  0x2,   0x1db, 0x2,   0x5b,
        0x3,   0x2,    0x2,    0x2,    0x4,    0x61,   0x3,    0x2,    0x2,    0x2,   0x6,   0x7a,  0x3,   0x2,   0x2,   0x2,
        0x8,   0x82,   0x3,    0x2,    0x2,    0x2,    0xa,    0x87,   0x3,    0x2,   0x2,   0x2,   0xc,   0x8b,  0x3,   0x2,
        0x2,   0x2,    0xe,    0x8d,   0x3,    0x2,    0x2,    0x2,    0x10,   0x91,  0x3,   0x2,   0x2,   0x2,   0x12,  0x94,
        0x3,   0x2,    0x2,    0x2,    0x14,   0x96,   0x3,    0x2,    0x2,    0x2,   0x16,  0x98,  0x3,   0x2,   0x2,   0x2,
        0x18,  0xa5,   0x3,    0x2,    0x2,    0x2,    0x1a,   0xa9,   0x3,    0x2,   0x2,   0x2,   0x1c,  0xb1,  0x3,   0x2,
        0x2,   0x2,    0x1e,   0xb5,   0x3,    0x2,    0x2,    0x2,    0x20,   0xc9,  0x3,   0x2,   0x2,   0x2,   0x22,  0xcb,
        0x3,   0x2,    0x2,    0x2,    0x24,   0xe3,   0x3,    0x2,    0x2,    0x2,   0x26,  0xe5,  0x3,   0x2,   0x2,   0x2,
        0x28,  0xe7,   0x3,    0x2,    0x2,    0x2,    0x2a,   0xea,   0x3,    0x2,   0x2,   0x2,   0x2c,  0xf1,  0x3,   0x2,
        0x2,   0x2,    0x2e,   0xf5,   0x3,    0x2,    0x2,    0x2,    0x30,   0xfa,  0x3,   0x2,   0x2,   0x2,   0x32,  0xfc,
        0x3,   0x2,    0x2,    0x2,    0x34,   0xff,   0x3,    0x2,    0x2,    0x2,   0x36,  0x108, 0x3,   0x2,   0x2,   0x2,
        0x38,  0x10a,  0x3,    0x2,    0x2,    0x2,    0x3a,   0x11d,  0x3,    0x2,   0x2,   0x2,   0x3c,  0x128, 0x3,   0x2,
        0x2,   0x2,    0x3e,   0x153,  0x3,    0x2,    0x2,    0x2,    0x40,   0x16f, 0x3,   0x2,   0x2,   0x2,   0x42,  0x171,
        0x3,   0x2,    0x2,    0x2,    0x44,   0x181,  0x3,    0x2,    0x2,    0x2,   0x46,  0x189, 0x3,   0x2,   0x2,   0x2,
        0x48,  0x18b,  0x3,    0x2,    0x2,    0x2,    0x4a,   0x193,  0x3,    0x2,   0x2,   0x2,   0x4c,  0x195, 0x3,   0x2,
        0x2,   0x2,    0x4e,   0x197,  0x3,    0x2,    0x2,    0x2,    0x50,   0x19f, 0x3,   0x2,   0x2,   0x2,   0x52,  0x1b1,
        0x3,   0x2,    0x2,    0x2,    0x54,   0x1b3,  0x3,    0x2,    0x2,    0x2,   0x56,  0x1bc, 0x3,   0x2,   0x2,   0x2,
        0x58,  0x1be,  0x3,    0x2,    0x2,    0x2,    0x5a,   0x5c,   0x5,    0x4,   0x3,   0x2,   0x5b,  0x5a,  0x3,   0x2,
        0x2,   0x2,    0x5c,   0x5d,   0x3,    0x2,    0x2,    0x2,    0x5d,   0x5b,  0x3,   0x2,   0x2,   0x2,   0x5d,  0x5e,
        0x3,   0x2,    0x2,    0x2,    0x5e,   0x5f,   0x3,    0x2,    0x2,    0x2,   0x5f,  0x60,  0x7,   0x2,   0x2,   0x3,
        0x60,  0x3,    0x3,    0x2,    0x2,    0x2,    0x61,   0x62,   0x7,    0xd,   0x2,   0x2,   0x62,  0x63,  0x7,   0x4d,
        0x2,   0x2,    0x63,   0x64,   0x7,    0x15,   0x2,    0x2,    0x64,   0x65,  0x5,   0xa,   0x6,   0x2,   0x65,  0x66,
        0x7,   0xc,    0x2,    0x2,    0x66,   0x69,   0x5,    0x6,    0x4,    0x2,   0x67,  0x68,  0x7,   0xe,   0x2,   0x2,
        0x68,  0x6a,   0x5,    0xc,    0x7,    0x2,    0x69,   0x67,   0x3,    0x2,   0x2,   0x2,   0x69,  0x6a,  0x3,   0x2,
        0x2,   0x2,    0x6a,   0x6d,   0x3,    0x2,    0x2,    0x2,    0x6b,   0x6c,  0x7,   0xf,   0x2,   0x2,   0x6c,  0x6e,
        0x5,   0xe,    0x8,    0x2,    0x6d,   0x6b,   0x3,    0x2,    0x2,    0x2,   0x6d,  0x6e,  0x3,   0x2,   0x2,   0x2,
        0x6e,  0x71,   0x3,    0x2,    0x2,    0x2,    0x6f,   0x70,   0x7,    0x10,  0x2,   0x2,   0x70,  0x72,  0x5,   0x14,
        0xb,   0x2,    0x71,   0x6f,   0x3,    0x2,    0x2,    0x2,    0x71,   0x72,  0x3,   0x2,   0x2,   0x2,   0x72,  0x75,
        0x3,   0x2,    0x2,    0x2,    0x73,   0x74,   0x7,    0x11,   0x2,    0x2,   0x74,  0x76,  0x5,   0x16,  0xc,   0x2,
        0x75,  0x73,   0x3,    0x2,    0x2,    0x2,    0x75,   0x76,   0x3,    0x2,   0x2,   0x2,   0x76,  0x77,  0x3,   0x2,
        0x2,   0x2,    0x77,   0x78,   0x7,    0x12,   0x2,    0x2,    0x78,   0x79,  0x5,   0x1a,  0xe,   0x2,   0x79,  0x5,
        0x3,   0x2,    0x2,    0x2,    0x7a,   0x7f,   0x5,    0x8,    0x5,    0x2,   0x7b,  0x7c,  0x7,   0x16,  0x2,   0x2,
        0x7c,  0x7e,   0x5,    0x8,    0x5,    0x2,    0x7d,   0x7b,   0x3,    0x2,   0x2,   0x2,   0x7e,  0x81,  0x3,   0x2,
        0x2,   0x2,    0x7f,   0x7d,   0x3,    0x2,    0x2,    0x2,    0x7f,   0x80,  0x3,   0x2,   0x2,   0x2,   0x80,  0x7,
        0x3,   0x2,    0x2,    0x2,    0x81,   0x7f,   0x3,    0x2,    0x2,    0x2,   0x82,  0x85,  0x7,   0x4d,  0x2,   0x2,
        0x83,  0x84,   0x7,    0x35,   0x2,    0x2,    0x84,   0x86,   0x7,    0x4d,  0x2,   0x2,   0x85,  0x83,  0x3,   0x2,
        0x2,   0x2,    0x85,   0x86,   0x3,    0x2,    0x2,    0x2,    0x86,   0x9,   0x3,   0x2,   0x2,   0x2,   0x87,  0x88,
        0x7,   0x17,   0x2,    0x2,    0x88,   0x89,   0x5,    0x1e,   0x10,   0x2,   0x89,  0x8a,  0x7,   0x18,  0x2,   0x2,
        0x8a,  0xb,    0x3,    0x2,    0x2,    0x2,    0x8b,   0x8c,   0x5,    0x3a,  0x1e,  0x2,   0x8c,  0xd,   0x3,   0x2,
        0x2,   0x2,    0x8d,   0x8e,   0x7,    0x22,   0x2,    0x2,    0x8e,   0x8f,  0x5,   0x10,  0x9,   0x2,   0x8f,  0x90,
        0x7,   0x23,   0x2,    0x2,    0x90,   0xf,    0x3,    0x2,    0x2,    0x2,   0x91,  0x92,  0x7,   0x4c,  0x2,   0x2,
        0x92,  0x93,   0x5,    0x12,   0xa,    0x2,    0x93,   0x11,   0x3,    0x2,   0x2,   0x2,   0x94,  0x95,  0x9,   0x2,
        0x2,   0x2,    0x95,   0x13,   0x3,    0x2,    0x2,    0x2,    0x96,   0x97,  0x9,   0x3,   0x2,   0x2,   0x97,  0x15,
        0x3,   0x2,    0x2,    0x2,    0x98,   0x99,   0x7,    0x4d,   0x2,    0x2,   0x99,  0x9a,  0x7,   0x15,  0x2,   0x2,
        0x9a,  0x9b,   0x7,    0x22,   0x2,    0x2,    0x9b,   0xa0,   0x5,    0x18,  0xd,   0x2,   0x9c,  0x9d,  0x7,   0x16,
        0x2,   0x2,    0x9d,   0x9f,   0x5,    0x18,   0xd,    0x2,    0x9e,   0x9c,  0x3,   0x2,   0x2,   0x2,   0x9f,  0xa2,
        0x3,   0x2,    0x2,    0x2,    0xa0,   0x9e,   0x3,    0x2,    0x2,    0x2,   0xa0,  0xa1,  0x3,   0x2,   0x2,   0x2,
        0xa1,  0xa3,   0x3,    0x2,    0x2,    0x2,    0xa2,   0xa0,   0x3,    0x2,   0x2,   0x2,   0xa3,  0xa4,  0x7,   0x23,
        0x2,   0x2,    0xa4,   0x17,   0x3,    0x2,    0x2,    0x2,    0xa5,   0xa6,  0x7,   0x4d,  0x2,   0x2,   0xa6,  0xa7,
        0x7,   0x36,   0x2,    0x2,    0xa7,   0xa8,   0x5,    0x4a,   0x26,   0x2,   0xa8,  0x19,  0x3,   0x2,   0x2,   0x2,
        0xa9,  0xae,   0x5,    0x1c,   0xf,    0x2,    0xaa,   0xab,   0x7,    0x16,  0x2,   0x2,   0xab,  0xad,  0x5,   0x1c,
        0xf,   0x2,    0xac,   0xaa,   0x3,    0x2,    0x2,    0x2,    0xad,   0xb0,  0x3,   0x2,   0x2,   0x2,   0xae,  0xac,
        0x3,   0x2,    0x2,    0x2,    0xae,   0xaf,   0x3,    0x2,    0x2,    0x2,   0xaf,  0x1b,  0x3,   0x2,   0x2,   0x2,
        0xb0,  0xae,   0x3,    0x2,    0x2,    0x2,    0xb1,   0xb2,   0x5,    0x32,  0x1a,  0x2,   0xb2,  0xb3,  0x7,   0x37,
        0x2,   0x2,    0xb3,   0xb4,   0x7,    0x4d,   0x2,    0x2,    0xb4,   0x1d,  0x3,   0x2,   0x2,   0x2,   0xb5,  0xbb,
        0x5,   0x20,   0x11,   0x2,    0xb6,   0xb7,   0x5,    0x2c,   0x17,   0x2,   0xb7,  0xb8,  0x5,   0x20,  0x11,  0x2,
        0xb8,  0xba,   0x3,    0x2,    0x2,    0x2,    0xb9,   0xb6,   0x3,    0x2,   0x2,   0x2,   0xba,  0xbd,  0x3,   0x2,
        0x2,   0x2,    0xbb,   0xb9,   0x3,    0x2,    0x2,    0x2,    0xbb,   0xbc,  0x3,   0x2,   0x2,   0x2,   0xbc,  0x1f,
        0x3,   0x2,    0x2,    0x2,    0xbd,   0xbb,   0x3,    0x2,    0x2,    0x2,   0xbe,  0xc0,  0x7,   0x19,  0x2,   0x2,
        0xbf,  0xbe,   0x3,    0x2,    0x2,    0x2,    0xbf,   0xc0,   0x3,    0x2,   0x2,   0x2,   0xc0,  0xc1,  0x3,   0x2,
        0x2,   0x2,    0xc1,   0xca,   0x5,    0x22,   0x12,   0x2,    0xc2,   0xc4,  0x7,   0x19,  0x2,   0x2,   0xc3,  0xc2,
        0x3,   0x2,    0x2,    0x2,    0xc3,   0xc4,   0x3,    0x2,    0x2,    0x2,   0xc4,  0xc5,  0x3,   0x2,   0x2,   0x2,
        0xc5,  0xc6,   0x7,    0x17,   0x2,    0x2,    0xc6,   0xc7,   0x5,    0x1e,  0x10,  0x2,   0xc7,  0xc8,  0x7,   0x18,
        0x2,   0x2,    0xc8,   0xca,   0x3,    0x2,    0x2,    0x2,    0xc9,   0xbf,  0x3,   0x2,   0x2,   0x2,   0xc9,  0xc3,
        0x3,   0x2,    0x2,    0x2,    0xca,   0x21,   0x3,    0x2,    0x2,    0x2,   0xcb,  0xcd,  0x7,   0x4d,  0x2,   0x2,
        0xcc,  0xce,   0x5,    0x24,   0x13,   0x2,    0xcd,   0xcc,   0x3,    0x2,   0x2,   0x2,   0xcd,  0xce,  0x3,   0x2,
        0x2,   0x2,    0xce,   0x23,   0x3,    0x2,    0x2,    0x2,    0xcf,   0xe4,  0x7,   0x1f,  0x2,   0x2,   0xd0,  0xe4,
        0x7,   0x20,   0x2,    0x2,    0xd1,   0xd3,   0x7,    0x22,   0x2,    0x2,   0xd2,  0xd4,  0x5,   0x2a,  0x16,  0x2,
        0xd3,  0xd2,   0x3,    0x2,    0x2,    0x2,    0xd3,   0xd4,   0x3,    0x2,   0x2,   0x2,   0xd4,  0xd5,  0x3,   0x2,
        0x2,   0x2,    0xd5,   0xd7,   0x7,    0x4c,   0x2,    0x2,    0xd6,   0xd8,  0x7,   0x20,  0x2,   0x2,   0xd7,  0xd6,
        0x3,   0x2,    0x2,    0x2,    0xd7,   0xd8,   0x3,    0x2,    0x2,    0x2,   0xd8,  0xd9,  0x3,   0x2,   0x2,   0x2,
        0xd9,  0xe4,   0x7,    0x23,   0x2,    0x2,    0xda,   0xdc,   0x7,    0x22,  0x2,   0x2,   0xdb,  0xdd,  0x5,   0x2a,
        0x16,  0x2,    0xdc,   0xdb,   0x3,    0x2,    0x2,    0x2,    0xdc,   0xdd,  0x3,   0x2,   0x2,   0x2,   0xdd,  0xde,
        0x3,   0x2,    0x2,    0x2,    0xde,   0xdf,   0x5,    0x28,   0x15,   0x2,   0xdf,  0xe0,  0x7,   0x21,  0x2,   0x2,
        0xe0,  0xe1,   0x5,    0x26,   0x14,   0x2,    0xe1,   0xe2,   0x7,    0x23,  0x2,   0x2,   0xe2,  0xe4,  0x3,   0x2,
        0x2,   0x2,    0xe3,   0xcf,   0x3,    0x2,    0x2,    0x2,    0xe3,   0xd0,  0x3,   0x2,   0x2,   0x2,   0xe3,  0xd1,
        0x3,   0x2,    0x2,    0x2,    0xe3,   0xda,   0x3,    0x2,    0x2,    0x2,   0xe4,  0x25,  0x3,   0x2,   0x2,   0x2,
        0xe5,  0xe6,   0x7,    0x4c,   0x2,    0x2,    0xe6,   0x27,   0x3,    0x2,   0x2,   0x2,   0xe7,  0xe8,  0x7,   0x4c,
        0x2,   0x2,    0xe8,   0x29,   0x3,    0x2,    0x2,    0x2,    0xe9,   0xeb,  0x7,   0x14,  0x2,   0x2,   0xea,  0xe9,
        0x3,   0x2,    0x2,    0x2,    0xea,   0xeb,   0x3,    0x2,    0x2,    0x2,   0xeb,  0xec,  0x3,   0x2,   0x2,   0x2,
        0xec,  0xed,   0x7,    0x1c,   0x2,    0x2,    0xed,   0x2b,   0x3,    0x2,   0x2,   0x2,   0xee,  0xf2,  0x7,   0x1d,
        0x2,   0x2,    0xef,   0xf2,   0x7,    0x1e,   0x2,    0x2,    0xf0,   0xf2,  0x5,   0x2e,  0x18,  0x2,   0xf1,  0xee,
        0x3,   0x2,    0x2,    0x2,    0xf1,   0xef,   0x3,    0x2,    0x2,    0x2,   0xf1,  0xf0,  0x3,   0x2,   0x2,   0x2,
        0xf2,  0x2d,   0x3,    0x2,    0x2,    0x2,    0xf3,   0xf6,   0x7,    0x1b,  0x2,   0x2,   0xf4,  0xf6,  0x5,   0x30,
        0x19,  0x2,    0xf5,   0xf3,   0x3,    0x2,    0x2,    0x2,    0xf5,   0xf4,  0x3,   0x2,   0x2,   0x2,   0xf6,  0x2f,
        0x3,   0x2,    0x2,    0x2,    0xf7,   0xfb,   0x7,    0x1c,   0x2,    0x2,   0xf8,  0xf9,  0x7,   0x14,  0x2,   0x2,
        0xf9,  0xfb,   0x7,    0x1c,   0x2,    0x2,    0xfa,   0xf7,   0x3,    0x2,   0x2,   0x2,   0xfa,  0xf8,  0x3,   0x2,
        0x2,   0x2,    0xfb,   0x31,   0x3,    0x2,    0x2,    0x2,    0xfc,   0xfd,  0x9,   0x4,   0x2,   0x2,   0xfd,  0x33,
        0x3,   0x2,    0x2,    0x2,    0xfe,   0x100,  0x7,    0x19,   0x2,    0x2,   0xff,  0xfe,  0x3,   0x2,   0x2,   0x2,
        0xff,  0x100,  0x3,    0x2,    0x2,    0x2,    0x100,  0x101,  0x3,    0x2,   0x2,   0x2,   0x101, 0x102, 0x7,   0x27,
        0x2,   0x2,    0x102,  0x35,   0x3,    0x2,    0x2,    0x2,    0x103,  0x104, 0x7,   0x41,  0x2,   0x2,   0x104, 0x105,
        0x7,   0x4d,   0x2,    0x2,    0x105,  0x109,  0x7,    0x41,   0x2,    0x2,   0x106, 0x109, 0x7,   0x4c,  0x2,   0x2,
        0x107, 0x109,  0x7,    0x4d,   0x2,    0x2,    0x108,  0x103,  0x3,    0x2,   0x2,   0x2,   0x108, 0x106, 0x3,   0x2,
        0x2,   0x2,    0x108,  0x107,  0x3,    0x2,    0x2,    0x2,    0x109,  0x37,  0x3,   0x2,   0x2,   0x2,   0x10a, 0x10f,
        0x5,   0x3a,   0x1e,   0x2,    0x10b,  0x10c,  0x7,    0x16,   0x2,    0x2,   0x10c, 0x10e, 0x5,   0x3a,  0x1e,  0x2,
        0x10d, 0x10b,  0x3,    0x2,    0x2,    0x2,    0x10e,  0x111,  0x3,    0x2,   0x2,   0x2,   0x10f, 0x10d, 0x3,   0x2,
        0x2,   0x2,    0x10f,  0x110,  0x3,    0x2,    0x2,    0x2,    0x110,  0x39,  0x3,   0x2,   0x2,   0x2,   0x111, 0x10f,
        0x3,   0x2,    0x2,    0x2,    0x112,  0x113,  0x8,    0x1e,   0x1,    0x2,   0x113, 0x114, 0x7,   0x1a,  0x2,   0x2,
        0x114, 0x11e,  0x5,    0x3a,   0x1e,   0x6,    0x115,  0x116,  0x5,    0x3c,  0x1f,  0x2,   0x116, 0x118, 0x7,   0x26,
        0x2,   0x2,    0x117,  0x119,  0x7,    0x19,   0x2,    0x2,    0x118,  0x117, 0x3,   0x2,   0x2,   0x2,   0x118, 0x119,
        0x3,   0x2,    0x2,    0x2,    0x119,  0x11a,  0x3,    0x2,    0x2,    0x2,   0x11a, 0x11b, 0x9,   0x5,   0x2,   0x2,
        0x11b, 0x11e,  0x3,    0x2,    0x2,    0x2,    0x11c,  0x11e,  0x5,    0x3c,  0x1f,  0x2,   0x11d, 0x112, 0x3,   0x2,
        0x2,   0x2,    0x11d,  0x115,  0x3,    0x2,    0x2,    0x2,    0x11d,  0x11c, 0x3,   0x2,   0x2,   0x2,   0x11e, 0x125,
        0x3,   0x2,    0x2,    0x2,    0x11f,  0x120,  0xc,    0x5,    0x2,    0x2,   0x120, 0x121, 0x5,   0x54,  0x2b,  0x2,
        0x121, 0x122,  0x5,    0x3a,   0x1e,   0x6,    0x122,  0x124,  0x3,    0x2,   0x2,   0x2,   0x123, 0x11f, 0x3,   0x2,
        0x2,   0x2,    0x124,  0x127,  0x3,    0x2,    0x2,    0x2,    0x125,  0x123, 0x3,   0x2,   0x2,   0x2,   0x125, 0x126,
        0x3,   0x2,    0x2,    0x2,    0x126,  0x3b,   0x3,    0x2,    0x2,    0x2,   0x127, 0x125, 0x3,   0x2,   0x2,   0x2,
        0x128, 0x129,  0x8,    0x1f,   0x1,    0x2,    0x129,  0x12a,  0x5,    0x3e,  0x20,  0x2,   0x12a, 0x13d, 0x3,   0x2,
        0x2,   0x2,    0x12b,  0x12c,  0xc,    0x4,    0x2,    0x2,    0x12c,  0x12d, 0x5,   0x52,  0x2a,  0x2,   0x12d, 0x12e,
        0x5,   0x3c,   0x1f,   0x5,    0x12e,  0x13c,  0x3,    0x2,    0x2,    0x2,   0x12f, 0x131, 0xc,   0x6,   0x2,   0x2,
        0x130, 0x132,  0x7,    0x19,   0x2,    0x2,    0x131,  0x130,  0x3,    0x2,   0x2,   0x2,   0x131, 0x132, 0x3,   0x2,
        0x2,   0x2,    0x132,  0x133,  0x3,    0x2,    0x2,    0x2,    0x133,  0x134, 0x7,   0x25,  0x2,   0x2,   0x134, 0x135,
        0x7,   0x17,   0x2,    0x2,    0x135,  0x136,  0x5,    0x38,   0x1d,   0x2,   0x136, 0x137, 0x7,   0x18,  0x2,   0x2,
        0x137, 0x13c,  0x3,    0x2,    0x2,    0x2,    0x138,  0x139,  0xc,    0x5,   0x2,   0x2,   0x139, 0x13a, 0x7,   0x26,
        0x2,   0x2,    0x13a,  0x13c,  0x5,    0x34,   0x1b,   0x2,    0x13b,  0x12b, 0x3,   0x2,   0x2,   0x2,   0x13b, 0x12f,
        0x3,   0x2,    0x2,    0x2,    0x13b,  0x138,  0x3,    0x2,    0x2,    0x2,   0x13c, 0x13f, 0x3,   0x2,   0x2,   0x2,
        0x13d, 0x13b,  0x3,    0x2,    0x2,    0x2,    0x13d,  0x13e,  0x3,    0x2,   0x2,   0x2,   0x13e, 0x3d,  0x3,   0x2,
        0x2,   0x2,    0x13f,  0x13d,  0x3,    0x2,    0x2,    0x2,    0x140,  0x141, 0x8,   0x20,  0x1,   0x2,   0x141, 0x154,
        0x5,   0x40,   0x21,   0x2,    0x142,  0x143,  0x5,    0x50,   0x29,   0x2,   0x143, 0x144, 0x5,   0x3e,  0x20,  0x8,
        0x144, 0x154,  0x3,    0x2,    0x2,    0x2,    0x145,  0x146,  0x7,    0x29,  0x2,   0x2,   0x146, 0x154, 0x5,   0x3e,
        0x20,  0x7,    0x147,  0x148,  0x7,    0x17,   0x2,    0x2,    0x148,  0x14d, 0x5,   0x3a,  0x1e,  0x2,   0x149, 0x14a,
        0x7,   0x16,   0x2,    0x2,    0x14a,  0x14c,  0x5,    0x3a,   0x1e,   0x2,   0x14b, 0x149, 0x3,   0x2,   0x2,   0x2,
        0x14c, 0x14f,  0x3,    0x2,    0x2,    0x2,    0x14d,  0x14b,  0x3,    0x2,   0x2,   0x2,   0x14d, 0x14e, 0x3,   0x2,
        0x2,   0x2,    0x14e,  0x150,  0x3,    0x2,    0x2,    0x2,    0x14f,  0x14d, 0x3,   0x2,   0x2,   0x2,   0x150, 0x151,
        0x7,   0x18,   0x2,    0x2,    0x151,  0x154,  0x3,    0x2,    0x2,    0x2,   0x152, 0x154, 0x5,   0x36,  0x1c,  0x2,
        0x153, 0x140,  0x3,    0x2,    0x2,    0x2,    0x153,  0x142,  0x3,    0x2,   0x2,   0x2,   0x153, 0x145, 0x3,   0x2,
        0x2,   0x2,    0x153,  0x147,  0x3,    0x2,    0x2,    0x2,    0x153,  0x152, 0x3,   0x2,   0x2,   0x2,   0x154, 0x15f,
        0x3,   0x2,    0x2,    0x2,    0x155,  0x156,  0xc,    0x5,    0x2,    0x2,   0x156, 0x157, 0x5,   0x56,  0x2c,  0x2,
        0x157, 0x158,  0x5,    0x3e,   0x20,   0x6,    0x158,  0x15e,  0x3,    0x2,   0x2,   0x2,   0x159, 0x15a, 0xc,   0x4,
        0x2,   0x2,    0x15a,  0x15b,  0x5,    0x58,   0x2d,   0x2,    0x15b,  0x15c, 0x5,   0x3e,  0x20,  0x5,   0x15c, 0x15e,
        0x3,   0x2,    0x2,    0x2,    0x15d,  0x155,  0x3,    0x2,    0x2,    0x2,   0x15d, 0x159, 0x3,   0x2,   0x2,   0x2,
        0x15e, 0x161,  0x3,    0x2,    0x2,    0x2,    0x15f,  0x15d,  0x3,    0x2,   0x2,   0x2,   0x15f, 0x160, 0x3,   0x2,
        0x2,   0x2,    0x160,  0x3f,   0x3,    0x2,    0x2,    0x2,    0x161,  0x15f, 0x3,   0x2,   0x2,   0x2,   0x162, 0x163,
        0x5,   0x46,   0x24,   0x2,    0x163,  0x164,  0x7,    0x17,   0x2,    0x2,   0x164, 0x165, 0x5,   0x38,  0x1d,  0x2,
        0x165, 0x166,  0x7,    0x18,   0x2,    0x2,    0x166,  0x170,  0x3,    0x2,   0x2,   0x2,   0x167, 0x16a, 0x5,   0x42,
        0x22,  0x2,    0x168,  0x169,  0x7,    0x40,   0x2,    0x2,    0x169,  0x16b, 0x5,   0x48,  0x25,  0x2,   0x16a, 0x168,
        0x3,   0x2,    0x2,    0x2,    0x16a,  0x16b,  0x3,    0x2,    0x2,    0x2,   0x16b, 0x170, 0x3,   0x2,   0x2,   0x2,
        0x16c, 0x16d,  0x7,    0x4d,   0x2,    0x2,    0x16d,  0x16e,  0x7,    0x40,  0x2,   0x2,   0x16e, 0x170, 0x5,   0x48,
        0x25,  0x2,    0x16f,  0x162,  0x3,    0x2,    0x2,    0x2,    0x16f,  0x167, 0x3,   0x2,   0x2,   0x2,   0x16f, 0x16c,
        0x3,   0x2,    0x2,    0x2,    0x170,  0x41,   0x3,    0x2,    0x2,    0x2,   0x171, 0x172, 0x7,   0x4d,  0x2,   0x2,
        0x172, 0x174,  0x7,    0x22,   0x2,    0x2,    0x173,  0x175,  0x5,    0x44,  0x23,  0x2,   0x174, 0x173, 0x3,   0x2,
        0x2,   0x2,    0x174,  0x175,  0x3,    0x2,    0x2,    0x2,    0x175,  0x176, 0x3,   0x2,   0x2,   0x2,   0x176, 0x177,
        0x7,   0x23,   0x2,    0x2,    0x177,  0x43,   0x3,    0x2,    0x2,    0x2,   0x178, 0x179, 0x5,   0x3e,  0x20,  0x2,
        0x179, 0x17a,  0x5,    0x58,   0x2d,   0x2,    0x17a,  0x17b,  0x5,    0x3e,  0x20,  0x2,   0x17b, 0x182, 0x3,   0x2,
        0x2,   0x2,    0x17c,  0x17f,  0x5,    0x36,   0x1c,   0x2,    0x17d,  0x17e, 0x7,   0x21,  0x2,   0x2,   0x17e, 0x180,
        0x5,   0x36,   0x1c,   0x2,    0x17f,  0x17d,  0x3,    0x2,    0x2,    0x2,   0x17f, 0x180, 0x3,   0x2,   0x2,   0x2,
        0x180, 0x182,  0x3,    0x2,    0x2,    0x2,    0x181,  0x178,  0x3,    0x2,   0x2,   0x2,   0x181, 0x17c, 0x3,   0x2,
        0x2,   0x2,    0x182,  0x45,   0x3,    0x2,    0x2,    0x2,    0x183,  0x18a, 0x3,   0x2,   0x2,   0x2,   0x184, 0x18a,
        0x7,   0x42,   0x2,    0x2,    0x185,  0x18a,  0x7,    0x43,   0x2,    0x2,   0x186, 0x18a, 0x7,   0x44,  0x2,   0x2,
        0x187, 0x18a,  0x7,    0x45,   0x2,    0x2,    0x188,  0x18a,  0x7,    0x46,  0x2,   0x2,   0x189, 0x183, 0x3,   0x2,
        0x2,   0x2,    0x189,  0x184,  0x3,    0x2,    0x2,    0x2,    0x189,  0x185, 0x3,   0x2,   0x2,   0x2,   0x189, 0x186,
        0x3,   0x2,    0x2,    0x2,    0x189,  0x187,  0x3,    0x2,    0x2,    0x2,   0x189, 0x188, 0x3,   0x2,   0x2,   0x2,
        0x18a, 0x47,   0x3,    0x2,    0x2,    0x2,    0x18b,  0x18c,  0x7,    0x4d,  0x2,   0x2,   0x18c, 0x49,  0x3,   0x2,
        0x2,   0x2,    0x18d,  0x18e,  0x7,    0x47,   0x2,    0x2,    0x18e,  0x194, 0x5,   0x4e,  0x28,  0x2,   0x18f, 0x194,
        0x5,   0x40,   0x21,   0x2,    0x190,  0x194,  0x5,    0x22,   0x12,   0x2,   0x191, 0x194, 0x5,   0x3a,  0x1e,  0x2,
        0x192, 0x194,  0x5,    0x4c,   0x27,   0x2,    0x193,  0x18d,  0x3,    0x2,   0x2,   0x2,   0x193, 0x18f, 0x3,   0x2,
        0x2,   0x2,    0x193,  0x190,  0x3,    0x2,    0x2,    0x2,    0x193,  0x191, 0x3,   0x2,   0x2,   0x2,   0x193, 0x192,
        0x3,   0x2,    0x2,    0x2,    0x194,  0x4b,   0x3,    0x2,    0x2,    0x2,   0x195, 0x196, 0x9,   0x6,   0x2,   0x2,
        0x196, 0x4d,   0x3,    0x2,    0x2,    0x2,    0x197,  0x198,  0x7,    0x17,  0x2,   0x2,   0x198, 0x199, 0x5,   0x3a,
        0x1e,  0x2,    0x199,  0x19a,  0x7,    0x16,   0x2,    0x2,    0x19a,  0x19b, 0x5,   0x4a,  0x26,  0x2,   0x19b, 0x19c,
        0x7,   0x16,   0x2,    0x2,    0x19c,  0x19d,  0x5,    0x4a,   0x26,   0x2,   0x19d, 0x19e, 0x7,   0x18,  0x2,   0x2,
        0x19e, 0x4f,   0x3,    0x2,    0x2,    0x2,    0x19f,  0x1a0,  0x9,    0x7,   0x2,   0x2,   0x1a0, 0x51,  0x3,   0x2,
        0x2,   0x2,    0x1a1,  0x1a2,  0x7,    0x36,   0x2,    0x2,    0x1a2,  0x1b2, 0x7,   0x36,  0x2,   0x2,   0x1a3, 0x1b2,
        0x7,   0x4,    0x2,    0x2,    0x1a4,  0x1b2,  0x7,    0x5,    0x2,    0x2,   0x1a5, 0x1a6, 0x7,   0x5,   0x2,   0x2,
        0x1a6, 0x1b2,  0x7,    0x36,   0x2,    0x2,    0x1a7,  0x1a8,  0x7,    0x4,   0x2,   0x2,   0x1a8, 0x1b2, 0x7,   0x36,
        0x2,   0x2,    0x1a9,  0x1aa,  0x7,    0x5,    0x2,    0x2,    0x1aa,  0x1b2, 0x7,   0x4,   0x2,   0x2,   0x1ab, 0x1ac,
        0x7,   0x1a,   0x2,    0x2,    0x1ac,  0x1b2,  0x7,    0x36,   0x2,    0x2,   0x1ad, 0x1ae, 0x7,   0x5,   0x2,   0x2,
        0x1ae, 0x1af,  0x7,    0x36,   0x2,    0x2,    0x1af,  0x1b2,  0x7,    0x4,   0x2,   0x2,   0x1b0, 0x1b2, 0x7,   0x36,
        0x2,   0x2,    0x1b1,  0x1a1,  0x3,    0x2,    0x2,    0x2,    0x1b1,  0x1a3, 0x3,   0x2,   0x2,   0x2,   0x1b1, 0x1a4,
        0x3,   0x2,    0x2,    0x2,    0x1b1,  0x1a5,  0x3,    0x2,    0x2,    0x2,   0x1b1, 0x1a7, 0x3,   0x2,   0x2,   0x2,
        0x1b1, 0x1a9,  0x3,    0x2,    0x2,    0x2,    0x1b1,  0x1ab,  0x3,    0x2,   0x2,   0x2,   0x1b1, 0x1ad, 0x3,   0x2,
        0x2,   0x2,    0x1b1,  0x1b0,  0x3,    0x2,    0x2,    0x2,    0x1b2,  0x53,  0x3,   0x2,   0x2,   0x2,   0x1b3, 0x1b4,
        0x9,   0x8,    0x2,    0x2,    0x1b4,  0x55,   0x3,    0x2,    0x2,    0x2,   0x1b5, 0x1b6, 0x7,   0x5,   0x2,   0x2,
        0x1b6, 0x1bd,  0x7,    0x5,    0x2,    0x2,    0x1b7,  0x1b8,  0x7,    0x4,   0x2,   0x2,   0x1b8, 0x1bd, 0x7,   0x4,
        0x2,   0x2,    0x1b9,  0x1bd,  0x7,    0x6,    0x2,    0x2,    0x1ba,  0x1bd, 0x7,   0x4a,  0x2,   0x2,   0x1bb, 0x1bd,
        0x7,   0x7,    0x2,    0x2,    0x1bc,  0x1b5,  0x3,    0x2,    0x2,    0x2,   0x1bc, 0x1b7, 0x3,   0x2,   0x2,   0x2,
        0x1bc, 0x1b9,  0x3,    0x2,    0x2,    0x2,    0x1bc,  0x1ba,  0x3,    0x2,   0x2,   0x2,   0x1bc, 0x1bb, 0x3,   0x2,
        0x2,   0x2,    0x1bd,  0x57,   0x3,    0x2,    0x2,    0x2,    0x1be,  0x1bf, 0x9,   0x9,   0x2,   0x2,   0x1bf, 0x59,
        0x3,   0x2,    0x2,    0x2,    0x2e,   0x5d,   0x69,   0x6d,   0x71,   0x75,  0x7f,  0x85,  0xa0,  0xae,  0xbb,  0xbf,
        0xc3,  0xc9,   0xcd,   0xd3,   0xd7,   0xdc,   0xe3,   0xea,   0xf1,   0xf5,  0xfa,  0xff,  0x108, 0x10f, 0x118, 0x11d,
        0x125, 0x131,  0x13b,  0x13d,  0x14d,  0x153,  0x15d,  0x15f,  0x16a,  0x16f, 0x174, 0x17f, 0x181, 0x189, 0x193, 0x1b1,
        0x1bc,
    };

    _serializedATN.insert(_serializedATN.end(),
                          serializedATNSegment0,
                          serializedATNSegment0 + sizeof(serializedATNSegment0) / sizeof(serializedATNSegment0[0]));

    atn::ATNDeserializer deserializer;
    _atn = deserializer.deserialize(_serializedATN);

    size_t count = _atn.getNumberOfDecisions();
    _decisionToDFA.reserve(count);
    for (size_t i = 0; i < count; i++) {
        _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
    }
}

NesCEPParser::Initializer NesCEPParser::_init;
