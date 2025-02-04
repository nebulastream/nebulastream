
// Generated from CLionProjects/nebulastream/nes-coordinator/src/Parsers/NebulaPSL/gen/NesCEP.g4 by ANTLR 4.9.2

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
        setState(109);
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
            setState(108);
            cepPattern();
            setState(111);
            _errHandler->sync(this);
            _la = _input->LA(1);
        } while (_la == NesCEPParser::PATTERN);
        setState(113);
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

NesCEPParser::WindowConstraintsContext* NesCEPParser::CepPatternContext::windowConstraints() {
    return getRuleContext<NesCEPParser::WindowConstraintsContext>(0);
}

tree::TerminalNode* NesCEPParser::CepPatternContext::CONSUMING() { return getToken(NesCEPParser::CONSUMING, 0); }

NesCEPParser::OptionContext* NesCEPParser::CepPatternContext::option() { return getRuleContext<NesCEPParser::OptionContext>(0); }

tree::TerminalNode* NesCEPParser::CepPatternContext::SELECT() { return getToken(NesCEPParser::SELECT, 0); }

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
        setState(115);
        match(NesCEPParser::PATTERN);
        setState(116);
        match(NesCEPParser::NAME);
        setState(117);
        match(NesCEPParser::SEP);
        setState(118);
        compositeEventExpressions();
        setState(119);
        match(NesCEPParser::FROM);
        setState(120);
        inputStreams();
        setState(123);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::WHERE) {
            setState(121);
            match(NesCEPParser::WHERE);
            setState(122);
            whereExp();
        }
        setState(127);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::WITHIN) {
            setState(125);
            match(NesCEPParser::WITHIN);
            setState(126);
            windowConstraints();
        }
        setState(131);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::CONSUMING) {
            setState(129);
            match(NesCEPParser::CONSUMING);
            setState(130);
            option();
        }
        setState(135);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::SELECT) {
            setState(133);
            match(NesCEPParser::SELECT);
            setState(134);
            outputExpression();
        }
        setState(137);
        match(NesCEPParser::INTO);
        setState(138);
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
        setState(140);
        inputStream();
        setState(145);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NesCEPParser::COMMA) {
            setState(141);
            match(NesCEPParser::COMMA);
            setState(142);
            inputStream();
            setState(147);
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
        setState(148);
        match(NesCEPParser::NAME);
        setState(151);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::AS) {
            setState(149);
            match(NesCEPParser::AS);
            setState(150);
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

NesCEPParser::ListEventsContext* NesCEPParser::CompositeEventExpressionsContext::listEvents() {
    return getRuleContext<NesCEPParser::ListEventsContext>(0);
}

tree::TerminalNode* NesCEPParser::CompositeEventExpressionsContext::LPARENTHESIS() {
    return getToken(NesCEPParser::LPARENTHESIS, 0);
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
        setState(154);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 7, _ctx)) {
            case 1: {
                setState(153);
                match(NesCEPParser::LPARENTHESIS);
                break;
            }

            default: break;
        }
        setState(156);
        listEvents();
        setState(158);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::RPARENTHESIS) {
            setState(157);
            match(NesCEPParser::RPARENTHESIS);
        }

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
        setState(160);
        expression(0);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- WindowConstraintsContext ------------------------------------------------------------------

NesCEPParser::WindowConstraintsContext::WindowConstraintsContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

NesCEPParser::WindowTypeContext* NesCEPParser::WindowConstraintsContext::windowType() {
    return getRuleContext<NesCEPParser::WindowTypeContext>(0);
}

tree::TerminalNode* NesCEPParser::WindowConstraintsContext::LBRACKET() { return getToken(NesCEPParser::LBRACKET, 0); }

NesCEPParser::TimeConstraintsContext* NesCEPParser::WindowConstraintsContext::timeConstraints() {
    return getRuleContext<NesCEPParser::TimeConstraintsContext>(0);
}

tree::TerminalNode* NesCEPParser::WindowConstraintsContext::RBRACKET() { return getToken(NesCEPParser::RBRACKET, 0); }

size_t NesCEPParser::WindowConstraintsContext::getRuleIndex() const { return NesCEPParser::RuleWindowConstraints; }

void NesCEPParser::WindowConstraintsContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterWindowConstraints(this);
}

void NesCEPParser::WindowConstraintsContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitWindowConstraints(this);
}

NesCEPParser::WindowConstraintsContext* NesCEPParser::windowConstraints() {
    WindowConstraintsContext* _localctx = _tracker.createInstance<WindowConstraintsContext>(_ctx, getState());
    enterRule(_localctx, 12, NesCEPParser::RuleWindowConstraints);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(162);
        windowType();
        setState(163);
        match(NesCEPParser::LBRACKET);
        setState(164);
        timeConstraints();
        setState(165);
        match(NesCEPParser::RBRACKET);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- WindowTypeContext ------------------------------------------------------------------

NesCEPParser::WindowTypeContext::WindowTypeContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t NesCEPParser::WindowTypeContext::getRuleIndex() const { return NesCEPParser::RuleWindowType; }

void NesCEPParser::WindowTypeContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterWindowType(this);
}

void NesCEPParser::WindowTypeContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitWindowType(this);
}

NesCEPParser::WindowTypeContext* NesCEPParser::windowType() {
    WindowTypeContext* _localctx = _tracker.createInstance<WindowTypeContext>(_ctx, getState());
    enterRule(_localctx, 14, NesCEPParser::RuleWindowType);
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
        _la = _input->LA(1);
        if (!((((_la & ~0x3fULL) == 0)
               && ((1ULL << _la) & ((1ULL << NesCEPParser::T__0) | (1ULL << NesCEPParser::T__1) | (1ULL << NesCEPParser::T__2)))
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

//----------------- TimeConstraintsContext ------------------------------------------------------------------

NesCEPParser::TimeConstraintsContext::TimeConstraintsContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::TimeConstraintsContext::NAME() { return getToken(NesCEPParser::NAME, 0); }

std::vector<tree::TerminalNode*> NesCEPParser::TimeConstraintsContext::COMMA() { return getTokens(NesCEPParser::COMMA); }

tree::TerminalNode* NesCEPParser::TimeConstraintsContext::COMMA(size_t i) { return getToken(NesCEPParser::COMMA, i); }

std::vector<NesCEPParser::ConstraintContext*> NesCEPParser::TimeConstraintsContext::constraint() {
    return getRuleContexts<NesCEPParser::ConstraintContext>();
}

NesCEPParser::ConstraintContext* NesCEPParser::TimeConstraintsContext::constraint(size_t i) {
    return getRuleContext<NesCEPParser::ConstraintContext>(i);
}

std::vector<NesCEPParser::IntervalContext*> NesCEPParser::TimeConstraintsContext::interval() {
    return getRuleContexts<NesCEPParser::IntervalContext>();
}

NesCEPParser::IntervalContext* NesCEPParser::TimeConstraintsContext::interval(size_t i) {
    return getRuleContext<NesCEPParser::IntervalContext>(i);
}

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
    enterRule(_localctx, 16, NesCEPParser::RuleTimeConstraints);
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
        setState(169);
        match(NesCEPParser::NAME);
        setState(170);
        match(NesCEPParser::COMMA);
        setState(171);
        constraint();
        setState(172);
        interval();
        setState(179);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NesCEPParser::COMMA) {
            setState(173);
            match(NesCEPParser::COMMA);
            setState(174);
            constraint();
            setState(175);
            interval();
            setState(181);
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

//----------------- ConstraintContext ------------------------------------------------------------------

NesCEPParser::ConstraintContext::ConstraintContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t NesCEPParser::ConstraintContext::getRuleIndex() const { return NesCEPParser::RuleConstraint; }

void NesCEPParser::ConstraintContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterConstraint(this);
}

void NesCEPParser::ConstraintContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitConstraint(this);
}

NesCEPParser::ConstraintContext* NesCEPParser::constraint() {
    ConstraintContext* _localctx = _tracker.createInstance<ConstraintContext>(_ctx, getState());
    enterRule(_localctx, 18, NesCEPParser::RuleConstraint);
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
        setState(182);
        _la = _input->LA(1);
        if (!((((_la & ~0x3fULL) == 0)
               && ((1ULL << _la) & ((1ULL << NesCEPParser::T__1) | (1ULL << NesCEPParser::T__3) | (1ULL << NesCEPParser::T__4)))
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

//----------------- IntervalContext ------------------------------------------------------------------

NesCEPParser::IntervalContext::IntervalContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::IntervalContext::INT() { return getToken(NesCEPParser::INT, 0); }

NesCEPParser::IntervalTypeContext* NesCEPParser::IntervalContext::intervalType() {
    return getRuleContext<NesCEPParser::IntervalTypeContext>(0);
}

tree::TerminalNode* NesCEPParser::IntervalContext::WS() { return getToken(NesCEPParser::WS, 0); }

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
    enterRule(_localctx, 20, NesCEPParser::RuleInterval);
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
        setState(184);
        match(NesCEPParser::INT);
        setState(186);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::WS) {
            setState(185);
            match(NesCEPParser::WS);
        }
        setState(188);
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
    enterRule(_localctx, 22, NesCEPParser::RuleIntervalType);
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
        setState(190);
        _la = _input->LA(1);
        if (!(((((_la - 61) & ~0x3fULL) == 0)
               && ((1ULL << (_la - 61))
                   & ((1ULL << (NesCEPParser::QUARTER - 61)) | (1ULL << (NesCEPParser::MONTH - 61))
                      | (1ULL << (NesCEPParser::DAY - 61)) | (1ULL << (NesCEPParser::HOUR - 61))
                      | (1ULL << (NesCEPParser::MINUTE - 61)) | (1ULL << (NesCEPParser::WEEK - 61))
                      | (1ULL << (NesCEPParser::SECOND - 61)) | (1ULL << (NesCEPParser::MICROSECOND - 61))))
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
    enterRule(_localctx, 24, NesCEPParser::RuleOption);
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
        setState(192);
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
    enterRule(_localctx, 26, NesCEPParser::RuleOutputExpression);
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
        setState(194);
        match(NesCEPParser::NAME);
        setState(195);
        match(NesCEPParser::SEP);
        setState(196);
        match(NesCEPParser::LBRACKET);
        setState(197);
        outAttribute();
        setState(202);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NesCEPParser::COMMA) {
            setState(198);
            match(NesCEPParser::COMMA);
            setState(199);
            outAttribute();
            setState(204);
            _errHandler->sync(this);
            _la = _input->LA(1);
        }
        setState(205);
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
    enterRule(_localctx, 28, NesCEPParser::RuleOutAttribute);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(207);
        match(NesCEPParser::NAME);
        setState(208);
        match(NesCEPParser::EQUAL);
        setState(209);
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
    enterRule(_localctx, 30, NesCEPParser::RuleSinkList);
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
        setState(211);
        sink();
        setState(216);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NesCEPParser::COMMA) {
            setState(212);
            match(NesCEPParser::COMMA);
            setState(213);
            sink();
            setState(218);
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
    enterRule(_localctx, 32, NesCEPParser::RuleListEvents);
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
        setState(219);
        eventElem();
        setState(225);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while ((((_la & ~0x3fULL) == 0)
                && ((1ULL << _la)
                    & ((1ULL << NesCEPParser::ANY) | (1ULL << NesCEPParser::SEQ) | (1ULL << NesCEPParser::NEXT)
                       | (1ULL << NesCEPParser::AND) | (1ULL << NesCEPParser::OR)))
                    != 0)) {
            setState(220);
            operatorRule();
            setState(221);
            eventElem();
            setState(227);
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
    enterRule(_localctx, 34, NesCEPParser::RuleEventElem);
    size_t _la = 0;

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
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 16, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(229);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::NOT) {
                    setState(228);
                    match(NesCEPParser::NOT);
                }
                setState(231);
                event();
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(233);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::NOT) {
                    setState(232);
                    match(NesCEPParser::NOT);
                }
                setState(235);
                match(NesCEPParser::LPARENTHESIS);
                setState(236);
                listEvents();
                setState(237);
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
    enterRule(_localctx, 36, NesCEPParser::RuleEvent);
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
        setState(241);
        match(NesCEPParser::NAME);
        setState(243);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0)
             && ((1ULL << _la) & ((1ULL << NesCEPParser::STAR) | (1ULL << NesCEPParser::PLUS) | (1ULL << NesCEPParser::LBRACKET)))
                 != 0)) {
            setState(242);
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
    enterRule(_localctx, 38, NesCEPParser::RuleQuantifiers);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(265);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 21, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(245);
                match(NesCEPParser::STAR);
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(246);
                match(NesCEPParser::PLUS);
                break;
            }

            case 3: {
                enterOuterAlt(_localctx, 3);
                setState(247);
                match(NesCEPParser::LBRACKET);
                setState(249);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::ANY

                    || _la == NesCEPParser::NEXT) {
                    setState(248);
                    consecutiveOption();
                }
                setState(251);
                match(NesCEPParser::INT);
                setState(253);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::PLUS) {
                    setState(252);
                    match(NesCEPParser::PLUS);
                }
                setState(255);
                match(NesCEPParser::RBRACKET);
                break;
            }

            case 4: {
                enterOuterAlt(_localctx, 4);
                setState(256);
                match(NesCEPParser::LBRACKET);
                setState(258);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::ANY

                    || _la == NesCEPParser::NEXT) {
                    setState(257);
                    consecutiveOption();
                }
                setState(260);
                iterMin();
                setState(261);
                match(NesCEPParser::D_POINTS);
                setState(262);
                iterMax();
                setState(263);
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
    enterRule(_localctx, 40, NesCEPParser::RuleIterMax);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(267);
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
    enterRule(_localctx, 42, NesCEPParser::RuleIterMin);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(269);
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
    enterRule(_localctx, 44, NesCEPParser::RuleConsecutiveOption);
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
        setState(272);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::ANY) {
            setState(271);
            match(NesCEPParser::ANY);
        }
        setState(274);
        match(NesCEPParser::NEXT);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- ParameterNameContext ------------------------------------------------------------------

NesCEPParser::ParameterNameContext::ParameterNameContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* NesCEPParser::ParameterNameContext::NAME() { return getToken(NesCEPParser::NAME, 0); }

size_t NesCEPParser::ParameterNameContext::getRuleIndex() const { return NesCEPParser::RuleParameterName; }

void NesCEPParser::ParameterNameContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterParameterName(this);
}

void NesCEPParser::ParameterNameContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitParameterName(this);
}

NesCEPParser::ParameterNameContext* NesCEPParser::parameterName() {
    ParameterNameContext* _localctx = _tracker.createInstance<ParameterNameContext>(_ctx, getState());
    enterRule(_localctx, 46, NesCEPParser::RuleParameterName);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(276);
        match(NesCEPParser::NAME);

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
    enterRule(_localctx, 48, NesCEPParser::RuleOperatorRule);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(281);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
            case NesCEPParser::AND: {
                enterOuterAlt(_localctx, 1);
                setState(278);
                match(NesCEPParser::AND);
                break;
            }

            case NesCEPParser::OR: {
                enterOuterAlt(_localctx, 2);
                setState(279);
                match(NesCEPParser::OR);
                break;
            }

            case NesCEPParser::ANY:
            case NesCEPParser::SEQ:
            case NesCEPParser::NEXT: {
                enterOuterAlt(_localctx, 3);
                setState(280);
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
    enterRule(_localctx, 50, NesCEPParser::RuleSequence);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(285);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
            case NesCEPParser::SEQ: {
                enterOuterAlt(_localctx, 1);
                setState(283);
                match(NesCEPParser::SEQ);
                break;
            }

            case NesCEPParser::ANY:
            case NesCEPParser::NEXT: {
                enterOuterAlt(_localctx, 2);
                setState(284);
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
    enterRule(_localctx, 52, NesCEPParser::RuleContiguity);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(290);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
            case NesCEPParser::NEXT: {
                enterOuterAlt(_localctx, 1);
                setState(287);
                match(NesCEPParser::NEXT);
                break;
            }

            case NesCEPParser::ANY: {
                enterOuterAlt(_localctx, 2);
                setState(288);
                match(NesCEPParser::ANY);
                setState(289);
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

tree::TerminalNode* NesCEPParser::SinkTypeContext::MQTT() { return getToken(NesCEPParser::MQTT, 0); }

tree::TerminalNode* NesCEPParser::SinkTypeContext::OPC() { return getToken(NesCEPParser::OPC, 0); }

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
    enterRule(_localctx, 54, NesCEPParser::RuleSinkType);
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
        setState(292);
        _la = _input->LA(1);
        if (!((((_la & ~0x3fULL) == 0)
               && ((1ULL << _la)
                   & ((1ULL << NesCEPParser::T__5) | (1ULL << NesCEPParser::T__6) | (1ULL << NesCEPParser::T__7)
                      | (1ULL << NesCEPParser::T__8) | (1ULL << NesCEPParser::T__9)))
                   != 0)
              || ((((_la - 73) & ~0x3fULL) == 0)
                  && ((1ULL << (_la - 73))
                      & ((1ULL << (NesCEPParser::MQTT - 73)) | (1ULL << (NesCEPParser::OPC - 73))
                         | (1ULL << (NesCEPParser::ZMQ - 73))))
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
    enterRule(_localctx, 56, NesCEPParser::RuleNullNotnull);
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
        setState(295);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NesCEPParser::NOT) {
            setState(294);
            match(NesCEPParser::NOT);
        }
        setState(297);
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

tree::TerminalNode* NesCEPParser::ConstantContext::FLOAT() { return getToken(NesCEPParser::FLOAT, 0); }

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
    enterRule(_localctx, 58, NesCEPParser::RuleConstant);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(305);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
            case NesCEPParser::QUOTE: {
                enterOuterAlt(_localctx, 1);
                setState(299);
                match(NesCEPParser::QUOTE);
                setState(300);
                match(NesCEPParser::NAME);
                setState(301);
                match(NesCEPParser::QUOTE);
                break;
            }

            case NesCEPParser::FLOAT: {
                enterOuterAlt(_localctx, 2);
                setState(302);
                match(NesCEPParser::FLOAT);
                break;
            }

            case NesCEPParser::INT: {
                enterOuterAlt(_localctx, 3);
                setState(303);
                match(NesCEPParser::INT);
                break;
            }

            case NesCEPParser::NAME: {
                enterOuterAlt(_localctx, 4);
                setState(304);
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
    enterRule(_localctx, 60, NesCEPParser::RuleExpressions);
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
        setState(307);
        expression(0);
        setState(312);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NesCEPParser::COMMA) {
            setState(308);
            match(NesCEPParser::COMMA);
            setState(309);
            expression(0);
            setState(314);
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
    size_t startState = 62;
    enterRecursionRule(_localctx, 62, NesCEPParser::RuleExpression, precedence);

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
        setState(326);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx)) {
            case 1: {
                _localctx = _tracker.createInstance<NotExpressionContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;

                setState(316);
                match(NesCEPParser::NOT_OP);
                setState(317);
                expression(4);
                break;
            }

            case 2: {
                _localctx = _tracker.createInstance<IsExpressionContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;
                setState(318);
                predicate(0);
                setState(319);
                match(NesCEPParser::IS);
                setState(321);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::NOT) {
                    setState(320);
                    match(NesCEPParser::NOT);
                }
                setState(323);
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
                setState(325);
                predicate(0);
                break;
            }

            default: break;
        }
        _ctx->stop = _input->LT(-1);
        setState(334);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
            if (alt == 1) {
                if (!_parseListeners.empty())
                    triggerExitRuleEvent();
                previousContext = _localctx;
                auto newContext = _tracker.createInstance<LogicalExpressionContext>(
                    _tracker.createInstance<ExpressionContext>(parentContext, parentState));
                _localctx = newContext;
                pushNewRecursionContext(newContext, startState, RuleExpression);
                setState(328);

                if (!(precpred(_ctx, 3)))
                    throw FailedPredicateException(this, "precpred(_ctx, 3)");
                setState(329);
                logicalOperator();
                setState(330);
                expression(4);
            }
            setState(336);
            _errHandler->sync(this);
            alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx);
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
//----------------- BinaryComparisonPredicateContext ------------------------------------------------------------------

NesCEPParser::ComparisonOperatorContext* NesCEPParser::BinaryComparisonPredicateContext::comparisonOperator() {
    return getRuleContext<NesCEPParser::ComparisonOperatorContext>(0);
}

std::vector<NesCEPParser::PredicateContext*> NesCEPParser::BinaryComparisonPredicateContext::predicate() {
    return getRuleContexts<NesCEPParser::PredicateContext>();
}

NesCEPParser::PredicateContext* NesCEPParser::BinaryComparisonPredicateContext::predicate(size_t i) {
    return getRuleContext<NesCEPParser::PredicateContext>(i);
}

NesCEPParser::BinaryComparisonPredicateContext::BinaryComparisonPredicateContext(PredicateContext* ctx) { copyFrom(ctx); }

void NesCEPParser::BinaryComparisonPredicateContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterBinaryComparisonPredicate(this);
}
void NesCEPParser::BinaryComparisonPredicateContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitBinaryComparisonPredicate(this);
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
    size_t startState = 64;
    enterRecursionRule(_localctx, 64, NesCEPParser::RulePredicate, precedence);

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

        setState(338);
        expressionAtom(0);
        _ctx->stop = _input->LT(-1);
        setState(358);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 34, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
            if (alt == 1) {
                if (!_parseListeners.empty())
                    triggerExitRuleEvent();
                previousContext = _localctx;
                setState(356);
                _errHandler->sync(this);
                switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 33, _ctx)) {
                    case 1: {
                        auto newContext = _tracker.createInstance<BinaryComparisonPredicateContext>(
                            _tracker.createInstance<PredicateContext>(parentContext, parentState));
                        _localctx = newContext;
                        newContext->left = previousContext;
                        pushNewRecursionContext(newContext, startState, RulePredicate);
                        setState(340);

                        if (!(precpred(_ctx, 2)))
                            throw FailedPredicateException(this, "precpred(_ctx, 2)");
                        setState(341);
                        comparisonOperator();
                        setState(342);
                        dynamic_cast<BinaryComparisonPredicateContext*>(_localctx)->right = predicate(3);
                        break;
                    }

                    case 2: {
                        auto newContext = _tracker.createInstance<InPredicateContext>(
                            _tracker.createInstance<PredicateContext>(parentContext, parentState));
                        _localctx = newContext;
                        pushNewRecursionContext(newContext, startState, RulePredicate);
                        setState(344);

                        if (!(precpred(_ctx, 4)))
                            throw FailedPredicateException(this, "precpred(_ctx, 4)");
                        setState(346);
                        _errHandler->sync(this);

                        _la = _input->LA(1);
                        if (_la == NesCEPParser::NOT) {
                            setState(345);
                            match(NesCEPParser::NOT);
                        }
                        setState(348);
                        match(NesCEPParser::IN);
                        setState(349);
                        match(NesCEPParser::LPARENTHESIS);
                        setState(350);
                        expressions();
                        setState(351);
                        match(NesCEPParser::RPARENTHESIS);
                        break;
                    }

                    case 3: {
                        auto newContext = _tracker.createInstance<IsNullPredicateContext>(
                            _tracker.createInstance<PredicateContext>(parentContext, parentState));
                        _localctx = newContext;
                        pushNewRecursionContext(newContext, startState, RulePredicate);
                        setState(353);

                        if (!(precpred(_ctx, 3)))
                            throw FailedPredicateException(this, "precpred(_ctx, 3)");
                        setState(354);
                        match(NesCEPParser::IS);
                        setState(355);
                        nullNotnull();
                        break;
                    }

                    default: break;
                }
            }
            setState(360);
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

tree::TerminalNode* NesCEPParser::UnaryExpressionAtomContext::WS() { return getToken(NesCEPParser::WS, 0); }

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
    size_t startState = 66;
    enterRecursionRule(_localctx, 66, NesCEPParser::RuleExpressionAtom, precedence);

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
        setState(383);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 37, _ctx)) {
            case 1: {
                _localctx = _tracker.createInstance<AttributeAtomContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;

                setState(362);
                eventAttribute();
                break;
            }

            case 2: {
                _localctx = _tracker.createInstance<UnaryExpressionAtomContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;
                setState(363);
                unaryOperator();
                setState(365);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::WS) {
                    setState(364);
                    match(NesCEPParser::WS);
                }
                setState(367);
                expressionAtom(6);
                break;
            }

            case 3: {
                _localctx = _tracker.createInstance<BinaryExpressionAtomContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;
                setState(369);
                match(NesCEPParser::BINARY);
                setState(370);
                expressionAtom(5);
                break;
            }

            case 4: {
                _localctx = _tracker.createInstance<NestedExpressionAtomContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;
                setState(371);
                match(NesCEPParser::LPARENTHESIS);
                setState(372);
                expression(0);
                setState(377);
                _errHandler->sync(this);
                _la = _input->LA(1);
                while (_la == NesCEPParser::COMMA) {
                    setState(373);
                    match(NesCEPParser::COMMA);
                    setState(374);
                    expression(0);
                    setState(379);
                    _errHandler->sync(this);
                    _la = _input->LA(1);
                }
                setState(380);
                match(NesCEPParser::RPARENTHESIS);
                break;
            }

            case 5: {
                _localctx = _tracker.createInstance<ConstantExpressionAtomContext>(_localctx);
                _ctx = _localctx;
                previousContext = _localctx;
                setState(382);
                constant();
                break;
            }

            default: break;
        }
        _ctx->stop = _input->LT(-1);
        setState(395);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 39, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
            if (alt == 1) {
                if (!_parseListeners.empty())
                    triggerExitRuleEvent();
                previousContext = _localctx;
                setState(393);
                _errHandler->sync(this);
                switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 38, _ctx)) {
                    case 1: {
                        auto newContext = _tracker.createInstance<BitExpressionAtomContext>(
                            _tracker.createInstance<ExpressionAtomContext>(parentContext, parentState));
                        _localctx = newContext;
                        newContext->left = previousContext;
                        pushNewRecursionContext(newContext, startState, RuleExpressionAtom);
                        setState(385);

                        if (!(precpred(_ctx, 3)))
                            throw FailedPredicateException(this, "precpred(_ctx, 3)");
                        setState(386);
                        bitOperator();
                        setState(387);
                        dynamic_cast<BitExpressionAtomContext*>(_localctx)->right = expressionAtom(4);
                        break;
                    }

                    case 2: {
                        auto newContext = _tracker.createInstance<MathExpressionAtomContext>(
                            _tracker.createInstance<ExpressionAtomContext>(parentContext, parentState));
                        _localctx = newContext;
                        newContext->left = previousContext;
                        pushNewRecursionContext(newContext, startState, RuleExpressionAtom);
                        setState(389);

                        if (!(precpred(_ctx, 2)))
                            throw FailedPredicateException(this, "precpred(_ctx, 2)");
                        setState(390);
                        mathOperator();
                        setState(391);
                        dynamic_cast<MathExpressionAtomContext*>(_localctx)->right = expressionAtom(3);
                        break;
                    }

                    default: break;
                }
            }
            setState(397);
            _errHandler->sync(this);
            alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 39, _ctx);
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
    enterRule(_localctx, 68, NesCEPParser::RuleEventAttribute);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(411);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 41, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(398);
                aggregation();
                setState(399);
                match(NesCEPParser::LPARENTHESIS);
                setState(400);
                expressions();
                setState(401);
                match(NesCEPParser::RPARENTHESIS);
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(403);
                eventIteration();
                setState(406);
                _errHandler->sync(this);

                switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 40, _ctx)) {
                    case 1: {
                        setState(404);
                        match(NesCEPParser::POINT);
                        setState(405);
                        attribute();
                        break;
                    }

                    default: break;
                }
                break;
            }

            case 3: {
                enterOuterAlt(_localctx, 3);
                setState(408);
                match(NesCEPParser::NAME);
                setState(409);
                match(NesCEPParser::POINT);
                setState(410);
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
    enterRule(_localctx, 70, NesCEPParser::RuleEventIteration);
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
        match(NesCEPParser::NAME);
        setState(414);
        match(NesCEPParser::LBRACKET);
        setState(416);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0)
             && ((1ULL << _la)
                 & ((1ULL << NesCEPParser::T__10) | (1ULL << NesCEPParser::INT) | (1ULL << NesCEPParser::FLOAT)
                    | (1ULL << NesCEPParser::LPARENTHESIS) | (1ULL << NesCEPParser::NOT) | (1ULL << NesCEPParser::PLUS)
                    | (1ULL << NesCEPParser::BINARY)))
                 != 0)
            || ((((_la - 80) & ~0x3fULL) == 0)
                && ((1ULL << (_la - 80))
                    & ((1ULL << (NesCEPParser::QUOTE - 80)) | (1ULL << (NesCEPParser::AVG - 80))
                       | (1ULL << (NesCEPParser::SUM - 80)) | (1ULL << (NesCEPParser::MIN - 80))
                       | (1ULL << (NesCEPParser::MAX - 80)) | (1ULL << (NesCEPParser::COUNT - 80))
                       | (1ULL << (NesCEPParser::NAME - 80))))
                    != 0)) {
            setState(415);
            mathExpression();
        }
        setState(418);
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
    enterRule(_localctx, 72, NesCEPParser::RuleMathExpression);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(429);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 44, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(420);
                dynamic_cast<MathExpressionContext*>(_localctx)->left = expressionAtom(0);
                setState(421);
                mathOperator();
                setState(422);
                dynamic_cast<MathExpressionContext*>(_localctx)->right = expressionAtom(0);
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(424);
                constant();
                setState(427);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::D_POINTS) {
                    setState(425);
                    match(NesCEPParser::D_POINTS);
                    setState(426);
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

tree::TerminalNode* NesCEPParser::AggregationContext::AVG() { return getToken(NesCEPParser::AVG, 0); }

tree::TerminalNode* NesCEPParser::AggregationContext::SUM() { return getToken(NesCEPParser::SUM, 0); }

tree::TerminalNode* NesCEPParser::AggregationContext::MIN() { return getToken(NesCEPParser::MIN, 0); }

tree::TerminalNode* NesCEPParser::AggregationContext::MAX() { return getToken(NesCEPParser::MAX, 0); }

tree::TerminalNode* NesCEPParser::AggregationContext::COUNT() { return getToken(NesCEPParser::COUNT, 0); }

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
    enterRule(_localctx, 74, NesCEPParser::RuleAggregation);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(437);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
            case NesCEPParser::LPARENTHESIS: {
                enterOuterAlt(_localctx, 1);

                break;
            }

            case NesCEPParser::AVG: {
                enterOuterAlt(_localctx, 2);
                setState(432);
                match(NesCEPParser::AVG);
                break;
            }

            case NesCEPParser::SUM: {
                enterOuterAlt(_localctx, 3);
                setState(433);
                match(NesCEPParser::SUM);
                break;
            }

            case NesCEPParser::MIN: {
                enterOuterAlt(_localctx, 4);
                setState(434);
                match(NesCEPParser::MIN);
                break;
            }

            case NesCEPParser::MAX: {
                enterOuterAlt(_localctx, 5);
                setState(435);
                match(NesCEPParser::MAX);
                break;
            }

            case NesCEPParser::COUNT: {
                enterOuterAlt(_localctx, 6);
                setState(436);
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

//----------------- ValueContext ------------------------------------------------------------------

NesCEPParser::ValueContext::ValueContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

NesCEPParser::AttributeContext* NesCEPParser::ValueContext::attribute() {
    return getRuleContext<NesCEPParser::AttributeContext>(0);
}

tree::TerminalNode* NesCEPParser::ValueContext::POINT() { return getToken(NesCEPParser::POINT, 0); }

tree::TerminalNode* NesCEPParser::ValueContext::FILETYPE() { return getToken(NesCEPParser::FILETYPE, 0); }

tree::TerminalNode* NesCEPParser::ValueContext::URL() { return getToken(NesCEPParser::URL, 0); }

size_t NesCEPParser::ValueContext::getRuleIndex() const { return NesCEPParser::RuleValue; }

void NesCEPParser::ValueContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterValue(this);
}

void NesCEPParser::ValueContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitValue(this);
}

NesCEPParser::ValueContext* NesCEPParser::value() {
    ValueContext* _localctx = _tracker.createInstance<ValueContext>(_ctx, getState());
    enterRule(_localctx, 76, NesCEPParser::RuleValue);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(445);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 46, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(439);
                attribute();
                setState(440);
                match(NesCEPParser::POINT);
                setState(441);
                match(NesCEPParser::FILETYPE);
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(443);
                attribute();
                break;
            }

            case 3: {
                enterOuterAlt(_localctx, 3);
                setState(444);
                match(NesCEPParser::URL);
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
    enterRule(_localctx, 78, NesCEPParser::RuleAttribute);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(447);
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
    enterRule(_localctx, 80, NesCEPParser::RuleAttVal);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(455);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 47, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(449);
                match(NesCEPParser::IF);
                setState(450);
                condition();
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(451);
                eventAttribute();
                break;
            }

            case 3: {
                enterOuterAlt(_localctx, 3);
                setState(452);
                event();
                break;
            }

            case 4: {
                enterOuterAlt(_localctx, 4);
                setState(453);
                expression(0);
                break;
            }

            case 5: {
                enterOuterAlt(_localctx, 5);
                setState(454);
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
    enterRule(_localctx, 82, NesCEPParser::RuleBoolRule);
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
        setState(457);
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
    enterRule(_localctx, 84, NesCEPParser::RuleCondition);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(459);
        match(NesCEPParser::LPARENTHESIS);
        setState(460);
        expression(0);
        setState(461);
        match(NesCEPParser::COMMA);
        setState(462);
        attVal();
        setState(463);
        match(NesCEPParser::COMMA);
        setState(464);
        attVal();
        setState(465);
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
    enterRule(_localctx, 86, NesCEPParser::RuleUnaryOperator);
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
        setState(467);
        _la = _input->LA(1);
        if (!((((_la & ~0x3fULL) == 0)
               && ((1ULL << _la) & ((1ULL << NesCEPParser::T__10) | (1ULL << NesCEPParser::NOT) | (1ULL << NesCEPParser::PLUS)))
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

std::vector<tree::TerminalNode*> NesCEPParser::ComparisonOperatorContext::WS() { return getTokens(NesCEPParser::WS); }

tree::TerminalNode* NesCEPParser::ComparisonOperatorContext::WS(size_t i) { return getToken(NesCEPParser::WS, i); }

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
    enterRule(_localctx, 88, NesCEPParser::RuleComparisonOperator);
    size_t _la = 0;

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(506);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 55, _ctx)) {
            case 1: {
                enterOuterAlt(_localctx, 1);
                setState(469);
                match(NesCEPParser::EQUAL);
                setState(471);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::WS) {
                    setState(470);
                    match(NesCEPParser::WS);
                }
                setState(473);
                match(NesCEPParser::EQUAL);
                break;
            }

            case 2: {
                enterOuterAlt(_localctx, 2);
                setState(474);
                match(NesCEPParser::T__11);
                break;
            }

            case 3: {
                enterOuterAlt(_localctx, 3);
                setState(475);
                match(NesCEPParser::T__12);
                break;
            }

            case 4: {
                enterOuterAlt(_localctx, 4);
                setState(476);
                match(NesCEPParser::T__12);
                setState(478);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::WS) {
                    setState(477);
                    match(NesCEPParser::WS);
                }
                setState(480);
                match(NesCEPParser::EQUAL);
                break;
            }

            case 5: {
                enterOuterAlt(_localctx, 5);
                setState(481);
                match(NesCEPParser::T__11);
                setState(483);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::WS) {
                    setState(482);
                    match(NesCEPParser::WS);
                }
                setState(485);
                match(NesCEPParser::EQUAL);
                break;
            }

            case 6: {
                enterOuterAlt(_localctx, 6);
                setState(486);
                match(NesCEPParser::T__12);
                setState(488);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::WS) {
                    setState(487);
                    match(NesCEPParser::WS);
                }
                setState(490);
                match(NesCEPParser::T__11);
                break;
            }

            case 7: {
                enterOuterAlt(_localctx, 7);
                setState(491);
                match(NesCEPParser::NOT_OP);
                setState(493);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::WS) {
                    setState(492);
                    match(NesCEPParser::WS);
                }
                setState(495);
                match(NesCEPParser::EQUAL);
                break;
            }

            case 8: {
                enterOuterAlt(_localctx, 8);
                setState(496);
                match(NesCEPParser::T__12);
                setState(498);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::WS) {
                    setState(497);
                    match(NesCEPParser::WS);
                }
                setState(500);
                match(NesCEPParser::EQUAL);
                setState(502);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == NesCEPParser::WS) {
                    setState(501);
                    match(NesCEPParser::WS);
                }
                setState(504);
                match(NesCEPParser::T__11);
                break;
            }

            case 9: {
                enterOuterAlt(_localctx, 9);
                setState(505);
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
    enterRule(_localctx, 90, NesCEPParser::RuleLogicalOperator);
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
        setState(508);
        _la = _input->LA(1);
        if (!(((((_la - 87) & ~0x3fULL) == 0)
               && ((1ULL << (_la - 87))
                   & ((1ULL << (NesCEPParser::LOGOR - 87)) | (1ULL << (NesCEPParser::LOGAND - 87))
                      | (1ULL << (NesCEPParser::LOGXOR - 87))))
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
    enterRule(_localctx, 92, NesCEPParser::RuleBitOperator);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(517);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
            case NesCEPParser::T__12: {
                enterOuterAlt(_localctx, 1);
                setState(510);
                match(NesCEPParser::T__12);
                setState(511);
                match(NesCEPParser::T__12);
                break;
            }

            case NesCEPParser::T__11: {
                enterOuterAlt(_localctx, 2);
                setState(512);
                match(NesCEPParser::T__11);
                setState(513);
                match(NesCEPParser::T__11);
                break;
            }

            case NesCEPParser::T__13: {
                enterOuterAlt(_localctx, 3);
                setState(514);
                match(NesCEPParser::T__13);
                break;
            }

            case NesCEPParser::LOGXOR: {
                enterOuterAlt(_localctx, 4);
                setState(515);
                match(NesCEPParser::LOGXOR);
                break;
            }

            case NesCEPParser::T__14: {
                enterOuterAlt(_localctx, 5);
                setState(516);
                match(NesCEPParser::T__14);
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
    enterRule(_localctx, 94, NesCEPParser::RuleMathOperator);
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
        setState(519);
        _la = _input->LA(1);
        if (!((((_la & ~0x3fULL) == 0)
               && ((1ULL << _la)
                   & ((1ULL << NesCEPParser::T__10) | (1ULL << NesCEPParser::T__15) | (1ULL << NesCEPParser::T__16)
                      | (1ULL << NesCEPParser::T__17) | (1ULL << NesCEPParser::STAR) | (1ULL << NesCEPParser::PLUS)))
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

//----------------- SinkContext ------------------------------------------------------------------

NesCEPParser::SinkContext::SinkContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t NesCEPParser::SinkContext::getRuleIndex() const { return NesCEPParser::RuleSink; }

void NesCEPParser::SinkContext::copyFrom(SinkContext* ctx) { ParserRuleContext::copyFrom(ctx); }

//----------------- SinkWithParametersContext ------------------------------------------------------------------

NesCEPParser::SinkTypeContext* NesCEPParser::SinkWithParametersContext::sinkType() {
    return getRuleContext<NesCEPParser::SinkTypeContext>(0);
}

tree::TerminalNode* NesCEPParser::SinkWithParametersContext::LPARENTHESIS() { return getToken(NesCEPParser::LPARENTHESIS, 0); }

NesCEPParser::ParametersContext* NesCEPParser::SinkWithParametersContext::parameters() {
    return getRuleContext<NesCEPParser::ParametersContext>(0);
}

tree::TerminalNode* NesCEPParser::SinkWithParametersContext::RPARENTHESIS() { return getToken(NesCEPParser::RPARENTHESIS, 0); }

NesCEPParser::SinkWithParametersContext::SinkWithParametersContext(SinkContext* ctx) { copyFrom(ctx); }

void NesCEPParser::SinkWithParametersContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterSinkWithParameters(this);
}
void NesCEPParser::SinkWithParametersContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitSinkWithParameters(this);
}
//----------------- SinkWithoutParametersContext ------------------------------------------------------------------

NesCEPParser::SinkTypeContext* NesCEPParser::SinkWithoutParametersContext::sinkType() {
    return getRuleContext<NesCEPParser::SinkTypeContext>(0);
}

NesCEPParser::SinkWithoutParametersContext::SinkWithoutParametersContext(SinkContext* ctx) { copyFrom(ctx); }

void NesCEPParser::SinkWithoutParametersContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterSinkWithoutParameters(this);
}
void NesCEPParser::SinkWithoutParametersContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitSinkWithoutParameters(this);
}
NesCEPParser::SinkContext* NesCEPParser::sink() {
    SinkContext* _localctx = _tracker.createInstance<SinkContext>(_ctx, getState());
    enterRule(_localctx, 96, NesCEPParser::RuleSink);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        setState(527);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 57, _ctx)) {
            case 1: {
                _localctx =
                    dynamic_cast<SinkContext*>(_tracker.createInstance<NesCEPParser::SinkWithParametersContext>(_localctx));
                enterOuterAlt(_localctx, 1);
                setState(521);
                sinkType();
                setState(522);
                match(NesCEPParser::LPARENTHESIS);
                setState(523);
                parameters();
                setState(524);
                match(NesCEPParser::RPARENTHESIS);
                break;
            }

            case 2: {
                _localctx =
                    dynamic_cast<SinkContext*>(_tracker.createInstance<NesCEPParser::SinkWithoutParametersContext>(_localctx));
                enterOuterAlt(_localctx, 2);
                setState(526);
                sinkType();
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

//----------------- ParametersContext ------------------------------------------------------------------

NesCEPParser::ParametersContext::ParametersContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<NesCEPParser::ParameterContext*> NesCEPParser::ParametersContext::parameter() {
    return getRuleContexts<NesCEPParser::ParameterContext>();
}

NesCEPParser::ParameterContext* NesCEPParser::ParametersContext::parameter(size_t i) {
    return getRuleContext<NesCEPParser::ParameterContext>(i);
}

std::vector<NesCEPParser::ValueContext*> NesCEPParser::ParametersContext::value() {
    return getRuleContexts<NesCEPParser::ValueContext>();
}

NesCEPParser::ValueContext* NesCEPParser::ParametersContext::value(size_t i) {
    return getRuleContext<NesCEPParser::ValueContext>(i);
}

std::vector<tree::TerminalNode*> NesCEPParser::ParametersContext::COMMA() { return getTokens(NesCEPParser::COMMA); }

tree::TerminalNode* NesCEPParser::ParametersContext::COMMA(size_t i) { return getToken(NesCEPParser::COMMA, i); }

size_t NesCEPParser::ParametersContext::getRuleIndex() const { return NesCEPParser::RuleParameters; }

void NesCEPParser::ParametersContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterParameters(this);
}

void NesCEPParser::ParametersContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitParameters(this);
}

NesCEPParser::ParametersContext* NesCEPParser::parameters() {
    ParametersContext* _localctx = _tracker.createInstance<ParametersContext>(_ctx, getState());
    enterRule(_localctx, 98, NesCEPParser::RuleParameters);
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
        setState(529);
        parameter();
        setState(530);
        value();
        setState(537);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NesCEPParser::COMMA) {
            setState(531);
            match(NesCEPParser::COMMA);
            setState(532);
            parameter();
            setState(533);
            value();
            setState(539);
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

//----------------- ParameterContext ------------------------------------------------------------------

NesCEPParser::ParameterContext::ParameterContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t NesCEPParser::ParameterContext::getRuleIndex() const { return NesCEPParser::RuleParameter; }

void NesCEPParser::ParameterContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterParameter(this);
}

void NesCEPParser::ParameterContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitParameter(this);
}

NesCEPParser::ParameterContext* NesCEPParser::parameter() {
    ParameterContext* _localctx = _tracker.createInstance<ParameterContext>(_ctx, getState());
    enterRule(_localctx, 100, NesCEPParser::RuleParameter);
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
        setState(540);
        _la = _input->LA(1);
        if (!((((_la & ~0x3fULL) == 0)
               && ((1ULL << _la)
                   & ((1ULL << NesCEPParser::T__18) | (1ULL << NesCEPParser::T__19) | (1ULL << NesCEPParser::T__20)))
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

//----------------- FileNameContext ------------------------------------------------------------------

NesCEPParser::FileNameContext::FileNameContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t NesCEPParser::FileNameContext::getRuleIndex() const { return NesCEPParser::RuleFileName; }

void NesCEPParser::FileNameContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterFileName(this);
}

void NesCEPParser::FileNameContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitFileName(this);
}

NesCEPParser::FileNameContext* NesCEPParser::fileName() {
    FileNameContext* _localctx = _tracker.createInstance<FileNameContext>(_ctx, getState());
    enterRule(_localctx, 102, NesCEPParser::RuleFileName);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(542);
        match(NesCEPParser::T__18);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- TopicContext ------------------------------------------------------------------

NesCEPParser::TopicContext::TopicContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t NesCEPParser::TopicContext::getRuleIndex() const { return NesCEPParser::RuleTopic; }

void NesCEPParser::TopicContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterTopic(this);
}

void NesCEPParser::TopicContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitTopic(this);
}

NesCEPParser::TopicContext* NesCEPParser::topic() {
    TopicContext* _localctx = _tracker.createInstance<TopicContext>(_ctx, getState());
    enterRule(_localctx, 104, NesCEPParser::RuleTopic);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(544);
        match(NesCEPParser::T__19);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

//----------------- AddressContext ------------------------------------------------------------------

NesCEPParser::AddressContext::AddressContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t NesCEPParser::AddressContext::getRuleIndex() const { return NesCEPParser::RuleAddress; }

void NesCEPParser::AddressContext::enterRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->enterAddress(this);
}

void NesCEPParser::AddressContext::exitRule(tree::ParseTreeListener* listener) {
    auto parserListener = dynamic_cast<NesCEPListener*>(listener);
    if (parserListener != nullptr)
        parserListener->exitAddress(this);
}

NesCEPParser::AddressContext* NesCEPParser::address() {
    AddressContext* _localctx = _tracker.createInstance<AddressContext>(_ctx, getState());
    enterRule(_localctx, 106, NesCEPParser::RuleAddress);

#if __cplusplus > 201703L
    auto onExit = finally([=, this] {
#else
    auto onExit = finally([=] {
#endif
        exitRule();
    });
    try {
        enterOuterAlt(_localctx, 1);
        setState(546);
        match(NesCEPParser::T__20);

    } catch (RecognitionException& e) {
        _errHandler->reportError(this, e);
        _localctx->exception = std::current_exception();
        _errHandler->recover(this, _localctx->exception);
    }

    return _localctx;
}

bool NesCEPParser::sempred(RuleContext* context, size_t ruleIndex, size_t predicateIndex) {
    switch (ruleIndex) {
        case 31: return expressionSempred(dynamic_cast<ExpressionContext*>(context), predicateIndex);
        case 32: return predicateSempred(dynamic_cast<PredicateContext*>(context), predicateIndex);
        case 33: return expressionAtomSempred(dynamic_cast<ExpressionAtomContext*>(context), predicateIndex);

        default: break;
    }
    return true;
}

bool NesCEPParser::expressionSempred(ExpressionContext*, size_t predicateIndex) {
    switch (predicateIndex) {
        case 0: return precpred(_ctx, 3);

        default: break;
    }
    return true;
}

bool NesCEPParser::predicateSempred(PredicateContext*, size_t predicateIndex) {
    switch (predicateIndex) {
        case 1: return precpred(_ctx, 2);
        case 2: return precpred(_ctx, 4);
        case 3: return precpred(_ctx, 3);

        default: break;
    }
    return true;
}

bool NesCEPParser::expressionAtomSempred(ExpressionAtomContext*, size_t predicateIndex) {
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
                                                     "windowConstraints",
                                                     "windowType",
                                                     "timeConstraints",
                                                     "constraint",
                                                     "interval",
                                                     "intervalType",
                                                     "option",
                                                     "outputExpression",
                                                     "outAttribute",
                                                     "sinkList",
                                                     "listEvents",
                                                     "eventElem",
                                                     "event",
                                                     "quantifiers",
                                                     "iterMax",
                                                     "iterMin",
                                                     "consecutiveOption",
                                                     "parameterName",
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
                                                     "value",
                                                     "attribute",
                                                     "attVal",
                                                     "boolRule",
                                                     "condition",
                                                     "unaryOperator",
                                                     "comparisonOperator",
                                                     "logicalOperator",
                                                     "bitOperator",
                                                     "mathOperator",
                                                     "sink",
                                                     "parameters",
                                                     "parameter",
                                                     "fileName",
                                                     "topic",
                                                     "address"};

std::vector<std::string> NesCEPParser::_literalNames = {"",
                                                        "'SLIDING'",
                                                        "'INTERVAL'",
                                                        "'TUMBLING'",
                                                        "'SIZE'",
                                                        "'SLIDE'",
                                                        "'KAFKA'",
                                                        "'FILE'",
                                                        "'NETWORK'",
                                                        "'NULLOUTPUT'",
                                                        "'PRINT'",
                                                        "'-'",
                                                        "'>'",
                                                        "'<'",
                                                        "'&'",
                                                        "'|'",
                                                        "'/'",
                                                        "'%'",
                                                        "'--'",
                                                        "'fileName'",
                                                        "'topic'",
                                                        "'address'",
                                                        "",
                                                        "",
                                                        "",
                                                        "",
                                                        "",
                                                        "",
                                                        "'FROM'",
                                                        "'PATTERN'",
                                                        "'WHERE'",
                                                        "'WITHIN'",
                                                        "'CONSUMING'",
                                                        "'SELECT'",
                                                        "'INTO'",
                                                        "'ALL'",
                                                        "'ANY'",
                                                        "':='",
                                                        "','",
                                                        "'('",
                                                        "')'",
                                                        "'NOT'",
                                                        "'!'",
                                                        "'SEQ'",
                                                        "'NEXT'",
                                                        "'AND'",
                                                        "'OR'",
                                                        "'*'",
                                                        "'+'",
                                                        "':'",
                                                        "'['",
                                                        "']'",
                                                        "'XOR'",
                                                        "'IN'",
                                                        "'IS'",
                                                        "'NULL'",
                                                        "'BETWEEN'",
                                                        "'BINARY'",
                                                        "'TRUE'",
                                                        "'FALSE'",
                                                        "'UNKNOWN'",
                                                        "'QUARTER'",
                                                        "'MONTH'",
                                                        "'DAY'",
                                                        "'HOUR'",
                                                        "'MINUTE'",
                                                        "'WEEK'",
                                                        "'SECOND'",
                                                        "'MICROSECOND'",
                                                        "'AS'",
                                                        "'='",
                                                        "'Kafka'",
                                                        "'File'",
                                                        "'MQTT'",
                                                        "'Network'",
                                                        "'NullOutput'",
                                                        "'OPC'",
                                                        "'Print'",
                                                        "'ZMQ'",
                                                        "'.'",
                                                        "'\"'",
                                                        "'AVG'",
                                                        "'SUM'",
                                                        "'MIN'",
                                                        "'MAX'",
                                                        "'COUNT'",
                                                        "'IF'",
                                                        "'||'",
                                                        "'&&'",
                                                        "'^'",
                                                        "'NONE'"};

std::vector<std::string> NesCEPParser::_symbolicNames = {"",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "",
                                                         "INT",
                                                         "FLOAT",
                                                         "PROTOCOL",
                                                         "FILETYPE",
                                                         "PORT",
                                                         "WS",
                                                         "FROM",
                                                         "PATTERN",
                                                         "WHERE",
                                                         "WITHIN",
                                                         "CONSUMING",
                                                         "SELECT",
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
                                                         "URL",
                                                         "NAME",
                                                         "ID",
                                                         "PATH"};

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
        0x3,   0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964, 0x3,   0x60,  0x227, 0x4,   0x2,   0x9,   0x2,
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
        0x4,   0x2b,   0x9,    0x2b,   0x4,    0x2c,   0x9,    0x2c,   0x4,    0x2d,  0x9,   0x2d,  0x4,   0x2e,  0x9,   0x2e,
        0x4,   0x2f,   0x9,    0x2f,   0x4,    0x30,   0x9,    0x30,   0x4,    0x31,  0x9,   0x31,  0x4,   0x32,  0x9,   0x32,
        0x4,   0x33,   0x9,    0x33,   0x4,    0x34,   0x9,    0x34,   0x4,    0x35,  0x9,   0x35,  0x4,   0x36,  0x9,   0x36,
        0x4,   0x37,   0x9,    0x37,   0x3,    0x2,    0x6,    0x2,    0x70,   0xa,   0x2,   0xd,   0x2,   0xe,   0x2,   0x71,
        0x3,   0x2,    0x3,    0x2,    0x3,    0x3,    0x3,    0x3,    0x3,    0x3,   0x3,   0x3,   0x3,   0x3,   0x3,   0x3,
        0x3,   0x3,    0x3,    0x3,    0x5,    0x3,    0x7e,   0xa,    0x3,    0x3,   0x3,   0x3,   0x3,   0x5,   0x3,   0x82,
        0xa,   0x3,    0x3,    0x3,    0x3,    0x3,    0x5,    0x3,    0x86,   0xa,   0x3,   0x3,   0x3,   0x3,   0x3,   0x5,
        0x3,   0x8a,   0xa,    0x3,    0x3,    0x3,    0x3,    0x3,    0x3,    0x3,   0x3,   0x4,   0x3,   0x4,   0x3,   0x4,
        0x7,   0x4,    0x92,   0xa,    0x4,    0xc,    0x4,    0xe,    0x4,    0x95,  0xb,   0x4,   0x3,   0x5,   0x3,   0x5,
        0x3,   0x5,    0x5,    0x5,    0x9a,   0xa,    0x5,    0x3,    0x6,    0x5,   0x6,   0x9d,  0xa,   0x6,   0x3,   0x6,
        0x3,   0x6,    0x5,    0x6,    0xa1,   0xa,    0x6,    0x3,    0x7,    0x3,   0x7,   0x3,   0x8,   0x3,   0x8,   0x3,
        0x8,   0x3,    0x8,    0x3,    0x8,    0x3,    0x9,    0x3,    0x9,    0x3,   0xa,   0x3,   0xa,   0x3,   0xa,   0x3,
        0xa,   0x3,    0xa,    0x3,    0xa,    0x3,    0xa,    0x3,    0xa,    0x7,   0xa,   0xb4,  0xa,   0xa,   0xc,   0xa,
        0xe,   0xa,    0xb7,   0xb,    0xa,    0x3,    0xb,    0x3,    0xb,    0x3,   0xc,   0x3,   0xc,   0x5,   0xc,   0xbd,
        0xa,   0xc,    0x3,    0xc,    0x3,    0xc,    0x3,    0xd,    0x3,    0xd,   0x3,   0xe,   0x3,   0xe,   0x3,   0xf,
        0x3,   0xf,    0x3,    0xf,    0x3,    0xf,    0x3,    0xf,    0x3,    0xf,   0x7,   0xf,   0xcb,  0xa,   0xf,   0xc,
        0xf,   0xe,    0xf,    0xce,   0xb,    0xf,    0x3,    0xf,    0x3,    0xf,   0x3,   0x10,  0x3,   0x10,  0x3,   0x10,
        0x3,   0x10,   0x3,    0x11,   0x3,    0x11,   0x3,    0x11,   0x7,    0x11,  0xd9,  0xa,   0x11,  0xc,   0x11,  0xe,
        0x11,  0xdc,   0xb,    0x11,   0x3,    0x12,   0x3,    0x12,   0x3,    0x12,  0x3,   0x12,  0x7,   0x12,  0xe2,  0xa,
        0x12,  0xc,    0x12,   0xe,    0x12,   0xe5,   0xb,    0x12,   0x3,    0x13,  0x5,   0x13,  0xe8,  0xa,   0x13,  0x3,
        0x13,  0x3,    0x13,   0x5,    0x13,   0xec,   0xa,    0x13,   0x3,    0x13,  0x3,   0x13,  0x3,   0x13,  0x3,   0x13,
        0x5,   0x13,   0xf2,   0xa,    0x13,   0x3,    0x14,   0x3,    0x14,   0x5,   0x14,  0xf6,  0xa,   0x14,  0x3,   0x15,
        0x3,   0x15,   0x3,    0x15,   0x3,    0x15,   0x5,    0x15,   0xfc,   0xa,   0x15,  0x3,   0x15,  0x3,   0x15,  0x5,
        0x15,  0x100,  0xa,    0x15,   0x3,    0x15,   0x3,    0x15,   0x3,    0x15,  0x5,   0x15,  0x105, 0xa,   0x15,  0x3,
        0x15,  0x3,    0x15,   0x3,    0x15,   0x3,    0x15,   0x3,    0x15,   0x5,   0x15,  0x10c, 0xa,   0x15,  0x3,   0x16,
        0x3,   0x16,   0x3,    0x17,   0x3,    0x17,   0x3,    0x18,   0x5,    0x18,  0x113, 0xa,   0x18,  0x3,   0x18,  0x3,
        0x18,  0x3,    0x19,   0x3,    0x19,   0x3,    0x1a,   0x3,    0x1a,   0x3,   0x1a,  0x5,   0x1a,  0x11c, 0xa,   0x1a,
        0x3,   0x1b,   0x3,    0x1b,   0x5,    0x1b,   0x120,  0xa,    0x1b,   0x3,   0x1c,  0x3,   0x1c,  0x3,   0x1c,  0x5,
        0x1c,  0x125,  0xa,    0x1c,   0x3,    0x1d,   0x3,    0x1d,   0x3,    0x1e,  0x5,   0x1e,  0x12a, 0xa,   0x1e,  0x3,
        0x1e,  0x3,    0x1e,   0x3,    0x1f,   0x3,    0x1f,   0x3,    0x1f,   0x3,   0x1f,  0x3,   0x1f,  0x3,   0x1f,  0x5,
        0x1f,  0x134,  0xa,    0x1f,   0x3,    0x20,   0x3,    0x20,   0x3,    0x20,  0x7,   0x20,  0x139, 0xa,   0x20,  0xc,
        0x20,  0xe,    0x20,   0x13c,  0xb,    0x20,   0x3,    0x21,   0x3,    0x21,  0x3,   0x21,  0x3,   0x21,  0x3,   0x21,
        0x3,   0x21,   0x5,    0x21,   0x144,  0xa,    0x21,   0x3,    0x21,   0x3,   0x21,  0x3,   0x21,  0x5,   0x21,  0x149,
        0xa,   0x21,   0x3,    0x21,   0x3,    0x21,   0x3,    0x21,   0x3,    0x21,  0x7,   0x21,  0x14f, 0xa,   0x21,  0xc,
        0x21,  0xe,    0x21,   0x152,  0xb,    0x21,   0x3,    0x22,   0x3,    0x22,  0x3,   0x22,  0x3,   0x22,  0x3,   0x22,
        0x3,   0x22,   0x3,    0x22,   0x3,    0x22,   0x3,    0x22,   0x5,    0x22,  0x15d, 0xa,   0x22,  0x3,   0x22,  0x3,
        0x22,  0x3,    0x22,   0x3,    0x22,   0x3,    0x22,   0x3,    0x22,   0x3,   0x22,  0x3,   0x22,  0x7,   0x22,  0x167,
        0xa,   0x22,   0xc,    0x22,   0xe,    0x22,   0x16a,  0xb,    0x22,   0x3,   0x23,  0x3,   0x23,  0x3,   0x23,  0x3,
        0x23,  0x5,    0x23,   0x170,  0xa,    0x23,   0x3,    0x23,   0x3,    0x23,  0x3,   0x23,  0x3,   0x23,  0x3,   0x23,
        0x3,   0x23,   0x3,    0x23,   0x3,    0x23,   0x7,    0x23,   0x17a,  0xa,   0x23,  0xc,   0x23,  0xe,   0x23,  0x17d,
        0xb,   0x23,   0x3,    0x23,   0x3,    0x23,   0x3,    0x23,   0x5,    0x23,  0x182, 0xa,   0x23,  0x3,   0x23,  0x3,
        0x23,  0x3,    0x23,   0x3,    0x23,   0x3,    0x23,   0x3,    0x23,   0x3,   0x23,  0x3,   0x23,  0x7,   0x23,  0x18c,
        0xa,   0x23,   0xc,    0x23,   0xe,    0x23,   0x18f,  0xb,    0x23,   0x3,   0x24,  0x3,   0x24,  0x3,   0x24,  0x3,
        0x24,  0x3,    0x24,   0x3,    0x24,   0x3,    0x24,   0x3,    0x24,   0x5,   0x24,  0x199, 0xa,   0x24,  0x3,   0x24,
        0x3,   0x24,   0x3,    0x24,   0x5,    0x24,   0x19e,  0xa,    0x24,   0x3,   0x25,  0x3,   0x25,  0x3,   0x25,  0x5,
        0x25,  0x1a3,  0xa,    0x25,   0x3,    0x25,   0x3,    0x25,   0x3,    0x26,  0x3,   0x26,  0x3,   0x26,  0x3,   0x26,
        0x3,   0x26,   0x3,    0x26,   0x3,    0x26,   0x5,    0x26,   0x1ae,  0xa,   0x26,  0x5,   0x26,  0x1b0, 0xa,   0x26,
        0x3,   0x27,   0x3,    0x27,   0x3,    0x27,   0x3,    0x27,   0x3,    0x27,  0x3,   0x27,  0x5,   0x27,  0x1b8, 0xa,
        0x27,  0x3,    0x28,   0x3,    0x28,   0x3,    0x28,   0x3,    0x28,   0x3,   0x28,  0x3,   0x28,  0x5,   0x28,  0x1c0,
        0xa,   0x28,   0x3,    0x29,   0x3,    0x29,   0x3,    0x2a,   0x3,    0x2a,  0x3,   0x2a,  0x3,   0x2a,  0x3,   0x2a,
        0x3,   0x2a,   0x5,    0x2a,   0x1ca,  0xa,    0x2a,   0x3,    0x2b,   0x3,   0x2b,  0x3,   0x2c,  0x3,   0x2c,  0x3,
        0x2c,  0x3,    0x2c,   0x3,    0x2c,   0x3,    0x2c,   0x3,    0x2c,   0x3,   0x2c,  0x3,   0x2d,  0x3,   0x2d,  0x3,
        0x2e,  0x3,    0x2e,   0x5,    0x2e,   0x1da,  0xa,    0x2e,   0x3,    0x2e,  0x3,   0x2e,  0x3,   0x2e,  0x3,   0x2e,
        0x3,   0x2e,   0x5,    0x2e,   0x1e1,  0xa,    0x2e,   0x3,    0x2e,   0x3,   0x2e,  0x3,   0x2e,  0x5,   0x2e,  0x1e6,
        0xa,   0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x5,    0x2e,  0x1eb, 0xa,   0x2e,  0x3,   0x2e,  0x3,
        0x2e,  0x3,    0x2e,   0x5,    0x2e,   0x1f0,  0xa,    0x2e,   0x3,    0x2e,  0x3,   0x2e,  0x3,   0x2e,  0x5,   0x2e,
        0x1f5, 0xa,    0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x5,    0x2e,   0x1f9, 0xa,   0x2e,  0x3,   0x2e,  0x3,   0x2e,
        0x5,   0x2e,   0x1fd,  0xa,    0x2e,   0x3,    0x2f,   0x3,    0x2f,   0x3,   0x30,  0x3,   0x30,  0x3,   0x30,  0x3,
        0x30,  0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x5,    0x30,   0x208, 0xa,   0x30,  0x3,   0x31,  0x3,   0x31,
        0x3,   0x32,   0x3,    0x32,   0x3,    0x32,   0x3,    0x32,   0x3,    0x32,  0x3,   0x32,  0x5,   0x32,  0x212, 0xa,
        0x32,  0x3,    0x33,   0x3,    0x33,   0x3,    0x33,   0x3,    0x33,   0x3,   0x33,  0x3,   0x33,  0x7,   0x33,  0x21a,
        0xa,   0x33,   0xc,    0x33,   0xe,    0x33,   0x21d,  0xb,    0x33,   0x3,   0x34,  0x3,   0x34,  0x3,   0x35,  0x3,
        0x35,  0x3,    0x36,   0x3,    0x36,   0x3,    0x37,   0x3,    0x37,   0x3,   0x37,  0x2,   0x5,   0x40,  0x42,  0x44,
        0x38,  0x2,    0x4,    0x6,    0x8,    0xa,    0xc,    0xe,    0x10,   0x12,  0x14,  0x16,  0x18,  0x1a,  0x1c,  0x1e,
        0x20,  0x22,   0x24,   0x26,   0x28,   0x2a,   0x2c,   0x2e,   0x30,   0x32,  0x34,  0x36,  0x38,  0x3a,  0x3c,  0x3e,
        0x40,  0x42,   0x44,   0x46,   0x48,   0x4a,   0x4c,   0x4e,   0x50,   0x52,  0x54,  0x56,  0x58,  0x5a,  0x5c,  0x5e,
        0x60,  0x62,   0x64,   0x66,   0x68,   0x6a,   0x6c,   0x2,    0xd,    0x3,   0x2,   0x3,   0x5,   0x4,   0x2,   0x4,
        0x4,   0x6,    0x7,    0x3,    0x2,    0x3f,   0x46,   0x4,    0x2,    0x25,  0x25,  0x5c,  0x5c,  0x6,   0x2,   0x8,
        0xc,   0x4b,   0x4b,   0x4e,   0x4e,   0x50,   0x50,   0x3,    0x2,    0x3c,  0x3e,  0x3,   0x2,   0x3c,  0x3d,  0x5,
        0x2,   0xd,    0xd,    0x2b,   0x2b,   0x32,   0x32,   0x3,    0x2,    0x59,  0x5b,  0x5,   0x2,   0xd,   0xd,   0x12,
        0x14,  0x31,   0x32,   0x3,    0x2,    0x15,   0x17,   0x2,    0x248,  0x2,   0x6f,  0x3,   0x2,   0x2,   0x2,   0x4,
        0x75,  0x3,    0x2,    0x2,    0x2,    0x6,    0x8e,   0x3,    0x2,    0x2,   0x2,   0x8,   0x96,  0x3,   0x2,   0x2,
        0x2,   0xa,    0x9c,   0x3,    0x2,    0x2,    0x2,    0xc,    0xa2,   0x3,   0x2,   0x2,   0x2,   0xe,   0xa4,  0x3,
        0x2,   0x2,    0x2,    0x10,   0xa9,   0x3,    0x2,    0x2,    0x2,    0x12,  0xab,  0x3,   0x2,   0x2,   0x2,   0x14,
        0xb8,  0x3,    0x2,    0x2,    0x2,    0x16,   0xba,   0x3,    0x2,    0x2,   0x2,   0x18,  0xc0,  0x3,   0x2,   0x2,
        0x2,   0x1a,   0xc2,   0x3,    0x2,    0x2,    0x2,    0x1c,   0xc4,   0x3,   0x2,   0x2,   0x2,   0x1e,  0xd1,  0x3,
        0x2,   0x2,    0x2,    0x20,   0xd5,   0x3,    0x2,    0x2,    0x2,    0x22,  0xdd,  0x3,   0x2,   0x2,   0x2,   0x24,
        0xf1,  0x3,    0x2,    0x2,    0x2,    0x26,   0xf3,   0x3,    0x2,    0x2,   0x2,   0x28,  0x10b, 0x3,   0x2,   0x2,
        0x2,   0x2a,   0x10d,  0x3,    0x2,    0x2,    0x2,    0x2c,   0x10f,  0x3,   0x2,   0x2,   0x2,   0x2e,  0x112, 0x3,
        0x2,   0x2,    0x2,    0x30,   0x116,  0x3,    0x2,    0x2,    0x2,    0x32,  0x11b, 0x3,   0x2,   0x2,   0x2,   0x34,
        0x11f, 0x3,    0x2,    0x2,    0x2,    0x36,   0x124,  0x3,    0x2,    0x2,   0x2,   0x38,  0x126, 0x3,   0x2,   0x2,
        0x2,   0x3a,   0x129,  0x3,    0x2,    0x2,    0x2,    0x3c,   0x133,  0x3,   0x2,   0x2,   0x2,   0x3e,  0x135, 0x3,
        0x2,   0x2,    0x2,    0x40,   0x148,  0x3,    0x2,    0x2,    0x2,    0x42,  0x153, 0x3,   0x2,   0x2,   0x2,   0x44,
        0x181, 0x3,    0x2,    0x2,    0x2,    0x46,   0x19d,  0x3,    0x2,    0x2,   0x2,   0x48,  0x19f, 0x3,   0x2,   0x2,
        0x2,   0x4a,   0x1af,  0x3,    0x2,    0x2,    0x2,    0x4c,   0x1b7,  0x3,   0x2,   0x2,   0x2,   0x4e,  0x1bf, 0x3,
        0x2,   0x2,    0x2,    0x50,   0x1c1,  0x3,    0x2,    0x2,    0x2,    0x52,  0x1c9, 0x3,   0x2,   0x2,   0x2,   0x54,
        0x1cb, 0x3,    0x2,    0x2,    0x2,    0x56,   0x1cd,  0x3,    0x2,    0x2,   0x2,   0x58,  0x1d5, 0x3,   0x2,   0x2,
        0x2,   0x5a,   0x1fc,  0x3,    0x2,    0x2,    0x2,    0x5c,   0x1fe,  0x3,   0x2,   0x2,   0x2,   0x5e,  0x207, 0x3,
        0x2,   0x2,    0x2,    0x60,   0x209,  0x3,    0x2,    0x2,    0x2,    0x62,  0x211, 0x3,   0x2,   0x2,   0x2,   0x64,
        0x213, 0x3,    0x2,    0x2,    0x2,    0x66,   0x21e,  0x3,    0x2,    0x2,   0x2,   0x68,  0x220, 0x3,   0x2,   0x2,
        0x2,   0x6a,   0x222,  0x3,    0x2,    0x2,    0x2,    0x6c,   0x224,  0x3,   0x2,   0x2,   0x2,   0x6e,  0x70,  0x5,
        0x4,   0x3,    0x2,    0x6f,   0x6e,   0x3,    0x2,    0x2,    0x2,    0x70,  0x71,  0x3,   0x2,   0x2,   0x2,   0x71,
        0x6f,  0x3,    0x2,    0x2,    0x2,    0x71,   0x72,   0x3,    0x2,    0x2,   0x2,   0x72,  0x73,  0x3,   0x2,   0x2,
        0x2,   0x73,   0x74,   0x7,    0x2,    0x2,    0x3,    0x74,   0x3,    0x3,   0x2,   0x2,   0x2,   0x75,  0x76,  0x7,
        0x1f,  0x2,    0x2,    0x76,   0x77,   0x7,    0x5e,   0x2,    0x2,    0x77,  0x78,  0x7,   0x27,  0x2,   0x2,   0x78,
        0x79,  0x5,    0xa,    0x6,    0x2,    0x79,   0x7a,   0x7,    0x1e,   0x2,   0x2,   0x7a,  0x7d,  0x5,   0x6,   0x4,
        0x2,   0x7b,   0x7c,   0x7,    0x20,   0x2,    0x2,    0x7c,   0x7e,   0x5,   0xc,   0x7,   0x2,   0x7d,  0x7b,  0x3,
        0x2,   0x2,    0x2,    0x7d,   0x7e,   0x3,    0x2,    0x2,    0x2,    0x7e,  0x81,  0x3,   0x2,   0x2,   0x2,   0x7f,
        0x80,  0x7,    0x21,   0x2,    0x2,    0x80,   0x82,   0x5,    0xe,    0x8,   0x2,   0x81,  0x7f,  0x3,   0x2,   0x2,
        0x2,   0x81,   0x82,   0x3,    0x2,    0x2,    0x2,    0x82,   0x85,   0x3,   0x2,   0x2,   0x2,   0x83,  0x84,  0x7,
        0x22,  0x2,    0x2,    0x84,   0x86,   0x5,    0x1a,   0xe,    0x2,    0x85,  0x83,  0x3,   0x2,   0x2,   0x2,   0x85,
        0x86,  0x3,    0x2,    0x2,    0x2,    0x86,   0x89,   0x3,    0x2,    0x2,   0x2,   0x87,  0x88,  0x7,   0x23,  0x2,
        0x2,   0x88,   0x8a,   0x5,    0x1c,   0xf,    0x2,    0x89,   0x87,   0x3,   0x2,   0x2,   0x2,   0x89,  0x8a,  0x3,
        0x2,   0x2,    0x2,    0x8a,   0x8b,   0x3,    0x2,    0x2,    0x2,    0x8b,  0x8c,  0x7,   0x24,  0x2,   0x2,   0x8c,
        0x8d,  0x5,    0x20,   0x11,   0x2,    0x8d,   0x5,    0x3,    0x2,    0x2,   0x2,   0x8e,  0x93,  0x5,   0x8,   0x5,
        0x2,   0x8f,   0x90,   0x7,    0x28,   0x2,    0x2,    0x90,   0x92,   0x5,   0x8,   0x5,   0x2,   0x91,  0x8f,  0x3,
        0x2,   0x2,    0x2,    0x92,   0x95,   0x3,    0x2,    0x2,    0x2,    0x93,  0x91,  0x3,   0x2,   0x2,   0x2,   0x93,
        0x94,  0x3,    0x2,    0x2,    0x2,    0x94,   0x7,    0x3,    0x2,    0x2,   0x2,   0x95,  0x93,  0x3,   0x2,   0x2,
        0x2,   0x96,   0x99,   0x7,    0x5e,   0x2,    0x2,    0x97,   0x98,   0x7,   0x47,  0x2,   0x2,   0x98,  0x9a,  0x7,
        0x5e,  0x2,    0x2,    0x99,   0x97,   0x3,    0x2,    0x2,    0x2,    0x99,  0x9a,  0x3,   0x2,   0x2,   0x2,   0x9a,
        0x9,   0x3,    0x2,    0x2,    0x2,    0x9b,   0x9d,   0x7,    0x29,   0x2,   0x2,   0x9c,  0x9b,  0x3,   0x2,   0x2,
        0x2,   0x9c,   0x9d,   0x3,    0x2,    0x2,    0x2,    0x9d,   0x9e,   0x3,   0x2,   0x2,   0x2,   0x9e,  0xa0,  0x5,
        0x22,  0x12,   0x2,    0x9f,   0xa1,   0x7,    0x2a,   0x2,    0x2,    0xa0,  0x9f,  0x3,   0x2,   0x2,   0x2,   0xa0,
        0xa1,  0x3,    0x2,    0x2,    0x2,    0xa1,   0xb,    0x3,    0x2,    0x2,   0x2,   0xa2,  0xa3,  0x5,   0x40,  0x21,
        0x2,   0xa3,   0xd,    0x3,    0x2,    0x2,    0x2,    0xa4,   0xa5,   0x5,   0x10,  0x9,   0x2,   0xa5,  0xa6,  0x7,
        0x34,  0x2,    0x2,    0xa6,   0xa7,   0x5,    0x12,   0xa,    0x2,    0xa7,  0xa8,  0x7,   0x35,  0x2,   0x2,   0xa8,
        0xf,   0x3,    0x2,    0x2,    0x2,    0xa9,   0xaa,   0x9,    0x2,    0x2,   0x2,   0xaa,  0x11,  0x3,   0x2,   0x2,
        0x2,   0xab,   0xac,   0x7,    0x5e,   0x2,    0x2,    0xac,   0xad,   0x7,   0x28,  0x2,   0x2,   0xad,  0xae,  0x5,
        0x14,  0xb,    0x2,    0xae,   0xb5,   0x5,    0x16,   0xc,    0x2,    0xaf,  0xb0,  0x7,   0x28,  0x2,   0x2,   0xb0,
        0xb1,  0x5,    0x14,   0xb,    0x2,    0xb1,   0xb2,   0x5,    0x16,   0xc,   0x2,   0xb2,  0xb4,  0x3,   0x2,   0x2,
        0x2,   0xb3,   0xaf,   0x3,    0x2,    0x2,    0x2,    0xb4,   0xb7,   0x3,   0x2,   0x2,   0x2,   0xb5,  0xb3,  0x3,
        0x2,   0x2,    0x2,    0xb5,   0xb6,   0x3,    0x2,    0x2,    0x2,    0xb6,  0x13,  0x3,   0x2,   0x2,   0x2,   0xb7,
        0xb5,  0x3,    0x2,    0x2,    0x2,    0xb8,   0xb9,   0x9,    0x3,    0x2,   0x2,   0xb9,  0x15,  0x3,   0x2,   0x2,
        0x2,   0xba,   0xbc,   0x7,    0x18,   0x2,    0x2,    0xbb,   0xbd,   0x7,   0x1d,  0x2,   0x2,   0xbc,  0xbb,  0x3,
        0x2,   0x2,    0x2,    0xbc,   0xbd,   0x3,    0x2,    0x2,    0x2,    0xbd,  0xbe,  0x3,   0x2,   0x2,   0x2,   0xbe,
        0xbf,  0x5,    0x18,   0xd,    0x2,    0xbf,   0x17,   0x3,    0x2,    0x2,   0x2,   0xc0,  0xc1,  0x9,   0x4,   0x2,
        0x2,   0xc1,   0x19,   0x3,    0x2,    0x2,    0x2,    0xc2,   0xc3,   0x9,   0x5,   0x2,   0x2,   0xc3,  0x1b,  0x3,
        0x2,   0x2,    0x2,    0xc4,   0xc5,   0x7,    0x5e,   0x2,    0x2,    0xc5,  0xc6,  0x7,   0x27,  0x2,   0x2,   0xc6,
        0xc7,  0x7,    0x34,   0x2,    0x2,    0xc7,   0xcc,   0x5,    0x1e,   0x10,  0x2,   0xc8,  0xc9,  0x7,   0x28,  0x2,
        0x2,   0xc9,   0xcb,   0x5,    0x1e,   0x10,   0x2,    0xca,   0xc8,   0x3,   0x2,   0x2,   0x2,   0xcb,  0xce,  0x3,
        0x2,   0x2,    0x2,    0xcc,   0xca,   0x3,    0x2,    0x2,    0x2,    0xcc,  0xcd,  0x3,   0x2,   0x2,   0x2,   0xcd,
        0xcf,  0x3,    0x2,    0x2,    0x2,    0xce,   0xcc,   0x3,    0x2,    0x2,   0x2,   0xcf,  0xd0,  0x7,   0x35,  0x2,
        0x2,   0xd0,   0x1d,   0x3,    0x2,    0x2,    0x2,    0xd1,   0xd2,   0x7,   0x5e,  0x2,   0x2,   0xd2,  0xd3,  0x7,
        0x48,  0x2,    0x2,    0xd3,   0xd4,   0x5,    0x52,   0x2a,   0x2,    0xd4,  0x1f,  0x3,   0x2,   0x2,   0x2,   0xd5,
        0xda,  0x5,    0x62,   0x32,   0x2,    0xd6,   0xd7,   0x7,    0x28,   0x2,   0x2,   0xd7,  0xd9,  0x5,   0x62,  0x32,
        0x2,   0xd8,   0xd6,   0x3,    0x2,    0x2,    0x2,    0xd9,   0xdc,   0x3,   0x2,   0x2,   0x2,   0xda,  0xd8,  0x3,
        0x2,   0x2,    0x2,    0xda,   0xdb,   0x3,    0x2,    0x2,    0x2,    0xdb,  0x21,  0x3,   0x2,   0x2,   0x2,   0xdc,
        0xda,  0x3,    0x2,    0x2,    0x2,    0xdd,   0xe3,   0x5,    0x24,   0x13,  0x2,   0xde,  0xdf,  0x5,   0x32,  0x1a,
        0x2,   0xdf,   0xe0,   0x5,    0x24,   0x13,   0x2,    0xe0,   0xe2,   0x3,   0x2,   0x2,   0x2,   0xe1,  0xde,  0x3,
        0x2,   0x2,    0x2,    0xe2,   0xe5,   0x3,    0x2,    0x2,    0x2,    0xe3,  0xe1,  0x3,   0x2,   0x2,   0x2,   0xe3,
        0xe4,  0x3,    0x2,    0x2,    0x2,    0xe4,   0x23,   0x3,    0x2,    0x2,   0x2,   0xe5,  0xe3,  0x3,   0x2,   0x2,
        0x2,   0xe6,   0xe8,   0x7,    0x2b,   0x2,    0x2,    0xe7,   0xe6,   0x3,   0x2,   0x2,   0x2,   0xe7,  0xe8,  0x3,
        0x2,   0x2,    0x2,    0xe8,   0xe9,   0x3,    0x2,    0x2,    0x2,    0xe9,  0xf2,  0x5,   0x26,  0x14,  0x2,   0xea,
        0xec,  0x7,    0x2b,   0x2,    0x2,    0xeb,   0xea,   0x3,    0x2,    0x2,   0x2,   0xeb,  0xec,  0x3,   0x2,   0x2,
        0x2,   0xec,   0xed,   0x3,    0x2,    0x2,    0x2,    0xed,   0xee,   0x7,   0x29,  0x2,   0x2,   0xee,  0xef,  0x5,
        0x22,  0x12,   0x2,    0xef,   0xf0,   0x7,    0x2a,   0x2,    0x2,    0xf0,  0xf2,  0x3,   0x2,   0x2,   0x2,   0xf1,
        0xe7,  0x3,    0x2,    0x2,    0x2,    0xf1,   0xeb,   0x3,    0x2,    0x2,   0x2,   0xf2,  0x25,  0x3,   0x2,   0x2,
        0x2,   0xf3,   0xf5,   0x7,    0x5e,   0x2,    0x2,    0xf4,   0xf6,   0x5,   0x28,  0x15,  0x2,   0xf5,  0xf4,  0x3,
        0x2,   0x2,    0x2,    0xf5,   0xf6,   0x3,    0x2,    0x2,    0x2,    0xf6,  0x27,  0x3,   0x2,   0x2,   0x2,   0xf7,
        0x10c, 0x7,    0x31,   0x2,    0x2,    0xf8,   0x10c,  0x7,    0x32,   0x2,   0x2,   0xf9,  0xfb,  0x7,   0x34,  0x2,
        0x2,   0xfa,   0xfc,   0x5,    0x2e,   0x18,   0x2,    0xfb,   0xfa,   0x3,   0x2,   0x2,   0x2,   0xfb,  0xfc,  0x3,
        0x2,   0x2,    0x2,    0xfc,   0xfd,   0x3,    0x2,    0x2,    0x2,    0xfd,  0xff,  0x7,   0x18,  0x2,   0x2,   0xfe,
        0x100, 0x7,    0x32,   0x2,    0x2,    0xff,   0xfe,   0x3,    0x2,    0x2,   0x2,   0xff,  0x100, 0x3,   0x2,   0x2,
        0x2,   0x100,  0x101,  0x3,    0x2,    0x2,    0x2,    0x101,  0x10c,  0x7,   0x35,  0x2,   0x2,   0x102, 0x104, 0x7,
        0x34,  0x2,    0x2,    0x103,  0x105,  0x5,    0x2e,   0x18,   0x2,    0x104, 0x103, 0x3,   0x2,   0x2,   0x2,   0x104,
        0x105, 0x3,    0x2,    0x2,    0x2,    0x105,  0x106,  0x3,    0x2,    0x2,   0x2,   0x106, 0x107, 0x5,   0x2c,  0x17,
        0x2,   0x107,  0x108,  0x7,    0x33,   0x2,    0x2,    0x108,  0x109,  0x5,   0x2a,  0x16,  0x2,   0x109, 0x10a, 0x7,
        0x35,  0x2,    0x2,    0x10a,  0x10c,  0x3,    0x2,    0x2,    0x2,    0x10b, 0xf7,  0x3,   0x2,   0x2,   0x2,   0x10b,
        0xf8,  0x3,    0x2,    0x2,    0x2,    0x10b,  0xf9,   0x3,    0x2,    0x2,   0x2,   0x10b, 0x102, 0x3,   0x2,   0x2,
        0x2,   0x10c,  0x29,   0x3,    0x2,    0x2,    0x2,    0x10d,  0x10e,  0x7,   0x18,  0x2,   0x2,   0x10e, 0x2b,  0x3,
        0x2,   0x2,    0x2,    0x10f,  0x110,  0x7,    0x18,   0x2,    0x2,    0x110, 0x2d,  0x3,   0x2,   0x2,   0x2,   0x111,
        0x113, 0x7,    0x26,   0x2,    0x2,    0x112,  0x111,  0x3,    0x2,    0x2,   0x2,   0x112, 0x113, 0x3,   0x2,   0x2,
        0x2,   0x113,  0x114,  0x3,    0x2,    0x2,    0x2,    0x114,  0x115,  0x7,   0x2e,  0x2,   0x2,   0x115, 0x2f,  0x3,
        0x2,   0x2,    0x2,    0x116,  0x117,  0x7,    0x5e,   0x2,    0x2,    0x117, 0x31,  0x3,   0x2,   0x2,   0x2,   0x118,
        0x11c, 0x7,    0x2f,   0x2,    0x2,    0x119,  0x11c,  0x7,    0x30,   0x2,   0x2,   0x11a, 0x11c, 0x5,   0x34,  0x1b,
        0x2,   0x11b,  0x118,  0x3,    0x2,    0x2,    0x2,    0x11b,  0x119,  0x3,   0x2,   0x2,   0x2,   0x11b, 0x11a, 0x3,
        0x2,   0x2,    0x2,    0x11c,  0x33,   0x3,    0x2,    0x2,    0x2,    0x11d, 0x120, 0x7,   0x2d,  0x2,   0x2,   0x11e,
        0x120, 0x5,    0x36,   0x1c,   0x2,    0x11f,  0x11d,  0x3,    0x2,    0x2,   0x2,   0x11f, 0x11e, 0x3,   0x2,   0x2,
        0x2,   0x120,  0x35,   0x3,    0x2,    0x2,    0x2,    0x121,  0x125,  0x7,   0x2e,  0x2,   0x2,   0x122, 0x123, 0x7,
        0x26,  0x2,    0x2,    0x123,  0x125,  0x7,    0x2e,   0x2,    0x2,    0x124, 0x121, 0x3,   0x2,   0x2,   0x2,   0x124,
        0x122, 0x3,    0x2,    0x2,    0x2,    0x125,  0x37,   0x3,    0x2,    0x2,   0x2,   0x126, 0x127, 0x9,   0x6,   0x2,
        0x2,   0x127,  0x39,   0x3,    0x2,    0x2,    0x2,    0x128,  0x12a,  0x7,   0x2b,  0x2,   0x2,   0x129, 0x128, 0x3,
        0x2,   0x2,    0x2,    0x129,  0x12a,  0x3,    0x2,    0x2,    0x2,    0x12a, 0x12b, 0x3,   0x2,   0x2,   0x2,   0x12b,
        0x12c, 0x7,    0x39,   0x2,    0x2,    0x12c,  0x3b,   0x3,    0x2,    0x2,   0x2,   0x12d, 0x12e, 0x7,   0x52,  0x2,
        0x2,   0x12e,  0x12f,  0x7,    0x5e,   0x2,    0x2,    0x12f,  0x134,  0x7,   0x52,  0x2,   0x2,   0x130, 0x134, 0x7,
        0x19,  0x2,    0x2,    0x131,  0x134,  0x7,    0x18,   0x2,    0x2,    0x132, 0x134, 0x7,   0x5e,  0x2,   0x2,   0x133,
        0x12d, 0x3,    0x2,    0x2,    0x2,    0x133,  0x130,  0x3,    0x2,    0x2,   0x2,   0x133, 0x131, 0x3,   0x2,   0x2,
        0x2,   0x133,  0x132,  0x3,    0x2,    0x2,    0x2,    0x134,  0x3d,   0x3,   0x2,   0x2,   0x2,   0x135, 0x13a, 0x5,
        0x40,  0x21,   0x2,    0x136,  0x137,  0x7,    0x28,   0x2,    0x2,    0x137, 0x139, 0x5,   0x40,  0x21,  0x2,   0x138,
        0x136, 0x3,    0x2,    0x2,    0x2,    0x139,  0x13c,  0x3,    0x2,    0x2,   0x2,   0x13a, 0x138, 0x3,   0x2,   0x2,
        0x2,   0x13a,  0x13b,  0x3,    0x2,    0x2,    0x2,    0x13b,  0x3f,   0x3,   0x2,   0x2,   0x2,   0x13c, 0x13a, 0x3,
        0x2,   0x2,    0x2,    0x13d,  0x13e,  0x8,    0x21,   0x1,    0x2,    0x13e, 0x13f, 0x7,   0x2c,  0x2,   0x2,   0x13f,
        0x149, 0x5,    0x40,   0x21,   0x6,    0x140,  0x141,  0x5,    0x42,   0x22,  0x2,   0x141, 0x143, 0x7,   0x38,  0x2,
        0x2,   0x142,  0x144,  0x7,    0x2b,   0x2,    0x2,    0x143,  0x142,  0x3,   0x2,   0x2,   0x2,   0x143, 0x144, 0x3,
        0x2,   0x2,    0x2,    0x144,  0x145,  0x3,    0x2,    0x2,    0x2,    0x145, 0x146, 0x9,   0x7,   0x2,   0x2,   0x146,
        0x149, 0x3,    0x2,    0x2,    0x2,    0x147,  0x149,  0x5,    0x42,   0x22,  0x2,   0x148, 0x13d, 0x3,   0x2,   0x2,
        0x2,   0x148,  0x140,  0x3,    0x2,    0x2,    0x2,    0x148,  0x147,  0x3,   0x2,   0x2,   0x2,   0x149, 0x150, 0x3,
        0x2,   0x2,    0x2,    0x14a,  0x14b,  0xc,    0x5,    0x2,    0x2,    0x14b, 0x14c, 0x5,   0x5c,  0x2f,  0x2,   0x14c,
        0x14d, 0x5,    0x40,   0x21,   0x6,    0x14d,  0x14f,  0x3,    0x2,    0x2,   0x2,   0x14e, 0x14a, 0x3,   0x2,   0x2,
        0x2,   0x14f,  0x152,  0x3,    0x2,    0x2,    0x2,    0x150,  0x14e,  0x3,   0x2,   0x2,   0x2,   0x150, 0x151, 0x3,
        0x2,   0x2,    0x2,    0x151,  0x41,   0x3,    0x2,    0x2,    0x2,    0x152, 0x150, 0x3,   0x2,   0x2,   0x2,   0x153,
        0x154, 0x8,    0x22,   0x1,    0x2,    0x154,  0x155,  0x5,    0x44,   0x23,  0x2,   0x155, 0x168, 0x3,   0x2,   0x2,
        0x2,   0x156,  0x157,  0xc,    0x4,    0x2,    0x2,    0x157,  0x158,  0x5,   0x5a,  0x2e,  0x2,   0x158, 0x159, 0x5,
        0x42,  0x22,   0x5,    0x159,  0x167,  0x3,    0x2,    0x2,    0x2,    0x15a, 0x15c, 0xc,   0x6,   0x2,   0x2,   0x15b,
        0x15d, 0x7,    0x2b,   0x2,    0x2,    0x15c,  0x15b,  0x3,    0x2,    0x2,   0x2,   0x15c, 0x15d, 0x3,   0x2,   0x2,
        0x2,   0x15d,  0x15e,  0x3,    0x2,    0x2,    0x2,    0x15e,  0x15f,  0x7,   0x37,  0x2,   0x2,   0x15f, 0x160, 0x7,
        0x29,  0x2,    0x2,    0x160,  0x161,  0x5,    0x3e,   0x20,   0x2,    0x161, 0x162, 0x7,   0x2a,  0x2,   0x2,   0x162,
        0x167, 0x3,    0x2,    0x2,    0x2,    0x163,  0x164,  0xc,    0x5,    0x2,   0x2,   0x164, 0x165, 0x7,   0x38,  0x2,
        0x2,   0x165,  0x167,  0x5,    0x3a,   0x1e,   0x2,    0x166,  0x156,  0x3,   0x2,   0x2,   0x2,   0x166, 0x15a, 0x3,
        0x2,   0x2,    0x2,    0x166,  0x163,  0x3,    0x2,    0x2,    0x2,    0x167, 0x16a, 0x3,   0x2,   0x2,   0x2,   0x168,
        0x166, 0x3,    0x2,    0x2,    0x2,    0x168,  0x169,  0x3,    0x2,    0x2,   0x2,   0x169, 0x43,  0x3,   0x2,   0x2,
        0x2,   0x16a,  0x168,  0x3,    0x2,    0x2,    0x2,    0x16b,  0x16c,  0x8,   0x23,  0x1,   0x2,   0x16c, 0x182, 0x5,
        0x46,  0x24,   0x2,    0x16d,  0x16f,  0x5,    0x58,   0x2d,   0x2,    0x16e, 0x170, 0x7,   0x1d,  0x2,   0x2,   0x16f,
        0x16e, 0x3,    0x2,    0x2,    0x2,    0x16f,  0x170,  0x3,    0x2,    0x2,   0x2,   0x170, 0x171, 0x3,   0x2,   0x2,
        0x2,   0x171,  0x172,  0x5,    0x44,   0x23,   0x8,    0x172,  0x182,  0x3,   0x2,   0x2,   0x2,   0x173, 0x174, 0x7,
        0x3b,  0x2,    0x2,    0x174,  0x182,  0x5,    0x44,   0x23,   0x7,    0x175, 0x176, 0x7,   0x29,  0x2,   0x2,   0x176,
        0x17b, 0x5,    0x40,   0x21,   0x2,    0x177,  0x178,  0x7,    0x28,   0x2,   0x2,   0x178, 0x17a, 0x5,   0x40,  0x21,
        0x2,   0x179,  0x177,  0x3,    0x2,    0x2,    0x2,    0x17a,  0x17d,  0x3,   0x2,   0x2,   0x2,   0x17b, 0x179, 0x3,
        0x2,   0x2,    0x2,    0x17b,  0x17c,  0x3,    0x2,    0x2,    0x2,    0x17c, 0x17e, 0x3,   0x2,   0x2,   0x2,   0x17d,
        0x17b, 0x3,    0x2,    0x2,    0x2,    0x17e,  0x17f,  0x7,    0x2a,   0x2,   0x2,   0x17f, 0x182, 0x3,   0x2,   0x2,
        0x2,   0x180,  0x182,  0x5,    0x3c,   0x1f,   0x2,    0x181,  0x16b,  0x3,   0x2,   0x2,   0x2,   0x181, 0x16d, 0x3,
        0x2,   0x2,    0x2,    0x181,  0x173,  0x3,    0x2,    0x2,    0x2,    0x181, 0x175, 0x3,   0x2,   0x2,   0x2,   0x181,
        0x180, 0x3,    0x2,    0x2,    0x2,    0x182,  0x18d,  0x3,    0x2,    0x2,   0x2,   0x183, 0x184, 0xc,   0x5,   0x2,
        0x2,   0x184,  0x185,  0x5,    0x5e,   0x30,   0x2,    0x185,  0x186,  0x5,   0x44,  0x23,  0x6,   0x186, 0x18c, 0x3,
        0x2,   0x2,    0x2,    0x187,  0x188,  0xc,    0x4,    0x2,    0x2,    0x188, 0x189, 0x5,   0x60,  0x31,  0x2,   0x189,
        0x18a, 0x5,    0x44,   0x23,   0x5,    0x18a,  0x18c,  0x3,    0x2,    0x2,   0x2,   0x18b, 0x183, 0x3,   0x2,   0x2,
        0x2,   0x18b,  0x187,  0x3,    0x2,    0x2,    0x2,    0x18c,  0x18f,  0x3,   0x2,   0x2,   0x2,   0x18d, 0x18b, 0x3,
        0x2,   0x2,    0x2,    0x18d,  0x18e,  0x3,    0x2,    0x2,    0x2,    0x18e, 0x45,  0x3,   0x2,   0x2,   0x2,   0x18f,
        0x18d, 0x3,    0x2,    0x2,    0x2,    0x190,  0x191,  0x5,    0x4c,   0x27,  0x2,   0x191, 0x192, 0x7,   0x29,  0x2,
        0x2,   0x192,  0x193,  0x5,    0x3e,   0x20,   0x2,    0x193,  0x194,  0x7,   0x2a,  0x2,   0x2,   0x194, 0x19e, 0x3,
        0x2,   0x2,    0x2,    0x195,  0x198,  0x5,    0x48,   0x25,   0x2,    0x196, 0x197, 0x7,   0x51,  0x2,   0x2,   0x197,
        0x199, 0x5,    0x50,   0x29,   0x2,    0x198,  0x196,  0x3,    0x2,    0x2,   0x2,   0x198, 0x199, 0x3,   0x2,   0x2,
        0x2,   0x199,  0x19e,  0x3,    0x2,    0x2,    0x2,    0x19a,  0x19b,  0x7,   0x5e,  0x2,   0x2,   0x19b, 0x19c, 0x7,
        0x51,  0x2,    0x2,    0x19c,  0x19e,  0x5,    0x50,   0x29,   0x2,    0x19d, 0x190, 0x3,   0x2,   0x2,   0x2,   0x19d,
        0x195, 0x3,    0x2,    0x2,    0x2,    0x19d,  0x19a,  0x3,    0x2,    0x2,   0x2,   0x19e, 0x47,  0x3,   0x2,   0x2,
        0x2,   0x19f,  0x1a0,  0x7,    0x5e,   0x2,    0x2,    0x1a0,  0x1a2,  0x7,   0x34,  0x2,   0x2,   0x1a1, 0x1a3, 0x5,
        0x4a,  0x26,   0x2,    0x1a2,  0x1a1,  0x3,    0x2,    0x2,    0x2,    0x1a2, 0x1a3, 0x3,   0x2,   0x2,   0x2,   0x1a3,
        0x1a4, 0x3,    0x2,    0x2,    0x2,    0x1a4,  0x1a5,  0x7,    0x35,   0x2,   0x2,   0x1a5, 0x49,  0x3,   0x2,   0x2,
        0x2,   0x1a6,  0x1a7,  0x5,    0x44,   0x23,   0x2,    0x1a7,  0x1a8,  0x5,   0x60,  0x31,  0x2,   0x1a8, 0x1a9, 0x5,
        0x44,  0x23,   0x2,    0x1a9,  0x1b0,  0x3,    0x2,    0x2,    0x2,    0x1aa, 0x1ad, 0x5,   0x3c,  0x1f,  0x2,   0x1ab,
        0x1ac, 0x7,    0x33,   0x2,    0x2,    0x1ac,  0x1ae,  0x5,    0x3c,   0x1f,  0x2,   0x1ad, 0x1ab, 0x3,   0x2,   0x2,
        0x2,   0x1ad,  0x1ae,  0x3,    0x2,    0x2,    0x2,    0x1ae,  0x1b0,  0x3,   0x2,   0x2,   0x2,   0x1af, 0x1a6, 0x3,
        0x2,   0x2,    0x2,    0x1af,  0x1aa,  0x3,    0x2,    0x2,    0x2,    0x1b0, 0x4b,  0x3,   0x2,   0x2,   0x2,   0x1b1,
        0x1b8, 0x3,    0x2,    0x2,    0x2,    0x1b2,  0x1b8,  0x7,    0x53,   0x2,   0x2,   0x1b3, 0x1b8, 0x7,   0x54,  0x2,
        0x2,   0x1b4,  0x1b8,  0x7,    0x55,   0x2,    0x2,    0x1b5,  0x1b8,  0x7,   0x56,  0x2,   0x2,   0x1b6, 0x1b8, 0x7,
        0x57,  0x2,    0x2,    0x1b7,  0x1b1,  0x3,    0x2,    0x2,    0x2,    0x1b7, 0x1b2, 0x3,   0x2,   0x2,   0x2,   0x1b7,
        0x1b3, 0x3,    0x2,    0x2,    0x2,    0x1b7,  0x1b4,  0x3,    0x2,    0x2,   0x2,   0x1b7, 0x1b5, 0x3,   0x2,   0x2,
        0x2,   0x1b7,  0x1b6,  0x3,    0x2,    0x2,    0x2,    0x1b8,  0x4d,   0x3,   0x2,   0x2,   0x2,   0x1b9, 0x1ba, 0x5,
        0x50,  0x29,   0x2,    0x1ba,  0x1bb,  0x7,    0x51,   0x2,    0x2,    0x1bb, 0x1bc, 0x7,   0x1b,  0x2,   0x2,   0x1bc,
        0x1c0, 0x3,    0x2,    0x2,    0x2,    0x1bd,  0x1c0,  0x5,    0x50,   0x29,  0x2,   0x1be, 0x1c0, 0x7,   0x5d,  0x2,
        0x2,   0x1bf,  0x1b9,  0x3,    0x2,    0x2,    0x2,    0x1bf,  0x1bd,  0x3,   0x2,   0x2,   0x2,   0x1bf, 0x1be, 0x3,
        0x2,   0x2,    0x2,    0x1c0,  0x4f,   0x3,    0x2,    0x2,    0x2,    0x1c1, 0x1c2, 0x7,   0x5e,  0x2,   0x2,   0x1c2,
        0x51,  0x3,    0x2,    0x2,    0x2,    0x1c3,  0x1c4,  0x7,    0x58,   0x2,   0x2,   0x1c4, 0x1ca, 0x5,   0x56,  0x2c,
        0x2,   0x1c5,  0x1ca,  0x5,    0x46,   0x24,   0x2,    0x1c6,  0x1ca,  0x5,   0x26,  0x14,  0x2,   0x1c7, 0x1ca, 0x5,
        0x40,  0x21,   0x2,    0x1c8,  0x1ca,  0x5,    0x54,   0x2b,   0x2,    0x1c9, 0x1c3, 0x3,   0x2,   0x2,   0x2,   0x1c9,
        0x1c5, 0x3,    0x2,    0x2,    0x2,    0x1c9,  0x1c6,  0x3,    0x2,    0x2,   0x2,   0x1c9, 0x1c7, 0x3,   0x2,   0x2,
        0x2,   0x1c9,  0x1c8,  0x3,    0x2,    0x2,    0x2,    0x1ca,  0x53,   0x3,   0x2,   0x2,   0x2,   0x1cb, 0x1cc, 0x9,
        0x8,   0x2,    0x2,    0x1cc,  0x55,   0x3,    0x2,    0x2,    0x2,    0x1cd, 0x1ce, 0x7,   0x29,  0x2,   0x2,   0x1ce,
        0x1cf, 0x5,    0x40,   0x21,   0x2,    0x1cf,  0x1d0,  0x7,    0x28,   0x2,   0x2,   0x1d0, 0x1d1, 0x5,   0x52,  0x2a,
        0x2,   0x1d1,  0x1d2,  0x7,    0x28,   0x2,    0x2,    0x1d2,  0x1d3,  0x5,   0x52,  0x2a,  0x2,   0x1d3, 0x1d4, 0x7,
        0x2a,  0x2,    0x2,    0x1d4,  0x57,   0x3,    0x2,    0x2,    0x2,    0x1d5, 0x1d6, 0x9,   0x9,   0x2,   0x2,   0x1d6,
        0x59,  0x3,    0x2,    0x2,    0x2,    0x1d7,  0x1d9,  0x7,    0x48,   0x2,   0x2,   0x1d8, 0x1da, 0x7,   0x1d,  0x2,
        0x2,   0x1d9,  0x1d8,  0x3,    0x2,    0x2,    0x2,    0x1d9,  0x1da,  0x3,   0x2,   0x2,   0x2,   0x1da, 0x1db, 0x3,
        0x2,   0x2,    0x2,    0x1db,  0x1fd,  0x7,    0x48,   0x2,    0x2,    0x1dc, 0x1fd, 0x7,   0xe,   0x2,   0x2,   0x1dd,
        0x1fd, 0x7,    0xf,    0x2,    0x2,    0x1de,  0x1e0,  0x7,    0xf,    0x2,   0x2,   0x1df, 0x1e1, 0x7,   0x1d,  0x2,
        0x2,   0x1e0,  0x1df,  0x3,    0x2,    0x2,    0x2,    0x1e0,  0x1e1,  0x3,   0x2,   0x2,   0x2,   0x1e1, 0x1e2, 0x3,
        0x2,   0x2,    0x2,    0x1e2,  0x1fd,  0x7,    0x48,   0x2,    0x2,    0x1e3, 0x1e5, 0x7,   0xe,   0x2,   0x2,   0x1e4,
        0x1e6, 0x7,    0x1d,   0x2,    0x2,    0x1e5,  0x1e4,  0x3,    0x2,    0x2,   0x2,   0x1e5, 0x1e6, 0x3,   0x2,   0x2,
        0x2,   0x1e6,  0x1e7,  0x3,    0x2,    0x2,    0x2,    0x1e7,  0x1fd,  0x7,   0x48,  0x2,   0x2,   0x1e8, 0x1ea, 0x7,
        0xf,   0x2,    0x2,    0x1e9,  0x1eb,  0x7,    0x1d,   0x2,    0x2,    0x1ea, 0x1e9, 0x3,   0x2,   0x2,   0x2,   0x1ea,
        0x1eb, 0x3,    0x2,    0x2,    0x2,    0x1eb,  0x1ec,  0x3,    0x2,    0x2,   0x2,   0x1ec, 0x1fd, 0x7,   0xe,   0x2,
        0x2,   0x1ed,  0x1ef,  0x7,    0x2c,   0x2,    0x2,    0x1ee,  0x1f0,  0x7,   0x1d,  0x2,   0x2,   0x1ef, 0x1ee, 0x3,
        0x2,   0x2,    0x2,    0x1ef,  0x1f0,  0x3,    0x2,    0x2,    0x2,    0x1f0, 0x1f1, 0x3,   0x2,   0x2,   0x2,   0x1f1,
        0x1fd, 0x7,    0x48,   0x2,    0x2,    0x1f2,  0x1f4,  0x7,    0xf,    0x2,   0x2,   0x1f3, 0x1f5, 0x7,   0x1d,  0x2,
        0x2,   0x1f4,  0x1f3,  0x3,    0x2,    0x2,    0x2,    0x1f4,  0x1f5,  0x3,   0x2,   0x2,   0x2,   0x1f5, 0x1f6, 0x3,
        0x2,   0x2,    0x2,    0x1f6,  0x1f8,  0x7,    0x48,   0x2,    0x2,    0x1f7, 0x1f9, 0x7,   0x1d,  0x2,   0x2,   0x1f8,
        0x1f7, 0x3,    0x2,    0x2,    0x2,    0x1f8,  0x1f9,  0x3,    0x2,    0x2,   0x2,   0x1f9, 0x1fa, 0x3,   0x2,   0x2,
        0x2,   0x1fa,  0x1fd,  0x7,    0xe,    0x2,    0x2,    0x1fb,  0x1fd,  0x7,   0x48,  0x2,   0x2,   0x1fc, 0x1d7, 0x3,
        0x2,   0x2,    0x2,    0x1fc,  0x1dc,  0x3,    0x2,    0x2,    0x2,    0x1fc, 0x1dd, 0x3,   0x2,   0x2,   0x2,   0x1fc,
        0x1de, 0x3,    0x2,    0x2,    0x2,    0x1fc,  0x1e3,  0x3,    0x2,    0x2,   0x2,   0x1fc, 0x1e8, 0x3,   0x2,   0x2,
        0x2,   0x1fc,  0x1ed,  0x3,    0x2,    0x2,    0x2,    0x1fc,  0x1f2,  0x3,   0x2,   0x2,   0x2,   0x1fc, 0x1fb, 0x3,
        0x2,   0x2,    0x2,    0x1fd,  0x5b,   0x3,    0x2,    0x2,    0x2,    0x1fe, 0x1ff, 0x9,   0xa,   0x2,   0x2,   0x1ff,
        0x5d,  0x3,    0x2,    0x2,    0x2,    0x200,  0x201,  0x7,    0xf,    0x2,   0x2,   0x201, 0x208, 0x7,   0xf,   0x2,
        0x2,   0x202,  0x203,  0x7,    0xe,    0x2,    0x2,    0x203,  0x208,  0x7,   0xe,   0x2,   0x2,   0x204, 0x208, 0x7,
        0x10,  0x2,    0x2,    0x205,  0x208,  0x7,    0x5b,   0x2,    0x2,    0x206, 0x208, 0x7,   0x11,  0x2,   0x2,   0x207,
        0x200, 0x3,    0x2,    0x2,    0x2,    0x207,  0x202,  0x3,    0x2,    0x2,   0x2,   0x207, 0x204, 0x3,   0x2,   0x2,
        0x2,   0x207,  0x205,  0x3,    0x2,    0x2,    0x2,    0x207,  0x206,  0x3,   0x2,   0x2,   0x2,   0x208, 0x5f,  0x3,
        0x2,   0x2,    0x2,    0x209,  0x20a,  0x9,    0xb,    0x2,    0x2,    0x20a, 0x61,  0x3,   0x2,   0x2,   0x2,   0x20b,
        0x20c, 0x5,    0x38,   0x1d,   0x2,    0x20c,  0x20d,  0x7,    0x29,   0x2,   0x2,   0x20d, 0x20e, 0x5,   0x64,  0x33,
        0x2,   0x20e,  0x20f,  0x7,    0x2a,   0x2,    0x2,    0x20f,  0x212,  0x3,   0x2,   0x2,   0x2,   0x210, 0x212, 0x5,
        0x38,  0x1d,   0x2,    0x211,  0x20b,  0x3,    0x2,    0x2,    0x2,    0x211, 0x210, 0x3,   0x2,   0x2,   0x2,   0x212,
        0x63,  0x3,    0x2,    0x2,    0x2,    0x213,  0x214,  0x5,    0x66,   0x34,  0x2,   0x214, 0x21b, 0x5,   0x4e,  0x28,
        0x2,   0x215,  0x216,  0x7,    0x28,   0x2,    0x2,    0x216,  0x217,  0x5,   0x66,  0x34,  0x2,   0x217, 0x218, 0x5,
        0x4e,  0x28,   0x2,    0x218,  0x21a,  0x3,    0x2,    0x2,    0x2,    0x219, 0x215, 0x3,   0x2,   0x2,   0x2,   0x21a,
        0x21d, 0x3,    0x2,    0x2,    0x2,    0x21b,  0x219,  0x3,    0x2,    0x2,   0x2,   0x21b, 0x21c, 0x3,   0x2,   0x2,
        0x2,   0x21c,  0x65,   0x3,    0x2,    0x2,    0x2,    0x21d,  0x21b,  0x3,   0x2,   0x2,   0x2,   0x21e, 0x21f, 0x9,
        0xc,   0x2,    0x2,    0x21f,  0x67,   0x3,    0x2,    0x2,    0x2,    0x220, 0x221, 0x7,   0x15,  0x2,   0x2,   0x221,
        0x69,  0x3,    0x2,    0x2,    0x2,    0x222,  0x223,  0x7,    0x16,   0x2,   0x2,   0x223, 0x6b,  0x3,   0x2,   0x2,
        0x2,   0x224,  0x225,  0x7,    0x17,   0x2,    0x2,    0x225,  0x6d,   0x3,   0x2,   0x2,   0x2,   0x3d,  0x71,  0x7d,
        0x81,  0x85,   0x89,   0x93,   0x99,   0x9c,   0xa0,   0xb5,   0xbc,   0xcc,  0xda,  0xe3,  0xe7,  0xeb,  0xf1,  0xf5,
        0xfb,  0xff,   0x104,  0x10b,  0x112,  0x11b,  0x11f,  0x124,  0x129,  0x133, 0x13a, 0x143, 0x148, 0x150, 0x15c, 0x166,
        0x168, 0x16f,  0x17b,  0x181,  0x18b,  0x18d,  0x198,  0x19d,  0x1a2,  0x1ad, 0x1af, 0x1b7, 0x1bf, 0x1c9, 0x1d9, 0x1e0,
        0x1e5, 0x1ea,  0x1ef,  0x1f4,  0x1f8,  0x1fc,  0x207,  0x211,  0x21b,
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
