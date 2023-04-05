
// Generated from IoTDB/nes-core/src/Parsers/NebulaSQL/gen/NebulaSQL.g4 by ANTLR 4.9.2


#include <Parsers/NebulaSQL/gen/NebulaSQLListener.h>

#include "Util/Logger/Logger.hpp"
#include <Parsers/NebulaSQL/gen/NebulaSQLParser.h>

using namespace antlrcpp;
using namespace antlr4;

NebulaSQLParser::NebulaSQLParser(TokenStream *input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(this, _atn, _decisionToDFA, _sharedContextCache);
}

NebulaSQLParser::~NebulaSQLParser() {
  delete _interpreter;
}

std::string NebulaSQLParser::getGrammarFileName() const {
  return "NebulaSQL.g4";
}

const std::vector<std::string>& NebulaSQLParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& NebulaSQLParser::getVocabulary() const {
  return _vocabulary;
}


//----------------- SingleStatementContext ------------------------------------------------------------------

NebulaSQLParser::SingleStatementContext::SingleStatementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::StatementContext* NebulaSQLParser::SingleStatementContext::statement() {
  return getRuleContext<NebulaSQLParser::StatementContext>(0);
}

tree::TerminalNode* NebulaSQLParser::SingleStatementContext::EOF() {
  return getToken(NebulaSQLParser::EOF, 0);
}


size_t NebulaSQLParser::SingleStatementContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSingleStatement;
}

void NebulaSQLParser::SingleStatementContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSingleStatement(this);
}

void NebulaSQLParser::SingleStatementContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSingleStatement(this);
}

NebulaSQLParser::SingleStatementContext* NebulaSQLParser::singleStatement() {
  SingleStatementContext *_localctx = _tracker.createInstance<SingleStatementContext>(_ctx, getState());
  enterRule(_localctx, 0, NebulaSQLParser::RuleSingleStatement);
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
    statement();
    setState(158);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == NebulaSQLParser::T__0) {
      setState(155);
      match(NebulaSQLParser::T__0);
      setState(160);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(161);
    match(NebulaSQLParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StatementContext ------------------------------------------------------------------

NebulaSQLParser::StatementContext::StatementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::QueryContext* NebulaSQLParser::StatementContext::query() {
  return getRuleContext<NebulaSQLParser::QueryContext>(0);
}


size_t NebulaSQLParser::StatementContext::getRuleIndex() const {
  return NebulaSQLParser::RuleStatement;
}

void NebulaSQLParser::StatementContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterStatement(this);
}

void NebulaSQLParser::StatementContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitStatement(this);
}

NebulaSQLParser::StatementContext* NebulaSQLParser::statement() {
  StatementContext *_localctx = _tracker.createInstance<StatementContext>(_ctx, getState());
  enterRule(_localctx, 2, NebulaSQLParser::RuleStatement);

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
    query();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QueryContext ------------------------------------------------------------------

NebulaSQLParser::QueryContext::QueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::QueryTermContext* NebulaSQLParser::QueryContext::queryTerm() {
  return getRuleContext<NebulaSQLParser::QueryTermContext>(0);
}

NebulaSQLParser::QueryOrganizationContext* NebulaSQLParser::QueryContext::queryOrganization() {
  return getRuleContext<NebulaSQLParser::QueryOrganizationContext>(0);
}


size_t NebulaSQLParser::QueryContext::getRuleIndex() const {
  return NebulaSQLParser::RuleQuery;
}

void NebulaSQLParser::QueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterQuery(this);
}

void NebulaSQLParser::QueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitQuery(this);
}

NebulaSQLParser::QueryContext* NebulaSQLParser::query() {
  QueryContext *_localctx = _tracker.createInstance<QueryContext>(_ctx, getState());
  enterRule(_localctx, 4, NebulaSQLParser::RuleQuery);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(165);
    queryTerm(0);
    setState(166);
    queryOrganization();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QueryOrganizationContext ------------------------------------------------------------------

NebulaSQLParser::QueryOrganizationContext::QueryOrganizationContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::QueryOrganizationContext::ORDER() {
  return getToken(NebulaSQLParser::ORDER, 0);
}

tree::TerminalNode* NebulaSQLParser::QueryOrganizationContext::BY() {
  return getToken(NebulaSQLParser::BY, 0);
}

tree::TerminalNode* NebulaSQLParser::QueryOrganizationContext::LIMIT() {
  return getToken(NebulaSQLParser::LIMIT, 0);
}

tree::TerminalNode* NebulaSQLParser::QueryOrganizationContext::OFFSET() {
  return getToken(NebulaSQLParser::OFFSET, 0);
}

std::vector<NebulaSQLParser::SortItemContext *> NebulaSQLParser::QueryOrganizationContext::sortItem() {
  return getRuleContexts<NebulaSQLParser::SortItemContext>();
}

NebulaSQLParser::SortItemContext* NebulaSQLParser::QueryOrganizationContext::sortItem(size_t i) {
  return getRuleContext<NebulaSQLParser::SortItemContext>(i);
}

std::vector<tree::TerminalNode *> NebulaSQLParser::QueryOrganizationContext::INTEGER_VALUE() {
  return getTokens(NebulaSQLParser::INTEGER_VALUE);
}

tree::TerminalNode* NebulaSQLParser::QueryOrganizationContext::INTEGER_VALUE(size_t i) {
  return getToken(NebulaSQLParser::INTEGER_VALUE, i);
}

tree::TerminalNode* NebulaSQLParser::QueryOrganizationContext::ALL() {
  return getToken(NebulaSQLParser::ALL, 0);
}


size_t NebulaSQLParser::QueryOrganizationContext::getRuleIndex() const {
  return NebulaSQLParser::RuleQueryOrganization;
}

void NebulaSQLParser::QueryOrganizationContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterQueryOrganization(this);
}

void NebulaSQLParser::QueryOrganizationContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitQueryOrganization(this);
}

NebulaSQLParser::QueryOrganizationContext* NebulaSQLParser::queryOrganization() {
  QueryOrganizationContext *_localctx = _tracker.createInstance<QueryOrganizationContext>(_ctx, getState());
  enterRule(_localctx, 6, NebulaSQLParser::RuleQueryOrganization);
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
    setState(178);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::ORDER) {
      setState(168);
      match(NebulaSQLParser::ORDER);
      setState(169);
      match(NebulaSQLParser::BY);
      setState(170);
      dynamic_cast<QueryOrganizationContext *>(_localctx)->sortItemContext = sortItem();
      dynamic_cast<QueryOrganizationContext *>(_localctx)->order.push_back(dynamic_cast<QueryOrganizationContext *>(_localctx)->sortItemContext);
      setState(175);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NebulaSQLParser::T__1) {
        setState(171);
        match(NebulaSQLParser::T__1);
        setState(172);
        dynamic_cast<QueryOrganizationContext *>(_localctx)->sortItemContext = sortItem();
        dynamic_cast<QueryOrganizationContext *>(_localctx)->order.push_back(dynamic_cast<QueryOrganizationContext *>(_localctx)->sortItemContext);
        setState(177);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(185);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::LIMIT) {
      setState(180);
      match(NebulaSQLParser::LIMIT);
      setState(183);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case NebulaSQLParser::ALL: {
          setState(181);
          match(NebulaSQLParser::ALL);
          break;
        }

        case NebulaSQLParser::INTEGER_VALUE: {
          setState(182);
          dynamic_cast<QueryOrganizationContext *>(_localctx)->limit = match(NebulaSQLParser::INTEGER_VALUE);
          break;
        }

      default:
        throw NoViableAltException(this);
      }
    }
    setState(189);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::OFFSET) {
      setState(187);
      match(NebulaSQLParser::OFFSET);
      setState(188);
      dynamic_cast<QueryOrganizationContext *>(_localctx)->offset = match(NebulaSQLParser::INTEGER_VALUE);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QueryTermContext ------------------------------------------------------------------

NebulaSQLParser::QueryTermContext::QueryTermContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::QueryTermContext::getRuleIndex() const {
  return NebulaSQLParser::RuleQueryTerm;
}

void NebulaSQLParser::QueryTermContext::copyFrom(QueryTermContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- PrimaryQueryContext ------------------------------------------------------------------

NebulaSQLParser::QueryPrimaryContext* NebulaSQLParser::PrimaryQueryContext::queryPrimary() {
  return getRuleContext<NebulaSQLParser::QueryPrimaryContext>(0);
}

NebulaSQLParser::PrimaryQueryContext::PrimaryQueryContext(QueryTermContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::PrimaryQueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPrimaryQuery(this);
}
void NebulaSQLParser::PrimaryQueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPrimaryQuery(this);
}
//----------------- SetOperationContext ------------------------------------------------------------------

std::vector<NebulaSQLParser::QueryTermContext *> NebulaSQLParser::SetOperationContext::queryTerm() {
  return getRuleContexts<NebulaSQLParser::QueryTermContext>();
}

NebulaSQLParser::QueryTermContext* NebulaSQLParser::SetOperationContext::queryTerm(size_t i) {
  return getRuleContext<NebulaSQLParser::QueryTermContext>(i);
}

tree::TerminalNode* NebulaSQLParser::SetOperationContext::UNION() {
  return getToken(NebulaSQLParser::UNION, 0);
}

NebulaSQLParser::SetOperationContext::SetOperationContext(QueryTermContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::SetOperationContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSetOperation(this);
}
void NebulaSQLParser::SetOperationContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSetOperation(this);
}

NebulaSQLParser::QueryTermContext* NebulaSQLParser::queryTerm() {
   return queryTerm(0);
}

NebulaSQLParser::QueryTermContext* NebulaSQLParser::queryTerm(int precedence) {
  ParserRuleContext *parentContext = _ctx;
  size_t parentState = getState();
  NebulaSQLParser::QueryTermContext *_localctx = _tracker.createInstance<QueryTermContext>(_ctx, parentState);
  NebulaSQLParser::QueryTermContext *previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by generated code.
  size_t startState = 8;
  enterRecursionRule(_localctx, 8, NebulaSQLParser::RuleQueryTerm, precedence);

    

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
    _localctx = _tracker.createInstance<PrimaryQueryContext>(_localctx);
    _ctx = _localctx;
    previousContext = _localctx;

    setState(192);
    queryPrimary();
    _ctx->stop = _input->LT(-1);
    setState(199);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 6, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        auto newContext = _tracker.createInstance<SetOperationContext>(_tracker.createInstance<QueryTermContext>(parentContext, parentState));
        _localctx = newContext;
        newContext->left = previousContext;
        pushNewRecursionContext(newContext, startState, RuleQueryTerm);
        setState(194);

        if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
        setState(195);
        dynamic_cast<SetOperationContext *>(_localctx)->setoperator = match(NebulaSQLParser::UNION);
        setState(196);
        dynamic_cast<SetOperationContext *>(_localctx)->right = queryTerm(2); 
      }
      setState(201);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 6, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- QueryPrimaryContext ------------------------------------------------------------------

NebulaSQLParser::QueryPrimaryContext::QueryPrimaryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::QueryPrimaryContext::getRuleIndex() const {
  return NebulaSQLParser::RuleQueryPrimary;
}

void NebulaSQLParser::QueryPrimaryContext::copyFrom(QueryPrimaryContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- SubqueryContext ------------------------------------------------------------------

NebulaSQLParser::QueryContext* NebulaSQLParser::SubqueryContext::query() {
  return getRuleContext<NebulaSQLParser::QueryContext>(0);
}

NebulaSQLParser::SubqueryContext::SubqueryContext(QueryPrimaryContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::SubqueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSubquery(this);
}
void NebulaSQLParser::SubqueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSubquery(this);
}
//----------------- QueryPrimaryDefaultContext ------------------------------------------------------------------

NebulaSQLParser::QuerySpecificationContext* NebulaSQLParser::QueryPrimaryDefaultContext::querySpecification() {
  return getRuleContext<NebulaSQLParser::QuerySpecificationContext>(0);
}

NebulaSQLParser::QueryPrimaryDefaultContext::QueryPrimaryDefaultContext(QueryPrimaryContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::QueryPrimaryDefaultContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterQueryPrimaryDefault(this);
}
void NebulaSQLParser::QueryPrimaryDefaultContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitQueryPrimaryDefault(this);
}
//----------------- InlineTableDefault1Context ------------------------------------------------------------------

NebulaSQLParser::InlineTableContext* NebulaSQLParser::InlineTableDefault1Context::inlineTable() {
  return getRuleContext<NebulaSQLParser::InlineTableContext>(0);
}

NebulaSQLParser::InlineTableDefault1Context::InlineTableDefault1Context(QueryPrimaryContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::InlineTableDefault1Context::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterInlineTableDefault1(this);
}
void NebulaSQLParser::InlineTableDefault1Context::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitInlineTableDefault1(this);
}
//----------------- FromStmtContext ------------------------------------------------------------------

NebulaSQLParser::FromStatementContext* NebulaSQLParser::FromStmtContext::fromStatement() {
  return getRuleContext<NebulaSQLParser::FromStatementContext>(0);
}

NebulaSQLParser::FromStmtContext::FromStmtContext(QueryPrimaryContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::FromStmtContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFromStmt(this);
}
void NebulaSQLParser::FromStmtContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFromStmt(this);
}
//----------------- TableContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::TableContext::TABLE() {
  return getToken(NebulaSQLParser::TABLE, 0);
}

NebulaSQLParser::MultipartIdentifierContext* NebulaSQLParser::TableContext::multipartIdentifier() {
  return getRuleContext<NebulaSQLParser::MultipartIdentifierContext>(0);
}

NebulaSQLParser::TableContext::TableContext(QueryPrimaryContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::TableContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTable(this);
}
void NebulaSQLParser::TableContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTable(this);
}
NebulaSQLParser::QueryPrimaryContext* NebulaSQLParser::queryPrimary() {
  QueryPrimaryContext *_localctx = _tracker.createInstance<QueryPrimaryContext>(_ctx, getState());
  enterRule(_localctx, 10, NebulaSQLParser::RuleQueryPrimary);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(211);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NebulaSQLParser::INSERT:
      case NebulaSQLParser::SELECT: {
        _localctx = dynamic_cast<QueryPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::QueryPrimaryDefaultContext>(_localctx));
        enterOuterAlt(_localctx, 1);
        setState(202);
        querySpecification();
        break;
      }

      case NebulaSQLParser::FROM: {
        _localctx = dynamic_cast<QueryPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::FromStmtContext>(_localctx));
        enterOuterAlt(_localctx, 2);
        setState(203);
        fromStatement();
        break;
      }

      case NebulaSQLParser::TABLE: {
        _localctx = dynamic_cast<QueryPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::TableContext>(_localctx));
        enterOuterAlt(_localctx, 3);
        setState(204);
        match(NebulaSQLParser::TABLE);
        setState(205);
        multipartIdentifier();
        break;
      }

      case NebulaSQLParser::VALUES: {
        _localctx = dynamic_cast<QueryPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::InlineTableDefault1Context>(_localctx));
        enterOuterAlt(_localctx, 4);
        setState(206);
        inlineTable();
        break;
      }

      case NebulaSQLParser::T__2: {
        _localctx = dynamic_cast<QueryPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::SubqueryContext>(_localctx));
        enterOuterAlt(_localctx, 5);
        setState(207);
        match(NebulaSQLParser::T__2);
        setState(208);
        query();
        setState(209);
        match(NebulaSQLParser::T__3);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QuerySpecificationContext ------------------------------------------------------------------

NebulaSQLParser::QuerySpecificationContext::QuerySpecificationContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::SelectClauseContext* NebulaSQLParser::QuerySpecificationContext::selectClause() {
  return getRuleContext<NebulaSQLParser::SelectClauseContext>(0);
}

NebulaSQLParser::SinkClauseContext* NebulaSQLParser::QuerySpecificationContext::sinkClause() {
  return getRuleContext<NebulaSQLParser::SinkClauseContext>(0);
}

NebulaSQLParser::FromClauseContext* NebulaSQLParser::QuerySpecificationContext::fromClause() {
  return getRuleContext<NebulaSQLParser::FromClauseContext>(0);
}

NebulaSQLParser::WhereClauseContext* NebulaSQLParser::QuerySpecificationContext::whereClause() {
  return getRuleContext<NebulaSQLParser::WhereClauseContext>(0);
}

NebulaSQLParser::WindowedAggregationClauseContext* NebulaSQLParser::QuerySpecificationContext::windowedAggregationClause() {
  return getRuleContext<NebulaSQLParser::WindowedAggregationClauseContext>(0);
}

NebulaSQLParser::HavingClauseContext* NebulaSQLParser::QuerySpecificationContext::havingClause() {
  return getRuleContext<NebulaSQLParser::HavingClauseContext>(0);
}


size_t NebulaSQLParser::QuerySpecificationContext::getRuleIndex() const {
  return NebulaSQLParser::RuleQuerySpecification;
}

void NebulaSQLParser::QuerySpecificationContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterQuerySpecification(this);
}

void NebulaSQLParser::QuerySpecificationContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitQuerySpecification(this);
}

NebulaSQLParser::QuerySpecificationContext* NebulaSQLParser::querySpecification() {
  QuerySpecificationContext *_localctx = _tracker.createInstance<QuerySpecificationContext>(_ctx, getState());
  enterRule(_localctx, 12, NebulaSQLParser::RuleQuerySpecification);
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
    setState(214);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::INSERT) {
      setState(213);
      sinkClause();
    }
    setState(216);
    selectClause();
    setState(218);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 9, _ctx)) {
    case 1: {
      setState(217);
      fromClause();
      break;
    }

    default:
      break;
    }
    setState(221);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 10, _ctx)) {
    case 1: {
      setState(220);
      whereClause();
      break;
    }

    default:
      break;
    }
    setState(224);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 11, _ctx)) {
    case 1: {
      setState(223);
      windowedAggregationClause();
      break;
    }

    default:
      break;
    }
    setState(227);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 12, _ctx)) {
    case 1: {
      setState(226);
      havingClause();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FromClauseContext ------------------------------------------------------------------

NebulaSQLParser::FromClauseContext::FromClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::FromClauseContext::FROM() {
  return getToken(NebulaSQLParser::FROM, 0);
}

std::vector<NebulaSQLParser::RelationContext *> NebulaSQLParser::FromClauseContext::relation() {
  return getRuleContexts<NebulaSQLParser::RelationContext>();
}

NebulaSQLParser::RelationContext* NebulaSQLParser::FromClauseContext::relation(size_t i) {
  return getRuleContext<NebulaSQLParser::RelationContext>(i);
}


size_t NebulaSQLParser::FromClauseContext::getRuleIndex() const {
  return NebulaSQLParser::RuleFromClause;
}

void NebulaSQLParser::FromClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFromClause(this);
}

void NebulaSQLParser::FromClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFromClause(this);
}

NebulaSQLParser::FromClauseContext* NebulaSQLParser::fromClause() {
  FromClauseContext *_localctx = _tracker.createInstance<FromClauseContext>(_ctx, getState());
  enterRule(_localctx, 14, NebulaSQLParser::RuleFromClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(229);
    match(NebulaSQLParser::FROM);
    setState(230);
    relation();
    setState(235);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 13, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(231);
        match(NebulaSQLParser::T__1);
        setState(232);
        relation(); 
      }
      setState(237);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 13, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RelationContext ------------------------------------------------------------------

NebulaSQLParser::RelationContext::RelationContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::RelationPrimaryContext* NebulaSQLParser::RelationContext::relationPrimary() {
  return getRuleContext<NebulaSQLParser::RelationPrimaryContext>(0);
}

std::vector<NebulaSQLParser::JoinRelationContext *> NebulaSQLParser::RelationContext::joinRelation() {
  return getRuleContexts<NebulaSQLParser::JoinRelationContext>();
}

NebulaSQLParser::JoinRelationContext* NebulaSQLParser::RelationContext::joinRelation(size_t i) {
  return getRuleContext<NebulaSQLParser::JoinRelationContext>(i);
}


size_t NebulaSQLParser::RelationContext::getRuleIndex() const {
  return NebulaSQLParser::RuleRelation;
}

void NebulaSQLParser::RelationContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterRelation(this);
}

void NebulaSQLParser::RelationContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitRelation(this);
}

NebulaSQLParser::RelationContext* NebulaSQLParser::relation() {
  RelationContext *_localctx = _tracker.createInstance<RelationContext>(_ctx, getState());
  enterRule(_localctx, 16, NebulaSQLParser::RuleRelation);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(238);
    relationPrimary();
    setState(242);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 14, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(239);
        joinRelation(); 
      }
      setState(244);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 14, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- JoinRelationContext ------------------------------------------------------------------

NebulaSQLParser::JoinRelationContext::JoinRelationContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::JoinRelationContext::JOIN() {
  return getToken(NebulaSQLParser::JOIN, 0);
}

NebulaSQLParser::RelationPrimaryContext* NebulaSQLParser::JoinRelationContext::relationPrimary() {
  return getRuleContext<NebulaSQLParser::RelationPrimaryContext>(0);
}

NebulaSQLParser::JoinTypeContext* NebulaSQLParser::JoinRelationContext::joinType() {
  return getRuleContext<NebulaSQLParser::JoinTypeContext>(0);
}

NebulaSQLParser::JoinCriteriaContext* NebulaSQLParser::JoinRelationContext::joinCriteria() {
  return getRuleContext<NebulaSQLParser::JoinCriteriaContext>(0);
}

tree::TerminalNode* NebulaSQLParser::JoinRelationContext::NATURAL() {
  return getToken(NebulaSQLParser::NATURAL, 0);
}


size_t NebulaSQLParser::JoinRelationContext::getRuleIndex() const {
  return NebulaSQLParser::RuleJoinRelation;
}

void NebulaSQLParser::JoinRelationContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterJoinRelation(this);
}

void NebulaSQLParser::JoinRelationContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitJoinRelation(this);
}

NebulaSQLParser::JoinRelationContext* NebulaSQLParser::joinRelation() {
  JoinRelationContext *_localctx = _tracker.createInstance<JoinRelationContext>(_ctx, getState());
  enterRule(_localctx, 18, NebulaSQLParser::RuleJoinRelation);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(256);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NebulaSQLParser::INNER:
      case NebulaSQLParser::JOIN: {
        enterOuterAlt(_localctx, 1);
        setState(245);
        joinType();
        setState(246);
        match(NebulaSQLParser::JOIN);
        setState(247);
        dynamic_cast<JoinRelationContext *>(_localctx)->right = relationPrimary();
        setState(249);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 15, _ctx)) {
        case 1: {
          setState(248);
          joinCriteria();
          break;
        }

        default:
          break;
        }
        break;
      }

      case NebulaSQLParser::NATURAL: {
        enterOuterAlt(_localctx, 2);
        setState(251);
        match(NebulaSQLParser::NATURAL);
        setState(252);
        joinType();
        setState(253);
        match(NebulaSQLParser::JOIN);
        setState(254);
        dynamic_cast<JoinRelationContext *>(_localctx)->right = relationPrimary();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- JoinTypeContext ------------------------------------------------------------------

NebulaSQLParser::JoinTypeContext::JoinTypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::JoinTypeContext::INNER() {
  return getToken(NebulaSQLParser::INNER, 0);
}


size_t NebulaSQLParser::JoinTypeContext::getRuleIndex() const {
  return NebulaSQLParser::RuleJoinType;
}

void NebulaSQLParser::JoinTypeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterJoinType(this);
}

void NebulaSQLParser::JoinTypeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitJoinType(this);
}

NebulaSQLParser::JoinTypeContext* NebulaSQLParser::joinType() {
  JoinTypeContext *_localctx = _tracker.createInstance<JoinTypeContext>(_ctx, getState());
  enterRule(_localctx, 20, NebulaSQLParser::RuleJoinType);
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
    setState(259);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::INNER) {
      setState(258);
      match(NebulaSQLParser::INNER);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- JoinCriteriaContext ------------------------------------------------------------------

NebulaSQLParser::JoinCriteriaContext::JoinCriteriaContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::JoinCriteriaContext::ON() {
  return getToken(NebulaSQLParser::ON, 0);
}

NebulaSQLParser::BooleanExpressionContext* NebulaSQLParser::JoinCriteriaContext::booleanExpression() {
  return getRuleContext<NebulaSQLParser::BooleanExpressionContext>(0);
}


size_t NebulaSQLParser::JoinCriteriaContext::getRuleIndex() const {
  return NebulaSQLParser::RuleJoinCriteria;
}

void NebulaSQLParser::JoinCriteriaContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterJoinCriteria(this);
}

void NebulaSQLParser::JoinCriteriaContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitJoinCriteria(this);
}

NebulaSQLParser::JoinCriteriaContext* NebulaSQLParser::joinCriteria() {
  JoinCriteriaContext *_localctx = _tracker.createInstance<JoinCriteriaContext>(_ctx, getState());
  enterRule(_localctx, 22, NebulaSQLParser::RuleJoinCriteria);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(261);
    match(NebulaSQLParser::ON);
    setState(262);
    booleanExpression(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RelationPrimaryContext ------------------------------------------------------------------

NebulaSQLParser::RelationPrimaryContext::RelationPrimaryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::RelationPrimaryContext::getRuleIndex() const {
  return NebulaSQLParser::RuleRelationPrimary;
}

void NebulaSQLParser::RelationPrimaryContext::copyFrom(RelationPrimaryContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- TableValuedFunctionContext ------------------------------------------------------------------

NebulaSQLParser::FunctionTableContext* NebulaSQLParser::TableValuedFunctionContext::functionTable() {
  return getRuleContext<NebulaSQLParser::FunctionTableContext>(0);
}

NebulaSQLParser::TableValuedFunctionContext::TableValuedFunctionContext(RelationPrimaryContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::TableValuedFunctionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTableValuedFunction(this);
}
void NebulaSQLParser::TableValuedFunctionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTableValuedFunction(this);
}
//----------------- InlineTableDefault2Context ------------------------------------------------------------------

NebulaSQLParser::InlineTableContext* NebulaSQLParser::InlineTableDefault2Context::inlineTable() {
  return getRuleContext<NebulaSQLParser::InlineTableContext>(0);
}

NebulaSQLParser::InlineTableDefault2Context::InlineTableDefault2Context(RelationPrimaryContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::InlineTableDefault2Context::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterInlineTableDefault2(this);
}
void NebulaSQLParser::InlineTableDefault2Context::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitInlineTableDefault2(this);
}
//----------------- AliasedRelationContext ------------------------------------------------------------------

NebulaSQLParser::RelationContext* NebulaSQLParser::AliasedRelationContext::relation() {
  return getRuleContext<NebulaSQLParser::RelationContext>(0);
}

NebulaSQLParser::TableAliasContext* NebulaSQLParser::AliasedRelationContext::tableAlias() {
  return getRuleContext<NebulaSQLParser::TableAliasContext>(0);
}

NebulaSQLParser::AliasedRelationContext::AliasedRelationContext(RelationPrimaryContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::AliasedRelationContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAliasedRelation(this);
}
void NebulaSQLParser::AliasedRelationContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAliasedRelation(this);
}
//----------------- AliasedQueryContext ------------------------------------------------------------------

NebulaSQLParser::QueryContext* NebulaSQLParser::AliasedQueryContext::query() {
  return getRuleContext<NebulaSQLParser::QueryContext>(0);
}

NebulaSQLParser::TableAliasContext* NebulaSQLParser::AliasedQueryContext::tableAlias() {
  return getRuleContext<NebulaSQLParser::TableAliasContext>(0);
}

NebulaSQLParser::AliasedQueryContext::AliasedQueryContext(RelationPrimaryContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::AliasedQueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAliasedQuery(this);
}
void NebulaSQLParser::AliasedQueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAliasedQuery(this);
}
//----------------- TableNameContext ------------------------------------------------------------------

NebulaSQLParser::MultipartIdentifierContext* NebulaSQLParser::TableNameContext::multipartIdentifier() {
  return getRuleContext<NebulaSQLParser::MultipartIdentifierContext>(0);
}

NebulaSQLParser::TableAliasContext* NebulaSQLParser::TableNameContext::tableAlias() {
  return getRuleContext<NebulaSQLParser::TableAliasContext>(0);
}

NebulaSQLParser::TableNameContext::TableNameContext(RelationPrimaryContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::TableNameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTableName(this);
}
void NebulaSQLParser::TableNameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTableName(this);
}
NebulaSQLParser::RelationPrimaryContext* NebulaSQLParser::relationPrimary() {
  RelationPrimaryContext *_localctx = _tracker.createInstance<RelationPrimaryContext>(_ctx, getState());
  enterRule(_localctx, 24, NebulaSQLParser::RuleRelationPrimary);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(279);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 18, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<RelationPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::TableNameContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(264);
      multipartIdentifier();
      setState(265);
      tableAlias();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<RelationPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::AliasedQueryContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(267);
      match(NebulaSQLParser::T__2);
      setState(268);
      query();
      setState(269);
      match(NebulaSQLParser::T__3);
      setState(270);
      tableAlias();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<RelationPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::AliasedRelationContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(272);
      match(NebulaSQLParser::T__2);
      setState(273);
      relation();
      setState(274);
      match(NebulaSQLParser::T__3);
      setState(275);
      tableAlias();
      break;
    }

    case 4: {
      _localctx = dynamic_cast<RelationPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::InlineTableDefault2Context>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(277);
      inlineTable();
      break;
    }

    case 5: {
      _localctx = dynamic_cast<RelationPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::TableValuedFunctionContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(278);
      functionTable();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FunctionTableContext ------------------------------------------------------------------

NebulaSQLParser::FunctionTableContext::FunctionTableContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::TableAliasContext* NebulaSQLParser::FunctionTableContext::tableAlias() {
  return getRuleContext<NebulaSQLParser::TableAliasContext>(0);
}

NebulaSQLParser::ErrorCapturingIdentifierContext* NebulaSQLParser::FunctionTableContext::errorCapturingIdentifier() {
  return getRuleContext<NebulaSQLParser::ErrorCapturingIdentifierContext>(0);
}

std::vector<NebulaSQLParser::ExpressionContext *> NebulaSQLParser::FunctionTableContext::expression() {
  return getRuleContexts<NebulaSQLParser::ExpressionContext>();
}

NebulaSQLParser::ExpressionContext* NebulaSQLParser::FunctionTableContext::expression(size_t i) {
  return getRuleContext<NebulaSQLParser::ExpressionContext>(i);
}


size_t NebulaSQLParser::FunctionTableContext::getRuleIndex() const {
  return NebulaSQLParser::RuleFunctionTable;
}

void NebulaSQLParser::FunctionTableContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFunctionTable(this);
}

void NebulaSQLParser::FunctionTableContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFunctionTable(this);
}

NebulaSQLParser::FunctionTableContext* NebulaSQLParser::functionTable() {
  FunctionTableContext *_localctx = _tracker.createInstance<FunctionTableContext>(_ctx, getState());
  enterRule(_localctx, 26, NebulaSQLParser::RuleFunctionTable);
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
    setState(281);
    dynamic_cast<FunctionTableContext *>(_localctx)->funcName = errorCapturingIdentifier();
    setState(282);
    match(NebulaSQLParser::T__2);
    setState(291);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 20, _ctx)) {
    case 1: {
      setState(283);
      expression();
      setState(288);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NebulaSQLParser::T__1) {
        setState(284);
        match(NebulaSQLParser::T__1);
        setState(285);
        expression();
        setState(290);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      break;
    }

    default:
      break;
    }
    setState(293);
    match(NebulaSQLParser::T__3);
    setState(294);
    tableAlias();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FromStatementContext ------------------------------------------------------------------

NebulaSQLParser::FromStatementContext::FromStatementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::FromClauseContext* NebulaSQLParser::FromStatementContext::fromClause() {
  return getRuleContext<NebulaSQLParser::FromClauseContext>(0);
}

std::vector<NebulaSQLParser::FromStatementBodyContext *> NebulaSQLParser::FromStatementContext::fromStatementBody() {
  return getRuleContexts<NebulaSQLParser::FromStatementBodyContext>();
}

NebulaSQLParser::FromStatementBodyContext* NebulaSQLParser::FromStatementContext::fromStatementBody(size_t i) {
  return getRuleContext<NebulaSQLParser::FromStatementBodyContext>(i);
}


size_t NebulaSQLParser::FromStatementContext::getRuleIndex() const {
  return NebulaSQLParser::RuleFromStatement;
}

void NebulaSQLParser::FromStatementContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFromStatement(this);
}

void NebulaSQLParser::FromStatementContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFromStatement(this);
}

NebulaSQLParser::FromStatementContext* NebulaSQLParser::fromStatement() {
  FromStatementContext *_localctx = _tracker.createInstance<FromStatementContext>(_ctx, getState());
  enterRule(_localctx, 28, NebulaSQLParser::RuleFromStatement);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(296);
    fromClause();
    setState(298); 
    _errHandler->sync(this);
    alt = 1;
    do {
      switch (alt) {
        case 1: {
              setState(297);
              fromStatementBody();
              break;
            }

      default:
        throw NoViableAltException(this);
      }
      setState(300); 
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 21, _ctx);
    } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FromStatementBodyContext ------------------------------------------------------------------

NebulaSQLParser::FromStatementBodyContext::FromStatementBodyContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::SelectClauseContext* NebulaSQLParser::FromStatementBodyContext::selectClause() {
  return getRuleContext<NebulaSQLParser::SelectClauseContext>(0);
}

NebulaSQLParser::WhereClauseContext* NebulaSQLParser::FromStatementBodyContext::whereClause() {
  return getRuleContext<NebulaSQLParser::WhereClauseContext>(0);
}

NebulaSQLParser::AggregationClauseContext* NebulaSQLParser::FromStatementBodyContext::aggregationClause() {
  return getRuleContext<NebulaSQLParser::AggregationClauseContext>(0);
}


size_t NebulaSQLParser::FromStatementBodyContext::getRuleIndex() const {
  return NebulaSQLParser::RuleFromStatementBody;
}

void NebulaSQLParser::FromStatementBodyContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFromStatementBody(this);
}

void NebulaSQLParser::FromStatementBodyContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFromStatementBody(this);
}

NebulaSQLParser::FromStatementBodyContext* NebulaSQLParser::fromStatementBody() {
  FromStatementBodyContext *_localctx = _tracker.createInstance<FromStatementBodyContext>(_ctx, getState());
  enterRule(_localctx, 30, NebulaSQLParser::RuleFromStatementBody);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(302);
    selectClause();
    setState(304);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 22, _ctx)) {
    case 1: {
      setState(303);
      whereClause();
      break;
    }

    default:
      break;
    }
    setState(307);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 23, _ctx)) {
    case 1: {
      setState(306);
      aggregationClause();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectClauseContext ------------------------------------------------------------------

NebulaSQLParser::SelectClauseContext::SelectClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::SelectClauseContext::SELECT() {
  return getToken(NebulaSQLParser::SELECT, 0);
}

NebulaSQLParser::NamedExpressionSeqContext* NebulaSQLParser::SelectClauseContext::namedExpressionSeq() {
  return getRuleContext<NebulaSQLParser::NamedExpressionSeqContext>(0);
}

std::vector<NebulaSQLParser::HintContext *> NebulaSQLParser::SelectClauseContext::hint() {
  return getRuleContexts<NebulaSQLParser::HintContext>();
}

NebulaSQLParser::HintContext* NebulaSQLParser::SelectClauseContext::hint(size_t i) {
  return getRuleContext<NebulaSQLParser::HintContext>(i);
}


size_t NebulaSQLParser::SelectClauseContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSelectClause;
}

void NebulaSQLParser::SelectClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSelectClause(this);
}

void NebulaSQLParser::SelectClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSelectClause(this);
}

NebulaSQLParser::SelectClauseContext* NebulaSQLParser::selectClause() {
  SelectClauseContext *_localctx = _tracker.createInstance<SelectClauseContext>(_ctx, getState());
  enterRule(_localctx, 32, NebulaSQLParser::RuleSelectClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(309);
    match(NebulaSQLParser::SELECT);
    setState(313);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 24, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(310);
        dynamic_cast<SelectClauseContext *>(_localctx)->hintContext = hint();
        dynamic_cast<SelectClauseContext *>(_localctx)->hints.push_back(dynamic_cast<SelectClauseContext *>(_localctx)->hintContext); 
      }
      setState(315);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 24, _ctx);
    }
    setState(316);
    namedExpressionSeq();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WhereClauseContext ------------------------------------------------------------------

NebulaSQLParser::WhereClauseContext::WhereClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::WhereClauseContext::WHERE() {
  return getToken(NebulaSQLParser::WHERE, 0);
}

NebulaSQLParser::BooleanExpressionContext* NebulaSQLParser::WhereClauseContext::booleanExpression() {
  return getRuleContext<NebulaSQLParser::BooleanExpressionContext>(0);
}


size_t NebulaSQLParser::WhereClauseContext::getRuleIndex() const {
  return NebulaSQLParser::RuleWhereClause;
}

void NebulaSQLParser::WhereClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterWhereClause(this);
}

void NebulaSQLParser::WhereClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitWhereClause(this);
}

NebulaSQLParser::WhereClauseContext* NebulaSQLParser::whereClause() {
  WhereClauseContext *_localctx = _tracker.createInstance<WhereClauseContext>(_ctx, getState());
  enterRule(_localctx, 34, NebulaSQLParser::RuleWhereClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(318);
    match(NebulaSQLParser::WHERE);
    setState(319);
    booleanExpression(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- HavingClauseContext ------------------------------------------------------------------

NebulaSQLParser::HavingClauseContext::HavingClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::HavingClauseContext::HAVING() {
  return getToken(NebulaSQLParser::HAVING, 0);
}

NebulaSQLParser::BooleanExpressionContext* NebulaSQLParser::HavingClauseContext::booleanExpression() {
  return getRuleContext<NebulaSQLParser::BooleanExpressionContext>(0);
}


size_t NebulaSQLParser::HavingClauseContext::getRuleIndex() const {
  return NebulaSQLParser::RuleHavingClause;
}

void NebulaSQLParser::HavingClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterHavingClause(this);
}

void NebulaSQLParser::HavingClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitHavingClause(this);
}

NebulaSQLParser::HavingClauseContext* NebulaSQLParser::havingClause() {
  HavingClauseContext *_localctx = _tracker.createInstance<HavingClauseContext>(_ctx, getState());
  enterRule(_localctx, 36, NebulaSQLParser::RuleHavingClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(321);
    match(NebulaSQLParser::HAVING);
    setState(322);
    booleanExpression(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- InlineTableContext ------------------------------------------------------------------

NebulaSQLParser::InlineTableContext::InlineTableContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::InlineTableContext::VALUES() {
  return getToken(NebulaSQLParser::VALUES, 0);
}

std::vector<NebulaSQLParser::ExpressionContext *> NebulaSQLParser::InlineTableContext::expression() {
  return getRuleContexts<NebulaSQLParser::ExpressionContext>();
}

NebulaSQLParser::ExpressionContext* NebulaSQLParser::InlineTableContext::expression(size_t i) {
  return getRuleContext<NebulaSQLParser::ExpressionContext>(i);
}

NebulaSQLParser::TableAliasContext* NebulaSQLParser::InlineTableContext::tableAlias() {
  return getRuleContext<NebulaSQLParser::TableAliasContext>(0);
}


size_t NebulaSQLParser::InlineTableContext::getRuleIndex() const {
  return NebulaSQLParser::RuleInlineTable;
}

void NebulaSQLParser::InlineTableContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterInlineTable(this);
}

void NebulaSQLParser::InlineTableContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitInlineTable(this);
}

NebulaSQLParser::InlineTableContext* NebulaSQLParser::inlineTable() {
  InlineTableContext *_localctx = _tracker.createInstance<InlineTableContext>(_ctx, getState());
  enterRule(_localctx, 38, NebulaSQLParser::RuleInlineTable);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(324);
    match(NebulaSQLParser::VALUES);
    setState(325);
    expression();
    setState(330);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 25, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(326);
        match(NebulaSQLParser::T__1);
        setState(327);
        expression(); 
      }
      setState(332);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 25, _ctx);
    }
    setState(333);
    tableAlias();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableAliasContext ------------------------------------------------------------------

NebulaSQLParser::TableAliasContext::TableAliasContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::StrictIdentifierContext* NebulaSQLParser::TableAliasContext::strictIdentifier() {
  return getRuleContext<NebulaSQLParser::StrictIdentifierContext>(0);
}

tree::TerminalNode* NebulaSQLParser::TableAliasContext::AS() {
  return getToken(NebulaSQLParser::AS, 0);
}

NebulaSQLParser::IdentifierListContext* NebulaSQLParser::TableAliasContext::identifierList() {
  return getRuleContext<NebulaSQLParser::IdentifierListContext>(0);
}


size_t NebulaSQLParser::TableAliasContext::getRuleIndex() const {
  return NebulaSQLParser::RuleTableAlias;
}

void NebulaSQLParser::TableAliasContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTableAlias(this);
}

void NebulaSQLParser::TableAliasContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTableAlias(this);
}

NebulaSQLParser::TableAliasContext* NebulaSQLParser::tableAlias() {
  TableAliasContext *_localctx = _tracker.createInstance<TableAliasContext>(_ctx, getState());
  enterRule(_localctx, 40, NebulaSQLParser::RuleTableAlias);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(342);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 28, _ctx)) {
    case 1: {
      setState(336);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 26, _ctx)) {
      case 1: {
        setState(335);
        match(NebulaSQLParser::AS);
        break;
      }

      default:
        break;
      }
      setState(338);
      strictIdentifier();
      setState(340);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 27, _ctx)) {
      case 1: {
        setState(339);
        identifierList();
        break;
      }

      default:
        break;
      }
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MultipartIdentifierListContext ------------------------------------------------------------------

NebulaSQLParser::MultipartIdentifierListContext::MultipartIdentifierListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<NebulaSQLParser::MultipartIdentifierContext *> NebulaSQLParser::MultipartIdentifierListContext::multipartIdentifier() {
  return getRuleContexts<NebulaSQLParser::MultipartIdentifierContext>();
}

NebulaSQLParser::MultipartIdentifierContext* NebulaSQLParser::MultipartIdentifierListContext::multipartIdentifier(size_t i) {
  return getRuleContext<NebulaSQLParser::MultipartIdentifierContext>(i);
}


size_t NebulaSQLParser::MultipartIdentifierListContext::getRuleIndex() const {
  return NebulaSQLParser::RuleMultipartIdentifierList;
}

void NebulaSQLParser::MultipartIdentifierListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterMultipartIdentifierList(this);
}

void NebulaSQLParser::MultipartIdentifierListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitMultipartIdentifierList(this);
}

NebulaSQLParser::MultipartIdentifierListContext* NebulaSQLParser::multipartIdentifierList() {
  MultipartIdentifierListContext *_localctx = _tracker.createInstance<MultipartIdentifierListContext>(_ctx, getState());
  enterRule(_localctx, 42, NebulaSQLParser::RuleMultipartIdentifierList);
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
    setState(344);
    multipartIdentifier();
    setState(349);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == NebulaSQLParser::T__1) {
      setState(345);
      match(NebulaSQLParser::T__1);
      setState(346);
      multipartIdentifier();
      setState(351);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MultipartIdentifierContext ------------------------------------------------------------------

NebulaSQLParser::MultipartIdentifierContext::MultipartIdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<NebulaSQLParser::ErrorCapturingIdentifierContext *> NebulaSQLParser::MultipartIdentifierContext::errorCapturingIdentifier() {
  return getRuleContexts<NebulaSQLParser::ErrorCapturingIdentifierContext>();
}

NebulaSQLParser::ErrorCapturingIdentifierContext* NebulaSQLParser::MultipartIdentifierContext::errorCapturingIdentifier(size_t i) {
  return getRuleContext<NebulaSQLParser::ErrorCapturingIdentifierContext>(i);
}


size_t NebulaSQLParser::MultipartIdentifierContext::getRuleIndex() const {
  return NebulaSQLParser::RuleMultipartIdentifier;
}

void NebulaSQLParser::MultipartIdentifierContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterMultipartIdentifier(this);
}

void NebulaSQLParser::MultipartIdentifierContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitMultipartIdentifier(this);
}

NebulaSQLParser::MultipartIdentifierContext* NebulaSQLParser::multipartIdentifier() {
  MultipartIdentifierContext *_localctx = _tracker.createInstance<MultipartIdentifierContext>(_ctx, getState());
  enterRule(_localctx, 44, NebulaSQLParser::RuleMultipartIdentifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(352);
    dynamic_cast<MultipartIdentifierContext *>(_localctx)->errorCapturingIdentifierContext = errorCapturingIdentifier();
    dynamic_cast<MultipartIdentifierContext *>(_localctx)->parts.push_back(dynamic_cast<MultipartIdentifierContext *>(_localctx)->errorCapturingIdentifierContext);
    setState(357);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(353);
        match(NebulaSQLParser::T__4);
        setState(354);
        dynamic_cast<MultipartIdentifierContext *>(_localctx)->errorCapturingIdentifierContext = errorCapturingIdentifier();
        dynamic_cast<MultipartIdentifierContext *>(_localctx)->parts.push_back(dynamic_cast<MultipartIdentifierContext *>(_localctx)->errorCapturingIdentifierContext); 
      }
      setState(359);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NamedExpressionContext ------------------------------------------------------------------

NebulaSQLParser::NamedExpressionContext::NamedExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::ExpressionContext* NebulaSQLParser::NamedExpressionContext::expression() {
  return getRuleContext<NebulaSQLParser::ExpressionContext>(0);
}

NebulaSQLParser::IdentifierListContext* NebulaSQLParser::NamedExpressionContext::identifierList() {
  return getRuleContext<NebulaSQLParser::IdentifierListContext>(0);
}

tree::TerminalNode* NebulaSQLParser::NamedExpressionContext::AS() {
  return getToken(NebulaSQLParser::AS, 0);
}

NebulaSQLParser::ErrorCapturingIdentifierContext* NebulaSQLParser::NamedExpressionContext::errorCapturingIdentifier() {
  return getRuleContext<NebulaSQLParser::ErrorCapturingIdentifierContext>(0);
}


size_t NebulaSQLParser::NamedExpressionContext::getRuleIndex() const {
  return NebulaSQLParser::RuleNamedExpression;
}

void NebulaSQLParser::NamedExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNamedExpression(this);
}

void NebulaSQLParser::NamedExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNamedExpression(this);
}

NebulaSQLParser::NamedExpressionContext* NebulaSQLParser::namedExpression() {
  NamedExpressionContext *_localctx = _tracker.createInstance<NamedExpressionContext>(_ctx, getState());
  enterRule(_localctx, 46, NebulaSQLParser::RuleNamedExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(360);
    expression();
    setState(368);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 33, _ctx)) {
    case 1: {
      setState(362);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx)) {
      case 1: {
        setState(361);
        match(NebulaSQLParser::AS);
        break;
      }

      default:
        break;
      }
      setState(366);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 32, _ctx)) {
      case 1: {
        setState(364);
        dynamic_cast<NamedExpressionContext *>(_localctx)->name = errorCapturingIdentifier();
        break;
      }

      case 2: {
        setState(365);
        identifierList();
        break;
      }

      default:
        break;
      }
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdentifierContext ------------------------------------------------------------------

NebulaSQLParser::IdentifierContext::IdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::StrictIdentifierContext* NebulaSQLParser::IdentifierContext::strictIdentifier() {
  return getRuleContext<NebulaSQLParser::StrictIdentifierContext>(0);
}

NebulaSQLParser::StrictNonReservedContext* NebulaSQLParser::IdentifierContext::strictNonReserved() {
  return getRuleContext<NebulaSQLParser::StrictNonReservedContext>(0);
}


size_t NebulaSQLParser::IdentifierContext::getRuleIndex() const {
  return NebulaSQLParser::RuleIdentifier;
}

void NebulaSQLParser::IdentifierContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIdentifier(this);
}

void NebulaSQLParser::IdentifierContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIdentifier(this);
}

NebulaSQLParser::IdentifierContext* NebulaSQLParser::identifier() {
  IdentifierContext *_localctx = _tracker.createInstance<IdentifierContext>(_ctx, getState());
  enterRule(_localctx, 48, NebulaSQLParser::RuleIdentifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(373);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 34, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(370);
      strictIdentifier();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(371);

      if (!(!SQL_standard_keyword_behavior)) throw FailedPredicateException(this, "!SQL_standard_keyword_behavior");
      setState(372);
      strictNonReserved();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StrictIdentifierContext ------------------------------------------------------------------

NebulaSQLParser::StrictIdentifierContext::StrictIdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::StrictIdentifierContext::getRuleIndex() const {
  return NebulaSQLParser::RuleStrictIdentifier;
}

void NebulaSQLParser::StrictIdentifierContext::copyFrom(StrictIdentifierContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- QuotedIdentifierAlternativeContext ------------------------------------------------------------------

NebulaSQLParser::QuotedIdentifierContext* NebulaSQLParser::QuotedIdentifierAlternativeContext::quotedIdentifier() {
  return getRuleContext<NebulaSQLParser::QuotedIdentifierContext>(0);
}

NebulaSQLParser::QuotedIdentifierAlternativeContext::QuotedIdentifierAlternativeContext(StrictIdentifierContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::QuotedIdentifierAlternativeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterQuotedIdentifierAlternative(this);
}
void NebulaSQLParser::QuotedIdentifierAlternativeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitQuotedIdentifierAlternative(this);
}
//----------------- UnquotedIdentifierContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::UnquotedIdentifierContext::IDENTIFIER() {
  return getToken(NebulaSQLParser::IDENTIFIER, 0);
}

NebulaSQLParser::AnsiNonReservedContext* NebulaSQLParser::UnquotedIdentifierContext::ansiNonReserved() {
  return getRuleContext<NebulaSQLParser::AnsiNonReservedContext>(0);
}

NebulaSQLParser::NonReservedContext* NebulaSQLParser::UnquotedIdentifierContext::nonReserved() {
  return getRuleContext<NebulaSQLParser::NonReservedContext>(0);
}

NebulaSQLParser::UnquotedIdentifierContext::UnquotedIdentifierContext(StrictIdentifierContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::UnquotedIdentifierContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterUnquotedIdentifier(this);
}
void NebulaSQLParser::UnquotedIdentifierContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitUnquotedIdentifier(this);
}
NebulaSQLParser::StrictIdentifierContext* NebulaSQLParser::strictIdentifier() {
  StrictIdentifierContext *_localctx = _tracker.createInstance<StrictIdentifierContext>(_ctx, getState());
  enterRule(_localctx, 50, NebulaSQLParser::RuleStrictIdentifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(381);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 35, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<StrictIdentifierContext *>(_tracker.createInstance<NebulaSQLParser::UnquotedIdentifierContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(375);
      match(NebulaSQLParser::IDENTIFIER);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<StrictIdentifierContext *>(_tracker.createInstance<NebulaSQLParser::QuotedIdentifierAlternativeContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(376);
      quotedIdentifier();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<StrictIdentifierContext *>(_tracker.createInstance<NebulaSQLParser::UnquotedIdentifierContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(377);

      if (!(SQL_standard_keyword_behavior)) throw FailedPredicateException(this, "SQL_standard_keyword_behavior");
      setState(378);
      ansiNonReserved();
      break;
    }

    case 4: {
      _localctx = dynamic_cast<StrictIdentifierContext *>(_tracker.createInstance<NebulaSQLParser::UnquotedIdentifierContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(379);

      if (!(!SQL_standard_keyword_behavior)) throw FailedPredicateException(this, "!SQL_standard_keyword_behavior");
      setState(380);
      nonReserved();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QuotedIdentifierContext ------------------------------------------------------------------

NebulaSQLParser::QuotedIdentifierContext::QuotedIdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::QuotedIdentifierContext::BACKQUOTED_IDENTIFIER() {
  return getToken(NebulaSQLParser::BACKQUOTED_IDENTIFIER, 0);
}


size_t NebulaSQLParser::QuotedIdentifierContext::getRuleIndex() const {
  return NebulaSQLParser::RuleQuotedIdentifier;
}

void NebulaSQLParser::QuotedIdentifierContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterQuotedIdentifier(this);
}

void NebulaSQLParser::QuotedIdentifierContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitQuotedIdentifier(this);
}

NebulaSQLParser::QuotedIdentifierContext* NebulaSQLParser::quotedIdentifier() {
  QuotedIdentifierContext *_localctx = _tracker.createInstance<QuotedIdentifierContext>(_ctx, getState());
  enterRule(_localctx, 52, NebulaSQLParser::RuleQuotedIdentifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(383);
    match(NebulaSQLParser::BACKQUOTED_IDENTIFIER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdentifierListContext ------------------------------------------------------------------

NebulaSQLParser::IdentifierListContext::IdentifierListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::IdentifierSeqContext* NebulaSQLParser::IdentifierListContext::identifierSeq() {
  return getRuleContext<NebulaSQLParser::IdentifierSeqContext>(0);
}


size_t NebulaSQLParser::IdentifierListContext::getRuleIndex() const {
  return NebulaSQLParser::RuleIdentifierList;
}

void NebulaSQLParser::IdentifierListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIdentifierList(this);
}

void NebulaSQLParser::IdentifierListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIdentifierList(this);
}

NebulaSQLParser::IdentifierListContext* NebulaSQLParser::identifierList() {
  IdentifierListContext *_localctx = _tracker.createInstance<IdentifierListContext>(_ctx, getState());
  enterRule(_localctx, 54, NebulaSQLParser::RuleIdentifierList);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(385);
    match(NebulaSQLParser::T__2);
    setState(386);
    identifierSeq();
    setState(387);
    match(NebulaSQLParser::T__3);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdentifierSeqContext ------------------------------------------------------------------

NebulaSQLParser::IdentifierSeqContext::IdentifierSeqContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<NebulaSQLParser::ErrorCapturingIdentifierContext *> NebulaSQLParser::IdentifierSeqContext::errorCapturingIdentifier() {
  return getRuleContexts<NebulaSQLParser::ErrorCapturingIdentifierContext>();
}

NebulaSQLParser::ErrorCapturingIdentifierContext* NebulaSQLParser::IdentifierSeqContext::errorCapturingIdentifier(size_t i) {
  return getRuleContext<NebulaSQLParser::ErrorCapturingIdentifierContext>(i);
}


size_t NebulaSQLParser::IdentifierSeqContext::getRuleIndex() const {
  return NebulaSQLParser::RuleIdentifierSeq;
}

void NebulaSQLParser::IdentifierSeqContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIdentifierSeq(this);
}

void NebulaSQLParser::IdentifierSeqContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIdentifierSeq(this);
}

NebulaSQLParser::IdentifierSeqContext* NebulaSQLParser::identifierSeq() {
  IdentifierSeqContext *_localctx = _tracker.createInstance<IdentifierSeqContext>(_ctx, getState());
  enterRule(_localctx, 56, NebulaSQLParser::RuleIdentifierSeq);
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
    setState(389);
    dynamic_cast<IdentifierSeqContext *>(_localctx)->errorCapturingIdentifierContext = errorCapturingIdentifier();
    dynamic_cast<IdentifierSeqContext *>(_localctx)->ident.push_back(dynamic_cast<IdentifierSeqContext *>(_localctx)->errorCapturingIdentifierContext);
    setState(394);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == NebulaSQLParser::T__1) {
      setState(390);
      match(NebulaSQLParser::T__1);
      setState(391);
      dynamic_cast<IdentifierSeqContext *>(_localctx)->errorCapturingIdentifierContext = errorCapturingIdentifier();
      dynamic_cast<IdentifierSeqContext *>(_localctx)->ident.push_back(dynamic_cast<IdentifierSeqContext *>(_localctx)->errorCapturingIdentifierContext);
      setState(396);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ErrorCapturingIdentifierContext ------------------------------------------------------------------

NebulaSQLParser::ErrorCapturingIdentifierContext::ErrorCapturingIdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::IdentifierContext* NebulaSQLParser::ErrorCapturingIdentifierContext::identifier() {
  return getRuleContext<NebulaSQLParser::IdentifierContext>(0);
}

NebulaSQLParser::ErrorCapturingIdentifierExtraContext* NebulaSQLParser::ErrorCapturingIdentifierContext::errorCapturingIdentifierExtra() {
  return getRuleContext<NebulaSQLParser::ErrorCapturingIdentifierExtraContext>(0);
}


size_t NebulaSQLParser::ErrorCapturingIdentifierContext::getRuleIndex() const {
  return NebulaSQLParser::RuleErrorCapturingIdentifier;
}

void NebulaSQLParser::ErrorCapturingIdentifierContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterErrorCapturingIdentifier(this);
}

void NebulaSQLParser::ErrorCapturingIdentifierContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitErrorCapturingIdentifier(this);
}

NebulaSQLParser::ErrorCapturingIdentifierContext* NebulaSQLParser::errorCapturingIdentifier() {
  ErrorCapturingIdentifierContext *_localctx = _tracker.createInstance<ErrorCapturingIdentifierContext>(_ctx, getState());
  enterRule(_localctx, 58, NebulaSQLParser::RuleErrorCapturingIdentifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(397);
    identifier();
    setState(398);
    errorCapturingIdentifierExtra();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ErrorCapturingIdentifierExtraContext ------------------------------------------------------------------

NebulaSQLParser::ErrorCapturingIdentifierExtraContext::ErrorCapturingIdentifierExtraContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::ErrorCapturingIdentifierExtraContext::getRuleIndex() const {
  return NebulaSQLParser::RuleErrorCapturingIdentifierExtra;
}

void NebulaSQLParser::ErrorCapturingIdentifierExtraContext::copyFrom(ErrorCapturingIdentifierExtraContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ErrorIdentContext ------------------------------------------------------------------

std::vector<tree::TerminalNode *> NebulaSQLParser::ErrorIdentContext::MINUS() {
  return getTokens(NebulaSQLParser::MINUS);
}

tree::TerminalNode* NebulaSQLParser::ErrorIdentContext::MINUS(size_t i) {
  return getToken(NebulaSQLParser::MINUS, i);
}

std::vector<NebulaSQLParser::IdentifierContext *> NebulaSQLParser::ErrorIdentContext::identifier() {
  return getRuleContexts<NebulaSQLParser::IdentifierContext>();
}

NebulaSQLParser::IdentifierContext* NebulaSQLParser::ErrorIdentContext::identifier(size_t i) {
  return getRuleContext<NebulaSQLParser::IdentifierContext>(i);
}

NebulaSQLParser::ErrorIdentContext::ErrorIdentContext(ErrorCapturingIdentifierExtraContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::ErrorIdentContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterErrorIdent(this);
}
void NebulaSQLParser::ErrorIdentContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitErrorIdent(this);
}
//----------------- RealIdentContext ------------------------------------------------------------------

NebulaSQLParser::RealIdentContext::RealIdentContext(ErrorCapturingIdentifierExtraContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::RealIdentContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterRealIdent(this);
}
void NebulaSQLParser::RealIdentContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitRealIdent(this);
}
NebulaSQLParser::ErrorCapturingIdentifierExtraContext* NebulaSQLParser::errorCapturingIdentifierExtra() {
  ErrorCapturingIdentifierExtraContext *_localctx = _tracker.createInstance<ErrorCapturingIdentifierExtraContext>(_ctx, getState());
  enterRule(_localctx, 60, NebulaSQLParser::RuleErrorCapturingIdentifierExtra);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(407);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 38, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ErrorCapturingIdentifierExtraContext *>(_tracker.createInstance<NebulaSQLParser::ErrorIdentContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(402); 
      _errHandler->sync(this);
      alt = 1;
      do {
        switch (alt) {
          case 1: {
                setState(400);
                match(NebulaSQLParser::MINUS);
                setState(401);
                identifier();
                break;
              }

        default:
          throw NoViableAltException(this);
        }
        setState(404); 
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 37, _ctx);
      } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ErrorCapturingIdentifierExtraContext *>(_tracker.createInstance<NebulaSQLParser::RealIdentContext>(_localctx));
      enterOuterAlt(_localctx, 2);

      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NamedExpressionSeqContext ------------------------------------------------------------------

NebulaSQLParser::NamedExpressionSeqContext::NamedExpressionSeqContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<NebulaSQLParser::NamedExpressionContext *> NebulaSQLParser::NamedExpressionSeqContext::namedExpression() {
  return getRuleContexts<NebulaSQLParser::NamedExpressionContext>();
}

NebulaSQLParser::NamedExpressionContext* NebulaSQLParser::NamedExpressionSeqContext::namedExpression(size_t i) {
  return getRuleContext<NebulaSQLParser::NamedExpressionContext>(i);
}


size_t NebulaSQLParser::NamedExpressionSeqContext::getRuleIndex() const {
  return NebulaSQLParser::RuleNamedExpressionSeq;
}

void NebulaSQLParser::NamedExpressionSeqContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNamedExpressionSeq(this);
}

void NebulaSQLParser::NamedExpressionSeqContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNamedExpressionSeq(this);
}

NebulaSQLParser::NamedExpressionSeqContext* NebulaSQLParser::namedExpressionSeq() {
  NamedExpressionSeqContext *_localctx = _tracker.createInstance<NamedExpressionSeqContext>(_ctx, getState());
  enterRule(_localctx, 62, NebulaSQLParser::RuleNamedExpressionSeq);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(409);
    namedExpression();
    setState(414);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 39, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(410);
        match(NebulaSQLParser::T__1);
        setState(411);
        namedExpression(); 
      }
      setState(416);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 39, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExpressionContext ------------------------------------------------------------------

NebulaSQLParser::ExpressionContext::ExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::BooleanExpressionContext* NebulaSQLParser::ExpressionContext::booleanExpression() {
  return getRuleContext<NebulaSQLParser::BooleanExpressionContext>(0);
}


size_t NebulaSQLParser::ExpressionContext::getRuleIndex() const {
  return NebulaSQLParser::RuleExpression;
}

void NebulaSQLParser::ExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpression(this);
}

void NebulaSQLParser::ExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpression(this);
}

NebulaSQLParser::ExpressionContext* NebulaSQLParser::expression() {
  ExpressionContext *_localctx = _tracker.createInstance<ExpressionContext>(_ctx, getState());
  enterRule(_localctx, 64, NebulaSQLParser::RuleExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(417);
    booleanExpression(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BooleanExpressionContext ------------------------------------------------------------------

NebulaSQLParser::BooleanExpressionContext::BooleanExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::BooleanExpressionContext::getRuleIndex() const {
  return NebulaSQLParser::RuleBooleanExpression;
}

void NebulaSQLParser::BooleanExpressionContext::copyFrom(BooleanExpressionContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- LogicalNotContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::LogicalNotContext::NOT() {
  return getToken(NebulaSQLParser::NOT, 0);
}

NebulaSQLParser::BooleanExpressionContext* NebulaSQLParser::LogicalNotContext::booleanExpression() {
  return getRuleContext<NebulaSQLParser::BooleanExpressionContext>(0);
}

NebulaSQLParser::LogicalNotContext::LogicalNotContext(BooleanExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::LogicalNotContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLogicalNot(this);
}
void NebulaSQLParser::LogicalNotContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLogicalNot(this);
}
//----------------- PredicatedContext ------------------------------------------------------------------

NebulaSQLParser::ValueExpressionContext* NebulaSQLParser::PredicatedContext::valueExpression() {
  return getRuleContext<NebulaSQLParser::ValueExpressionContext>(0);
}

NebulaSQLParser::PredicateContext* NebulaSQLParser::PredicatedContext::predicate() {
  return getRuleContext<NebulaSQLParser::PredicateContext>(0);
}

NebulaSQLParser::PredicatedContext::PredicatedContext(BooleanExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::PredicatedContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPredicated(this);
}
void NebulaSQLParser::PredicatedContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPredicated(this);
}
//----------------- ExistsContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::ExistsContext::EXISTS() {
  return getToken(NebulaSQLParser::EXISTS, 0);
}

NebulaSQLParser::QueryContext* NebulaSQLParser::ExistsContext::query() {
  return getRuleContext<NebulaSQLParser::QueryContext>(0);
}

NebulaSQLParser::ExistsContext::ExistsContext(BooleanExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::ExistsContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExists(this);
}
void NebulaSQLParser::ExistsContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExists(this);
}
//----------------- LogicalBinaryContext ------------------------------------------------------------------

std::vector<NebulaSQLParser::BooleanExpressionContext *> NebulaSQLParser::LogicalBinaryContext::booleanExpression() {
  return getRuleContexts<NebulaSQLParser::BooleanExpressionContext>();
}

NebulaSQLParser::BooleanExpressionContext* NebulaSQLParser::LogicalBinaryContext::booleanExpression(size_t i) {
  return getRuleContext<NebulaSQLParser::BooleanExpressionContext>(i);
}

tree::TerminalNode* NebulaSQLParser::LogicalBinaryContext::AND() {
  return getToken(NebulaSQLParser::AND, 0);
}

tree::TerminalNode* NebulaSQLParser::LogicalBinaryContext::OR() {
  return getToken(NebulaSQLParser::OR, 0);
}

NebulaSQLParser::LogicalBinaryContext::LogicalBinaryContext(BooleanExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::LogicalBinaryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLogicalBinary(this);
}
void NebulaSQLParser::LogicalBinaryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLogicalBinary(this);
}

NebulaSQLParser::BooleanExpressionContext* NebulaSQLParser::booleanExpression() {
   return booleanExpression(0);
}

NebulaSQLParser::BooleanExpressionContext* NebulaSQLParser::booleanExpression(int precedence) {
  ParserRuleContext *parentContext = _ctx;
  size_t parentState = getState();
  NebulaSQLParser::BooleanExpressionContext *_localctx = _tracker.createInstance<BooleanExpressionContext>(_ctx, parentState);
  NebulaSQLParser::BooleanExpressionContext *previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by generated code.
  size_t startState = 66;
  enterRecursionRule(_localctx, 66, NebulaSQLParser::RuleBooleanExpression, precedence);

    

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
    setState(431);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 41, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<LogicalNotContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(420);
      match(NebulaSQLParser::NOT);
      setState(421);
      booleanExpression(5);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<ExistsContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(422);
      match(NebulaSQLParser::EXISTS);
      setState(423);
      match(NebulaSQLParser::T__2);
      setState(424);
      query();
      setState(425);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<PredicatedContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(427);
      valueExpression(0);
      setState(429);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 40, _ctx)) {
      case 1: {
        setState(428);
        predicate();
        break;
      }

      default:
        break;
      }
      break;
    }

    default:
      break;
    }
    _ctx->stop = _input->LT(-1);
    setState(441);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 43, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(439);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 42, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<LogicalBinaryContext>(_tracker.createInstance<BooleanExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleBooleanExpression);
          setState(433);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(434);
          dynamic_cast<LogicalBinaryContext *>(_localctx)->op = match(NebulaSQLParser::AND);
          setState(435);
          dynamic_cast<LogicalBinaryContext *>(_localctx)->right = booleanExpression(3);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<LogicalBinaryContext>(_tracker.createInstance<BooleanExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleBooleanExpression);
          setState(436);

          if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(437);
          dynamic_cast<LogicalBinaryContext *>(_localctx)->op = match(NebulaSQLParser::OR);
          setState(438);
          dynamic_cast<LogicalBinaryContext *>(_localctx)->right = booleanExpression(2);
          break;
        }

        default:
          break;
        } 
      }
      setState(443);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 43, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- WindowedAggregationClauseContext ------------------------------------------------------------------

NebulaSQLParser::WindowedAggregationClauseContext::WindowedAggregationClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::AggregationClauseContext* NebulaSQLParser::WindowedAggregationClauseContext::aggregationClause() {
  return getRuleContext<NebulaSQLParser::AggregationClauseContext>(0);
}

NebulaSQLParser::WindowClauseContext* NebulaSQLParser::WindowedAggregationClauseContext::windowClause() {
  return getRuleContext<NebulaSQLParser::WindowClauseContext>(0);
}

NebulaSQLParser::WatermarkClauseContext* NebulaSQLParser::WindowedAggregationClauseContext::watermarkClause() {
  return getRuleContext<NebulaSQLParser::WatermarkClauseContext>(0);
}


size_t NebulaSQLParser::WindowedAggregationClauseContext::getRuleIndex() const {
  return NebulaSQLParser::RuleWindowedAggregationClause;
}

void NebulaSQLParser::WindowedAggregationClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterWindowedAggregationClause(this);
}

void NebulaSQLParser::WindowedAggregationClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitWindowedAggregationClause(this);
}

NebulaSQLParser::WindowedAggregationClauseContext* NebulaSQLParser::windowedAggregationClause() {
  WindowedAggregationClauseContext *_localctx = _tracker.createInstance<WindowedAggregationClauseContext>(_ctx, getState());
  enterRule(_localctx, 68, NebulaSQLParser::RuleWindowedAggregationClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(445);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 44, _ctx)) {
    case 1: {
      setState(444);
      aggregationClause();
      break;
    }

    default:
      break;
    }
    setState(448);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 45, _ctx)) {
    case 1: {
      setState(447);
      windowClause();
      break;
    }

    default:
      break;
    }
    setState(451);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 46, _ctx)) {
    case 1: {
      setState(450);
      watermarkClause();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AggregationClauseContext ------------------------------------------------------------------

NebulaSQLParser::AggregationClauseContext::AggregationClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::AggregationClauseContext::GROUP() {
  return getToken(NebulaSQLParser::GROUP, 0);
}

tree::TerminalNode* NebulaSQLParser::AggregationClauseContext::BY() {
  return getToken(NebulaSQLParser::BY, 0);
}

std::vector<NebulaSQLParser::ExpressionContext *> NebulaSQLParser::AggregationClauseContext::expression() {
  return getRuleContexts<NebulaSQLParser::ExpressionContext>();
}

NebulaSQLParser::ExpressionContext* NebulaSQLParser::AggregationClauseContext::expression(size_t i) {
  return getRuleContext<NebulaSQLParser::ExpressionContext>(i);
}

tree::TerminalNode* NebulaSQLParser::AggregationClauseContext::WITH() {
  return getToken(NebulaSQLParser::WITH, 0);
}

tree::TerminalNode* NebulaSQLParser::AggregationClauseContext::SETS() {
  return getToken(NebulaSQLParser::SETS, 0);
}

std::vector<NebulaSQLParser::GroupingSetContext *> NebulaSQLParser::AggregationClauseContext::groupingSet() {
  return getRuleContexts<NebulaSQLParser::GroupingSetContext>();
}

NebulaSQLParser::GroupingSetContext* NebulaSQLParser::AggregationClauseContext::groupingSet(size_t i) {
  return getRuleContext<NebulaSQLParser::GroupingSetContext>(i);
}

tree::TerminalNode* NebulaSQLParser::AggregationClauseContext::ROLLUP() {
  return getToken(NebulaSQLParser::ROLLUP, 0);
}

tree::TerminalNode* NebulaSQLParser::AggregationClauseContext::CUBE() {
  return getToken(NebulaSQLParser::CUBE, 0);
}

tree::TerminalNode* NebulaSQLParser::AggregationClauseContext::GROUPING() {
  return getToken(NebulaSQLParser::GROUPING, 0);
}


size_t NebulaSQLParser::AggregationClauseContext::getRuleIndex() const {
  return NebulaSQLParser::RuleAggregationClause;
}

void NebulaSQLParser::AggregationClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAggregationClause(this);
}

void NebulaSQLParser::AggregationClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAggregationClause(this);
}

NebulaSQLParser::AggregationClauseContext* NebulaSQLParser::aggregationClause() {
  AggregationClauseContext *_localctx = _tracker.createInstance<AggregationClauseContext>(_ctx, getState());
  enterRule(_localctx, 70, NebulaSQLParser::RuleAggregationClause);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(497);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 51, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(453);
      match(NebulaSQLParser::GROUP);
      setState(454);
      match(NebulaSQLParser::BY);
      setState(455);
      dynamic_cast<AggregationClauseContext *>(_localctx)->expressionContext = expression();
      dynamic_cast<AggregationClauseContext *>(_localctx)->groupingExpressions.push_back(dynamic_cast<AggregationClauseContext *>(_localctx)->expressionContext);
      setState(460);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 47, _ctx);
      while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
        if (alt == 1) {
          setState(456);
          match(NebulaSQLParser::T__1);
          setState(457);
          dynamic_cast<AggregationClauseContext *>(_localctx)->expressionContext = expression();
          dynamic_cast<AggregationClauseContext *>(_localctx)->groupingExpressions.push_back(dynamic_cast<AggregationClauseContext *>(_localctx)->expressionContext); 
        }
        setState(462);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 47, _ctx);
      }
      setState(480);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 49, _ctx)) {
      case 1: {
        setState(463);
        match(NebulaSQLParser::WITH);
        setState(464);
        dynamic_cast<AggregationClauseContext *>(_localctx)->kind = match(NebulaSQLParser::ROLLUP);
        break;
      }

      case 2: {
        setState(465);
        match(NebulaSQLParser::WITH);
        setState(466);
        dynamic_cast<AggregationClauseContext *>(_localctx)->kind = match(NebulaSQLParser::CUBE);
        break;
      }

      case 3: {
        setState(467);
        dynamic_cast<AggregationClauseContext *>(_localctx)->kind = match(NebulaSQLParser::GROUPING);
        setState(468);
        match(NebulaSQLParser::SETS);
        setState(469);
        match(NebulaSQLParser::T__2);
        setState(470);
        groupingSet();
        setState(475);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NebulaSQLParser::T__1) {
          setState(471);
          match(NebulaSQLParser::T__1);
          setState(472);
          groupingSet();
          setState(477);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(478);
        match(NebulaSQLParser::T__3);
        break;
      }

      default:
        break;
      }
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(482);
      match(NebulaSQLParser::GROUP);
      setState(483);
      match(NebulaSQLParser::BY);
      setState(484);
      dynamic_cast<AggregationClauseContext *>(_localctx)->kind = match(NebulaSQLParser::GROUPING);
      setState(485);
      match(NebulaSQLParser::SETS);
      setState(486);
      match(NebulaSQLParser::T__2);
      setState(487);
      groupingSet();
      setState(492);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NebulaSQLParser::T__1) {
        setState(488);
        match(NebulaSQLParser::T__1);
        setState(489);
        groupingSet();
        setState(494);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(495);
      match(NebulaSQLParser::T__3);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupingSetContext ------------------------------------------------------------------

NebulaSQLParser::GroupingSetContext::GroupingSetContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<NebulaSQLParser::ExpressionContext *> NebulaSQLParser::GroupingSetContext::expression() {
  return getRuleContexts<NebulaSQLParser::ExpressionContext>();
}

NebulaSQLParser::ExpressionContext* NebulaSQLParser::GroupingSetContext::expression(size_t i) {
  return getRuleContext<NebulaSQLParser::ExpressionContext>(i);
}


size_t NebulaSQLParser::GroupingSetContext::getRuleIndex() const {
  return NebulaSQLParser::RuleGroupingSet;
}

void NebulaSQLParser::GroupingSetContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupingSet(this);
}

void NebulaSQLParser::GroupingSetContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupingSet(this);
}

NebulaSQLParser::GroupingSetContext* NebulaSQLParser::groupingSet() {
  GroupingSetContext *_localctx = _tracker.createInstance<GroupingSetContext>(_ctx, getState());
  enterRule(_localctx, 72, NebulaSQLParser::RuleGroupingSet);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(512);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 54, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(499);
      match(NebulaSQLParser::T__2);
      setState(508);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 53, _ctx)) {
      case 1: {
        setState(500);
        expression();
        setState(505);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NebulaSQLParser::T__1) {
          setState(501);
          match(NebulaSQLParser::T__1);
          setState(502);
          expression();
          setState(507);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

      default:
        break;
      }
      setState(510);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(511);
      expression();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WindowClauseContext ------------------------------------------------------------------

NebulaSQLParser::WindowClauseContext::WindowClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::WindowClauseContext::WINDOW() {
  return getToken(NebulaSQLParser::WINDOW, 0);
}

NebulaSQLParser::WindowSpecContext* NebulaSQLParser::WindowClauseContext::windowSpec() {
  return getRuleContext<NebulaSQLParser::WindowSpecContext>(0);
}


size_t NebulaSQLParser::WindowClauseContext::getRuleIndex() const {
  return NebulaSQLParser::RuleWindowClause;
}

void NebulaSQLParser::WindowClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterWindowClause(this);
}

void NebulaSQLParser::WindowClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitWindowClause(this);
}

NebulaSQLParser::WindowClauseContext* NebulaSQLParser::windowClause() {
  WindowClauseContext *_localctx = _tracker.createInstance<WindowClauseContext>(_ctx, getState());
  enterRule(_localctx, 74, NebulaSQLParser::RuleWindowClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(514);
    match(NebulaSQLParser::WINDOW);
    setState(515);
    windowSpec();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WatermarkClauseContext ------------------------------------------------------------------

NebulaSQLParser::WatermarkClauseContext::WatermarkClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::WatermarkClauseContext::WATERMARK() {
  return getToken(NebulaSQLParser::WATERMARK, 0);
}

NebulaSQLParser::WatermarkParametersContext* NebulaSQLParser::WatermarkClauseContext::watermarkParameters() {
  return getRuleContext<NebulaSQLParser::WatermarkParametersContext>(0);
}


size_t NebulaSQLParser::WatermarkClauseContext::getRuleIndex() const {
  return NebulaSQLParser::RuleWatermarkClause;
}

void NebulaSQLParser::WatermarkClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterWatermarkClause(this);
}

void NebulaSQLParser::WatermarkClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitWatermarkClause(this);
}

NebulaSQLParser::WatermarkClauseContext* NebulaSQLParser::watermarkClause() {
  WatermarkClauseContext *_localctx = _tracker.createInstance<WatermarkClauseContext>(_ctx, getState());
  enterRule(_localctx, 76, NebulaSQLParser::RuleWatermarkClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(517);
    match(NebulaSQLParser::WATERMARK);
    setState(518);
    match(NebulaSQLParser::T__2);
    setState(519);
    watermarkParameters();
    setState(520);
    match(NebulaSQLParser::T__3);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WatermarkParametersContext ------------------------------------------------------------------

NebulaSQLParser::WatermarkParametersContext::WatermarkParametersContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::IdentifierContext* NebulaSQLParser::WatermarkParametersContext::identifier() {
  return getRuleContext<NebulaSQLParser::IdentifierContext>(0);
}

tree::TerminalNode* NebulaSQLParser::WatermarkParametersContext::INTEGER_VALUE() {
  return getToken(NebulaSQLParser::INTEGER_VALUE, 0);
}

NebulaSQLParser::TimeUnitContext* NebulaSQLParser::WatermarkParametersContext::timeUnit() {
  return getRuleContext<NebulaSQLParser::TimeUnitContext>(0);
}


size_t NebulaSQLParser::WatermarkParametersContext::getRuleIndex() const {
  return NebulaSQLParser::RuleWatermarkParameters;
}

void NebulaSQLParser::WatermarkParametersContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterWatermarkParameters(this);
}

void NebulaSQLParser::WatermarkParametersContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitWatermarkParameters(this);
}

NebulaSQLParser::WatermarkParametersContext* NebulaSQLParser::watermarkParameters() {
  WatermarkParametersContext *_localctx = _tracker.createInstance<WatermarkParametersContext>(_ctx, getState());
  enterRule(_localctx, 78, NebulaSQLParser::RuleWatermarkParameters);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(522);
    dynamic_cast<WatermarkParametersContext *>(_localctx)->watermarkIdentifier = identifier();
    setState(523);
    match(NebulaSQLParser::T__1);
    setState(524);
    dynamic_cast<WatermarkParametersContext *>(_localctx)->watermark = match(NebulaSQLParser::INTEGER_VALUE);
    setState(525);
    dynamic_cast<WatermarkParametersContext *>(_localctx)->watermarkTimeUnit = timeUnit();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WindowSpecContext ------------------------------------------------------------------

NebulaSQLParser::WindowSpecContext::WindowSpecContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::WindowSpecContext::getRuleIndex() const {
  return NebulaSQLParser::RuleWindowSpec;
}

void NebulaSQLParser::WindowSpecContext::copyFrom(WindowSpecContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- TimeBasedWindowContext ------------------------------------------------------------------

NebulaSQLParser::TimeWindowContext* NebulaSQLParser::TimeBasedWindowContext::timeWindow() {
  return getRuleContext<NebulaSQLParser::TimeWindowContext>(0);
}

NebulaSQLParser::TimeBasedWindowContext::TimeBasedWindowContext(WindowSpecContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::TimeBasedWindowContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTimeBasedWindow(this);
}
void NebulaSQLParser::TimeBasedWindowContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTimeBasedWindow(this);
}
//----------------- CountBasedWindowContext ------------------------------------------------------------------

NebulaSQLParser::CountWindowContext* NebulaSQLParser::CountBasedWindowContext::countWindow() {
  return getRuleContext<NebulaSQLParser::CountWindowContext>(0);
}

NebulaSQLParser::CountBasedWindowContext::CountBasedWindowContext(WindowSpecContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::CountBasedWindowContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterCountBasedWindow(this);
}
void NebulaSQLParser::CountBasedWindowContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitCountBasedWindow(this);
}
NebulaSQLParser::WindowSpecContext* NebulaSQLParser::windowSpec() {
  WindowSpecContext *_localctx = _tracker.createInstance<WindowSpecContext>(_ctx, getState());
  enterRule(_localctx, 80, NebulaSQLParser::RuleWindowSpec);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(529);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 55, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<WindowSpecContext *>(_tracker.createInstance<NebulaSQLParser::TimeBasedWindowContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(527);
      timeWindow();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<WindowSpecContext *>(_tracker.createInstance<NebulaSQLParser::CountBasedWindowContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(528);
      countWindow();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TimeWindowContext ------------------------------------------------------------------

NebulaSQLParser::TimeWindowContext::TimeWindowContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::TimeWindowContext::getRuleIndex() const {
  return NebulaSQLParser::RuleTimeWindow;
}

void NebulaSQLParser::TimeWindowContext::copyFrom(TimeWindowContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- TumblingWindowContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::TumblingWindowContext::TUMBLING() {
  return getToken(NebulaSQLParser::TUMBLING, 0);
}

NebulaSQLParser::SizeParameterContext* NebulaSQLParser::TumblingWindowContext::sizeParameter() {
  return getRuleContext<NebulaSQLParser::SizeParameterContext>(0);
}

NebulaSQLParser::TimestampParameterContext* NebulaSQLParser::TumblingWindowContext::timestampParameter() {
  return getRuleContext<NebulaSQLParser::TimestampParameterContext>(0);
}

NebulaSQLParser::TumblingWindowContext::TumblingWindowContext(TimeWindowContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::TumblingWindowContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTumblingWindow(this);
}
void NebulaSQLParser::TumblingWindowContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTumblingWindow(this);
}
//----------------- SlidingWindowContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::SlidingWindowContext::SLIDING() {
  return getToken(NebulaSQLParser::SLIDING, 0);
}

NebulaSQLParser::SizeParameterContext* NebulaSQLParser::SlidingWindowContext::sizeParameter() {
  return getRuleContext<NebulaSQLParser::SizeParameterContext>(0);
}

NebulaSQLParser::AdvancebyParameterContext* NebulaSQLParser::SlidingWindowContext::advancebyParameter() {
  return getRuleContext<NebulaSQLParser::AdvancebyParameterContext>(0);
}

NebulaSQLParser::TimestampParameterContext* NebulaSQLParser::SlidingWindowContext::timestampParameter() {
  return getRuleContext<NebulaSQLParser::TimestampParameterContext>(0);
}

NebulaSQLParser::SlidingWindowContext::SlidingWindowContext(TimeWindowContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::SlidingWindowContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSlidingWindow(this);
}
void NebulaSQLParser::SlidingWindowContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSlidingWindow(this);
}
NebulaSQLParser::TimeWindowContext* NebulaSQLParser::timeWindow() {
  TimeWindowContext *_localctx = _tracker.createInstance<TimeWindowContext>(_ctx, getState());
  enterRule(_localctx, 82, NebulaSQLParser::RuleTimeWindow);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(553);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NebulaSQLParser::TUMBLING: {
        _localctx = dynamic_cast<TimeWindowContext *>(_tracker.createInstance<NebulaSQLParser::TumblingWindowContext>(_localctx));
        enterOuterAlt(_localctx, 1);
        setState(531);
        match(NebulaSQLParser::TUMBLING);
        setState(532);
        match(NebulaSQLParser::T__2);
        setState(536);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NebulaSQLParser::IDENTIFIER) {
          setState(533);
          timestampParameter();
          setState(534);
          match(NebulaSQLParser::T__1);
        }
        setState(538);
        sizeParameter();
        setState(539);
        match(NebulaSQLParser::T__3);
        break;
      }

      case NebulaSQLParser::SLIDING: {
        _localctx = dynamic_cast<TimeWindowContext *>(_tracker.createInstance<NebulaSQLParser::SlidingWindowContext>(_localctx));
        enterOuterAlt(_localctx, 2);
        setState(541);
        match(NebulaSQLParser::SLIDING);
        setState(542);
        match(NebulaSQLParser::T__2);
        setState(546);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NebulaSQLParser::IDENTIFIER) {
          setState(543);
          timestampParameter();
          setState(544);
          match(NebulaSQLParser::T__1);
        }
        setState(548);
        sizeParameter();
        setState(549);
        match(NebulaSQLParser::T__1);
        setState(550);
        advancebyParameter();
        setState(551);
        match(NebulaSQLParser::T__3);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CountWindowContext ------------------------------------------------------------------

NebulaSQLParser::CountWindowContext::CountWindowContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::CountWindowContext::getRuleIndex() const {
  return NebulaSQLParser::RuleCountWindow;
}

void NebulaSQLParser::CountWindowContext::copyFrom(CountWindowContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- CountBasedTumblingContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::CountBasedTumblingContext::TUMBLING() {
  return getToken(NebulaSQLParser::TUMBLING, 0);
}

tree::TerminalNode* NebulaSQLParser::CountBasedTumblingContext::INTEGER_VALUE() {
  return getToken(NebulaSQLParser::INTEGER_VALUE, 0);
}

NebulaSQLParser::CountBasedTumblingContext::CountBasedTumblingContext(CountWindowContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::CountBasedTumblingContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterCountBasedTumbling(this);
}
void NebulaSQLParser::CountBasedTumblingContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitCountBasedTumbling(this);
}
NebulaSQLParser::CountWindowContext* NebulaSQLParser::countWindow() {
  CountWindowContext *_localctx = _tracker.createInstance<CountWindowContext>(_ctx, getState());
  enterRule(_localctx, 84, NebulaSQLParser::RuleCountWindow);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    _localctx = dynamic_cast<CountWindowContext *>(_tracker.createInstance<NebulaSQLParser::CountBasedTumblingContext>(_localctx));
    enterOuterAlt(_localctx, 1);
    setState(555);
    match(NebulaSQLParser::TUMBLING);
    setState(556);
    match(NebulaSQLParser::T__2);
    setState(557);
    match(NebulaSQLParser::INTEGER_VALUE);
    setState(558);
    match(NebulaSQLParser::T__3);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SizeParameterContext ------------------------------------------------------------------

NebulaSQLParser::SizeParameterContext::SizeParameterContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::SizeParameterContext::SIZE() {
  return getToken(NebulaSQLParser::SIZE, 0);
}

tree::TerminalNode* NebulaSQLParser::SizeParameterContext::INTEGER_VALUE() {
  return getToken(NebulaSQLParser::INTEGER_VALUE, 0);
}

NebulaSQLParser::TimeUnitContext* NebulaSQLParser::SizeParameterContext::timeUnit() {
  return getRuleContext<NebulaSQLParser::TimeUnitContext>(0);
}


size_t NebulaSQLParser::SizeParameterContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSizeParameter;
}

void NebulaSQLParser::SizeParameterContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSizeParameter(this);
}

void NebulaSQLParser::SizeParameterContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSizeParameter(this);
}

NebulaSQLParser::SizeParameterContext* NebulaSQLParser::sizeParameter() {
  SizeParameterContext *_localctx = _tracker.createInstance<SizeParameterContext>(_ctx, getState());
  enterRule(_localctx, 86, NebulaSQLParser::RuleSizeParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(560);
    match(NebulaSQLParser::SIZE);
    setState(561);
    match(NebulaSQLParser::INTEGER_VALUE);
    setState(562);
    timeUnit();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AdvancebyParameterContext ------------------------------------------------------------------

NebulaSQLParser::AdvancebyParameterContext::AdvancebyParameterContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::AdvancebyParameterContext::ADVANCE() {
  return getToken(NebulaSQLParser::ADVANCE, 0);
}

tree::TerminalNode* NebulaSQLParser::AdvancebyParameterContext::BY() {
  return getToken(NebulaSQLParser::BY, 0);
}

tree::TerminalNode* NebulaSQLParser::AdvancebyParameterContext::INTEGER_VALUE() {
  return getToken(NebulaSQLParser::INTEGER_VALUE, 0);
}

NebulaSQLParser::TimeUnitContext* NebulaSQLParser::AdvancebyParameterContext::timeUnit() {
  return getRuleContext<NebulaSQLParser::TimeUnitContext>(0);
}


size_t NebulaSQLParser::AdvancebyParameterContext::getRuleIndex() const {
  return NebulaSQLParser::RuleAdvancebyParameter;
}

void NebulaSQLParser::AdvancebyParameterContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAdvancebyParameter(this);
}

void NebulaSQLParser::AdvancebyParameterContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAdvancebyParameter(this);
}

NebulaSQLParser::AdvancebyParameterContext* NebulaSQLParser::advancebyParameter() {
  AdvancebyParameterContext *_localctx = _tracker.createInstance<AdvancebyParameterContext>(_ctx, getState());
  enterRule(_localctx, 88, NebulaSQLParser::RuleAdvancebyParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(564);
    match(NebulaSQLParser::ADVANCE);
    setState(565);
    match(NebulaSQLParser::BY);
    setState(566);
    match(NebulaSQLParser::INTEGER_VALUE);
    setState(567);
    timeUnit();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TimeUnitContext ------------------------------------------------------------------

NebulaSQLParser::TimeUnitContext::TimeUnitContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::TimeUnitContext::MS() {
  return getToken(NebulaSQLParser::MS, 0);
}

tree::TerminalNode* NebulaSQLParser::TimeUnitContext::SEC() {
  return getToken(NebulaSQLParser::SEC, 0);
}

tree::TerminalNode* NebulaSQLParser::TimeUnitContext::MIN() {
  return getToken(NebulaSQLParser::MIN, 0);
}

tree::TerminalNode* NebulaSQLParser::TimeUnitContext::HOUR() {
  return getToken(NebulaSQLParser::HOUR, 0);
}

tree::TerminalNode* NebulaSQLParser::TimeUnitContext::DAY() {
  return getToken(NebulaSQLParser::DAY, 0);
}


size_t NebulaSQLParser::TimeUnitContext::getRuleIndex() const {
  return NebulaSQLParser::RuleTimeUnit;
}

void NebulaSQLParser::TimeUnitContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTimeUnit(this);
}

void NebulaSQLParser::TimeUnitContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTimeUnit(this);
}

NebulaSQLParser::TimeUnitContext* NebulaSQLParser::timeUnit() {
  TimeUnitContext *_localctx = _tracker.createInstance<TimeUnitContext>(_ctx, getState());
  enterRule(_localctx, 90, NebulaSQLParser::RuleTimeUnit);
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
    setState(569);
    _la = _input->LA(1);
    if (!(((((_la - 83) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 83)) & ((1ULL << (NebulaSQLParser::MS - 83))
      | (1ULL << (NebulaSQLParser::SEC - 83))
      | (1ULL << (NebulaSQLParser::MIN - 83))
      | (1ULL << (NebulaSQLParser::HOUR - 83))
      | (1ULL << (NebulaSQLParser::DAY - 83)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TimestampParameterContext ------------------------------------------------------------------

NebulaSQLParser::TimestampParameterContext::TimestampParameterContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::TimestampParameterContext::IDENTIFIER() {
  return getToken(NebulaSQLParser::IDENTIFIER, 0);
}


size_t NebulaSQLParser::TimestampParameterContext::getRuleIndex() const {
  return NebulaSQLParser::RuleTimestampParameter;
}

void NebulaSQLParser::TimestampParameterContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTimestampParameter(this);
}

void NebulaSQLParser::TimestampParameterContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTimestampParameter(this);
}

NebulaSQLParser::TimestampParameterContext* NebulaSQLParser::timestampParameter() {
  TimestampParameterContext *_localctx = _tracker.createInstance<TimestampParameterContext>(_ctx, getState());
  enterRule(_localctx, 92, NebulaSQLParser::RuleTimestampParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(571);
    match(NebulaSQLParser::IDENTIFIER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FunctionNameContext ------------------------------------------------------------------

NebulaSQLParser::FunctionNameContext::FunctionNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::FunctionNameContext::AVG() {
  return getToken(NebulaSQLParser::AVG, 0);
}

tree::TerminalNode* NebulaSQLParser::FunctionNameContext::MAX() {
  return getToken(NebulaSQLParser::MAX, 0);
}

tree::TerminalNode* NebulaSQLParser::FunctionNameContext::MIN() {
  return getToken(NebulaSQLParser::MIN, 0);
}

tree::TerminalNode* NebulaSQLParser::FunctionNameContext::SUM() {
  return getToken(NebulaSQLParser::SUM, 0);
}

tree::TerminalNode* NebulaSQLParser::FunctionNameContext::COUNT() {
  return getToken(NebulaSQLParser::COUNT, 0);
}


size_t NebulaSQLParser::FunctionNameContext::getRuleIndex() const {
  return NebulaSQLParser::RuleFunctionName;
}

void NebulaSQLParser::FunctionNameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFunctionName(this);
}

void NebulaSQLParser::FunctionNameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFunctionName(this);
}

NebulaSQLParser::FunctionNameContext* NebulaSQLParser::functionName() {
  FunctionNameContext *_localctx = _tracker.createInstance<FunctionNameContext>(_ctx, getState());
  enterRule(_localctx, 94, NebulaSQLParser::RuleFunctionName);
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
    setState(573);
    _la = _input->LA(1);
    if (!(((((_la - 85) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 85)) & ((1ULL << (NebulaSQLParser::MIN - 85))
      | (1ULL << (NebulaSQLParser::MAX - 85))
      | (1ULL << (NebulaSQLParser::AVG - 85))
      | (1ULL << (NebulaSQLParser::SUM - 85))
      | (1ULL << (NebulaSQLParser::COUNT - 85)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SinkClauseContext ------------------------------------------------------------------

NebulaSQLParser::SinkClauseContext::SinkClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::SinkClauseContext::INSERT() {
  return getToken(NebulaSQLParser::INSERT, 0);
}

tree::TerminalNode* NebulaSQLParser::SinkClauseContext::INTO() {
  return getToken(NebulaSQLParser::INTO, 0);
}

NebulaSQLParser::SinkTypeContext* NebulaSQLParser::SinkClauseContext::sinkType() {
  return getRuleContext<NebulaSQLParser::SinkTypeContext>(0);
}

tree::TerminalNode* NebulaSQLParser::SinkClauseContext::AS() {
  return getToken(NebulaSQLParser::AS, 0);
}


size_t NebulaSQLParser::SinkClauseContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSinkClause;
}

void NebulaSQLParser::SinkClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSinkClause(this);
}

void NebulaSQLParser::SinkClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSinkClause(this);
}

NebulaSQLParser::SinkClauseContext* NebulaSQLParser::sinkClause() {
  SinkClauseContext *_localctx = _tracker.createInstance<SinkClauseContext>(_ctx, getState());
  enterRule(_localctx, 96, NebulaSQLParser::RuleSinkClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(575);
    match(NebulaSQLParser::INSERT);
    setState(576);
    match(NebulaSQLParser::INTO);
    setState(577);
    sinkType();
    setState(578);
    match(NebulaSQLParser::AS);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SinkTypeContext ------------------------------------------------------------------

NebulaSQLParser::SinkTypeContext::SinkTypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::SinkTypeCSVContext* NebulaSQLParser::SinkTypeContext::sinkTypeCSV() {
  return getRuleContext<NebulaSQLParser::SinkTypeCSVContext>(0);
}

NebulaSQLParser::SinkTypeZMQContext* NebulaSQLParser::SinkTypeContext::sinkTypeZMQ() {
  return getRuleContext<NebulaSQLParser::SinkTypeZMQContext>(0);
}

NebulaSQLParser::SinkTypeKafkaContext* NebulaSQLParser::SinkTypeContext::sinkTypeKafka() {
  return getRuleContext<NebulaSQLParser::SinkTypeKafkaContext>(0);
}


size_t NebulaSQLParser::SinkTypeContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSinkType;
}

void NebulaSQLParser::SinkTypeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSinkType(this);
}

void NebulaSQLParser::SinkTypeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSinkType(this);
}

NebulaSQLParser::SinkTypeContext* NebulaSQLParser::sinkType() {
  SinkTypeContext *_localctx = _tracker.createInstance<SinkTypeContext>(_ctx, getState());
  enterRule(_localctx, 98, NebulaSQLParser::RuleSinkType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(583);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NebulaSQLParser::CSV: {
        enterOuterAlt(_localctx, 1);
        setState(580);
        sinkTypeCSV();
        break;
      }

      case NebulaSQLParser::ZMQ: {
        enterOuterAlt(_localctx, 2);
        setState(581);
        sinkTypeZMQ();
        break;
      }

      case NebulaSQLParser::KAFKA: {
        enterOuterAlt(_localctx, 3);
        setState(582);
        sinkTypeKafka();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SinkTypeCSVContext ------------------------------------------------------------------

NebulaSQLParser::SinkTypeCSVContext::SinkTypeCSVContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::CsvKeywordContext* NebulaSQLParser::SinkTypeCSVContext::csvKeyword() {
  return getRuleContext<NebulaSQLParser::CsvKeywordContext>(0);
}

tree::TerminalNode* NebulaSQLParser::SinkTypeCSVContext::STRING() {
  return getToken(NebulaSQLParser::STRING, 0);
}


size_t NebulaSQLParser::SinkTypeCSVContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSinkTypeCSV;
}

void NebulaSQLParser::SinkTypeCSVContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSinkTypeCSV(this);
}

void NebulaSQLParser::SinkTypeCSVContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSinkTypeCSV(this);
}

NebulaSQLParser::SinkTypeCSVContext* NebulaSQLParser::sinkTypeCSV() {
  SinkTypeCSVContext *_localctx = _tracker.createInstance<SinkTypeCSVContext>(_ctx, getState());
  enterRule(_localctx, 100, NebulaSQLParser::RuleSinkTypeCSV);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(585);
    csvKeyword();
    setState(586);
    match(NebulaSQLParser::T__2);
    setState(587);
    dynamic_cast<SinkTypeCSVContext *>(_localctx)->path = match(NebulaSQLParser::STRING);
    setState(588);
    match(NebulaSQLParser::T__3);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CsvKeywordContext ------------------------------------------------------------------

NebulaSQLParser::CsvKeywordContext::CsvKeywordContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::CsvKeywordContext::CSV() {
  return getToken(NebulaSQLParser::CSV, 0);
}


size_t NebulaSQLParser::CsvKeywordContext::getRuleIndex() const {
  return NebulaSQLParser::RuleCsvKeyword;
}

void NebulaSQLParser::CsvKeywordContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterCsvKeyword(this);
}

void NebulaSQLParser::CsvKeywordContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitCsvKeyword(this);
}

NebulaSQLParser::CsvKeywordContext* NebulaSQLParser::csvKeyword() {
  CsvKeywordContext *_localctx = _tracker.createInstance<CsvKeywordContext>(_ctx, getState());
  enterRule(_localctx, 102, NebulaSQLParser::RuleCsvKeyword);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(590);
    match(NebulaSQLParser::CSV);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SinkTypeZMQContext ------------------------------------------------------------------

NebulaSQLParser::SinkTypeZMQContext::SinkTypeZMQContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::ZmqKeywordContext* NebulaSQLParser::SinkTypeZMQContext::zmqKeyword() {
  return getRuleContext<NebulaSQLParser::ZmqKeywordContext>(0);
}

NebulaSQLParser::StreamNameContext* NebulaSQLParser::SinkTypeZMQContext::streamName() {
  return getRuleContext<NebulaSQLParser::StreamNameContext>(0);
}

NebulaSQLParser::HostContext* NebulaSQLParser::SinkTypeZMQContext::host() {
  return getRuleContext<NebulaSQLParser::HostContext>(0);
}

NebulaSQLParser::PortContext* NebulaSQLParser::SinkTypeZMQContext::port() {
  return getRuleContext<NebulaSQLParser::PortContext>(0);
}


size_t NebulaSQLParser::SinkTypeZMQContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSinkTypeZMQ;
}

void NebulaSQLParser::SinkTypeZMQContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSinkTypeZMQ(this);
}

void NebulaSQLParser::SinkTypeZMQContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSinkTypeZMQ(this);
}

NebulaSQLParser::SinkTypeZMQContext* NebulaSQLParser::sinkTypeZMQ() {
  SinkTypeZMQContext *_localctx = _tracker.createInstance<SinkTypeZMQContext>(_ctx, getState());
  enterRule(_localctx, 104, NebulaSQLParser::RuleSinkTypeZMQ);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(592);
    zmqKeyword();
    setState(593);
    match(NebulaSQLParser::T__2);
    setState(594);
    dynamic_cast<SinkTypeZMQContext *>(_localctx)->zmqStreamName = streamName();
    setState(595);
    match(NebulaSQLParser::T__1);
    setState(596);
    dynamic_cast<SinkTypeZMQContext *>(_localctx)->zmqHost = host();
    setState(597);
    match(NebulaSQLParser::T__1);
    setState(598);
    dynamic_cast<SinkTypeZMQContext *>(_localctx)->zmqPort = port();
    setState(599);
    match(NebulaSQLParser::T__3);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NullNotnullContext ------------------------------------------------------------------

NebulaSQLParser::NullNotnullContext::NullNotnullContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::NullNotnullContext::NULLTOKEN() {
  return getToken(NebulaSQLParser::NULLTOKEN, 0);
}

tree::TerminalNode* NebulaSQLParser::NullNotnullContext::NOT() {
  return getToken(NebulaSQLParser::NOT, 0);
}


size_t NebulaSQLParser::NullNotnullContext::getRuleIndex() const {
  return NebulaSQLParser::RuleNullNotnull;
}

void NebulaSQLParser::NullNotnullContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNullNotnull(this);
}

void NebulaSQLParser::NullNotnullContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNullNotnull(this);
}

NebulaSQLParser::NullNotnullContext* NebulaSQLParser::nullNotnull() {
  NullNotnullContext *_localctx = _tracker.createInstance<NullNotnullContext>(_ctx, getState());
  enterRule(_localctx, 106, NebulaSQLParser::RuleNullNotnull);
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
    setState(602);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::NOT) {
      setState(601);
      match(NebulaSQLParser::NOT);
    }
    setState(604);
    match(NebulaSQLParser::NULLTOKEN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ZmqKeywordContext ------------------------------------------------------------------

NebulaSQLParser::ZmqKeywordContext::ZmqKeywordContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::ZmqKeywordContext::ZMQ() {
  return getToken(NebulaSQLParser::ZMQ, 0);
}


size_t NebulaSQLParser::ZmqKeywordContext::getRuleIndex() const {
  return NebulaSQLParser::RuleZmqKeyword;
}

void NebulaSQLParser::ZmqKeywordContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterZmqKeyword(this);
}

void NebulaSQLParser::ZmqKeywordContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitZmqKeyword(this);
}

NebulaSQLParser::ZmqKeywordContext* NebulaSQLParser::zmqKeyword() {
  ZmqKeywordContext *_localctx = _tracker.createInstance<ZmqKeywordContext>(_ctx, getState());
  enterRule(_localctx, 108, NebulaSQLParser::RuleZmqKeyword);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(606);
    match(NebulaSQLParser::ZMQ);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StreamNameContext ------------------------------------------------------------------

NebulaSQLParser::StreamNameContext::StreamNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::StreamNameContext::IDENTIFIER() {
  return getToken(NebulaSQLParser::IDENTIFIER, 0);
}


size_t NebulaSQLParser::StreamNameContext::getRuleIndex() const {
  return NebulaSQLParser::RuleStreamName;
}

void NebulaSQLParser::StreamNameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterStreamName(this);
}

void NebulaSQLParser::StreamNameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitStreamName(this);
}

NebulaSQLParser::StreamNameContext* NebulaSQLParser::streamName() {
  StreamNameContext *_localctx = _tracker.createInstance<StreamNameContext>(_ctx, getState());
  enterRule(_localctx, 110, NebulaSQLParser::RuleStreamName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(608);
    match(NebulaSQLParser::IDENTIFIER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- HostContext ------------------------------------------------------------------

NebulaSQLParser::HostContext::HostContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::HostContext::FOUR_OCTETS() {
  return getToken(NebulaSQLParser::FOUR_OCTETS, 0);
}

tree::TerminalNode* NebulaSQLParser::HostContext::LOCALHOST() {
  return getToken(NebulaSQLParser::LOCALHOST, 0);
}


size_t NebulaSQLParser::HostContext::getRuleIndex() const {
  return NebulaSQLParser::RuleHost;
}

void NebulaSQLParser::HostContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterHost(this);
}

void NebulaSQLParser::HostContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitHost(this);
}

NebulaSQLParser::HostContext* NebulaSQLParser::host() {
  HostContext *_localctx = _tracker.createInstance<HostContext>(_ctx, getState());
  enterRule(_localctx, 112, NebulaSQLParser::RuleHost);
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
    setState(610);
    _la = _input->LA(1);
    if (!(_la == NebulaSQLParser::LOCALHOST

    || _la == NebulaSQLParser::FOUR_OCTETS)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PortContext ------------------------------------------------------------------

NebulaSQLParser::PortContext::PortContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::PortContext::INTEGER_VALUE() {
  return getToken(NebulaSQLParser::INTEGER_VALUE, 0);
}


size_t NebulaSQLParser::PortContext::getRuleIndex() const {
  return NebulaSQLParser::RulePort;
}

void NebulaSQLParser::PortContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPort(this);
}

void NebulaSQLParser::PortContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPort(this);
}

NebulaSQLParser::PortContext* NebulaSQLParser::port() {
  PortContext *_localctx = _tracker.createInstance<PortContext>(_ctx, getState());
  enterRule(_localctx, 114, NebulaSQLParser::RulePort);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(612);
    match(NebulaSQLParser::INTEGER_VALUE);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SinkTypeKafkaContext ------------------------------------------------------------------

NebulaSQLParser::SinkTypeKafkaContext::SinkTypeKafkaContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::KafkaKeywordContext* NebulaSQLParser::SinkTypeKafkaContext::kafkaKeyword() {
  return getRuleContext<NebulaSQLParser::KafkaKeywordContext>(0);
}

NebulaSQLParser::KafkaBrokerContext* NebulaSQLParser::SinkTypeKafkaContext::kafkaBroker() {
  return getRuleContext<NebulaSQLParser::KafkaBrokerContext>(0);
}

NebulaSQLParser::KafkaTopicContext* NebulaSQLParser::SinkTypeKafkaContext::kafkaTopic() {
  return getRuleContext<NebulaSQLParser::KafkaTopicContext>(0);
}

NebulaSQLParser::KafkaProducerTimoutContext* NebulaSQLParser::SinkTypeKafkaContext::kafkaProducerTimout() {
  return getRuleContext<NebulaSQLParser::KafkaProducerTimoutContext>(0);
}


size_t NebulaSQLParser::SinkTypeKafkaContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSinkTypeKafka;
}

void NebulaSQLParser::SinkTypeKafkaContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSinkTypeKafka(this);
}

void NebulaSQLParser::SinkTypeKafkaContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSinkTypeKafka(this);
}

NebulaSQLParser::SinkTypeKafkaContext* NebulaSQLParser::sinkTypeKafka() {
  SinkTypeKafkaContext *_localctx = _tracker.createInstance<SinkTypeKafkaContext>(_ctx, getState());
  enterRule(_localctx, 116, NebulaSQLParser::RuleSinkTypeKafka);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(614);
    kafkaKeyword();
    setState(615);
    match(NebulaSQLParser::T__2);
    setState(616);
    dynamic_cast<SinkTypeKafkaContext *>(_localctx)->broker = kafkaBroker();
    setState(617);
    match(NebulaSQLParser::T__1);
    setState(618);
    dynamic_cast<SinkTypeKafkaContext *>(_localctx)->topic = kafkaTopic();
    setState(619);
    match(NebulaSQLParser::T__1);
    setState(620);
    dynamic_cast<SinkTypeKafkaContext *>(_localctx)->timeout = kafkaProducerTimout();
    setState(621);
    match(NebulaSQLParser::T__3);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KafkaKeywordContext ------------------------------------------------------------------

NebulaSQLParser::KafkaKeywordContext::KafkaKeywordContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::KafkaKeywordContext::KAFKA() {
  return getToken(NebulaSQLParser::KAFKA, 0);
}


size_t NebulaSQLParser::KafkaKeywordContext::getRuleIndex() const {
  return NebulaSQLParser::RuleKafkaKeyword;
}

void NebulaSQLParser::KafkaKeywordContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterKafkaKeyword(this);
}

void NebulaSQLParser::KafkaKeywordContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitKafkaKeyword(this);
}

NebulaSQLParser::KafkaKeywordContext* NebulaSQLParser::kafkaKeyword() {
  KafkaKeywordContext *_localctx = _tracker.createInstance<KafkaKeywordContext>(_ctx, getState());
  enterRule(_localctx, 118, NebulaSQLParser::RuleKafkaKeyword);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(623);
    match(NebulaSQLParser::KAFKA);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KafkaBrokerContext ------------------------------------------------------------------

NebulaSQLParser::KafkaBrokerContext::KafkaBrokerContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::KafkaBrokerContext::IDENTIFIER() {
  return getToken(NebulaSQLParser::IDENTIFIER, 0);
}


size_t NebulaSQLParser::KafkaBrokerContext::getRuleIndex() const {
  return NebulaSQLParser::RuleKafkaBroker;
}

void NebulaSQLParser::KafkaBrokerContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterKafkaBroker(this);
}

void NebulaSQLParser::KafkaBrokerContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitKafkaBroker(this);
}

NebulaSQLParser::KafkaBrokerContext* NebulaSQLParser::kafkaBroker() {
  KafkaBrokerContext *_localctx = _tracker.createInstance<KafkaBrokerContext>(_ctx, getState());
  enterRule(_localctx, 120, NebulaSQLParser::RuleKafkaBroker);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(625);
    match(NebulaSQLParser::IDENTIFIER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KafkaTopicContext ------------------------------------------------------------------

NebulaSQLParser::KafkaTopicContext::KafkaTopicContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::KafkaTopicContext::IDENTIFIER() {
  return getToken(NebulaSQLParser::IDENTIFIER, 0);
}


size_t NebulaSQLParser::KafkaTopicContext::getRuleIndex() const {
  return NebulaSQLParser::RuleKafkaTopic;
}

void NebulaSQLParser::KafkaTopicContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterKafkaTopic(this);
}

void NebulaSQLParser::KafkaTopicContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitKafkaTopic(this);
}

NebulaSQLParser::KafkaTopicContext* NebulaSQLParser::kafkaTopic() {
  KafkaTopicContext *_localctx = _tracker.createInstance<KafkaTopicContext>(_ctx, getState());
  enterRule(_localctx, 122, NebulaSQLParser::RuleKafkaTopic);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(627);
    match(NebulaSQLParser::IDENTIFIER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KafkaProducerTimoutContext ------------------------------------------------------------------

NebulaSQLParser::KafkaProducerTimoutContext::KafkaProducerTimoutContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::KafkaProducerTimoutContext::INTEGER_VALUE() {
  return getToken(NebulaSQLParser::INTEGER_VALUE, 0);
}


size_t NebulaSQLParser::KafkaProducerTimoutContext::getRuleIndex() const {
  return NebulaSQLParser::RuleKafkaProducerTimout;
}

void NebulaSQLParser::KafkaProducerTimoutContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterKafkaProducerTimout(this);
}

void NebulaSQLParser::KafkaProducerTimoutContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitKafkaProducerTimout(this);
}

NebulaSQLParser::KafkaProducerTimoutContext* NebulaSQLParser::kafkaProducerTimout() {
  KafkaProducerTimoutContext *_localctx = _tracker.createInstance<KafkaProducerTimoutContext>(_ctx, getState());
  enterRule(_localctx, 124, NebulaSQLParser::RuleKafkaProducerTimout);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(629);
    match(NebulaSQLParser::INTEGER_VALUE);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SortItemContext ------------------------------------------------------------------

NebulaSQLParser::SortItemContext::SortItemContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::ExpressionContext* NebulaSQLParser::SortItemContext::expression() {
  return getRuleContext<NebulaSQLParser::ExpressionContext>(0);
}

tree::TerminalNode* NebulaSQLParser::SortItemContext::NULLS() {
  return getToken(NebulaSQLParser::NULLS, 0);
}

tree::TerminalNode* NebulaSQLParser::SortItemContext::ASC() {
  return getToken(NebulaSQLParser::ASC, 0);
}

tree::TerminalNode* NebulaSQLParser::SortItemContext::DESC() {
  return getToken(NebulaSQLParser::DESC, 0);
}

tree::TerminalNode* NebulaSQLParser::SortItemContext::LAST() {
  return getToken(NebulaSQLParser::LAST, 0);
}

tree::TerminalNode* NebulaSQLParser::SortItemContext::FIRST() {
  return getToken(NebulaSQLParser::FIRST, 0);
}


size_t NebulaSQLParser::SortItemContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSortItem;
}

void NebulaSQLParser::SortItemContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSortItem(this);
}

void NebulaSQLParser::SortItemContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSortItem(this);
}

NebulaSQLParser::SortItemContext* NebulaSQLParser::sortItem() {
  SortItemContext *_localctx = _tracker.createInstance<SortItemContext>(_ctx, getState());
  enterRule(_localctx, 126, NebulaSQLParser::RuleSortItem);
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
    setState(631);
    expression();
    setState(633);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::ASC

    || _la == NebulaSQLParser::DESC) {
      setState(632);
      dynamic_cast<SortItemContext *>(_localctx)->ordering = _input->LT(1);
      _la = _input->LA(1);
      if (!(_la == NebulaSQLParser::ASC

      || _la == NebulaSQLParser::DESC)) {
        dynamic_cast<SortItemContext *>(_localctx)->ordering = _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(637);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::NULLS) {
      setState(635);
      match(NebulaSQLParser::NULLS);
      setState(636);
      dynamic_cast<SortItemContext *>(_localctx)->nullOrder = _input->LT(1);
      _la = _input->LA(1);
      if (!(_la == NebulaSQLParser::FIRST

      || _la == NebulaSQLParser::LAST)) {
        dynamic_cast<SortItemContext *>(_localctx)->nullOrder = _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PredicateContext ------------------------------------------------------------------

NebulaSQLParser::PredicateContext::PredicateContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::AND() {
  return getToken(NebulaSQLParser::AND, 0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::BETWEEN() {
  return getToken(NebulaSQLParser::BETWEEN, 0);
}

std::vector<NebulaSQLParser::ValueExpressionContext *> NebulaSQLParser::PredicateContext::valueExpression() {
  return getRuleContexts<NebulaSQLParser::ValueExpressionContext>();
}

NebulaSQLParser::ValueExpressionContext* NebulaSQLParser::PredicateContext::valueExpression(size_t i) {
  return getRuleContext<NebulaSQLParser::ValueExpressionContext>(i);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::NOT() {
  return getToken(NebulaSQLParser::NOT, 0);
}

std::vector<NebulaSQLParser::ExpressionContext *> NebulaSQLParser::PredicateContext::expression() {
  return getRuleContexts<NebulaSQLParser::ExpressionContext>();
}

NebulaSQLParser::ExpressionContext* NebulaSQLParser::PredicateContext::expression(size_t i) {
  return getRuleContext<NebulaSQLParser::ExpressionContext>(i);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::IN() {
  return getToken(NebulaSQLParser::IN, 0);
}

NebulaSQLParser::QueryContext* NebulaSQLParser::PredicateContext::query() {
  return getRuleContext<NebulaSQLParser::QueryContext>(0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::RLIKE() {
  return getToken(NebulaSQLParser::RLIKE, 0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::LIKE() {
  return getToken(NebulaSQLParser::LIKE, 0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::ANY() {
  return getToken(NebulaSQLParser::ANY, 0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::SOME() {
  return getToken(NebulaSQLParser::SOME, 0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::ALL() {
  return getToken(NebulaSQLParser::ALL, 0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::ESCAPE() {
  return getToken(NebulaSQLParser::ESCAPE, 0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::STRING() {
  return getToken(NebulaSQLParser::STRING, 0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::IS() {
  return getToken(NebulaSQLParser::IS, 0);
}

NebulaSQLParser::NullNotnullContext* NebulaSQLParser::PredicateContext::nullNotnull() {
  return getRuleContext<NebulaSQLParser::NullNotnullContext>(0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::TRUE() {
  return getToken(NebulaSQLParser::TRUE, 0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::FALSE() {
  return getToken(NebulaSQLParser::FALSE, 0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::UNKNOWN() {
  return getToken(NebulaSQLParser::UNKNOWN, 0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::FROM() {
  return getToken(NebulaSQLParser::FROM, 0);
}

tree::TerminalNode* NebulaSQLParser::PredicateContext::DISTINCT() {
  return getToken(NebulaSQLParser::DISTINCT, 0);
}


size_t NebulaSQLParser::PredicateContext::getRuleIndex() const {
  return NebulaSQLParser::RulePredicate;
}

void NebulaSQLParser::PredicateContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPredicate(this);
}

void NebulaSQLParser::PredicateContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPredicate(this);
}

NebulaSQLParser::PredicateContext* NebulaSQLParser::predicate() {
  PredicateContext *_localctx = _tracker.createInstance<PredicateContext>(_ctx, getState());
  enterRule(_localctx, 128, NebulaSQLParser::RulePredicate);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(718);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 75, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(640);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(639);
        match(NebulaSQLParser::NOT);
      }
      setState(642);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::BETWEEN);
      setState(643);
      dynamic_cast<PredicateContext *>(_localctx)->lower = valueExpression(0);
      setState(644);
      match(NebulaSQLParser::AND);
      setState(645);
      dynamic_cast<PredicateContext *>(_localctx)->upper = valueExpression(0);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(648);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(647);
        match(NebulaSQLParser::NOT);
      }
      setState(650);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::IN);
      setState(651);
      match(NebulaSQLParser::T__2);
      setState(652);
      expression();
      setState(657);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NebulaSQLParser::T__1) {
        setState(653);
        match(NebulaSQLParser::T__1);
        setState(654);
        expression();
        setState(659);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(660);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(663);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(662);
        match(NebulaSQLParser::NOT);
      }
      setState(665);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::IN);
      setState(666);
      match(NebulaSQLParser::T__2);
      setState(667);
      query();
      setState(668);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(671);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(670);
        match(NebulaSQLParser::NOT);
      }
      setState(673);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::RLIKE);
      setState(674);
      dynamic_cast<PredicateContext *>(_localctx)->pattern = valueExpression(0);
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(676);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(675);
        match(NebulaSQLParser::NOT);
      }
      setState(678);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::LIKE);
      setState(679);
      dynamic_cast<PredicateContext *>(_localctx)->quantifier = _input->LT(1);
      _la = _input->LA(1);
      if (!(((((_la - 9) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 9)) & ((1ULL << (NebulaSQLParser::ALL - 9))
        | (1ULL << (NebulaSQLParser::ANY - 9))
        | (1ULL << (NebulaSQLParser::SOME - 9)))) != 0))) {
        dynamic_cast<PredicateContext *>(_localctx)->quantifier = _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(693);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 70, _ctx)) {
      case 1: {
        setState(680);
        match(NebulaSQLParser::T__2);
        setState(681);
        match(NebulaSQLParser::T__3);
        break;
      }

      case 2: {
        setState(682);
        match(NebulaSQLParser::T__2);
        setState(683);
        expression();
        setState(688);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NebulaSQLParser::T__1) {
          setState(684);
          match(NebulaSQLParser::T__1);
          setState(685);
          expression();
          setState(690);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(691);
        match(NebulaSQLParser::T__3);
        break;
      }

      default:
        break;
      }
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(696);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(695);
        match(NebulaSQLParser::NOT);
      }
      setState(698);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::LIKE);
      setState(699);
      dynamic_cast<PredicateContext *>(_localctx)->pattern = valueExpression(0);
      setState(702);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 72, _ctx)) {
      case 1: {
        setState(700);
        match(NebulaSQLParser::ESCAPE);
        setState(701);
        dynamic_cast<PredicateContext *>(_localctx)->escapeChar = match(NebulaSQLParser::STRING);
        break;
      }

      default:
        break;
      }
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(704);
      match(NebulaSQLParser::IS);
      setState(705);
      nullNotnull();
      break;
    }

    case 8: {
      enterOuterAlt(_localctx, 8);
      setState(706);
      match(NebulaSQLParser::IS);
      setState(708);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(707);
        match(NebulaSQLParser::NOT);
      }
      setState(710);
      dynamic_cast<PredicateContext *>(_localctx)->kind = _input->LT(1);
      _la = _input->LA(1);
      if (!(((((_la - 28) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 28)) & ((1ULL << (NebulaSQLParser::FALSE - 28))
        | (1ULL << (NebulaSQLParser::TRUE - 28))
        | (1ULL << (NebulaSQLParser::UNKNOWN - 28)))) != 0))) {
        dynamic_cast<PredicateContext *>(_localctx)->kind = _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      break;
    }

    case 9: {
      enterOuterAlt(_localctx, 9);
      setState(711);
      match(NebulaSQLParser::IS);
      setState(713);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(712);
        match(NebulaSQLParser::NOT);
      }
      setState(715);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::DISTINCT);
      setState(716);
      match(NebulaSQLParser::FROM);
      setState(717);
      dynamic_cast<PredicateContext *>(_localctx)->right = valueExpression(0);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ValueExpressionContext ------------------------------------------------------------------

NebulaSQLParser::ValueExpressionContext::ValueExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::ValueExpressionContext::getRuleIndex() const {
  return NebulaSQLParser::RuleValueExpression;
}

void NebulaSQLParser::ValueExpressionContext::copyFrom(ValueExpressionContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ValueExpressionDefaultContext ------------------------------------------------------------------

NebulaSQLParser::PrimaryExpressionContext* NebulaSQLParser::ValueExpressionDefaultContext::primaryExpression() {
  return getRuleContext<NebulaSQLParser::PrimaryExpressionContext>(0);
}

NebulaSQLParser::ValueExpressionDefaultContext::ValueExpressionDefaultContext(ValueExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::ValueExpressionDefaultContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterValueExpressionDefault(this);
}
void NebulaSQLParser::ValueExpressionDefaultContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitValueExpressionDefault(this);
}
//----------------- ComparisonContext ------------------------------------------------------------------

NebulaSQLParser::ComparisonOperatorContext* NebulaSQLParser::ComparisonContext::comparisonOperator() {
  return getRuleContext<NebulaSQLParser::ComparisonOperatorContext>(0);
}

std::vector<NebulaSQLParser::ValueExpressionContext *> NebulaSQLParser::ComparisonContext::valueExpression() {
  return getRuleContexts<NebulaSQLParser::ValueExpressionContext>();
}

NebulaSQLParser::ValueExpressionContext* NebulaSQLParser::ComparisonContext::valueExpression(size_t i) {
  return getRuleContext<NebulaSQLParser::ValueExpressionContext>(i);
}

NebulaSQLParser::ComparisonContext::ComparisonContext(ValueExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::ComparisonContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterComparison(this);
}
void NebulaSQLParser::ComparisonContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitComparison(this);
}
//----------------- ArithmeticBinaryContext ------------------------------------------------------------------

std::vector<NebulaSQLParser::ValueExpressionContext *> NebulaSQLParser::ArithmeticBinaryContext::valueExpression() {
  return getRuleContexts<NebulaSQLParser::ValueExpressionContext>();
}

NebulaSQLParser::ValueExpressionContext* NebulaSQLParser::ArithmeticBinaryContext::valueExpression(size_t i) {
  return getRuleContext<NebulaSQLParser::ValueExpressionContext>(i);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticBinaryContext::ASTERISK() {
  return getToken(NebulaSQLParser::ASTERISK, 0);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticBinaryContext::SLASH() {
  return getToken(NebulaSQLParser::SLASH, 0);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticBinaryContext::PERCENT() {
  return getToken(NebulaSQLParser::PERCENT, 0);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticBinaryContext::DIV() {
  return getToken(NebulaSQLParser::DIV, 0);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticBinaryContext::PLUS() {
  return getToken(NebulaSQLParser::PLUS, 0);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticBinaryContext::MINUS() {
  return getToken(NebulaSQLParser::MINUS, 0);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticBinaryContext::CONCAT_PIPE() {
  return getToken(NebulaSQLParser::CONCAT_PIPE, 0);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticBinaryContext::AMPERSAND() {
  return getToken(NebulaSQLParser::AMPERSAND, 0);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticBinaryContext::HAT() {
  return getToken(NebulaSQLParser::HAT, 0);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticBinaryContext::PIPE() {
  return getToken(NebulaSQLParser::PIPE, 0);
}

NebulaSQLParser::ArithmeticBinaryContext::ArithmeticBinaryContext(ValueExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::ArithmeticBinaryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterArithmeticBinary(this);
}
void NebulaSQLParser::ArithmeticBinaryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitArithmeticBinary(this);
}
//----------------- ArithmeticUnaryContext ------------------------------------------------------------------

NebulaSQLParser::ValueExpressionContext* NebulaSQLParser::ArithmeticUnaryContext::valueExpression() {
  return getRuleContext<NebulaSQLParser::ValueExpressionContext>(0);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticUnaryContext::MINUS() {
  return getToken(NebulaSQLParser::MINUS, 0);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticUnaryContext::PLUS() {
  return getToken(NebulaSQLParser::PLUS, 0);
}

tree::TerminalNode* NebulaSQLParser::ArithmeticUnaryContext::TILDE() {
  return getToken(NebulaSQLParser::TILDE, 0);
}

NebulaSQLParser::ArithmeticUnaryContext::ArithmeticUnaryContext(ValueExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::ArithmeticUnaryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterArithmeticUnary(this);
}
void NebulaSQLParser::ArithmeticUnaryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitArithmeticUnary(this);
}

NebulaSQLParser::ValueExpressionContext* NebulaSQLParser::valueExpression() {
   return valueExpression(0);
}

NebulaSQLParser::ValueExpressionContext* NebulaSQLParser::valueExpression(int precedence) {
  ParserRuleContext *parentContext = _ctx;
  size_t parentState = getState();
  NebulaSQLParser::ValueExpressionContext *_localctx = _tracker.createInstance<ValueExpressionContext>(_ctx, parentState);
  NebulaSQLParser::ValueExpressionContext *previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by generated code.
  size_t startState = 130;
  enterRecursionRule(_localctx, 130, NebulaSQLParser::RuleValueExpression, precedence);

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
    setState(724);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 76, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<ValueExpressionDefaultContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(721);
      primaryExpression(0);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<ArithmeticUnaryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(722);
      dynamic_cast<ArithmeticUnaryContext *>(_localctx)->op = _input->LT(1);
      _la = _input->LA(1);
      if (!(((((_la - 106) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 106)) & ((1ULL << (NebulaSQLParser::PLUS - 106))
        | (1ULL << (NebulaSQLParser::MINUS - 106))
        | (1ULL << (NebulaSQLParser::TILDE - 106)))) != 0))) {
        dynamic_cast<ArithmeticUnaryContext *>(_localctx)->op = _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(723);
      valueExpression(7);
      break;
    }

    default:
      break;
    }
    _ctx->stop = _input->LT(-1);
    setState(747);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 78, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(745);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 77, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(_tracker.createInstance<ValueExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleValueExpression);
          setState(726);

          if (!(precpred(_ctx, 6))) throw FailedPredicateException(this, "precpred(_ctx, 6)");
          setState(727);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = _input->LT(1);
          _la = _input->LA(1);
          if (!(_la == NebulaSQLParser::DIV || ((((_la - 108) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 108)) & ((1ULL << (NebulaSQLParser::ASTERISK - 108))
            | (1ULL << (NebulaSQLParser::SLASH - 108))
            | (1ULL << (NebulaSQLParser::PERCENT - 108)))) != 0))) {
            dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(728);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->right = valueExpression(7);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(_tracker.createInstance<ValueExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleValueExpression);
          setState(729);

          if (!(precpred(_ctx, 5))) throw FailedPredicateException(this, "precpred(_ctx, 5)");
          setState(730);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = _input->LT(1);
          _la = _input->LA(1);
          if (!(((((_la - 106) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 106)) & ((1ULL << (NebulaSQLParser::PLUS - 106))
            | (1ULL << (NebulaSQLParser::MINUS - 106))
            | (1ULL << (NebulaSQLParser::CONCAT_PIPE - 106)))) != 0))) {
            dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(731);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->right = valueExpression(6);
          break;
        }

        case 3: {
          auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(_tracker.createInstance<ValueExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleValueExpression);
          setState(732);

          if (!(precpred(_ctx, 4))) throw FailedPredicateException(this, "precpred(_ctx, 4)");
          setState(733);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = match(NebulaSQLParser::AMPERSAND);
          setState(734);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->right = valueExpression(5);
          break;
        }

        case 4: {
          auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(_tracker.createInstance<ValueExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleValueExpression);
          setState(735);

          if (!(precpred(_ctx, 3))) throw FailedPredicateException(this, "precpred(_ctx, 3)");
          setState(736);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = match(NebulaSQLParser::HAT);
          setState(737);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->right = valueExpression(4);
          break;
        }

        case 5: {
          auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(_tracker.createInstance<ValueExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleValueExpression);
          setState(738);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(739);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = match(NebulaSQLParser::PIPE);
          setState(740);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->right = valueExpression(3);
          break;
        }

        case 6: {
          auto newContext = _tracker.createInstance<ComparisonContext>(_tracker.createInstance<ValueExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleValueExpression);
          setState(741);

          if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(742);
          comparisonOperator();
          setState(743);
          dynamic_cast<ComparisonContext *>(_localctx)->right = valueExpression(2);
          break;
        }

        default:
          break;
        } 
      }
      setState(749);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 78, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- ComparisonOperatorContext ------------------------------------------------------------------

NebulaSQLParser::ComparisonOperatorContext::ComparisonOperatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::ComparisonOperatorContext::EQ() {
  return getToken(NebulaSQLParser::EQ, 0);
}

tree::TerminalNode* NebulaSQLParser::ComparisonOperatorContext::NEQ() {
  return getToken(NebulaSQLParser::NEQ, 0);
}

tree::TerminalNode* NebulaSQLParser::ComparisonOperatorContext::NEQJ() {
  return getToken(NebulaSQLParser::NEQJ, 0);
}

tree::TerminalNode* NebulaSQLParser::ComparisonOperatorContext::LT() {
  return getToken(NebulaSQLParser::LT, 0);
}

tree::TerminalNode* NebulaSQLParser::ComparisonOperatorContext::LTE() {
  return getToken(NebulaSQLParser::LTE, 0);
}

tree::TerminalNode* NebulaSQLParser::ComparisonOperatorContext::GT() {
  return getToken(NebulaSQLParser::GT, 0);
}

tree::TerminalNode* NebulaSQLParser::ComparisonOperatorContext::GTE() {
  return getToken(NebulaSQLParser::GTE, 0);
}

tree::TerminalNode* NebulaSQLParser::ComparisonOperatorContext::NSEQ() {
  return getToken(NebulaSQLParser::NSEQ, 0);
}


size_t NebulaSQLParser::ComparisonOperatorContext::getRuleIndex() const {
  return NebulaSQLParser::RuleComparisonOperator;
}

void NebulaSQLParser::ComparisonOperatorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterComparisonOperator(this);
}

void NebulaSQLParser::ComparisonOperatorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitComparisonOperator(this);
}

NebulaSQLParser::ComparisonOperatorContext* NebulaSQLParser::comparisonOperator() {
  ComparisonOperatorContext *_localctx = _tracker.createInstance<ComparisonOperatorContext>(_ctx, getState());
  enterRule(_localctx, 132, NebulaSQLParser::RuleComparisonOperator);
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
    setState(750);
    _la = _input->LA(1);
    if (!(((((_la - 98) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 98)) & ((1ULL << (NebulaSQLParser::EQ - 98))
      | (1ULL << (NebulaSQLParser::NSEQ - 98))
      | (1ULL << (NebulaSQLParser::NEQ - 98))
      | (1ULL << (NebulaSQLParser::NEQJ - 98))
      | (1ULL << (NebulaSQLParser::LT - 98))
      | (1ULL << (NebulaSQLParser::LTE - 98))
      | (1ULL << (NebulaSQLParser::GT - 98))
      | (1ULL << (NebulaSQLParser::GTE - 98)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- HintContext ------------------------------------------------------------------

NebulaSQLParser::HintContext::HintContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<NebulaSQLParser::HintStatementContext *> NebulaSQLParser::HintContext::hintStatement() {
  return getRuleContexts<NebulaSQLParser::HintStatementContext>();
}

NebulaSQLParser::HintStatementContext* NebulaSQLParser::HintContext::hintStatement(size_t i) {
  return getRuleContext<NebulaSQLParser::HintStatementContext>(i);
}


size_t NebulaSQLParser::HintContext::getRuleIndex() const {
  return NebulaSQLParser::RuleHint;
}

void NebulaSQLParser::HintContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterHint(this);
}

void NebulaSQLParser::HintContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitHint(this);
}

NebulaSQLParser::HintContext* NebulaSQLParser::hint() {
  HintContext *_localctx = _tracker.createInstance<HintContext>(_ctx, getState());
  enterRule(_localctx, 134, NebulaSQLParser::RuleHint);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(752);
    match(NebulaSQLParser::T__5);
    setState(753);
    dynamic_cast<HintContext *>(_localctx)->hintStatementContext = hintStatement();
    dynamic_cast<HintContext *>(_localctx)->hintStatements.push_back(dynamic_cast<HintContext *>(_localctx)->hintStatementContext);
    setState(760);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 80, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(755);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 79, _ctx)) {
        case 1: {
          setState(754);
          match(NebulaSQLParser::T__1);
          break;
        }

        default:
          break;
        }
        setState(757);
        dynamic_cast<HintContext *>(_localctx)->hintStatementContext = hintStatement();
        dynamic_cast<HintContext *>(_localctx)->hintStatements.push_back(dynamic_cast<HintContext *>(_localctx)->hintStatementContext); 
      }
      setState(762);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 80, _ctx);
    }
    setState(763);
    match(NebulaSQLParser::T__6);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- HintStatementContext ------------------------------------------------------------------

NebulaSQLParser::HintStatementContext::HintStatementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::IdentifierContext* NebulaSQLParser::HintStatementContext::identifier() {
  return getRuleContext<NebulaSQLParser::IdentifierContext>(0);
}

std::vector<NebulaSQLParser::PrimaryExpressionContext *> NebulaSQLParser::HintStatementContext::primaryExpression() {
  return getRuleContexts<NebulaSQLParser::PrimaryExpressionContext>();
}

NebulaSQLParser::PrimaryExpressionContext* NebulaSQLParser::HintStatementContext::primaryExpression(size_t i) {
  return getRuleContext<NebulaSQLParser::PrimaryExpressionContext>(i);
}


size_t NebulaSQLParser::HintStatementContext::getRuleIndex() const {
  return NebulaSQLParser::RuleHintStatement;
}

void NebulaSQLParser::HintStatementContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterHintStatement(this);
}

void NebulaSQLParser::HintStatementContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitHintStatement(this);
}

NebulaSQLParser::HintStatementContext* NebulaSQLParser::hintStatement() {
  HintStatementContext *_localctx = _tracker.createInstance<HintStatementContext>(_ctx, getState());
  enterRule(_localctx, 136, NebulaSQLParser::RuleHintStatement);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(778);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 82, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(765);
      dynamic_cast<HintStatementContext *>(_localctx)->hintName = identifier();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(766);
      dynamic_cast<HintStatementContext *>(_localctx)->hintName = identifier();
      setState(767);
      match(NebulaSQLParser::T__2);
      setState(768);
      dynamic_cast<HintStatementContext *>(_localctx)->primaryExpressionContext = primaryExpression(0);
      dynamic_cast<HintStatementContext *>(_localctx)->parameters.push_back(dynamic_cast<HintStatementContext *>(_localctx)->primaryExpressionContext);
      setState(773);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NebulaSQLParser::T__1) {
        setState(769);
        match(NebulaSQLParser::T__1);
        setState(770);
        dynamic_cast<HintStatementContext *>(_localctx)->primaryExpressionContext = primaryExpression(0);
        dynamic_cast<HintStatementContext *>(_localctx)->parameters.push_back(dynamic_cast<HintStatementContext *>(_localctx)->primaryExpressionContext);
        setState(775);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(776);
      match(NebulaSQLParser::T__3);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrimaryExpressionContext ------------------------------------------------------------------

NebulaSQLParser::PrimaryExpressionContext::PrimaryExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::PrimaryExpressionContext::getRuleIndex() const {
  return NebulaSQLParser::RulePrimaryExpression;
}

void NebulaSQLParser::PrimaryExpressionContext::copyFrom(PrimaryExpressionContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- DereferenceContext ------------------------------------------------------------------

NebulaSQLParser::PrimaryExpressionContext* NebulaSQLParser::DereferenceContext::primaryExpression() {
  return getRuleContext<NebulaSQLParser::PrimaryExpressionContext>(0);
}

NebulaSQLParser::IdentifierContext* NebulaSQLParser::DereferenceContext::identifier() {
  return getRuleContext<NebulaSQLParser::IdentifierContext>(0);
}

NebulaSQLParser::DereferenceContext::DereferenceContext(PrimaryExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::DereferenceContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDereference(this);
}
void NebulaSQLParser::DereferenceContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDereference(this);
}
//----------------- ConstantDefaultContext ------------------------------------------------------------------

NebulaSQLParser::ConstantContext* NebulaSQLParser::ConstantDefaultContext::constant() {
  return getRuleContext<NebulaSQLParser::ConstantContext>(0);
}

NebulaSQLParser::ConstantDefaultContext::ConstantDefaultContext(PrimaryExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::ConstantDefaultContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstantDefault(this);
}
void NebulaSQLParser::ConstantDefaultContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstantDefault(this);
}
//----------------- ColumnReferenceContext ------------------------------------------------------------------

NebulaSQLParser::IdentifierContext* NebulaSQLParser::ColumnReferenceContext::identifier() {
  return getRuleContext<NebulaSQLParser::IdentifierContext>(0);
}

NebulaSQLParser::ColumnReferenceContext::ColumnReferenceContext(PrimaryExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::ColumnReferenceContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterColumnReference(this);
}
void NebulaSQLParser::ColumnReferenceContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitColumnReference(this);
}
//----------------- RowConstructorContext ------------------------------------------------------------------

std::vector<NebulaSQLParser::NamedExpressionContext *> NebulaSQLParser::RowConstructorContext::namedExpression() {
  return getRuleContexts<NebulaSQLParser::NamedExpressionContext>();
}

NebulaSQLParser::NamedExpressionContext* NebulaSQLParser::RowConstructorContext::namedExpression(size_t i) {
  return getRuleContext<NebulaSQLParser::NamedExpressionContext>(i);
}

NebulaSQLParser::RowConstructorContext::RowConstructorContext(PrimaryExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::RowConstructorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterRowConstructor(this);
}
void NebulaSQLParser::RowConstructorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitRowConstructor(this);
}
//----------------- ParenthesizedExpressionContext ------------------------------------------------------------------

NebulaSQLParser::ExpressionContext* NebulaSQLParser::ParenthesizedExpressionContext::expression() {
  return getRuleContext<NebulaSQLParser::ExpressionContext>(0);
}

NebulaSQLParser::ParenthesizedExpressionContext::ParenthesizedExpressionContext(PrimaryExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::ParenthesizedExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterParenthesizedExpression(this);
}
void NebulaSQLParser::ParenthesizedExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitParenthesizedExpression(this);
}
//----------------- StarContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::StarContext::ASTERISK() {
  return getToken(NebulaSQLParser::ASTERISK, 0);
}

NebulaSQLParser::QualifiedNameContext* NebulaSQLParser::StarContext::qualifiedName() {
  return getRuleContext<NebulaSQLParser::QualifiedNameContext>(0);
}

NebulaSQLParser::StarContext::StarContext(PrimaryExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::StarContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterStar(this);
}
void NebulaSQLParser::StarContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitStar(this);
}
//----------------- FunctionCallContext ------------------------------------------------------------------

NebulaSQLParser::FunctionNameContext* NebulaSQLParser::FunctionCallContext::functionName() {
  return getRuleContext<NebulaSQLParser::FunctionNameContext>(0);
}

std::vector<NebulaSQLParser::ExpressionContext *> NebulaSQLParser::FunctionCallContext::expression() {
  return getRuleContexts<NebulaSQLParser::ExpressionContext>();
}

NebulaSQLParser::ExpressionContext* NebulaSQLParser::FunctionCallContext::expression(size_t i) {
  return getRuleContext<NebulaSQLParser::ExpressionContext>(i);
}

NebulaSQLParser::FunctionCallContext::FunctionCallContext(PrimaryExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::FunctionCallContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFunctionCall(this);
}
void NebulaSQLParser::FunctionCallContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFunctionCall(this);
}
//----------------- SubqueryExpressionContext ------------------------------------------------------------------

NebulaSQLParser::QueryContext* NebulaSQLParser::SubqueryExpressionContext::query() {
  return getRuleContext<NebulaSQLParser::QueryContext>(0);
}

NebulaSQLParser::SubqueryExpressionContext::SubqueryExpressionContext(PrimaryExpressionContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::SubqueryExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSubqueryExpression(this);
}
void NebulaSQLParser::SubqueryExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSubqueryExpression(this);
}

NebulaSQLParser::PrimaryExpressionContext* NebulaSQLParser::primaryExpression() {
   return primaryExpression(0);
}

NebulaSQLParser::PrimaryExpressionContext* NebulaSQLParser::primaryExpression(int precedence) {
  ParserRuleContext *parentContext = _ctx;
  size_t parentState = getState();
  NebulaSQLParser::PrimaryExpressionContext *_localctx = _tracker.createInstance<PrimaryExpressionContext>(_ctx, parentState);
  NebulaSQLParser::PrimaryExpressionContext *previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by generated code.
  size_t startState = 138;
  enterRecursionRule(_localctx, 138, NebulaSQLParser::RulePrimaryExpression, precedence);

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
    setState(820);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 86, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<StarContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(781);
      match(NebulaSQLParser::ASTERISK);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<StarContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(782);
      qualifiedName();
      setState(783);
      match(NebulaSQLParser::T__4);
      setState(784);
      match(NebulaSQLParser::ASTERISK);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<SubqueryExpressionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(786);
      match(NebulaSQLParser::T__2);
      setState(787);
      query();
      setState(788);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 4: {
      _localctx = _tracker.createInstance<RowConstructorContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(790);
      match(NebulaSQLParser::T__2);
      setState(791);
      namedExpression();
      setState(794); 
      _errHandler->sync(this);
      _la = _input->LA(1);
      do {
        setState(792);
        match(NebulaSQLParser::T__1);
        setState(793);
        namedExpression();
        setState(796); 
        _errHandler->sync(this);
        _la = _input->LA(1);
      } while (_la == NebulaSQLParser::T__1);
      setState(798);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 5: {
      _localctx = _tracker.createInstance<FunctionCallContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(800);
      functionName();
      setState(801);
      match(NebulaSQLParser::T__2);
      setState(810);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 85, _ctx)) {
      case 1: {
        setState(802);
        dynamic_cast<FunctionCallContext *>(_localctx)->expressionContext = expression();
        dynamic_cast<FunctionCallContext *>(_localctx)->argument.push_back(dynamic_cast<FunctionCallContext *>(_localctx)->expressionContext);
        setState(807);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NebulaSQLParser::T__1) {
          setState(803);
          match(NebulaSQLParser::T__1);
          setState(804);
          dynamic_cast<FunctionCallContext *>(_localctx)->expressionContext = expression();
          dynamic_cast<FunctionCallContext *>(_localctx)->argument.push_back(dynamic_cast<FunctionCallContext *>(_localctx)->expressionContext);
          setState(809);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

      default:
        break;
      }
      setState(812);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 6: {
      _localctx = _tracker.createInstance<ParenthesizedExpressionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(814);
      match(NebulaSQLParser::T__2);
      setState(815);
      expression();
      setState(816);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 7: {
      _localctx = _tracker.createInstance<ConstantDefaultContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(818);
      constant();
      break;
    }

    case 8: {
      _localctx = _tracker.createInstance<ColumnReferenceContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(819);
      identifier();
      break;
    }

    default:
      break;
    }
    _ctx->stop = _input->LT(-1);
    setState(827);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 87, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        auto newContext = _tracker.createInstance<DereferenceContext>(_tracker.createInstance<PrimaryExpressionContext>(parentContext, parentState));
        _localctx = newContext;
        newContext->base = previousContext;
        pushNewRecursionContext(newContext, startState, RulePrimaryExpression);
        setState(822);

        if (!(precpred(_ctx, 7))) throw FailedPredicateException(this, "precpred(_ctx, 7)");
        setState(823);
        match(NebulaSQLParser::T__4);
        setState(824);
        dynamic_cast<DereferenceContext *>(_localctx)->fieldName = identifier(); 
      }
      setState(829);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 87, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- QualifiedNameContext ------------------------------------------------------------------

NebulaSQLParser::QualifiedNameContext::QualifiedNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<NebulaSQLParser::IdentifierContext *> NebulaSQLParser::QualifiedNameContext::identifier() {
  return getRuleContexts<NebulaSQLParser::IdentifierContext>();
}

NebulaSQLParser::IdentifierContext* NebulaSQLParser::QualifiedNameContext::identifier(size_t i) {
  return getRuleContext<NebulaSQLParser::IdentifierContext>(i);
}


size_t NebulaSQLParser::QualifiedNameContext::getRuleIndex() const {
  return NebulaSQLParser::RuleQualifiedName;
}

void NebulaSQLParser::QualifiedNameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterQualifiedName(this);
}

void NebulaSQLParser::QualifiedNameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitQualifiedName(this);
}

NebulaSQLParser::QualifiedNameContext* NebulaSQLParser::qualifiedName() {
  QualifiedNameContext *_localctx = _tracker.createInstance<QualifiedNameContext>(_ctx, getState());
  enterRule(_localctx, 140, NebulaSQLParser::RuleQualifiedName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(830);
    identifier();
    setState(835);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 88, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(831);
        match(NebulaSQLParser::T__4);
        setState(832);
        identifier(); 
      }
      setState(837);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 88, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumberContext ------------------------------------------------------------------

NebulaSQLParser::NumberContext::NumberContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::NumberContext::getRuleIndex() const {
  return NebulaSQLParser::RuleNumber;
}

void NebulaSQLParser::NumberContext::copyFrom(NumberContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- DecimalLiteralContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::DecimalLiteralContext::DECIMAL_VALUE() {
  return getToken(NebulaSQLParser::DECIMAL_VALUE, 0);
}

tree::TerminalNode* NebulaSQLParser::DecimalLiteralContext::MINUS() {
  return getToken(NebulaSQLParser::MINUS, 0);
}

NebulaSQLParser::DecimalLiteralContext::DecimalLiteralContext(NumberContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::DecimalLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDecimalLiteral(this);
}
void NebulaSQLParser::DecimalLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDecimalLiteral(this);
}
//----------------- BigIntLiteralContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::BigIntLiteralContext::BIGINT_LITERAL() {
  return getToken(NebulaSQLParser::BIGINT_LITERAL, 0);
}

tree::TerminalNode* NebulaSQLParser::BigIntLiteralContext::MINUS() {
  return getToken(NebulaSQLParser::MINUS, 0);
}

NebulaSQLParser::BigIntLiteralContext::BigIntLiteralContext(NumberContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::BigIntLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBigIntLiteral(this);
}
void NebulaSQLParser::BigIntLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBigIntLiteral(this);
}
//----------------- TinyIntLiteralContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::TinyIntLiteralContext::TINYINT_LITERAL() {
  return getToken(NebulaSQLParser::TINYINT_LITERAL, 0);
}

tree::TerminalNode* NebulaSQLParser::TinyIntLiteralContext::MINUS() {
  return getToken(NebulaSQLParser::MINUS, 0);
}

NebulaSQLParser::TinyIntLiteralContext::TinyIntLiteralContext(NumberContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::TinyIntLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTinyIntLiteral(this);
}
void NebulaSQLParser::TinyIntLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTinyIntLiteral(this);
}
//----------------- LegacyDecimalLiteralContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::LegacyDecimalLiteralContext::EXPONENT_VALUE() {
  return getToken(NebulaSQLParser::EXPONENT_VALUE, 0);
}

tree::TerminalNode* NebulaSQLParser::LegacyDecimalLiteralContext::DECIMAL_VALUE() {
  return getToken(NebulaSQLParser::DECIMAL_VALUE, 0);
}

tree::TerminalNode* NebulaSQLParser::LegacyDecimalLiteralContext::MINUS() {
  return getToken(NebulaSQLParser::MINUS, 0);
}

NebulaSQLParser::LegacyDecimalLiteralContext::LegacyDecimalLiteralContext(NumberContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::LegacyDecimalLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLegacyDecimalLiteral(this);
}
void NebulaSQLParser::LegacyDecimalLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLegacyDecimalLiteral(this);
}
//----------------- BigDecimalLiteralContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::BigDecimalLiteralContext::BIGDECIMAL_LITERAL() {
  return getToken(NebulaSQLParser::BIGDECIMAL_LITERAL, 0);
}

tree::TerminalNode* NebulaSQLParser::BigDecimalLiteralContext::MINUS() {
  return getToken(NebulaSQLParser::MINUS, 0);
}

NebulaSQLParser::BigDecimalLiteralContext::BigDecimalLiteralContext(NumberContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::BigDecimalLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBigDecimalLiteral(this);
}
void NebulaSQLParser::BigDecimalLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBigDecimalLiteral(this);
}
//----------------- ExponentLiteralContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::ExponentLiteralContext::EXPONENT_VALUE() {
  return getToken(NebulaSQLParser::EXPONENT_VALUE, 0);
}

tree::TerminalNode* NebulaSQLParser::ExponentLiteralContext::MINUS() {
  return getToken(NebulaSQLParser::MINUS, 0);
}

NebulaSQLParser::ExponentLiteralContext::ExponentLiteralContext(NumberContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::ExponentLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExponentLiteral(this);
}
void NebulaSQLParser::ExponentLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExponentLiteral(this);
}
//----------------- DoubleLiteralContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::DoubleLiteralContext::DOUBLE_LITERAL() {
  return getToken(NebulaSQLParser::DOUBLE_LITERAL, 0);
}

tree::TerminalNode* NebulaSQLParser::DoubleLiteralContext::MINUS() {
  return getToken(NebulaSQLParser::MINUS, 0);
}

NebulaSQLParser::DoubleLiteralContext::DoubleLiteralContext(NumberContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::DoubleLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDoubleLiteral(this);
}
void NebulaSQLParser::DoubleLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDoubleLiteral(this);
}
//----------------- IntegerLiteralContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::IntegerLiteralContext::INTEGER_VALUE() {
  return getToken(NebulaSQLParser::INTEGER_VALUE, 0);
}

tree::TerminalNode* NebulaSQLParser::IntegerLiteralContext::MINUS() {
  return getToken(NebulaSQLParser::MINUS, 0);
}

NebulaSQLParser::IntegerLiteralContext::IntegerLiteralContext(NumberContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::IntegerLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIntegerLiteral(this);
}
void NebulaSQLParser::IntegerLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIntegerLiteral(this);
}
//----------------- FloatLiteralContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::FloatLiteralContext::FLOAT_LITERAL() {
  return getToken(NebulaSQLParser::FLOAT_LITERAL, 0);
}

tree::TerminalNode* NebulaSQLParser::FloatLiteralContext::MINUS() {
  return getToken(NebulaSQLParser::MINUS, 0);
}

NebulaSQLParser::FloatLiteralContext::FloatLiteralContext(NumberContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::FloatLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFloatLiteral(this);
}
void NebulaSQLParser::FloatLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFloatLiteral(this);
}
//----------------- SmallIntLiteralContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::SmallIntLiteralContext::SMALLINT_LITERAL() {
  return getToken(NebulaSQLParser::SMALLINT_LITERAL, 0);
}

tree::TerminalNode* NebulaSQLParser::SmallIntLiteralContext::MINUS() {
  return getToken(NebulaSQLParser::MINUS, 0);
}

NebulaSQLParser::SmallIntLiteralContext::SmallIntLiteralContext(NumberContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::SmallIntLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSmallIntLiteral(this);
}
void NebulaSQLParser::SmallIntLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSmallIntLiteral(this);
}
NebulaSQLParser::NumberContext* NebulaSQLParser::number() {
  NumberContext *_localctx = _tracker.createInstance<NumberContext>(_ctx, getState());
  enterRule(_localctx, 142, NebulaSQLParser::RuleNumber);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(881);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 99, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::ExponentLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(838);

      if (!(!legacy_exponent_literal_as_decimal_enabled)) throw FailedPredicateException(this, "!legacy_exponent_literal_as_decimal_enabled");
      setState(840);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(839);
        match(NebulaSQLParser::MINUS);
      }
      setState(842);
      match(NebulaSQLParser::EXPONENT_VALUE);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::DecimalLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(843);

      if (!(!legacy_exponent_literal_as_decimal_enabled)) throw FailedPredicateException(this, "!legacy_exponent_literal_as_decimal_enabled");
      setState(845);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(844);
        match(NebulaSQLParser::MINUS);
      }
      setState(847);
      match(NebulaSQLParser::DECIMAL_VALUE);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::LegacyDecimalLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(848);

      if (!(legacy_exponent_literal_as_decimal_enabled)) throw FailedPredicateException(this, "legacy_exponent_literal_as_decimal_enabled");
      setState(850);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(849);
        match(NebulaSQLParser::MINUS);
      }
      setState(852);
      _la = _input->LA(1);
      if (!(_la == NebulaSQLParser::EXPONENT_VALUE

      || _la == NebulaSQLParser::DECIMAL_VALUE)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      break;
    }

    case 4: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::IntegerLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(854);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(853);
        match(NebulaSQLParser::MINUS);
      }
      setState(856);
      match(NebulaSQLParser::INTEGER_VALUE);
      break;
    }

    case 5: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::BigIntLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(858);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(857);
        match(NebulaSQLParser::MINUS);
      }
      setState(860);
      match(NebulaSQLParser::BIGINT_LITERAL);
      break;
    }

    case 6: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::SmallIntLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 6);
      setState(862);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(861);
        match(NebulaSQLParser::MINUS);
      }
      setState(864);
      match(NebulaSQLParser::SMALLINT_LITERAL);
      break;
    }

    case 7: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::TinyIntLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 7);
      setState(866);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(865);
        match(NebulaSQLParser::MINUS);
      }
      setState(868);
      match(NebulaSQLParser::TINYINT_LITERAL);
      break;
    }

    case 8: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::DoubleLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 8);
      setState(870);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(869);
        match(NebulaSQLParser::MINUS);
      }
      setState(872);
      match(NebulaSQLParser::DOUBLE_LITERAL);
      break;
    }

    case 9: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::FloatLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 9);
      setState(874);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(873);
        match(NebulaSQLParser::MINUS);
      }
      setState(876);
      match(NebulaSQLParser::FLOAT_LITERAL);
      break;
    }

    case 10: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::BigDecimalLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 10);
      setState(878);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(877);
        match(NebulaSQLParser::MINUS);
      }
      setState(880);
      match(NebulaSQLParser::BIGDECIMAL_LITERAL);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstantContext ------------------------------------------------------------------

NebulaSQLParser::ConstantContext::ConstantContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::ConstantContext::getRuleIndex() const {
  return NebulaSQLParser::RuleConstant;
}

void NebulaSQLParser::ConstantContext::copyFrom(ConstantContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- NullLiteralContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::NullLiteralContext::NULLTOKEN() {
  return getToken(NebulaSQLParser::NULLTOKEN, 0);
}

NebulaSQLParser::NullLiteralContext::NullLiteralContext(ConstantContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::NullLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNullLiteral(this);
}
void NebulaSQLParser::NullLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNullLiteral(this);
}
//----------------- StringLiteralContext ------------------------------------------------------------------

std::vector<tree::TerminalNode *> NebulaSQLParser::StringLiteralContext::STRING() {
  return getTokens(NebulaSQLParser::STRING);
}

tree::TerminalNode* NebulaSQLParser::StringLiteralContext::STRING(size_t i) {
  return getToken(NebulaSQLParser::STRING, i);
}

NebulaSQLParser::StringLiteralContext::StringLiteralContext(ConstantContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::StringLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterStringLiteral(this);
}
void NebulaSQLParser::StringLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitStringLiteral(this);
}
//----------------- TypeConstructorContext ------------------------------------------------------------------

NebulaSQLParser::IdentifierContext* NebulaSQLParser::TypeConstructorContext::identifier() {
  return getRuleContext<NebulaSQLParser::IdentifierContext>(0);
}

tree::TerminalNode* NebulaSQLParser::TypeConstructorContext::STRING() {
  return getToken(NebulaSQLParser::STRING, 0);
}

NebulaSQLParser::TypeConstructorContext::TypeConstructorContext(ConstantContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::TypeConstructorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTypeConstructor(this);
}
void NebulaSQLParser::TypeConstructorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTypeConstructor(this);
}
//----------------- NumericLiteralContext ------------------------------------------------------------------

NebulaSQLParser::NumberContext* NebulaSQLParser::NumericLiteralContext::number() {
  return getRuleContext<NebulaSQLParser::NumberContext>(0);
}

NebulaSQLParser::NumericLiteralContext::NumericLiteralContext(ConstantContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::NumericLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteral(this);
}
void NebulaSQLParser::NumericLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteral(this);
}
//----------------- BooleanLiteralContext ------------------------------------------------------------------

NebulaSQLParser::BooleanValueContext* NebulaSQLParser::BooleanLiteralContext::booleanValue() {
  return getRuleContext<NebulaSQLParser::BooleanValueContext>(0);
}

NebulaSQLParser::BooleanLiteralContext::BooleanLiteralContext(ConstantContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::BooleanLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBooleanLiteral(this);
}
void NebulaSQLParser::BooleanLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBooleanLiteral(this);
}
NebulaSQLParser::ConstantContext* NebulaSQLParser::constant() {
  ConstantContext *_localctx = _tracker.createInstance<ConstantContext>(_ctx, getState());
  enterRule(_localctx, 144, NebulaSQLParser::RuleConstant);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(894);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 101, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ConstantContext *>(_tracker.createInstance<NebulaSQLParser::NullLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(883);
      match(NebulaSQLParser::NULLTOKEN);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ConstantContext *>(_tracker.createInstance<NebulaSQLParser::TypeConstructorContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(884);
      identifier();
      setState(885);
      match(NebulaSQLParser::STRING);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ConstantContext *>(_tracker.createInstance<NebulaSQLParser::NumericLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(887);
      number();
      break;
    }

    case 4: {
      _localctx = dynamic_cast<ConstantContext *>(_tracker.createInstance<NebulaSQLParser::BooleanLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(888);
      booleanValue();
      break;
    }

    case 5: {
      _localctx = dynamic_cast<ConstantContext *>(_tracker.createInstance<NebulaSQLParser::StringLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(890); 
      _errHandler->sync(this);
      alt = 1;
      do {
        switch (alt) {
          case 1: {
                setState(889);
                match(NebulaSQLParser::STRING);
                break;
              }

        default:
          throw NoViableAltException(this);
        }
        setState(892); 
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 100, _ctx);
      } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BooleanValueContext ------------------------------------------------------------------

NebulaSQLParser::BooleanValueContext::BooleanValueContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::BooleanValueContext::TRUE() {
  return getToken(NebulaSQLParser::TRUE, 0);
}

tree::TerminalNode* NebulaSQLParser::BooleanValueContext::FALSE() {
  return getToken(NebulaSQLParser::FALSE, 0);
}


size_t NebulaSQLParser::BooleanValueContext::getRuleIndex() const {
  return NebulaSQLParser::RuleBooleanValue;
}

void NebulaSQLParser::BooleanValueContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBooleanValue(this);
}

void NebulaSQLParser::BooleanValueContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBooleanValue(this);
}

NebulaSQLParser::BooleanValueContext* NebulaSQLParser::booleanValue() {
  BooleanValueContext *_localctx = _tracker.createInstance<BooleanValueContext>(_ctx, getState());
  enterRule(_localctx, 146, NebulaSQLParser::RuleBooleanValue);
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
    setState(896);
    _la = _input->LA(1);
    if (!(_la == NebulaSQLParser::FALSE

    || _la == NebulaSQLParser::TRUE)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StrictNonReservedContext ------------------------------------------------------------------

NebulaSQLParser::StrictNonReservedContext::StrictNonReservedContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::StrictNonReservedContext::FULL() {
  return getToken(NebulaSQLParser::FULL, 0);
}

tree::TerminalNode* NebulaSQLParser::StrictNonReservedContext::INNER() {
  return getToken(NebulaSQLParser::INNER, 0);
}

tree::TerminalNode* NebulaSQLParser::StrictNonReservedContext::JOIN() {
  return getToken(NebulaSQLParser::JOIN, 0);
}

tree::TerminalNode* NebulaSQLParser::StrictNonReservedContext::LEFT() {
  return getToken(NebulaSQLParser::LEFT, 0);
}

tree::TerminalNode* NebulaSQLParser::StrictNonReservedContext::NATURAL() {
  return getToken(NebulaSQLParser::NATURAL, 0);
}

tree::TerminalNode* NebulaSQLParser::StrictNonReservedContext::ON() {
  return getToken(NebulaSQLParser::ON, 0);
}

tree::TerminalNode* NebulaSQLParser::StrictNonReservedContext::RIGHT() {
  return getToken(NebulaSQLParser::RIGHT, 0);
}

tree::TerminalNode* NebulaSQLParser::StrictNonReservedContext::UNION() {
  return getToken(NebulaSQLParser::UNION, 0);
}

tree::TerminalNode* NebulaSQLParser::StrictNonReservedContext::USING() {
  return getToken(NebulaSQLParser::USING, 0);
}


size_t NebulaSQLParser::StrictNonReservedContext::getRuleIndex() const {
  return NebulaSQLParser::RuleStrictNonReserved;
}

void NebulaSQLParser::StrictNonReservedContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterStrictNonReserved(this);
}

void NebulaSQLParser::StrictNonReservedContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitStrictNonReserved(this);
}

NebulaSQLParser::StrictNonReservedContext* NebulaSQLParser::strictNonReserved() {
  StrictNonReservedContext *_localctx = _tracker.createInstance<StrictNonReservedContext>(_ctx, getState());
  enterRule(_localctx, 148, NebulaSQLParser::RuleStrictNonReserved);
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
    setState(898);
    _la = _input->LA(1);
    if (!(((((_la - 32) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 32)) & ((1ULL << (NebulaSQLParser::FULL - 32))
      | (1ULL << (NebulaSQLParser::INNER - 32))
      | (1ULL << (NebulaSQLParser::JOIN - 32))
      | (1ULL << (NebulaSQLParser::LEFT - 32))
      | (1ULL << (NebulaSQLParser::NATURAL - 32))
      | (1ULL << (NebulaSQLParser::ON - 32))
      | (1ULL << (NebulaSQLParser::RIGHT - 32))
      | (1ULL << (NebulaSQLParser::UNION - 32))
      | (1ULL << (NebulaSQLParser::USING - 32)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AnsiNonReservedContext ------------------------------------------------------------------

NebulaSQLParser::AnsiNonReservedContext::AnsiNonReservedContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::ASC() {
  return getToken(NebulaSQLParser::ASC, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::AT() {
  return getToken(NebulaSQLParser::AT, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::BETWEEN() {
  return getToken(NebulaSQLParser::BETWEEN, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::BY() {
  return getToken(NebulaSQLParser::BY, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::CUBE() {
  return getToken(NebulaSQLParser::CUBE, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::DELETE() {
  return getToken(NebulaSQLParser::DELETE, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::DESC() {
  return getToken(NebulaSQLParser::DESC, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::DIV() {
  return getToken(NebulaSQLParser::DIV, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::DROP() {
  return getToken(NebulaSQLParser::DROP, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::EXISTS() {
  return getToken(NebulaSQLParser::EXISTS, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::FIRST() {
  return getToken(NebulaSQLParser::FIRST, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::GROUPING() {
  return getToken(NebulaSQLParser::GROUPING, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::INSERT() {
  return getToken(NebulaSQLParser::INSERT, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::LAST() {
  return getToken(NebulaSQLParser::LAST, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::LIKE() {
  return getToken(NebulaSQLParser::LIKE, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::LIMIT() {
  return getToken(NebulaSQLParser::LIMIT, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::MERGE() {
  return getToken(NebulaSQLParser::MERGE, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::NULLS() {
  return getToken(NebulaSQLParser::NULLS, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::QUERY() {
  return getToken(NebulaSQLParser::QUERY, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::RLIKE() {
  return getToken(NebulaSQLParser::RLIKE, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::ROLLUP() {
  return getToken(NebulaSQLParser::ROLLUP, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::SETS() {
  return getToken(NebulaSQLParser::SETS, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::TRUE() {
  return getToken(NebulaSQLParser::TRUE, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::TYPE() {
  return getToken(NebulaSQLParser::TYPE, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::VALUES() {
  return getToken(NebulaSQLParser::VALUES, 0);
}

tree::TerminalNode* NebulaSQLParser::AnsiNonReservedContext::WINDOW() {
  return getToken(NebulaSQLParser::WINDOW, 0);
}


size_t NebulaSQLParser::AnsiNonReservedContext::getRuleIndex() const {
  return NebulaSQLParser::RuleAnsiNonReserved;
}

void NebulaSQLParser::AnsiNonReservedContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAnsiNonReserved(this);
}

void NebulaSQLParser::AnsiNonReservedContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAnsiNonReserved(this);
}

NebulaSQLParser::AnsiNonReservedContext* NebulaSQLParser::ansiNonReserved() {
  AnsiNonReservedContext *_localctx = _tracker.createInstance<AnsiNonReservedContext>(_ctx, getState());
  enterRule(_localctx, 150, NebulaSQLParser::RuleAnsiNonReserved);
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
    setState(900);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << NebulaSQLParser::ASC)
      | (1ULL << NebulaSQLParser::AT)
      | (1ULL << NebulaSQLParser::BETWEEN)
      | (1ULL << NebulaSQLParser::BY)
      | (1ULL << NebulaSQLParser::CUBE)
      | (1ULL << NebulaSQLParser::DELETE)
      | (1ULL << NebulaSQLParser::DESC)
      | (1ULL << NebulaSQLParser::DIV)
      | (1ULL << NebulaSQLParser::DROP)
      | (1ULL << NebulaSQLParser::EXISTS)
      | (1ULL << NebulaSQLParser::FIRST)
      | (1ULL << NebulaSQLParser::GROUPING)
      | (1ULL << NebulaSQLParser::INSERT)
      | (1ULL << NebulaSQLParser::LAST)
      | (1ULL << NebulaSQLParser::LIKE)
      | (1ULL << NebulaSQLParser::LIMIT)
      | (1ULL << NebulaSQLParser::MERGE)
      | (1ULL << NebulaSQLParser::NULLS)
      | (1ULL << NebulaSQLParser::QUERY)
      | (1ULL << NebulaSQLParser::RLIKE)
      | (1ULL << NebulaSQLParser::ROLLUP)
      | (1ULL << NebulaSQLParser::SETS))) != 0) || ((((_la - 68) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 68)) & ((1ULL << (NebulaSQLParser::TRUE - 68))
      | (1ULL << (NebulaSQLParser::TYPE - 68))
      | (1ULL << (NebulaSQLParser::VALUES - 68))
      | (1ULL << (NebulaSQLParser::WINDOW - 68)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NonReservedContext ------------------------------------------------------------------

NebulaSQLParser::NonReservedContext::NonReservedContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::ASC() {
  return getToken(NebulaSQLParser::ASC, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::AT() {
  return getToken(NebulaSQLParser::AT, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::BETWEEN() {
  return getToken(NebulaSQLParser::BETWEEN, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::CUBE() {
  return getToken(NebulaSQLParser::CUBE, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::BY() {
  return getToken(NebulaSQLParser::BY, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::DELETE() {
  return getToken(NebulaSQLParser::DELETE, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::DESC() {
  return getToken(NebulaSQLParser::DESC, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::DIV() {
  return getToken(NebulaSQLParser::DIV, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::DROP() {
  return getToken(NebulaSQLParser::DROP, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::EXISTS() {
  return getToken(NebulaSQLParser::EXISTS, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::FIRST() {
  return getToken(NebulaSQLParser::FIRST, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::GROUPING() {
  return getToken(NebulaSQLParser::GROUPING, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::INSERT() {
  return getToken(NebulaSQLParser::INSERT, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::LAST() {
  return getToken(NebulaSQLParser::LAST, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::LIKE() {
  return getToken(NebulaSQLParser::LIKE, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::LIMIT() {
  return getToken(NebulaSQLParser::LIMIT, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::MERGE() {
  return getToken(NebulaSQLParser::MERGE, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::NULLS() {
  return getToken(NebulaSQLParser::NULLS, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::QUERY() {
  return getToken(NebulaSQLParser::QUERY, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::RLIKE() {
  return getToken(NebulaSQLParser::RLIKE, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::ROLLUP() {
  return getToken(NebulaSQLParser::ROLLUP, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::SETS() {
  return getToken(NebulaSQLParser::SETS, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::TRUE() {
  return getToken(NebulaSQLParser::TRUE, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::TYPE() {
  return getToken(NebulaSQLParser::TYPE, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::VALUES() {
  return getToken(NebulaSQLParser::VALUES, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::WINDOW() {
  return getToken(NebulaSQLParser::WINDOW, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::ALL() {
  return getToken(NebulaSQLParser::ALL, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::AND() {
  return getToken(NebulaSQLParser::AND, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::ANY() {
  return getToken(NebulaSQLParser::ANY, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::AS() {
  return getToken(NebulaSQLParser::AS, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::DISTINCT() {
  return getToken(NebulaSQLParser::DISTINCT, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::ESCAPE() {
  return getToken(NebulaSQLParser::ESCAPE, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::FALSE() {
  return getToken(NebulaSQLParser::FALSE, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::FROM() {
  return getToken(NebulaSQLParser::FROM, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::GROUP() {
  return getToken(NebulaSQLParser::GROUP, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::HAVING() {
  return getToken(NebulaSQLParser::HAVING, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::IN() {
  return getToken(NebulaSQLParser::IN, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::INTO() {
  return getToken(NebulaSQLParser::INTO, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::IS() {
  return getToken(NebulaSQLParser::IS, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::NOT() {
  return getToken(NebulaSQLParser::NOT, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::NULLTOKEN() {
  return getToken(NebulaSQLParser::NULLTOKEN, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::OR() {
  return getToken(NebulaSQLParser::OR, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::ORDER() {
  return getToken(NebulaSQLParser::ORDER, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::SELECT() {
  return getToken(NebulaSQLParser::SELECT, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::SOME() {
  return getToken(NebulaSQLParser::SOME, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::TABLE() {
  return getToken(NebulaSQLParser::TABLE, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::WHERE() {
  return getToken(NebulaSQLParser::WHERE, 0);
}

tree::TerminalNode* NebulaSQLParser::NonReservedContext::WITH() {
  return getToken(NebulaSQLParser::WITH, 0);
}


size_t NebulaSQLParser::NonReservedContext::getRuleIndex() const {
  return NebulaSQLParser::RuleNonReserved;
}

void NebulaSQLParser::NonReservedContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNonReserved(this);
}

void NebulaSQLParser::NonReservedContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNonReserved(this);
}

NebulaSQLParser::NonReservedContext* NebulaSQLParser::nonReserved() {
  NonReservedContext *_localctx = _tracker.createInstance<NonReservedContext>(_ctx, getState());
  enterRule(_localctx, 152, NebulaSQLParser::RuleNonReserved);
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
    setState(902);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << NebulaSQLParser::ALL)
      | (1ULL << NebulaSQLParser::AND)
      | (1ULL << NebulaSQLParser::ANY)
      | (1ULL << NebulaSQLParser::AS)
      | (1ULL << NebulaSQLParser::ASC)
      | (1ULL << NebulaSQLParser::AT)
      | (1ULL << NebulaSQLParser::BETWEEN)
      | (1ULL << NebulaSQLParser::BY)
      | (1ULL << NebulaSQLParser::CUBE)
      | (1ULL << NebulaSQLParser::DELETE)
      | (1ULL << NebulaSQLParser::DESC)
      | (1ULL << NebulaSQLParser::DISTINCT)
      | (1ULL << NebulaSQLParser::DIV)
      | (1ULL << NebulaSQLParser::DROP)
      | (1ULL << NebulaSQLParser::ESCAPE)
      | (1ULL << NebulaSQLParser::EXISTS)
      | (1ULL << NebulaSQLParser::FALSE)
      | (1ULL << NebulaSQLParser::FIRST)
      | (1ULL << NebulaSQLParser::FROM)
      | (1ULL << NebulaSQLParser::GROUP)
      | (1ULL << NebulaSQLParser::GROUPING)
      | (1ULL << NebulaSQLParser::HAVING)
      | (1ULL << NebulaSQLParser::IN)
      | (1ULL << NebulaSQLParser::INSERT)
      | (1ULL << NebulaSQLParser::INTO)
      | (1ULL << NebulaSQLParser::IS)
      | (1ULL << NebulaSQLParser::LAST)
      | (1ULL << NebulaSQLParser::LIKE)
      | (1ULL << NebulaSQLParser::LIMIT)
      | (1ULL << NebulaSQLParser::MERGE)
      | (1ULL << NebulaSQLParser::NOT)
      | (1ULL << NebulaSQLParser::NULLTOKEN)
      | (1ULL << NebulaSQLParser::NULLS)
      | (1ULL << NebulaSQLParser::OR)
      | (1ULL << NebulaSQLParser::ORDER)
      | (1ULL << NebulaSQLParser::QUERY)
      | (1ULL << NebulaSQLParser::RLIKE)
      | (1ULL << NebulaSQLParser::ROLLUP)
      | (1ULL << NebulaSQLParser::SELECT)
      | (1ULL << NebulaSQLParser::SETS))) != 0) || ((((_la - 64) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 64)) & ((1ULL << (NebulaSQLParser::SOME - 64))
      | (1ULL << (NebulaSQLParser::TABLE - 64))
      | (1ULL << (NebulaSQLParser::TRUE - 64))
      | (1ULL << (NebulaSQLParser::TYPE - 64))
      | (1ULL << (NebulaSQLParser::VALUES - 64))
      | (1ULL << (NebulaSQLParser::WHERE - 64))
      | (1ULL << (NebulaSQLParser::WINDOW - 64))
      | (1ULL << (NebulaSQLParser::WITH - 64)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

bool NebulaSQLParser::sempred(RuleContext *context, size_t ruleIndex, size_t predicateIndex) {
  switch (ruleIndex) {
    case 4: return queryTermSempred(dynamic_cast<QueryTermContext *>(context), predicateIndex);
    case 24: return identifierSempred(dynamic_cast<IdentifierContext *>(context), predicateIndex);
    case 25: return strictIdentifierSempred(dynamic_cast<StrictIdentifierContext *>(context), predicateIndex);
    case 33: return booleanExpressionSempred(dynamic_cast<BooleanExpressionContext *>(context), predicateIndex);
    case 65: return valueExpressionSempred(dynamic_cast<ValueExpressionContext *>(context), predicateIndex);
    case 69: return primaryExpressionSempred(dynamic_cast<PrimaryExpressionContext *>(context), predicateIndex);
    case 71: return numberSempred(dynamic_cast<NumberContext *>(context), predicateIndex);

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::queryTermSempred(QueryTermContext *_localctx, size_t predicateIndex) {
    NES_DEBUG("NebulaSQLParser: queryTermSempred : received context " + _localctx->getText());
  switch (predicateIndex) {
    case 0: return precpred(_ctx, 1);

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::identifierSempred(IdentifierContext *_localctx, size_t predicateIndex) {
    NES_DEBUG("NebulaSQLParser: identifierSempred : received context " + _localctx->getText());
  switch (predicateIndex) {
    case 1: return !SQL_standard_keyword_behavior;

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::strictIdentifierSempred(StrictIdentifierContext *_localctx, size_t predicateIndex) {
    NES_DEBUG("NebulaSQLParser: strictIdentifierSempred : received context " + _localctx->getText());
  switch (predicateIndex) {
    case 2: return SQL_standard_keyword_behavior;
    case 3: return !SQL_standard_keyword_behavior;

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::booleanExpressionSempred(BooleanExpressionContext *_localctx, size_t predicateIndex) {
    NES_DEBUG("NebulaSQLParser: booleanExpressionSempred : received context " + _localctx->getText());
  switch (predicateIndex) {
    case 4: return precpred(_ctx, 2);
    case 5: return precpred(_ctx, 1);

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::valueExpressionSempred(ValueExpressionContext *_localctx, size_t predicateIndex) {
    NES_DEBUG("NebulaSQLParser: valueExpressionSempred : received context " + _localctx->getText());
  switch (predicateIndex) {
    case 6: return precpred(_ctx, 6);
    case 7: return precpred(_ctx, 5);
    case 8: return precpred(_ctx, 4);
    case 9: return precpred(_ctx, 3);
    case 10: return precpred(_ctx, 2);
    case 11: return precpred(_ctx, 1);

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::primaryExpressionSempred(PrimaryExpressionContext *_localctx, size_t predicateIndex) {
    NES_DEBUG("NebulaSQLParser: primaryExpressionSempred : received context " + _localctx->getText());
  switch (predicateIndex) {
    case 12: return precpred(_ctx, 7);

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::numberSempred(NumberContext *_localctx, size_t predicateIndex) {
    NES_DEBUG("NebulaSQLParser: numberSempred : received context " + _localctx->getText());
  switch (predicateIndex) {
    case 13: return !legacy_exponent_literal_as_decimal_enabled;
    case 14: return !legacy_exponent_literal_as_decimal_enabled;
    case 15: return legacy_exponent_literal_as_decimal_enabled;

  default:
    break;
  }
  return true;
}

// Static vars and initialization.
std::vector<dfa::DFA> NebulaSQLParser::_decisionToDFA;
atn::PredictionContextCache NebulaSQLParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN NebulaSQLParser::_atn;
std::vector<uint16_t> NebulaSQLParser::_serializedATN;

std::vector<std::string> NebulaSQLParser::_ruleNames = {
  "singleStatement", "statement", "query", "queryOrganization", "queryTerm", 
  "queryPrimary", "querySpecification", "fromClause", "relation", "joinRelation", 
  "joinType", "joinCriteria", "relationPrimary", "functionTable", "fromStatement", 
  "fromStatementBody", "selectClause", "whereClause", "havingClause", "inlineTable", 
  "tableAlias", "multipartIdentifierList", "multipartIdentifier", "namedExpression", 
  "identifier", "strictIdentifier", "quotedIdentifier", "identifierList", 
  "identifierSeq", "errorCapturingIdentifier", "errorCapturingIdentifierExtra", 
  "namedExpressionSeq", "expression", "booleanExpression", "windowedAggregationClause", 
  "aggregationClause", "groupingSet", "windowClause", "watermarkClause", 
  "watermarkParameters", "windowSpec", "timeWindow", "countWindow", "sizeParameter", 
  "advancebyParameter", "timeUnit", "timestampParameter", "functionName", 
  "sinkClause", "sinkType", "sinkTypeCSV", "csvKeyword", "sinkTypeZMQ", 
  "nullNotnull", "zmqKeyword", "streamName", "host", "port", "sinkTypeKafka", 
  "kafkaKeyword", "kafkaBroker", "kafkaTopic", "kafkaProducerTimout", "sortItem", 
  "predicate", "valueExpression", "comparisonOperator", "hint", "hintStatement", 
  "primaryExpression", "qualifiedName", "number", "constant", "booleanValue", 
  "strictNonReserved", "ansiNonReserved", "nonReserved"
};

std::vector<std::string> NebulaSQLParser::_literalNames = {
  "", "';'", "','", "'('", "')'", "'.'", "'/*+'", "'*/'", "", "", "", "'ANY'", 
  "", "", "'AT'", "", "", "'COMMENT'", "'CUBE'", "'DELETE'", "", "'DISTINCT'", 
  "'DIV'", "'DROP'", "'ELSE'", "'END'", "'ESCAPE'", "'EXISTS'", "'FALSE'", 
  "'FIRST'", "'FOR'", "", "'FULL'", "", "'GROUPING'", "", "'IF'", "", "", 
  "", "", "", "", "'LAST'", "'LEFT'", "'LIKE'", "", "'LIST'", "", "'NATURAL'", 
  "", "'NULL'", "'NULLS'", "'OF'", "", "", "", "'QUERY'", "'RECOVER'", "'RIGHT'", 
  "", "'ROLLUP'", "", "'SETS'", "'SOME'", "'START'", "'TABLE'", "'TO'", 
  "'TRUE'", "'TYPE'", "", "'UNKNOWN'", "'USE'", "'USING'", "'VALUES'", "'WHEN'", 
  "", "", "'WITH'", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "'<=>'", "'<>'", "'!='", "'<'", "", "'>'", 
  "", "'+'", "'-'", "'*'", "'/'", "'%'", "'~'", "'&'", "'|'", "'||'", "'^'"
};

std::vector<std::string> NebulaSQLParser::_symbolicNames = {
  "", "", "", "", "", "", "", "", "BACKQUOTED_IDENTIFIER", "ALL", "AND", 
  "ANY", "AS", "ASC", "AT", "BETWEEN", "BY", "COMMENT", "CUBE", "DELETE", 
  "DESC", "DISTINCT", "DIV", "DROP", "ELSE", "END", "ESCAPE", "EXISTS", 
  "FALSE", "FIRST", "FOR", "FROM", "FULL", "GROUP", "GROUPING", "HAVING", 
  "IF", "IN", "INNER", "INSERT", "INTO", "IS", "JOIN", "LAST", "LEFT", "LIKE", 
  "LIMIT", "LIST", "MERGE", "NATURAL", "NOT", "NULLTOKEN", "NULLS", "OF", 
  "ON", "OR", "ORDER", "QUERY", "RECOVER", "RIGHT", "RLIKE", "ROLLUP", "SELECT", 
  "SETS", "SOME", "START", "TABLE", "TO", "TRUE", "TYPE", "UNION", "UNKNOWN", 
  "USE", "USING", "VALUES", "WHEN", "WHERE", "WINDOW", "WITH", "TUMBLING", 
  "SLIDING", "SIZE", "ADVANCE", "MS", "SEC", "MIN", "HOUR", "DAY", "MAX", 
  "AVG", "SUM", "COUNT", "WATERMARK", "OFFSET", "CSV", "ZMQ", "KAFKA", "LOCALHOST", 
  "EQ", "NSEQ", "NEQ", "NEQJ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", 
  "ASTERISK", "SLASH", "PERCENT", "TILDE", "AMPERSAND", "PIPE", "CONCAT_PIPE", 
  "HAT", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", 
  "INTEGER_VALUE", "EXPONENT_VALUE", "DECIMAL_VALUE", "FLOAT_LITERAL", "DOUBLE_LITERAL", 
  "BIGDECIMAL_LITERAL", "IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", 
  "WS", "FOUR_OCTETS", "OCTET", "UNRECOGNIZED"
};

dfa::Vocabulary NebulaSQLParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> NebulaSQLParser::_tokenNames;

NebulaSQLParser::Initializer::Initializer() {
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
    0x3, 0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964, 
       0x3, 0x86, 0x38b, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
       0x9, 0x4, 0x4, 0x5, 0x9, 0x5, 0x4, 0x6, 0x9, 0x6, 0x4, 0x7, 0x9, 
       0x7, 0x4, 0x8, 0x9, 0x8, 0x4, 0x9, 0x9, 0x9, 0x4, 0xa, 0x9, 0xa, 
       0x4, 0xb, 0x9, 0xb, 0x4, 0xc, 0x9, 0xc, 0x4, 0xd, 0x9, 0xd, 0x4, 
       0xe, 0x9, 0xe, 0x4, 0xf, 0x9, 0xf, 0x4, 0x10, 0x9, 0x10, 0x4, 0x11, 
       0x9, 0x11, 0x4, 0x12, 0x9, 0x12, 0x4, 0x13, 0x9, 0x13, 0x4, 0x14, 
       0x9, 0x14, 0x4, 0x15, 0x9, 0x15, 0x4, 0x16, 0x9, 0x16, 0x4, 0x17, 
       0x9, 0x17, 0x4, 0x18, 0x9, 0x18, 0x4, 0x19, 0x9, 0x19, 0x4, 0x1a, 
       0x9, 0x1a, 0x4, 0x1b, 0x9, 0x1b, 0x4, 0x1c, 0x9, 0x1c, 0x4, 0x1d, 
       0x9, 0x1d, 0x4, 0x1e, 0x9, 0x1e, 0x4, 0x1f, 0x9, 0x1f, 0x4, 0x20, 
       0x9, 0x20, 0x4, 0x21, 0x9, 0x21, 0x4, 0x22, 0x9, 0x22, 0x4, 0x23, 
       0x9, 0x23, 0x4, 0x24, 0x9, 0x24, 0x4, 0x25, 0x9, 0x25, 0x4, 0x26, 
       0x9, 0x26, 0x4, 0x27, 0x9, 0x27, 0x4, 0x28, 0x9, 0x28, 0x4, 0x29, 
       0x9, 0x29, 0x4, 0x2a, 0x9, 0x2a, 0x4, 0x2b, 0x9, 0x2b, 0x4, 0x2c, 
       0x9, 0x2c, 0x4, 0x2d, 0x9, 0x2d, 0x4, 0x2e, 0x9, 0x2e, 0x4, 0x2f, 
       0x9, 0x2f, 0x4, 0x30, 0x9, 0x30, 0x4, 0x31, 0x9, 0x31, 0x4, 0x32, 
       0x9, 0x32, 0x4, 0x33, 0x9, 0x33, 0x4, 0x34, 0x9, 0x34, 0x4, 0x35, 
       0x9, 0x35, 0x4, 0x36, 0x9, 0x36, 0x4, 0x37, 0x9, 0x37, 0x4, 0x38, 
       0x9, 0x38, 0x4, 0x39, 0x9, 0x39, 0x4, 0x3a, 0x9, 0x3a, 0x4, 0x3b, 
       0x9, 0x3b, 0x4, 0x3c, 0x9, 0x3c, 0x4, 0x3d, 0x9, 0x3d, 0x4, 0x3e, 
       0x9, 0x3e, 0x4, 0x3f, 0x9, 0x3f, 0x4, 0x40, 0x9, 0x40, 0x4, 0x41, 
       0x9, 0x41, 0x4, 0x42, 0x9, 0x42, 0x4, 0x43, 0x9, 0x43, 0x4, 0x44, 
       0x9, 0x44, 0x4, 0x45, 0x9, 0x45, 0x4, 0x46, 0x9, 0x46, 0x4, 0x47, 
       0x9, 0x47, 0x4, 0x48, 0x9, 0x48, 0x4, 0x49, 0x9, 0x49, 0x4, 0x4a, 
       0x9, 0x4a, 0x4, 0x4b, 0x9, 0x4b, 0x4, 0x4c, 0x9, 0x4c, 0x4, 0x4d, 
       0x9, 0x4d, 0x4, 0x4e, 0x9, 0x4e, 0x3, 0x2, 0x3, 0x2, 0x7, 0x2, 0x9f, 
       0xa, 0x2, 0xc, 0x2, 0xe, 0x2, 0xa2, 0xb, 0x2, 0x3, 0x2, 0x3, 0x2, 
       0x3, 0x3, 0x3, 0x3, 0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x5, 0x3, 
       0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x7, 0x5, 0xb0, 0xa, 0x5, 0xc, 
       0x5, 0xe, 0x5, 0xb3, 0xb, 0x5, 0x5, 0x5, 0xb5, 0xa, 0x5, 0x3, 0x5, 
       0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0xba, 0xa, 0x5, 0x5, 0x5, 0xbc, 0xa, 
       0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0xc0, 0xa, 0x5, 0x3, 0x6, 0x3, 
       0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x7, 0x6, 0xc8, 0xa, 
       0x6, 0xc, 0x6, 0xe, 0x6, 0xcb, 0xb, 0x6, 0x3, 0x7, 0x3, 0x7, 0x3, 
       0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 
       0x5, 0x7, 0xd6, 0xa, 0x7, 0x3, 0x8, 0x5, 0x8, 0xd9, 0xa, 0x8, 0x3, 
       0x8, 0x3, 0x8, 0x5, 0x8, 0xdd, 0xa, 0x8, 0x3, 0x8, 0x5, 0x8, 0xe0, 
       0xa, 0x8, 0x3, 0x8, 0x5, 0x8, 0xe3, 0xa, 0x8, 0x3, 0x8, 0x5, 0x8, 
       0xe6, 0xa, 0x8, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x7, 0x9, 
       0xec, 0xa, 0x9, 0xc, 0x9, 0xe, 0x9, 0xef, 0xb, 0x9, 0x3, 0xa, 0x3, 
       0xa, 0x7, 0xa, 0xf3, 0xa, 0xa, 0xc, 0xa, 0xe, 0xa, 0xf6, 0xb, 0xa, 
       0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x5, 0xb, 0xfc, 0xa, 0xb, 
       0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x5, 0xb, 0x103, 
       0xa, 0xb, 0x3, 0xc, 0x5, 0xc, 0x106, 0xa, 0xc, 0x3, 0xd, 0x3, 0xd, 
       0x3, 0xd, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 
       0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 
       0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x5, 0xe, 0x11a, 0xa, 0xe, 0x3, 0xf, 
       0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x7, 0xf, 0x121, 0xa, 0xf, 
       0xc, 0xf, 0xe, 0xf, 0x124, 0xb, 0xf, 0x5, 0xf, 0x126, 0xa, 0xf, 0x3, 
       0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0x10, 0x3, 0x10, 0x6, 0x10, 0x12d, 
       0xa, 0x10, 0xd, 0x10, 0xe, 0x10, 0x12e, 0x3, 0x11, 0x3, 0x11, 0x5, 
       0x11, 0x133, 0xa, 0x11, 0x3, 0x11, 0x5, 0x11, 0x136, 0xa, 0x11, 0x3, 
       0x12, 0x3, 0x12, 0x7, 0x12, 0x13a, 0xa, 0x12, 0xc, 0x12, 0xe, 0x12, 
       0x13d, 0xb, 0x12, 0x3, 0x12, 0x3, 0x12, 0x3, 0x13, 0x3, 0x13, 0x3, 
       0x13, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x15, 0x3, 0x15, 0x3, 
       0x15, 0x3, 0x15, 0x7, 0x15, 0x14b, 0xa, 0x15, 0xc, 0x15, 0xe, 0x15, 
       0x14e, 0xb, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x16, 0x5, 0x16, 0x153, 
       0xa, 0x16, 0x3, 0x16, 0x3, 0x16, 0x5, 0x16, 0x157, 0xa, 0x16, 0x5, 
       0x16, 0x159, 0xa, 0x16, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x7, 0x17, 
       0x15e, 0xa, 0x17, 0xc, 0x17, 0xe, 0x17, 0x161, 0xb, 0x17, 0x3, 0x18, 
       0x3, 0x18, 0x3, 0x18, 0x7, 0x18, 0x166, 0xa, 0x18, 0xc, 0x18, 0xe, 
       0x18, 0x169, 0xb, 0x18, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x16d, 0xa, 
       0x19, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x171, 0xa, 0x19, 0x5, 0x19, 
       0x173, 0xa, 0x19, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x5, 0x1a, 0x178, 
       0xa, 0x1a, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 
       0x3, 0x1b, 0x5, 0x1b, 0x180, 0xa, 0x1b, 0x3, 0x1c, 0x3, 0x1c, 0x3, 
       0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1e, 0x3, 0x1e, 0x3, 
       0x1e, 0x7, 0x1e, 0x18b, 0xa, 0x1e, 0xc, 0x1e, 0xe, 0x1e, 0x18e, 0xb, 
       0x1e, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x20, 0x3, 0x20, 0x6, 
       0x20, 0x195, 0xa, 0x20, 0xd, 0x20, 0xe, 0x20, 0x196, 0x3, 0x20, 0x5, 
       0x20, 0x19a, 0xa, 0x20, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x7, 0x21, 
       0x19f, 0xa, 0x21, 0xc, 0x21, 0xe, 0x21, 0x1a2, 0xb, 0x21, 0x3, 0x22, 
       0x3, 0x22, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 
       0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 
       0x1b0, 0xa, 0x23, 0x5, 0x23, 0x1b2, 0xa, 0x23, 0x3, 0x23, 0x3, 0x23, 
       0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x7, 0x23, 0x1ba, 0xa, 
       0x23, 0xc, 0x23, 0xe, 0x23, 0x1bd, 0xb, 0x23, 0x3, 0x24, 0x5, 0x24, 
       0x1c0, 0xa, 0x24, 0x3, 0x24, 0x5, 0x24, 0x1c3, 0xa, 0x24, 0x3, 0x24, 
       0x5, 0x24, 0x1c6, 0xa, 0x24, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 
       0x25, 0x3, 0x25, 0x7, 0x25, 0x1cd, 0xa, 0x25, 0xc, 0x25, 0xe, 0x25, 
       0x1d0, 0xb, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 
       0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x7, 
       0x25, 0x1dc, 0xa, 0x25, 0xc, 0x25, 0xe, 0x25, 0x1df, 0xb, 0x25, 0x3, 
       0x25, 0x3, 0x25, 0x5, 0x25, 0x1e3, 0xa, 0x25, 0x3, 0x25, 0x3, 0x25, 
       0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 
       0x7, 0x25, 0x1ed, 0xa, 0x25, 0xc, 0x25, 0xe, 0x25, 0x1f0, 0xb, 0x25, 
       0x3, 0x25, 0x3, 0x25, 0x5, 0x25, 0x1f4, 0xa, 0x25, 0x3, 0x26, 0x3, 
       0x26, 0x3, 0x26, 0x3, 0x26, 0x7, 0x26, 0x1fa, 0xa, 0x26, 0xc, 0x26, 
       0xe, 0x26, 0x1fd, 0xb, 0x26, 0x5, 0x26, 0x1ff, 0xa, 0x26, 0x3, 0x26, 
       0x3, 0x26, 0x5, 0x26, 0x203, 0xa, 0x26, 0x3, 0x27, 0x3, 0x27, 0x3, 
       0x27, 0x3, 0x28, 0x3, 0x28, 0x3, 0x28, 0x3, 0x28, 0x3, 0x28, 0x3, 
       0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x3, 0x2a, 0x3, 
       0x2a, 0x5, 0x2a, 0x214, 0xa, 0x2a, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 
       0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 0x21b, 0xa, 0x2b, 0x3, 0x2b, 0x3, 
       0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 
       0x2b, 0x5, 0x2b, 0x225, 0xa, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 
       0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 0x22c, 0xa, 0x2b, 0x3, 0x2c, 0x3, 
       0x2c, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2d, 0x3, 0x2d, 0x3, 
       0x2d, 0x3, 0x2d, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x3, 
       0x2e, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x30, 0x3, 0x30, 0x3, 0x31, 0x3, 
       0x31, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x3, 
       0x33, 0x3, 0x33, 0x3, 0x33, 0x5, 0x33, 0x24a, 0xa, 0x33, 0x3, 0x34, 
       0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x3, 0x35, 0x3, 0x35, 
       0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 
       0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x37, 0x5, 0x37, 0x25d, 0xa, 
       0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x38, 0x3, 0x38, 0x3, 0x39, 0x3, 
       0x39, 0x3, 0x3a, 0x3, 0x3a, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3c, 0x3, 
       0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 
       0x3c, 0x3, 0x3c, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3e, 0x3, 0x3e, 0x3, 
       0x3f, 0x3, 0x3f, 0x3, 0x40, 0x3, 0x40, 0x3, 0x41, 0x3, 0x41, 0x5, 
       0x41, 0x27c, 0xa, 0x41, 0x3, 0x41, 0x3, 0x41, 0x5, 0x41, 0x280, 0xa, 
       0x41, 0x3, 0x42, 0x5, 0x42, 0x283, 0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 
       0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x28b, 0xa, 
       0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x7, 
       0x42, 0x292, 0xa, 0x42, 0xc, 0x42, 0xe, 0x42, 0x295, 0xb, 0x42, 0x3, 
       0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x29a, 0xa, 0x42, 0x3, 0x42, 
       0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 
       0x2a2, 0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x2a7, 
       0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
       0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x7, 0x42, 0x2b1, 0xa, 0x42, 0xc, 
       0x42, 0xe, 0x42, 0x2b4, 0xb, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 
       0x2b8, 0xa, 0x42, 0x3, 0x42, 0x5, 0x42, 0x2bb, 0xa, 0x42, 0x3, 0x42, 
       0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x2c1, 0xa, 0x42, 0x3, 
       0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x2c7, 0xa, 0x42, 
       0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x2cc, 0xa, 0x42, 0x3, 
       0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x2d1, 0xa, 0x42, 0x3, 0x43, 
       0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x5, 0x43, 0x2d7, 0xa, 0x43, 0x3, 
       0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 
       0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 
       0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 0x43, 0x3, 
       0x43, 0x7, 0x43, 0x2ec, 0xa, 0x43, 0xc, 0x43, 0xe, 0x43, 0x2ef, 0xb, 
       0x43, 0x3, 0x44, 0x3, 0x44, 0x3, 0x45, 0x3, 0x45, 0x3, 0x45, 0x5, 
       0x45, 0x2f6, 0xa, 0x45, 0x3, 0x45, 0x7, 0x45, 0x2f9, 0xa, 0x45, 0xc, 
       0x45, 0xe, 0x45, 0x2fc, 0xb, 0x45, 0x3, 0x45, 0x3, 0x45, 0x3, 0x46, 
       0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x7, 0x46, 
       0x306, 0xa, 0x46, 0xc, 0x46, 0xe, 0x46, 0x309, 0xb, 0x46, 0x3, 0x46, 
       0x3, 0x46, 0x5, 0x46, 0x30d, 0xa, 0x46, 0x3, 0x47, 0x3, 0x47, 0x3, 
       0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 
       0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x6, 
       0x47, 0x31d, 0xa, 0x47, 0xd, 0x47, 0xe, 0x47, 0x31e, 0x3, 0x47, 0x3, 
       0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x7, 
       0x47, 0x328, 0xa, 0x47, 0xc, 0x47, 0xe, 0x47, 0x32b, 0xb, 0x47, 0x5, 
       0x47, 0x32d, 0xa, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 
       0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x5, 0x47, 0x337, 0xa, 
       0x47, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x7, 0x47, 0x33c, 0xa, 0x47, 
       0xc, 0x47, 0xe, 0x47, 0x33f, 0xb, 0x47, 0x3, 0x48, 0x3, 0x48, 0x3, 
       0x48, 0x7, 0x48, 0x344, 0xa, 0x48, 0xc, 0x48, 0xe, 0x48, 0x347, 0xb, 
       0x48, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x34b, 0xa, 0x49, 0x3, 0x49, 
       0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x350, 0xa, 0x49, 0x3, 0x49, 0x3, 
       0x49, 0x3, 0x49, 0x5, 0x49, 0x355, 0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 
       0x5, 0x49, 0x359, 0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x35d, 
       0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x361, 0xa, 0x49, 0x3, 
       0x49, 0x3, 0x49, 0x5, 0x49, 0x365, 0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 
       0x5, 0x49, 0x369, 0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x36d, 
       0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x371, 0xa, 0x49, 0x3, 
       0x49, 0x5, 0x49, 0x374, 0xa, 0x49, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 
       0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x6, 0x4a, 0x37d, 0xa, 
       0x4a, 0xd, 0x4a, 0xe, 0x4a, 0x37e, 0x5, 0x4a, 0x381, 0xa, 0x4a, 0x3, 
       0x4b, 0x3, 0x4b, 0x3, 0x4c, 0x3, 0x4c, 0x3, 0x4d, 0x3, 0x4d, 0x3, 
       0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x2, 0x6, 0xa, 0x44, 0x84, 0x8c, 0x4f, 
       0x2, 0x4, 0x6, 0x8, 0xa, 0xc, 0xe, 0x10, 0x12, 0x14, 0x16, 0x18, 
       0x1a, 0x1c, 0x1e, 0x20, 0x22, 0x24, 0x26, 0x28, 0x2a, 0x2c, 0x2e, 
       0x30, 0x32, 0x34, 0x36, 0x38, 0x3a, 0x3c, 0x3e, 0x40, 0x42, 0x44, 
       0x46, 0x48, 0x4a, 0x4c, 0x4e, 0x50, 0x52, 0x54, 0x56, 0x58, 0x5a, 
       0x5c, 0x5e, 0x60, 0x62, 0x64, 0x66, 0x68, 0x6a, 0x6c, 0x6e, 0x70, 
       0x72, 0x74, 0x76, 0x78, 0x7a, 0x7c, 0x7e, 0x80, 0x82, 0x84, 0x86, 
       0x88, 0x8a, 0x8c, 0x8e, 0x90, 0x92, 0x94, 0x96, 0x98, 0x9a, 0x2, 
       0x12, 0x3, 0x2, 0x55, 0x59, 0x4, 0x2, 0x57, 0x57, 0x5a, 0x5d, 0x4, 
       0x2, 0x63, 0x63, 0x84, 0x84, 0x4, 0x2, 0xf, 0xf, 0x16, 0x16, 0x4, 
       0x2, 0x1f, 0x1f, 0x2d, 0x2d, 0x5, 0x2, 0xb, 0xb, 0xd, 0xd, 0x42, 
       0x42, 0x5, 0x2, 0x1e, 0x1e, 0x46, 0x46, 0x49, 0x49, 0x4, 0x2, 0x6c, 
       0x6d, 0x71, 0x71, 0x4, 0x2, 0x18, 0x18, 0x6e, 0x70, 0x4, 0x2, 0x6c, 
       0x6d, 0x74, 0x74, 0x3, 0x2, 0x64, 0x6b, 0x3, 0x2, 0x7b, 0x7c, 0x4, 
       0x2, 0x1e, 0x1e, 0x46, 0x46, 0xb, 0x2, 0x22, 0x22, 0x28, 0x28, 0x2c, 
       0x2c, 0x2e, 0x2e, 0x33, 0x33, 0x38, 0x38, 0x3d, 0x3d, 0x48, 0x48, 
       0x4b, 0x4b, 0x13, 0x2, 0xf, 0x12, 0x14, 0x16, 0x18, 0x19, 0x1d, 0x1d, 
       0x1f, 0x1f, 0x24, 0x24, 0x29, 0x29, 0x2d, 0x2d, 0x2f, 0x30, 0x32, 
       0x32, 0x36, 0x36, 0x3b, 0x3b, 0x3e, 0x3f, 0x41, 0x41, 0x46, 0x47, 
       0x4c, 0x4c, 0x4f, 0x4f, 0x13, 0x2, 0xb, 0x12, 0x14, 0x19, 0x1c, 0x1f, 
       0x21, 0x21, 0x23, 0x25, 0x27, 0x27, 0x29, 0x2b, 0x2d, 0x2d, 0x2f, 
       0x30, 0x32, 0x32, 0x34, 0x36, 0x39, 0x3b, 0x3e, 0x42, 0x44, 0x44, 
       0x46, 0x47, 0x4c, 0x4c, 0x4e, 0x50, 0x2, 0x3cb, 0x2, 0x9c, 0x3, 0x2, 
       0x2, 0x2, 0x4, 0xa5, 0x3, 0x2, 0x2, 0x2, 0x6, 0xa7, 0x3, 0x2, 0x2, 
       0x2, 0x8, 0xb4, 0x3, 0x2, 0x2, 0x2, 0xa, 0xc1, 0x3, 0x2, 0x2, 0x2, 
       0xc, 0xd5, 0x3, 0x2, 0x2, 0x2, 0xe, 0xd8, 0x3, 0x2, 0x2, 0x2, 0x10, 
       0xe7, 0x3, 0x2, 0x2, 0x2, 0x12, 0xf0, 0x3, 0x2, 0x2, 0x2, 0x14, 0x102, 
       0x3, 0x2, 0x2, 0x2, 0x16, 0x105, 0x3, 0x2, 0x2, 0x2, 0x18, 0x107, 
       0x3, 0x2, 0x2, 0x2, 0x1a, 0x119, 0x3, 0x2, 0x2, 0x2, 0x1c, 0x11b, 
       0x3, 0x2, 0x2, 0x2, 0x1e, 0x12a, 0x3, 0x2, 0x2, 0x2, 0x20, 0x130, 
       0x3, 0x2, 0x2, 0x2, 0x22, 0x137, 0x3, 0x2, 0x2, 0x2, 0x24, 0x140, 
       0x3, 0x2, 0x2, 0x2, 0x26, 0x143, 0x3, 0x2, 0x2, 0x2, 0x28, 0x146, 
       0x3, 0x2, 0x2, 0x2, 0x2a, 0x158, 0x3, 0x2, 0x2, 0x2, 0x2c, 0x15a, 
       0x3, 0x2, 0x2, 0x2, 0x2e, 0x162, 0x3, 0x2, 0x2, 0x2, 0x30, 0x16a, 
       0x3, 0x2, 0x2, 0x2, 0x32, 0x177, 0x3, 0x2, 0x2, 0x2, 0x34, 0x17f, 
       0x3, 0x2, 0x2, 0x2, 0x36, 0x181, 0x3, 0x2, 0x2, 0x2, 0x38, 0x183, 
       0x3, 0x2, 0x2, 0x2, 0x3a, 0x187, 0x3, 0x2, 0x2, 0x2, 0x3c, 0x18f, 
       0x3, 0x2, 0x2, 0x2, 0x3e, 0x199, 0x3, 0x2, 0x2, 0x2, 0x40, 0x19b, 
       0x3, 0x2, 0x2, 0x2, 0x42, 0x1a3, 0x3, 0x2, 0x2, 0x2, 0x44, 0x1b1, 
       0x3, 0x2, 0x2, 0x2, 0x46, 0x1bf, 0x3, 0x2, 0x2, 0x2, 0x48, 0x1f3, 
       0x3, 0x2, 0x2, 0x2, 0x4a, 0x202, 0x3, 0x2, 0x2, 0x2, 0x4c, 0x204, 
       0x3, 0x2, 0x2, 0x2, 0x4e, 0x207, 0x3, 0x2, 0x2, 0x2, 0x50, 0x20c, 
       0x3, 0x2, 0x2, 0x2, 0x52, 0x213, 0x3, 0x2, 0x2, 0x2, 0x54, 0x22b, 
       0x3, 0x2, 0x2, 0x2, 0x56, 0x22d, 0x3, 0x2, 0x2, 0x2, 0x58, 0x232, 
       0x3, 0x2, 0x2, 0x2, 0x5a, 0x236, 0x3, 0x2, 0x2, 0x2, 0x5c, 0x23b, 
       0x3, 0x2, 0x2, 0x2, 0x5e, 0x23d, 0x3, 0x2, 0x2, 0x2, 0x60, 0x23f, 
       0x3, 0x2, 0x2, 0x2, 0x62, 0x241, 0x3, 0x2, 0x2, 0x2, 0x64, 0x249, 
       0x3, 0x2, 0x2, 0x2, 0x66, 0x24b, 0x3, 0x2, 0x2, 0x2, 0x68, 0x250, 
       0x3, 0x2, 0x2, 0x2, 0x6a, 0x252, 0x3, 0x2, 0x2, 0x2, 0x6c, 0x25c, 
       0x3, 0x2, 0x2, 0x2, 0x6e, 0x260, 0x3, 0x2, 0x2, 0x2, 0x70, 0x262, 
       0x3, 0x2, 0x2, 0x2, 0x72, 0x264, 0x3, 0x2, 0x2, 0x2, 0x74, 0x266, 
       0x3, 0x2, 0x2, 0x2, 0x76, 0x268, 0x3, 0x2, 0x2, 0x2, 0x78, 0x271, 
       0x3, 0x2, 0x2, 0x2, 0x7a, 0x273, 0x3, 0x2, 0x2, 0x2, 0x7c, 0x275, 
       0x3, 0x2, 0x2, 0x2, 0x7e, 0x277, 0x3, 0x2, 0x2, 0x2, 0x80, 0x279, 
       0x3, 0x2, 0x2, 0x2, 0x82, 0x2d0, 0x3, 0x2, 0x2, 0x2, 0x84, 0x2d6, 
       0x3, 0x2, 0x2, 0x2, 0x86, 0x2f0, 0x3, 0x2, 0x2, 0x2, 0x88, 0x2f2, 
       0x3, 0x2, 0x2, 0x2, 0x8a, 0x30c, 0x3, 0x2, 0x2, 0x2, 0x8c, 0x336, 
       0x3, 0x2, 0x2, 0x2, 0x8e, 0x340, 0x3, 0x2, 0x2, 0x2, 0x90, 0x373, 
       0x3, 0x2, 0x2, 0x2, 0x92, 0x380, 0x3, 0x2, 0x2, 0x2, 0x94, 0x382, 
       0x3, 0x2, 0x2, 0x2, 0x96, 0x384, 0x3, 0x2, 0x2, 0x2, 0x98, 0x386, 
       0x3, 0x2, 0x2, 0x2, 0x9a, 0x388, 0x3, 0x2, 0x2, 0x2, 0x9c, 0xa0, 
       0x5, 0x4, 0x3, 0x2, 0x9d, 0x9f, 0x7, 0x3, 0x2, 0x2, 0x9e, 0x9d, 0x3, 
       0x2, 0x2, 0x2, 0x9f, 0xa2, 0x3, 0x2, 0x2, 0x2, 0xa0, 0x9e, 0x3, 0x2, 
       0x2, 0x2, 0xa0, 0xa1, 0x3, 0x2, 0x2, 0x2, 0xa1, 0xa3, 0x3, 0x2, 0x2, 
       0x2, 0xa2, 0xa0, 0x3, 0x2, 0x2, 0x2, 0xa3, 0xa4, 0x7, 0x2, 0x2, 0x3, 
       0xa4, 0x3, 0x3, 0x2, 0x2, 0x2, 0xa5, 0xa6, 0x5, 0x6, 0x4, 0x2, 0xa6, 
       0x5, 0x3, 0x2, 0x2, 0x2, 0xa7, 0xa8, 0x5, 0xa, 0x6, 0x2, 0xa8, 0xa9, 
       0x5, 0x8, 0x5, 0x2, 0xa9, 0x7, 0x3, 0x2, 0x2, 0x2, 0xaa, 0xab, 0x7, 
       0x3a, 0x2, 0x2, 0xab, 0xac, 0x7, 0x12, 0x2, 0x2, 0xac, 0xb1, 0x5, 
       0x80, 0x41, 0x2, 0xad, 0xae, 0x7, 0x4, 0x2, 0x2, 0xae, 0xb0, 0x5, 
       0x80, 0x41, 0x2, 0xaf, 0xad, 0x3, 0x2, 0x2, 0x2, 0xb0, 0xb3, 0x3, 
       0x2, 0x2, 0x2, 0xb1, 0xaf, 0x3, 0x2, 0x2, 0x2, 0xb1, 0xb2, 0x3, 0x2, 
       0x2, 0x2, 0xb2, 0xb5, 0x3, 0x2, 0x2, 0x2, 0xb3, 0xb1, 0x3, 0x2, 0x2, 
       0x2, 0xb4, 0xaa, 0x3, 0x2, 0x2, 0x2, 0xb4, 0xb5, 0x3, 0x2, 0x2, 0x2, 
       0xb5, 0xbb, 0x3, 0x2, 0x2, 0x2, 0xb6, 0xb9, 0x7, 0x30, 0x2, 0x2, 
       0xb7, 0xba, 0x7, 0xb, 0x2, 0x2, 0xb8, 0xba, 0x7, 0x7a, 0x2, 0x2, 
       0xb9, 0xb7, 0x3, 0x2, 0x2, 0x2, 0xb9, 0xb8, 0x3, 0x2, 0x2, 0x2, 0xba, 
       0xbc, 0x3, 0x2, 0x2, 0x2, 0xbb, 0xb6, 0x3, 0x2, 0x2, 0x2, 0xbb, 0xbc, 
       0x3, 0x2, 0x2, 0x2, 0xbc, 0xbf, 0x3, 0x2, 0x2, 0x2, 0xbd, 0xbe, 0x7, 
       0x5f, 0x2, 0x2, 0xbe, 0xc0, 0x7, 0x7a, 0x2, 0x2, 0xbf, 0xbd, 0x3, 
       0x2, 0x2, 0x2, 0xbf, 0xc0, 0x3, 0x2, 0x2, 0x2, 0xc0, 0x9, 0x3, 0x2, 
       0x2, 0x2, 0xc1, 0xc2, 0x8, 0x6, 0x1, 0x2, 0xc2, 0xc3, 0x5, 0xc, 0x7, 
       0x2, 0xc3, 0xc9, 0x3, 0x2, 0x2, 0x2, 0xc4, 0xc5, 0xc, 0x3, 0x2, 0x2, 
       0xc5, 0xc6, 0x7, 0x48, 0x2, 0x2, 0xc6, 0xc8, 0x5, 0xa, 0x6, 0x4, 
       0xc7, 0xc4, 0x3, 0x2, 0x2, 0x2, 0xc8, 0xcb, 0x3, 0x2, 0x2, 0x2, 0xc9, 
       0xc7, 0x3, 0x2, 0x2, 0x2, 0xc9, 0xca, 0x3, 0x2, 0x2, 0x2, 0xca, 0xb, 
       0x3, 0x2, 0x2, 0x2, 0xcb, 0xc9, 0x3, 0x2, 0x2, 0x2, 0xcc, 0xd6, 0x5, 
       0xe, 0x8, 0x2, 0xcd, 0xd6, 0x5, 0x1e, 0x10, 0x2, 0xce, 0xcf, 0x7, 
       0x44, 0x2, 0x2, 0xcf, 0xd6, 0x5, 0x2e, 0x18, 0x2, 0xd0, 0xd6, 0x5, 
       0x28, 0x15, 0x2, 0xd1, 0xd2, 0x7, 0x5, 0x2, 0x2, 0xd2, 0xd3, 0x5, 
       0x6, 0x4, 0x2, 0xd3, 0xd4, 0x7, 0x6, 0x2, 0x2, 0xd4, 0xd6, 0x3, 0x2, 
       0x2, 0x2, 0xd5, 0xcc, 0x3, 0x2, 0x2, 0x2, 0xd5, 0xcd, 0x3, 0x2, 0x2, 
       0x2, 0xd5, 0xce, 0x3, 0x2, 0x2, 0x2, 0xd5, 0xd0, 0x3, 0x2, 0x2, 0x2, 
       0xd5, 0xd1, 0x3, 0x2, 0x2, 0x2, 0xd6, 0xd, 0x3, 0x2, 0x2, 0x2, 0xd7, 
       0xd9, 0x5, 0x62, 0x32, 0x2, 0xd8, 0xd7, 0x3, 0x2, 0x2, 0x2, 0xd8, 
       0xd9, 0x3, 0x2, 0x2, 0x2, 0xd9, 0xda, 0x3, 0x2, 0x2, 0x2, 0xda, 0xdc, 
       0x5, 0x22, 0x12, 0x2, 0xdb, 0xdd, 0x5, 0x10, 0x9, 0x2, 0xdc, 0xdb, 
       0x3, 0x2, 0x2, 0x2, 0xdc, 0xdd, 0x3, 0x2, 0x2, 0x2, 0xdd, 0xdf, 0x3, 
       0x2, 0x2, 0x2, 0xde, 0xe0, 0x5, 0x24, 0x13, 0x2, 0xdf, 0xde, 0x3, 
       0x2, 0x2, 0x2, 0xdf, 0xe0, 0x3, 0x2, 0x2, 0x2, 0xe0, 0xe2, 0x3, 0x2, 
       0x2, 0x2, 0xe1, 0xe3, 0x5, 0x46, 0x24, 0x2, 0xe2, 0xe1, 0x3, 0x2, 
       0x2, 0x2, 0xe2, 0xe3, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xe5, 0x3, 0x2, 0x2, 
       0x2, 0xe4, 0xe6, 0x5, 0x26, 0x14, 0x2, 0xe5, 0xe4, 0x3, 0x2, 0x2, 
       0x2, 0xe5, 0xe6, 0x3, 0x2, 0x2, 0x2, 0xe6, 0xf, 0x3, 0x2, 0x2, 0x2, 
       0xe7, 0xe8, 0x7, 0x21, 0x2, 0x2, 0xe8, 0xed, 0x5, 0x12, 0xa, 0x2, 
       0xe9, 0xea, 0x7, 0x4, 0x2, 0x2, 0xea, 0xec, 0x5, 0x12, 0xa, 0x2, 
       0xeb, 0xe9, 0x3, 0x2, 0x2, 0x2, 0xec, 0xef, 0x3, 0x2, 0x2, 0x2, 0xed, 
       0xeb, 0x3, 0x2, 0x2, 0x2, 0xed, 0xee, 0x3, 0x2, 0x2, 0x2, 0xee, 0x11, 
       0x3, 0x2, 0x2, 0x2, 0xef, 0xed, 0x3, 0x2, 0x2, 0x2, 0xf0, 0xf4, 0x5, 
       0x1a, 0xe, 0x2, 0xf1, 0xf3, 0x5, 0x14, 0xb, 0x2, 0xf2, 0xf1, 0x3, 
       0x2, 0x2, 0x2, 0xf3, 0xf6, 0x3, 0x2, 0x2, 0x2, 0xf4, 0xf2, 0x3, 0x2, 
       0x2, 0x2, 0xf4, 0xf5, 0x3, 0x2, 0x2, 0x2, 0xf5, 0x13, 0x3, 0x2, 0x2, 
       0x2, 0xf6, 0xf4, 0x3, 0x2, 0x2, 0x2, 0xf7, 0xf8, 0x5, 0x16, 0xc, 
       0x2, 0xf8, 0xf9, 0x7, 0x2c, 0x2, 0x2, 0xf9, 0xfb, 0x5, 0x1a, 0xe, 
       0x2, 0xfa, 0xfc, 0x5, 0x18, 0xd, 0x2, 0xfb, 0xfa, 0x3, 0x2, 0x2, 
       0x2, 0xfb, 0xfc, 0x3, 0x2, 0x2, 0x2, 0xfc, 0x103, 0x3, 0x2, 0x2, 
       0x2, 0xfd, 0xfe, 0x7, 0x33, 0x2, 0x2, 0xfe, 0xff, 0x5, 0x16, 0xc, 
       0x2, 0xff, 0x100, 0x7, 0x2c, 0x2, 0x2, 0x100, 0x101, 0x5, 0x1a, 0xe, 
       0x2, 0x101, 0x103, 0x3, 0x2, 0x2, 0x2, 0x102, 0xf7, 0x3, 0x2, 0x2, 
       0x2, 0x102, 0xfd, 0x3, 0x2, 0x2, 0x2, 0x103, 0x15, 0x3, 0x2, 0x2, 
       0x2, 0x104, 0x106, 0x7, 0x28, 0x2, 0x2, 0x105, 0x104, 0x3, 0x2, 0x2, 
       0x2, 0x105, 0x106, 0x3, 0x2, 0x2, 0x2, 0x106, 0x17, 0x3, 0x2, 0x2, 
       0x2, 0x107, 0x108, 0x7, 0x38, 0x2, 0x2, 0x108, 0x109, 0x5, 0x44, 
       0x23, 0x2, 0x109, 0x19, 0x3, 0x2, 0x2, 0x2, 0x10a, 0x10b, 0x5, 0x2e, 
       0x18, 0x2, 0x10b, 0x10c, 0x5, 0x2a, 0x16, 0x2, 0x10c, 0x11a, 0x3, 
       0x2, 0x2, 0x2, 0x10d, 0x10e, 0x7, 0x5, 0x2, 0x2, 0x10e, 0x10f, 0x5, 
       0x6, 0x4, 0x2, 0x10f, 0x110, 0x7, 0x6, 0x2, 0x2, 0x110, 0x111, 0x5, 
       0x2a, 0x16, 0x2, 0x111, 0x11a, 0x3, 0x2, 0x2, 0x2, 0x112, 0x113, 
       0x7, 0x5, 0x2, 0x2, 0x113, 0x114, 0x5, 0x12, 0xa, 0x2, 0x114, 0x115, 
       0x7, 0x6, 0x2, 0x2, 0x115, 0x116, 0x5, 0x2a, 0x16, 0x2, 0x116, 0x11a, 
       0x3, 0x2, 0x2, 0x2, 0x117, 0x11a, 0x5, 0x28, 0x15, 0x2, 0x118, 0x11a, 
       0x5, 0x1c, 0xf, 0x2, 0x119, 0x10a, 0x3, 0x2, 0x2, 0x2, 0x119, 0x10d, 
       0x3, 0x2, 0x2, 0x2, 0x119, 0x112, 0x3, 0x2, 0x2, 0x2, 0x119, 0x117, 
       0x3, 0x2, 0x2, 0x2, 0x119, 0x118, 0x3, 0x2, 0x2, 0x2, 0x11a, 0x1b, 
       0x3, 0x2, 0x2, 0x2, 0x11b, 0x11c, 0x5, 0x3c, 0x1f, 0x2, 0x11c, 0x125, 
       0x7, 0x5, 0x2, 0x2, 0x11d, 0x122, 0x5, 0x42, 0x22, 0x2, 0x11e, 0x11f, 
       0x7, 0x4, 0x2, 0x2, 0x11f, 0x121, 0x5, 0x42, 0x22, 0x2, 0x120, 0x11e, 
       0x3, 0x2, 0x2, 0x2, 0x121, 0x124, 0x3, 0x2, 0x2, 0x2, 0x122, 0x120, 
       0x3, 0x2, 0x2, 0x2, 0x122, 0x123, 0x3, 0x2, 0x2, 0x2, 0x123, 0x126, 
       0x3, 0x2, 0x2, 0x2, 0x124, 0x122, 0x3, 0x2, 0x2, 0x2, 0x125, 0x11d, 
       0x3, 0x2, 0x2, 0x2, 0x125, 0x126, 0x3, 0x2, 0x2, 0x2, 0x126, 0x127, 
       0x3, 0x2, 0x2, 0x2, 0x127, 0x128, 0x7, 0x6, 0x2, 0x2, 0x128, 0x129, 
       0x5, 0x2a, 0x16, 0x2, 0x129, 0x1d, 0x3, 0x2, 0x2, 0x2, 0x12a, 0x12c, 
       0x5, 0x10, 0x9, 0x2, 0x12b, 0x12d, 0x5, 0x20, 0x11, 0x2, 0x12c, 0x12b, 
       0x3, 0x2, 0x2, 0x2, 0x12d, 0x12e, 0x3, 0x2, 0x2, 0x2, 0x12e, 0x12c, 
       0x3, 0x2, 0x2, 0x2, 0x12e, 0x12f, 0x3, 0x2, 0x2, 0x2, 0x12f, 0x1f, 
       0x3, 0x2, 0x2, 0x2, 0x130, 0x132, 0x5, 0x22, 0x12, 0x2, 0x131, 0x133, 
       0x5, 0x24, 0x13, 0x2, 0x132, 0x131, 0x3, 0x2, 0x2, 0x2, 0x132, 0x133, 
       0x3, 0x2, 0x2, 0x2, 0x133, 0x135, 0x3, 0x2, 0x2, 0x2, 0x134, 0x136, 
       0x5, 0x48, 0x25, 0x2, 0x135, 0x134, 0x3, 0x2, 0x2, 0x2, 0x135, 0x136, 
       0x3, 0x2, 0x2, 0x2, 0x136, 0x21, 0x3, 0x2, 0x2, 0x2, 0x137, 0x13b, 
       0x7, 0x40, 0x2, 0x2, 0x138, 0x13a, 0x5, 0x88, 0x45, 0x2, 0x139, 0x138, 
       0x3, 0x2, 0x2, 0x2, 0x13a, 0x13d, 0x3, 0x2, 0x2, 0x2, 0x13b, 0x139, 
       0x3, 0x2, 0x2, 0x2, 0x13b, 0x13c, 0x3, 0x2, 0x2, 0x2, 0x13c, 0x13e, 
       0x3, 0x2, 0x2, 0x2, 0x13d, 0x13b, 0x3, 0x2, 0x2, 0x2, 0x13e, 0x13f, 
       0x5, 0x40, 0x21, 0x2, 0x13f, 0x23, 0x3, 0x2, 0x2, 0x2, 0x140, 0x141, 
       0x7, 0x4e, 0x2, 0x2, 0x141, 0x142, 0x5, 0x44, 0x23, 0x2, 0x142, 0x25, 
       0x3, 0x2, 0x2, 0x2, 0x143, 0x144, 0x7, 0x25, 0x2, 0x2, 0x144, 0x145, 
       0x5, 0x44, 0x23, 0x2, 0x145, 0x27, 0x3, 0x2, 0x2, 0x2, 0x146, 0x147, 
       0x7, 0x4c, 0x2, 0x2, 0x147, 0x14c, 0x5, 0x42, 0x22, 0x2, 0x148, 0x149, 
       0x7, 0x4, 0x2, 0x2, 0x149, 0x14b, 0x5, 0x42, 0x22, 0x2, 0x14a, 0x148, 
       0x3, 0x2, 0x2, 0x2, 0x14b, 0x14e, 0x3, 0x2, 0x2, 0x2, 0x14c, 0x14a, 
       0x3, 0x2, 0x2, 0x2, 0x14c, 0x14d, 0x3, 0x2, 0x2, 0x2, 0x14d, 0x14f, 
       0x3, 0x2, 0x2, 0x2, 0x14e, 0x14c, 0x3, 0x2, 0x2, 0x2, 0x14f, 0x150, 
       0x5, 0x2a, 0x16, 0x2, 0x150, 0x29, 0x3, 0x2, 0x2, 0x2, 0x151, 0x153, 
       0x7, 0xe, 0x2, 0x2, 0x152, 0x151, 0x3, 0x2, 0x2, 0x2, 0x152, 0x153, 
       0x3, 0x2, 0x2, 0x2, 0x153, 0x154, 0x3, 0x2, 0x2, 0x2, 0x154, 0x156, 
       0x5, 0x34, 0x1b, 0x2, 0x155, 0x157, 0x5, 0x38, 0x1d, 0x2, 0x156, 
       0x155, 0x3, 0x2, 0x2, 0x2, 0x156, 0x157, 0x3, 0x2, 0x2, 0x2, 0x157, 
       0x159, 0x3, 0x2, 0x2, 0x2, 0x158, 0x152, 0x3, 0x2, 0x2, 0x2, 0x158, 
       0x159, 0x3, 0x2, 0x2, 0x2, 0x159, 0x2b, 0x3, 0x2, 0x2, 0x2, 0x15a, 
       0x15f, 0x5, 0x2e, 0x18, 0x2, 0x15b, 0x15c, 0x7, 0x4, 0x2, 0x2, 0x15c, 
       0x15e, 0x5, 0x2e, 0x18, 0x2, 0x15d, 0x15b, 0x3, 0x2, 0x2, 0x2, 0x15e, 
       0x161, 0x3, 0x2, 0x2, 0x2, 0x15f, 0x15d, 0x3, 0x2, 0x2, 0x2, 0x15f, 
       0x160, 0x3, 0x2, 0x2, 0x2, 0x160, 0x2d, 0x3, 0x2, 0x2, 0x2, 0x161, 
       0x15f, 0x3, 0x2, 0x2, 0x2, 0x162, 0x167, 0x5, 0x3c, 0x1f, 0x2, 0x163, 
       0x164, 0x7, 0x7, 0x2, 0x2, 0x164, 0x166, 0x5, 0x3c, 0x1f, 0x2, 0x165, 
       0x163, 0x3, 0x2, 0x2, 0x2, 0x166, 0x169, 0x3, 0x2, 0x2, 0x2, 0x167, 
       0x165, 0x3, 0x2, 0x2, 0x2, 0x167, 0x168, 0x3, 0x2, 0x2, 0x2, 0x168, 
       0x2f, 0x3, 0x2, 0x2, 0x2, 0x169, 0x167, 0x3, 0x2, 0x2, 0x2, 0x16a, 
       0x172, 0x5, 0x42, 0x22, 0x2, 0x16b, 0x16d, 0x7, 0xe, 0x2, 0x2, 0x16c, 
       0x16b, 0x3, 0x2, 0x2, 0x2, 0x16c, 0x16d, 0x3, 0x2, 0x2, 0x2, 0x16d, 
       0x170, 0x3, 0x2, 0x2, 0x2, 0x16e, 0x171, 0x5, 0x3c, 0x1f, 0x2, 0x16f, 
       0x171, 0x5, 0x38, 0x1d, 0x2, 0x170, 0x16e, 0x3, 0x2, 0x2, 0x2, 0x170, 
       0x16f, 0x3, 0x2, 0x2, 0x2, 0x171, 0x173, 0x3, 0x2, 0x2, 0x2, 0x172, 
       0x16c, 0x3, 0x2, 0x2, 0x2, 0x172, 0x173, 0x3, 0x2, 0x2, 0x2, 0x173, 
       0x31, 0x3, 0x2, 0x2, 0x2, 0x174, 0x178, 0x5, 0x34, 0x1b, 0x2, 0x175, 
       0x176, 0x6, 0x1a, 0x3, 0x2, 0x176, 0x178, 0x5, 0x96, 0x4c, 0x2, 0x177, 
       0x174, 0x3, 0x2, 0x2, 0x2, 0x177, 0x175, 0x3, 0x2, 0x2, 0x2, 0x178, 
       0x33, 0x3, 0x2, 0x2, 0x2, 0x179, 0x180, 0x7, 0x80, 0x2, 0x2, 0x17a, 
       0x180, 0x5, 0x36, 0x1c, 0x2, 0x17b, 0x17c, 0x6, 0x1b, 0x4, 0x2, 0x17c, 
       0x180, 0x5, 0x98, 0x4d, 0x2, 0x17d, 0x17e, 0x6, 0x1b, 0x5, 0x2, 0x17e, 
       0x180, 0x5, 0x9a, 0x4e, 0x2, 0x17f, 0x179, 0x3, 0x2, 0x2, 0x2, 0x17f, 
       0x17a, 0x3, 0x2, 0x2, 0x2, 0x17f, 0x17b, 0x3, 0x2, 0x2, 0x2, 0x17f, 
       0x17d, 0x3, 0x2, 0x2, 0x2, 0x180, 0x35, 0x3, 0x2, 0x2, 0x2, 0x181, 
       0x182, 0x7, 0xa, 0x2, 0x2, 0x182, 0x37, 0x3, 0x2, 0x2, 0x2, 0x183, 
       0x184, 0x7, 0x5, 0x2, 0x2, 0x184, 0x185, 0x5, 0x3a, 0x1e, 0x2, 0x185, 
       0x186, 0x7, 0x6, 0x2, 0x2, 0x186, 0x39, 0x3, 0x2, 0x2, 0x2, 0x187, 
       0x18c, 0x5, 0x3c, 0x1f, 0x2, 0x188, 0x189, 0x7, 0x4, 0x2, 0x2, 0x189, 
       0x18b, 0x5, 0x3c, 0x1f, 0x2, 0x18a, 0x188, 0x3, 0x2, 0x2, 0x2, 0x18b, 
       0x18e, 0x3, 0x2, 0x2, 0x2, 0x18c, 0x18a, 0x3, 0x2, 0x2, 0x2, 0x18c, 
       0x18d, 0x3, 0x2, 0x2, 0x2, 0x18d, 0x3b, 0x3, 0x2, 0x2, 0x2, 0x18e, 
       0x18c, 0x3, 0x2, 0x2, 0x2, 0x18f, 0x190, 0x5, 0x32, 0x1a, 0x2, 0x190, 
       0x191, 0x5, 0x3e, 0x20, 0x2, 0x191, 0x3d, 0x3, 0x2, 0x2, 0x2, 0x192, 
       0x193, 0x7, 0x6d, 0x2, 0x2, 0x193, 0x195, 0x5, 0x32, 0x1a, 0x2, 0x194, 
       0x192, 0x3, 0x2, 0x2, 0x2, 0x195, 0x196, 0x3, 0x2, 0x2, 0x2, 0x196, 
       0x194, 0x3, 0x2, 0x2, 0x2, 0x196, 0x197, 0x3, 0x2, 0x2, 0x2, 0x197, 
       0x19a, 0x3, 0x2, 0x2, 0x2, 0x198, 0x19a, 0x3, 0x2, 0x2, 0x2, 0x199, 
       0x194, 0x3, 0x2, 0x2, 0x2, 0x199, 0x198, 0x3, 0x2, 0x2, 0x2, 0x19a, 
       0x3f, 0x3, 0x2, 0x2, 0x2, 0x19b, 0x1a0, 0x5, 0x30, 0x19, 0x2, 0x19c, 
       0x19d, 0x7, 0x4, 0x2, 0x2, 0x19d, 0x19f, 0x5, 0x30, 0x19, 0x2, 0x19e, 
       0x19c, 0x3, 0x2, 0x2, 0x2, 0x19f, 0x1a2, 0x3, 0x2, 0x2, 0x2, 0x1a0, 
       0x19e, 0x3, 0x2, 0x2, 0x2, 0x1a0, 0x1a1, 0x3, 0x2, 0x2, 0x2, 0x1a1, 
       0x41, 0x3, 0x2, 0x2, 0x2, 0x1a2, 0x1a0, 0x3, 0x2, 0x2, 0x2, 0x1a3, 
       0x1a4, 0x5, 0x44, 0x23, 0x2, 0x1a4, 0x43, 0x3, 0x2, 0x2, 0x2, 0x1a5, 
       0x1a6, 0x8, 0x23, 0x1, 0x2, 0x1a6, 0x1a7, 0x7, 0x34, 0x2, 0x2, 0x1a7, 
       0x1b2, 0x5, 0x44, 0x23, 0x7, 0x1a8, 0x1a9, 0x7, 0x1d, 0x2, 0x2, 0x1a9, 
       0x1aa, 0x7, 0x5, 0x2, 0x2, 0x1aa, 0x1ab, 0x5, 0x6, 0x4, 0x2, 0x1ab, 
       0x1ac, 0x7, 0x6, 0x2, 0x2, 0x1ac, 0x1b2, 0x3, 0x2, 0x2, 0x2, 0x1ad, 
       0x1af, 0x5, 0x84, 0x43, 0x2, 0x1ae, 0x1b0, 0x5, 0x82, 0x42, 0x2, 
       0x1af, 0x1ae, 0x3, 0x2, 0x2, 0x2, 0x1af, 0x1b0, 0x3, 0x2, 0x2, 0x2, 
       0x1b0, 0x1b2, 0x3, 0x2, 0x2, 0x2, 0x1b1, 0x1a5, 0x3, 0x2, 0x2, 0x2, 
       0x1b1, 0x1a8, 0x3, 0x2, 0x2, 0x2, 0x1b1, 0x1ad, 0x3, 0x2, 0x2, 0x2, 
       0x1b2, 0x1bb, 0x3, 0x2, 0x2, 0x2, 0x1b3, 0x1b4, 0xc, 0x4, 0x2, 0x2, 
       0x1b4, 0x1b5, 0x7, 0xc, 0x2, 0x2, 0x1b5, 0x1ba, 0x5, 0x44, 0x23, 
       0x5, 0x1b6, 0x1b7, 0xc, 0x3, 0x2, 0x2, 0x1b7, 0x1b8, 0x7, 0x39, 0x2, 
       0x2, 0x1b8, 0x1ba, 0x5, 0x44, 0x23, 0x4, 0x1b9, 0x1b3, 0x3, 0x2, 
       0x2, 0x2, 0x1b9, 0x1b6, 0x3, 0x2, 0x2, 0x2, 0x1ba, 0x1bd, 0x3, 0x2, 
       0x2, 0x2, 0x1bb, 0x1b9, 0x3, 0x2, 0x2, 0x2, 0x1bb, 0x1bc, 0x3, 0x2, 
       0x2, 0x2, 0x1bc, 0x45, 0x3, 0x2, 0x2, 0x2, 0x1bd, 0x1bb, 0x3, 0x2, 
       0x2, 0x2, 0x1be, 0x1c0, 0x5, 0x48, 0x25, 0x2, 0x1bf, 0x1be, 0x3, 
       0x2, 0x2, 0x2, 0x1bf, 0x1c0, 0x3, 0x2, 0x2, 0x2, 0x1c0, 0x1c2, 0x3, 
       0x2, 0x2, 0x2, 0x1c1, 0x1c3, 0x5, 0x4c, 0x27, 0x2, 0x1c2, 0x1c1, 
       0x3, 0x2, 0x2, 0x2, 0x1c2, 0x1c3, 0x3, 0x2, 0x2, 0x2, 0x1c3, 0x1c5, 
       0x3, 0x2, 0x2, 0x2, 0x1c4, 0x1c6, 0x5, 0x4e, 0x28, 0x2, 0x1c5, 0x1c4, 
       0x3, 0x2, 0x2, 0x2, 0x1c5, 0x1c6, 0x3, 0x2, 0x2, 0x2, 0x1c6, 0x47, 
       0x3, 0x2, 0x2, 0x2, 0x1c7, 0x1c8, 0x7, 0x23, 0x2, 0x2, 0x1c8, 0x1c9, 
       0x7, 0x12, 0x2, 0x2, 0x1c9, 0x1ce, 0x5, 0x42, 0x22, 0x2, 0x1ca, 0x1cb, 
       0x7, 0x4, 0x2, 0x2, 0x1cb, 0x1cd, 0x5, 0x42, 0x22, 0x2, 0x1cc, 0x1ca, 
       0x3, 0x2, 0x2, 0x2, 0x1cd, 0x1d0, 0x3, 0x2, 0x2, 0x2, 0x1ce, 0x1cc, 
       0x3, 0x2, 0x2, 0x2, 0x1ce, 0x1cf, 0x3, 0x2, 0x2, 0x2, 0x1cf, 0x1e2, 
       0x3, 0x2, 0x2, 0x2, 0x1d0, 0x1ce, 0x3, 0x2, 0x2, 0x2, 0x1d1, 0x1d2, 
       0x7, 0x50, 0x2, 0x2, 0x1d2, 0x1e3, 0x7, 0x3f, 0x2, 0x2, 0x1d3, 0x1d4, 
       0x7, 0x50, 0x2, 0x2, 0x1d4, 0x1e3, 0x7, 0x14, 0x2, 0x2, 0x1d5, 0x1d6, 
       0x7, 0x24, 0x2, 0x2, 0x1d6, 0x1d7, 0x7, 0x41, 0x2, 0x2, 0x1d7, 0x1d8, 
       0x7, 0x5, 0x2, 0x2, 0x1d8, 0x1dd, 0x5, 0x4a, 0x26, 0x2, 0x1d9, 0x1da, 
       0x7, 0x4, 0x2, 0x2, 0x1da, 0x1dc, 0x5, 0x4a, 0x26, 0x2, 0x1db, 0x1d9, 
       0x3, 0x2, 0x2, 0x2, 0x1dc, 0x1df, 0x3, 0x2, 0x2, 0x2, 0x1dd, 0x1db, 
       0x3, 0x2, 0x2, 0x2, 0x1dd, 0x1de, 0x3, 0x2, 0x2, 0x2, 0x1de, 0x1e0, 
       0x3, 0x2, 0x2, 0x2, 0x1df, 0x1dd, 0x3, 0x2, 0x2, 0x2, 0x1e0, 0x1e1, 
       0x7, 0x6, 0x2, 0x2, 0x1e1, 0x1e3, 0x3, 0x2, 0x2, 0x2, 0x1e2, 0x1d1, 
       0x3, 0x2, 0x2, 0x2, 0x1e2, 0x1d3, 0x3, 0x2, 0x2, 0x2, 0x1e2, 0x1d5, 
       0x3, 0x2, 0x2, 0x2, 0x1e2, 0x1e3, 0x3, 0x2, 0x2, 0x2, 0x1e3, 0x1f4, 
       0x3, 0x2, 0x2, 0x2, 0x1e4, 0x1e5, 0x7, 0x23, 0x2, 0x2, 0x1e5, 0x1e6, 
       0x7, 0x12, 0x2, 0x2, 0x1e6, 0x1e7, 0x7, 0x24, 0x2, 0x2, 0x1e7, 0x1e8, 
       0x7, 0x41, 0x2, 0x2, 0x1e8, 0x1e9, 0x7, 0x5, 0x2, 0x2, 0x1e9, 0x1ee, 
       0x5, 0x4a, 0x26, 0x2, 0x1ea, 0x1eb, 0x7, 0x4, 0x2, 0x2, 0x1eb, 0x1ed, 
       0x5, 0x4a, 0x26, 0x2, 0x1ec, 0x1ea, 0x3, 0x2, 0x2, 0x2, 0x1ed, 0x1f0, 
       0x3, 0x2, 0x2, 0x2, 0x1ee, 0x1ec, 0x3, 0x2, 0x2, 0x2, 0x1ee, 0x1ef, 
       0x3, 0x2, 0x2, 0x2, 0x1ef, 0x1f1, 0x3, 0x2, 0x2, 0x2, 0x1f0, 0x1ee, 
       0x3, 0x2, 0x2, 0x2, 0x1f1, 0x1f2, 0x7, 0x6, 0x2, 0x2, 0x1f2, 0x1f4, 
       0x3, 0x2, 0x2, 0x2, 0x1f3, 0x1c7, 0x3, 0x2, 0x2, 0x2, 0x1f3, 0x1e4, 
       0x3, 0x2, 0x2, 0x2, 0x1f4, 0x49, 0x3, 0x2, 0x2, 0x2, 0x1f5, 0x1fe, 
       0x7, 0x5, 0x2, 0x2, 0x1f6, 0x1fb, 0x5, 0x42, 0x22, 0x2, 0x1f7, 0x1f8, 
       0x7, 0x4, 0x2, 0x2, 0x1f8, 0x1fa, 0x5, 0x42, 0x22, 0x2, 0x1f9, 0x1f7, 
       0x3, 0x2, 0x2, 0x2, 0x1fa, 0x1fd, 0x3, 0x2, 0x2, 0x2, 0x1fb, 0x1f9, 
       0x3, 0x2, 0x2, 0x2, 0x1fb, 0x1fc, 0x3, 0x2, 0x2, 0x2, 0x1fc, 0x1ff, 
       0x3, 0x2, 0x2, 0x2, 0x1fd, 0x1fb, 0x3, 0x2, 0x2, 0x2, 0x1fe, 0x1f6, 
       0x3, 0x2, 0x2, 0x2, 0x1fe, 0x1ff, 0x3, 0x2, 0x2, 0x2, 0x1ff, 0x200, 
       0x3, 0x2, 0x2, 0x2, 0x200, 0x203, 0x7, 0x6, 0x2, 0x2, 0x201, 0x203, 
       0x5, 0x42, 0x22, 0x2, 0x202, 0x1f5, 0x3, 0x2, 0x2, 0x2, 0x202, 0x201, 
       0x3, 0x2, 0x2, 0x2, 0x203, 0x4b, 0x3, 0x2, 0x2, 0x2, 0x204, 0x205, 
       0x7, 0x4f, 0x2, 0x2, 0x205, 0x206, 0x5, 0x52, 0x2a, 0x2, 0x206, 0x4d, 
       0x3, 0x2, 0x2, 0x2, 0x207, 0x208, 0x7, 0x5e, 0x2, 0x2, 0x208, 0x209, 
       0x7, 0x5, 0x2, 0x2, 0x209, 0x20a, 0x5, 0x50, 0x29, 0x2, 0x20a, 0x20b, 
       0x7, 0x6, 0x2, 0x2, 0x20b, 0x4f, 0x3, 0x2, 0x2, 0x2, 0x20c, 0x20d, 
       0x5, 0x32, 0x1a, 0x2, 0x20d, 0x20e, 0x7, 0x4, 0x2, 0x2, 0x20e, 0x20f, 
       0x7, 0x7a, 0x2, 0x2, 0x20f, 0x210, 0x5, 0x5c, 0x2f, 0x2, 0x210, 0x51, 
       0x3, 0x2, 0x2, 0x2, 0x211, 0x214, 0x5, 0x54, 0x2b, 0x2, 0x212, 0x214, 
       0x5, 0x56, 0x2c, 0x2, 0x213, 0x211, 0x3, 0x2, 0x2, 0x2, 0x213, 0x212, 
       0x3, 0x2, 0x2, 0x2, 0x214, 0x53, 0x3, 0x2, 0x2, 0x2, 0x215, 0x216, 
       0x7, 0x51, 0x2, 0x2, 0x216, 0x21a, 0x7, 0x5, 0x2, 0x2, 0x217, 0x218, 
       0x5, 0x5e, 0x30, 0x2, 0x218, 0x219, 0x7, 0x4, 0x2, 0x2, 0x219, 0x21b, 
       0x3, 0x2, 0x2, 0x2, 0x21a, 0x217, 0x3, 0x2, 0x2, 0x2, 0x21a, 0x21b, 
       0x3, 0x2, 0x2, 0x2, 0x21b, 0x21c, 0x3, 0x2, 0x2, 0x2, 0x21c, 0x21d, 
       0x5, 0x58, 0x2d, 0x2, 0x21d, 0x21e, 0x7, 0x6, 0x2, 0x2, 0x21e, 0x22c, 
       0x3, 0x2, 0x2, 0x2, 0x21f, 0x220, 0x7, 0x52, 0x2, 0x2, 0x220, 0x224, 
       0x7, 0x5, 0x2, 0x2, 0x221, 0x222, 0x5, 0x5e, 0x30, 0x2, 0x222, 0x223, 
       0x7, 0x4, 0x2, 0x2, 0x223, 0x225, 0x3, 0x2, 0x2, 0x2, 0x224, 0x221, 
       0x3, 0x2, 0x2, 0x2, 0x224, 0x225, 0x3, 0x2, 0x2, 0x2, 0x225, 0x226, 
       0x3, 0x2, 0x2, 0x2, 0x226, 0x227, 0x5, 0x58, 0x2d, 0x2, 0x227, 0x228, 
       0x7, 0x4, 0x2, 0x2, 0x228, 0x229, 0x5, 0x5a, 0x2e, 0x2, 0x229, 0x22a, 
       0x7, 0x6, 0x2, 0x2, 0x22a, 0x22c, 0x3, 0x2, 0x2, 0x2, 0x22b, 0x215, 
       0x3, 0x2, 0x2, 0x2, 0x22b, 0x21f, 0x3, 0x2, 0x2, 0x2, 0x22c, 0x55, 
       0x3, 0x2, 0x2, 0x2, 0x22d, 0x22e, 0x7, 0x51, 0x2, 0x2, 0x22e, 0x22f, 
       0x7, 0x5, 0x2, 0x2, 0x22f, 0x230, 0x7, 0x7a, 0x2, 0x2, 0x230, 0x231, 
       0x7, 0x6, 0x2, 0x2, 0x231, 0x57, 0x3, 0x2, 0x2, 0x2, 0x232, 0x233, 
       0x7, 0x53, 0x2, 0x2, 0x233, 0x234, 0x7, 0x7a, 0x2, 0x2, 0x234, 0x235, 
       0x5, 0x5c, 0x2f, 0x2, 0x235, 0x59, 0x3, 0x2, 0x2, 0x2, 0x236, 0x237, 
       0x7, 0x54, 0x2, 0x2, 0x237, 0x238, 0x7, 0x12, 0x2, 0x2, 0x238, 0x239, 
       0x7, 0x7a, 0x2, 0x2, 0x239, 0x23a, 0x5, 0x5c, 0x2f, 0x2, 0x23a, 0x5b, 
       0x3, 0x2, 0x2, 0x2, 0x23b, 0x23c, 0x9, 0x2, 0x2, 0x2, 0x23c, 0x5d, 
       0x3, 0x2, 0x2, 0x2, 0x23d, 0x23e, 0x7, 0x80, 0x2, 0x2, 0x23e, 0x5f, 
       0x3, 0x2, 0x2, 0x2, 0x23f, 0x240, 0x9, 0x3, 0x2, 0x2, 0x240, 0x61, 
       0x3, 0x2, 0x2, 0x2, 0x241, 0x242, 0x7, 0x29, 0x2, 0x2, 0x242, 0x243, 
       0x7, 0x2a, 0x2, 0x2, 0x243, 0x244, 0x5, 0x64, 0x33, 0x2, 0x244, 0x245, 
       0x7, 0xe, 0x2, 0x2, 0x245, 0x63, 0x3, 0x2, 0x2, 0x2, 0x246, 0x24a, 
       0x5, 0x66, 0x34, 0x2, 0x247, 0x24a, 0x5, 0x6a, 0x36, 0x2, 0x248, 
       0x24a, 0x5, 0x76, 0x3c, 0x2, 0x249, 0x246, 0x3, 0x2, 0x2, 0x2, 0x249, 
       0x247, 0x3, 0x2, 0x2, 0x2, 0x249, 0x248, 0x3, 0x2, 0x2, 0x2, 0x24a, 
       0x65, 0x3, 0x2, 0x2, 0x2, 0x24b, 0x24c, 0x5, 0x68, 0x35, 0x2, 0x24c, 
       0x24d, 0x7, 0x5, 0x2, 0x2, 0x24d, 0x24e, 0x7, 0x76, 0x2, 0x2, 0x24e, 
       0x24f, 0x7, 0x6, 0x2, 0x2, 0x24f, 0x67, 0x3, 0x2, 0x2, 0x2, 0x250, 
       0x251, 0x7, 0x60, 0x2, 0x2, 0x251, 0x69, 0x3, 0x2, 0x2, 0x2, 0x252, 
       0x253, 0x5, 0x6e, 0x38, 0x2, 0x253, 0x254, 0x7, 0x5, 0x2, 0x2, 0x254, 
       0x255, 0x5, 0x70, 0x39, 0x2, 0x255, 0x256, 0x7, 0x4, 0x2, 0x2, 0x256, 
       0x257, 0x5, 0x72, 0x3a, 0x2, 0x257, 0x258, 0x7, 0x4, 0x2, 0x2, 0x258, 
       0x259, 0x5, 0x74, 0x3b, 0x2, 0x259, 0x25a, 0x7, 0x6, 0x2, 0x2, 0x25a, 
       0x6b, 0x3, 0x2, 0x2, 0x2, 0x25b, 0x25d, 0x7, 0x34, 0x2, 0x2, 0x25c, 
       0x25b, 0x3, 0x2, 0x2, 0x2, 0x25c, 0x25d, 0x3, 0x2, 0x2, 0x2, 0x25d, 
       0x25e, 0x3, 0x2, 0x2, 0x2, 0x25e, 0x25f, 0x7, 0x35, 0x2, 0x2, 0x25f, 
       0x6d, 0x3, 0x2, 0x2, 0x2, 0x260, 0x261, 0x7, 0x61, 0x2, 0x2, 0x261, 
       0x6f, 0x3, 0x2, 0x2, 0x2, 0x262, 0x263, 0x7, 0x80, 0x2, 0x2, 0x263, 
       0x71, 0x3, 0x2, 0x2, 0x2, 0x264, 0x265, 0x9, 0x4, 0x2, 0x2, 0x265, 
       0x73, 0x3, 0x2, 0x2, 0x2, 0x266, 0x267, 0x7, 0x7a, 0x2, 0x2, 0x267, 
       0x75, 0x3, 0x2, 0x2, 0x2, 0x268, 0x269, 0x5, 0x78, 0x3d, 0x2, 0x269, 
       0x26a, 0x7, 0x5, 0x2, 0x2, 0x26a, 0x26b, 0x5, 0x7a, 0x3e, 0x2, 0x26b, 
       0x26c, 0x7, 0x4, 0x2, 0x2, 0x26c, 0x26d, 0x5, 0x7c, 0x3f, 0x2, 0x26d, 
       0x26e, 0x7, 0x4, 0x2, 0x2, 0x26e, 0x26f, 0x5, 0x7e, 0x40, 0x2, 0x26f, 
       0x270, 0x7, 0x6, 0x2, 0x2, 0x270, 0x77, 0x3, 0x2, 0x2, 0x2, 0x271, 
       0x272, 0x7, 0x62, 0x2, 0x2, 0x272, 0x79, 0x3, 0x2, 0x2, 0x2, 0x273, 
       0x274, 0x7, 0x80, 0x2, 0x2, 0x274, 0x7b, 0x3, 0x2, 0x2, 0x2, 0x275, 
       0x276, 0x7, 0x80, 0x2, 0x2, 0x276, 0x7d, 0x3, 0x2, 0x2, 0x2, 0x277, 
       0x278, 0x7, 0x7a, 0x2, 0x2, 0x278, 0x7f, 0x3, 0x2, 0x2, 0x2, 0x279, 
       0x27b, 0x5, 0x42, 0x22, 0x2, 0x27a, 0x27c, 0x9, 0x5, 0x2, 0x2, 0x27b, 
       0x27a, 0x3, 0x2, 0x2, 0x2, 0x27b, 0x27c, 0x3, 0x2, 0x2, 0x2, 0x27c, 
       0x27f, 0x3, 0x2, 0x2, 0x2, 0x27d, 0x27e, 0x7, 0x36, 0x2, 0x2, 0x27e, 
       0x280, 0x9, 0x6, 0x2, 0x2, 0x27f, 0x27d, 0x3, 0x2, 0x2, 0x2, 0x27f, 
       0x280, 0x3, 0x2, 0x2, 0x2, 0x280, 0x81, 0x3, 0x2, 0x2, 0x2, 0x281, 
       0x283, 0x7, 0x34, 0x2, 0x2, 0x282, 0x281, 0x3, 0x2, 0x2, 0x2, 0x282, 
       0x283, 0x3, 0x2, 0x2, 0x2, 0x283, 0x284, 0x3, 0x2, 0x2, 0x2, 0x284, 
       0x285, 0x7, 0x11, 0x2, 0x2, 0x285, 0x286, 0x5, 0x84, 0x43, 0x2, 0x286, 
       0x287, 0x7, 0xc, 0x2, 0x2, 0x287, 0x288, 0x5, 0x84, 0x43, 0x2, 0x288, 
       0x2d1, 0x3, 0x2, 0x2, 0x2, 0x289, 0x28b, 0x7, 0x34, 0x2, 0x2, 0x28a, 
       0x289, 0x3, 0x2, 0x2, 0x2, 0x28a, 0x28b, 0x3, 0x2, 0x2, 0x2, 0x28b, 
       0x28c, 0x3, 0x2, 0x2, 0x2, 0x28c, 0x28d, 0x7, 0x27, 0x2, 0x2, 0x28d, 
       0x28e, 0x7, 0x5, 0x2, 0x2, 0x28e, 0x293, 0x5, 0x42, 0x22, 0x2, 0x28f, 
       0x290, 0x7, 0x4, 0x2, 0x2, 0x290, 0x292, 0x5, 0x42, 0x22, 0x2, 0x291, 
       0x28f, 0x3, 0x2, 0x2, 0x2, 0x292, 0x295, 0x3, 0x2, 0x2, 0x2, 0x293, 
       0x291, 0x3, 0x2, 0x2, 0x2, 0x293, 0x294, 0x3, 0x2, 0x2, 0x2, 0x294, 
       0x296, 0x3, 0x2, 0x2, 0x2, 0x295, 0x293, 0x3, 0x2, 0x2, 0x2, 0x296, 
       0x297, 0x7, 0x6, 0x2, 0x2, 0x297, 0x2d1, 0x3, 0x2, 0x2, 0x2, 0x298, 
       0x29a, 0x7, 0x34, 0x2, 0x2, 0x299, 0x298, 0x3, 0x2, 0x2, 0x2, 0x299, 
       0x29a, 0x3, 0x2, 0x2, 0x2, 0x29a, 0x29b, 0x3, 0x2, 0x2, 0x2, 0x29b, 
       0x29c, 0x7, 0x27, 0x2, 0x2, 0x29c, 0x29d, 0x7, 0x5, 0x2, 0x2, 0x29d, 
       0x29e, 0x5, 0x6, 0x4, 0x2, 0x29e, 0x29f, 0x7, 0x6, 0x2, 0x2, 0x29f, 
       0x2d1, 0x3, 0x2, 0x2, 0x2, 0x2a0, 0x2a2, 0x7, 0x34, 0x2, 0x2, 0x2a1, 
       0x2a0, 0x3, 0x2, 0x2, 0x2, 0x2a1, 0x2a2, 0x3, 0x2, 0x2, 0x2, 0x2a2, 
       0x2a3, 0x3, 0x2, 0x2, 0x2, 0x2a3, 0x2a4, 0x7, 0x3e, 0x2, 0x2, 0x2a4, 
       0x2d1, 0x5, 0x84, 0x43, 0x2, 0x2a5, 0x2a7, 0x7, 0x34, 0x2, 0x2, 0x2a6, 
       0x2a5, 0x3, 0x2, 0x2, 0x2, 0x2a6, 0x2a7, 0x3, 0x2, 0x2, 0x2, 0x2a7, 
       0x2a8, 0x3, 0x2, 0x2, 0x2, 0x2a8, 0x2a9, 0x7, 0x2f, 0x2, 0x2, 0x2a9, 
       0x2b7, 0x9, 0x7, 0x2, 0x2, 0x2aa, 0x2ab, 0x7, 0x5, 0x2, 0x2, 0x2ab, 
       0x2b8, 0x7, 0x6, 0x2, 0x2, 0x2ac, 0x2ad, 0x7, 0x5, 0x2, 0x2, 0x2ad, 
       0x2b2, 0x5, 0x42, 0x22, 0x2, 0x2ae, 0x2af, 0x7, 0x4, 0x2, 0x2, 0x2af, 
       0x2b1, 0x5, 0x42, 0x22, 0x2, 0x2b0, 0x2ae, 0x3, 0x2, 0x2, 0x2, 0x2b1, 
       0x2b4, 0x3, 0x2, 0x2, 0x2, 0x2b2, 0x2b0, 0x3, 0x2, 0x2, 0x2, 0x2b2, 
       0x2b3, 0x3, 0x2, 0x2, 0x2, 0x2b3, 0x2b5, 0x3, 0x2, 0x2, 0x2, 0x2b4, 
       0x2b2, 0x3, 0x2, 0x2, 0x2, 0x2b5, 0x2b6, 0x7, 0x6, 0x2, 0x2, 0x2b6, 
       0x2b8, 0x3, 0x2, 0x2, 0x2, 0x2b7, 0x2aa, 0x3, 0x2, 0x2, 0x2, 0x2b7, 
       0x2ac, 0x3, 0x2, 0x2, 0x2, 0x2b8, 0x2d1, 0x3, 0x2, 0x2, 0x2, 0x2b9, 
       0x2bb, 0x7, 0x34, 0x2, 0x2, 0x2ba, 0x2b9, 0x3, 0x2, 0x2, 0x2, 0x2ba, 
       0x2bb, 0x3, 0x2, 0x2, 0x2, 0x2bb, 0x2bc, 0x3, 0x2, 0x2, 0x2, 0x2bc, 
       0x2bd, 0x7, 0x2f, 0x2, 0x2, 0x2bd, 0x2c0, 0x5, 0x84, 0x43, 0x2, 0x2be, 
       0x2bf, 0x7, 0x1c, 0x2, 0x2, 0x2bf, 0x2c1, 0x7, 0x76, 0x2, 0x2, 0x2c0, 
       0x2be, 0x3, 0x2, 0x2, 0x2, 0x2c0, 0x2c1, 0x3, 0x2, 0x2, 0x2, 0x2c1, 
       0x2d1, 0x3, 0x2, 0x2, 0x2, 0x2c2, 0x2c3, 0x7, 0x2b, 0x2, 0x2, 0x2c3, 
       0x2d1, 0x5, 0x6c, 0x37, 0x2, 0x2c4, 0x2c6, 0x7, 0x2b, 0x2, 0x2, 0x2c5, 
       0x2c7, 0x7, 0x34, 0x2, 0x2, 0x2c6, 0x2c5, 0x3, 0x2, 0x2, 0x2, 0x2c6, 
       0x2c7, 0x3, 0x2, 0x2, 0x2, 0x2c7, 0x2c8, 0x3, 0x2, 0x2, 0x2, 0x2c8, 
       0x2d1, 0x9, 0x8, 0x2, 0x2, 0x2c9, 0x2cb, 0x7, 0x2b, 0x2, 0x2, 0x2ca, 
       0x2cc, 0x7, 0x34, 0x2, 0x2, 0x2cb, 0x2ca, 0x3, 0x2, 0x2, 0x2, 0x2cb, 
       0x2cc, 0x3, 0x2, 0x2, 0x2, 0x2cc, 0x2cd, 0x3, 0x2, 0x2, 0x2, 0x2cd, 
       0x2ce, 0x7, 0x17, 0x2, 0x2, 0x2ce, 0x2cf, 0x7, 0x21, 0x2, 0x2, 0x2cf, 
       0x2d1, 0x5, 0x84, 0x43, 0x2, 0x2d0, 0x282, 0x3, 0x2, 0x2, 0x2, 0x2d0, 
       0x28a, 0x3, 0x2, 0x2, 0x2, 0x2d0, 0x299, 0x3, 0x2, 0x2, 0x2, 0x2d0, 
       0x2a1, 0x3, 0x2, 0x2, 0x2, 0x2d0, 0x2a6, 0x3, 0x2, 0x2, 0x2, 0x2d0, 
       0x2ba, 0x3, 0x2, 0x2, 0x2, 0x2d0, 0x2c2, 0x3, 0x2, 0x2, 0x2, 0x2d0, 
       0x2c4, 0x3, 0x2, 0x2, 0x2, 0x2d0, 0x2c9, 0x3, 0x2, 0x2, 0x2, 0x2d1, 
       0x83, 0x3, 0x2, 0x2, 0x2, 0x2d2, 0x2d3, 0x8, 0x43, 0x1, 0x2, 0x2d3, 
       0x2d7, 0x5, 0x8c, 0x47, 0x2, 0x2d4, 0x2d5, 0x9, 0x9, 0x2, 0x2, 0x2d5, 
       0x2d7, 0x5, 0x84, 0x43, 0x9, 0x2d6, 0x2d2, 0x3, 0x2, 0x2, 0x2, 0x2d6, 
       0x2d4, 0x3, 0x2, 0x2, 0x2, 0x2d7, 0x2ed, 0x3, 0x2, 0x2, 0x2, 0x2d8, 
       0x2d9, 0xc, 0x8, 0x2, 0x2, 0x2d9, 0x2da, 0x9, 0xa, 0x2, 0x2, 0x2da, 
       0x2ec, 0x5, 0x84, 0x43, 0x9, 0x2db, 0x2dc, 0xc, 0x7, 0x2, 0x2, 0x2dc, 
       0x2dd, 0x9, 0xb, 0x2, 0x2, 0x2dd, 0x2ec, 0x5, 0x84, 0x43, 0x8, 0x2de, 
       0x2df, 0xc, 0x6, 0x2, 0x2, 0x2df, 0x2e0, 0x7, 0x72, 0x2, 0x2, 0x2e0, 
       0x2ec, 0x5, 0x84, 0x43, 0x7, 0x2e1, 0x2e2, 0xc, 0x5, 0x2, 0x2, 0x2e2, 
       0x2e3, 0x7, 0x75, 0x2, 0x2, 0x2e3, 0x2ec, 0x5, 0x84, 0x43, 0x6, 0x2e4, 
       0x2e5, 0xc, 0x4, 0x2, 0x2, 0x2e5, 0x2e6, 0x7, 0x73, 0x2, 0x2, 0x2e6, 
       0x2ec, 0x5, 0x84, 0x43, 0x5, 0x2e7, 0x2e8, 0xc, 0x3, 0x2, 0x2, 0x2e8, 
       0x2e9, 0x5, 0x86, 0x44, 0x2, 0x2e9, 0x2ea, 0x5, 0x84, 0x43, 0x4, 
       0x2ea, 0x2ec, 0x3, 0x2, 0x2, 0x2, 0x2eb, 0x2d8, 0x3, 0x2, 0x2, 0x2, 
       0x2eb, 0x2db, 0x3, 0x2, 0x2, 0x2, 0x2eb, 0x2de, 0x3, 0x2, 0x2, 0x2, 
       0x2eb, 0x2e1, 0x3, 0x2, 0x2, 0x2, 0x2eb, 0x2e4, 0x3, 0x2, 0x2, 0x2, 
       0x2eb, 0x2e7, 0x3, 0x2, 0x2, 0x2, 0x2ec, 0x2ef, 0x3, 0x2, 0x2, 0x2, 
       0x2ed, 0x2eb, 0x3, 0x2, 0x2, 0x2, 0x2ed, 0x2ee, 0x3, 0x2, 0x2, 0x2, 
       0x2ee, 0x85, 0x3, 0x2, 0x2, 0x2, 0x2ef, 0x2ed, 0x3, 0x2, 0x2, 0x2, 
       0x2f0, 0x2f1, 0x9, 0xc, 0x2, 0x2, 0x2f1, 0x87, 0x3, 0x2, 0x2, 0x2, 
       0x2f2, 0x2f3, 0x7, 0x8, 0x2, 0x2, 0x2f3, 0x2fa, 0x5, 0x8a, 0x46, 
       0x2, 0x2f4, 0x2f6, 0x7, 0x4, 0x2, 0x2, 0x2f5, 0x2f4, 0x3, 0x2, 0x2, 
       0x2, 0x2f5, 0x2f6, 0x3, 0x2, 0x2, 0x2, 0x2f6, 0x2f7, 0x3, 0x2, 0x2, 
       0x2, 0x2f7, 0x2f9, 0x5, 0x8a, 0x46, 0x2, 0x2f8, 0x2f5, 0x3, 0x2, 
       0x2, 0x2, 0x2f9, 0x2fc, 0x3, 0x2, 0x2, 0x2, 0x2fa, 0x2f8, 0x3, 0x2, 
       0x2, 0x2, 0x2fa, 0x2fb, 0x3, 0x2, 0x2, 0x2, 0x2fb, 0x2fd, 0x3, 0x2, 
       0x2, 0x2, 0x2fc, 0x2fa, 0x3, 0x2, 0x2, 0x2, 0x2fd, 0x2fe, 0x7, 0x9, 
       0x2, 0x2, 0x2fe, 0x89, 0x3, 0x2, 0x2, 0x2, 0x2ff, 0x30d, 0x5, 0x32, 
       0x1a, 0x2, 0x300, 0x301, 0x5, 0x32, 0x1a, 0x2, 0x301, 0x302, 0x7, 
       0x5, 0x2, 0x2, 0x302, 0x307, 0x5, 0x8c, 0x47, 0x2, 0x303, 0x304, 
       0x7, 0x4, 0x2, 0x2, 0x304, 0x306, 0x5, 0x8c, 0x47, 0x2, 0x305, 0x303, 
       0x3, 0x2, 0x2, 0x2, 0x306, 0x309, 0x3, 0x2, 0x2, 0x2, 0x307, 0x305, 
       0x3, 0x2, 0x2, 0x2, 0x307, 0x308, 0x3, 0x2, 0x2, 0x2, 0x308, 0x30a, 
       0x3, 0x2, 0x2, 0x2, 0x309, 0x307, 0x3, 0x2, 0x2, 0x2, 0x30a, 0x30b, 
       0x7, 0x6, 0x2, 0x2, 0x30b, 0x30d, 0x3, 0x2, 0x2, 0x2, 0x30c, 0x2ff, 
       0x3, 0x2, 0x2, 0x2, 0x30c, 0x300, 0x3, 0x2, 0x2, 0x2, 0x30d, 0x8b, 
       0x3, 0x2, 0x2, 0x2, 0x30e, 0x30f, 0x8, 0x47, 0x1, 0x2, 0x30f, 0x337, 
       0x7, 0x6e, 0x2, 0x2, 0x310, 0x311, 0x5, 0x8e, 0x48, 0x2, 0x311, 0x312, 
       0x7, 0x7, 0x2, 0x2, 0x312, 0x313, 0x7, 0x6e, 0x2, 0x2, 0x313, 0x337, 
       0x3, 0x2, 0x2, 0x2, 0x314, 0x315, 0x7, 0x5, 0x2, 0x2, 0x315, 0x316, 
       0x5, 0x6, 0x4, 0x2, 0x316, 0x317, 0x7, 0x6, 0x2, 0x2, 0x317, 0x337, 
       0x3, 0x2, 0x2, 0x2, 0x318, 0x319, 0x7, 0x5, 0x2, 0x2, 0x319, 0x31c, 
       0x5, 0x30, 0x19, 0x2, 0x31a, 0x31b, 0x7, 0x4, 0x2, 0x2, 0x31b, 0x31d, 
       0x5, 0x30, 0x19, 0x2, 0x31c, 0x31a, 0x3, 0x2, 0x2, 0x2, 0x31d, 0x31e, 
       0x3, 0x2, 0x2, 0x2, 0x31e, 0x31c, 0x3, 0x2, 0x2, 0x2, 0x31e, 0x31f, 
       0x3, 0x2, 0x2, 0x2, 0x31f, 0x320, 0x3, 0x2, 0x2, 0x2, 0x320, 0x321, 
       0x7, 0x6, 0x2, 0x2, 0x321, 0x337, 0x3, 0x2, 0x2, 0x2, 0x322, 0x323, 
       0x5, 0x60, 0x31, 0x2, 0x323, 0x32c, 0x7, 0x5, 0x2, 0x2, 0x324, 0x329, 
       0x5, 0x42, 0x22, 0x2, 0x325, 0x326, 0x7, 0x4, 0x2, 0x2, 0x326, 0x328, 
       0x5, 0x42, 0x22, 0x2, 0x327, 0x325, 0x3, 0x2, 0x2, 0x2, 0x328, 0x32b, 
       0x3, 0x2, 0x2, 0x2, 0x329, 0x327, 0x3, 0x2, 0x2, 0x2, 0x329, 0x32a, 
       0x3, 0x2, 0x2, 0x2, 0x32a, 0x32d, 0x3, 0x2, 0x2, 0x2, 0x32b, 0x329, 
       0x3, 0x2, 0x2, 0x2, 0x32c, 0x324, 0x3, 0x2, 0x2, 0x2, 0x32c, 0x32d, 
       0x3, 0x2, 0x2, 0x2, 0x32d, 0x32e, 0x3, 0x2, 0x2, 0x2, 0x32e, 0x32f, 
       0x7, 0x6, 0x2, 0x2, 0x32f, 0x337, 0x3, 0x2, 0x2, 0x2, 0x330, 0x331, 
       0x7, 0x5, 0x2, 0x2, 0x331, 0x332, 0x5, 0x42, 0x22, 0x2, 0x332, 0x333, 
       0x7, 0x6, 0x2, 0x2, 0x333, 0x337, 0x3, 0x2, 0x2, 0x2, 0x334, 0x337, 
       0x5, 0x92, 0x4a, 0x2, 0x335, 0x337, 0x5, 0x32, 0x1a, 0x2, 0x336, 
       0x30e, 0x3, 0x2, 0x2, 0x2, 0x336, 0x310, 0x3, 0x2, 0x2, 0x2, 0x336, 
       0x314, 0x3, 0x2, 0x2, 0x2, 0x336, 0x318, 0x3, 0x2, 0x2, 0x2, 0x336, 
       0x322, 0x3, 0x2, 0x2, 0x2, 0x336, 0x330, 0x3, 0x2, 0x2, 0x2, 0x336, 
       0x334, 0x3, 0x2, 0x2, 0x2, 0x336, 0x335, 0x3, 0x2, 0x2, 0x2, 0x337, 
       0x33d, 0x3, 0x2, 0x2, 0x2, 0x338, 0x339, 0xc, 0x9, 0x2, 0x2, 0x339, 
       0x33a, 0x7, 0x7, 0x2, 0x2, 0x33a, 0x33c, 0x5, 0x32, 0x1a, 0x2, 0x33b, 
       0x338, 0x3, 0x2, 0x2, 0x2, 0x33c, 0x33f, 0x3, 0x2, 0x2, 0x2, 0x33d, 
       0x33b, 0x3, 0x2, 0x2, 0x2, 0x33d, 0x33e, 0x3, 0x2, 0x2, 0x2, 0x33e, 
       0x8d, 0x3, 0x2, 0x2, 0x2, 0x33f, 0x33d, 0x3, 0x2, 0x2, 0x2, 0x340, 
       0x345, 0x5, 0x32, 0x1a, 0x2, 0x341, 0x342, 0x7, 0x7, 0x2, 0x2, 0x342, 
       0x344, 0x5, 0x32, 0x1a, 0x2, 0x343, 0x341, 0x3, 0x2, 0x2, 0x2, 0x344, 
       0x347, 0x3, 0x2, 0x2, 0x2, 0x345, 0x343, 0x3, 0x2, 0x2, 0x2, 0x345, 
       0x346, 0x3, 0x2, 0x2, 0x2, 0x346, 0x8f, 0x3, 0x2, 0x2, 0x2, 0x347, 
       0x345, 0x3, 0x2, 0x2, 0x2, 0x348, 0x34a, 0x6, 0x49, 0xf, 0x2, 0x349, 
       0x34b, 0x7, 0x6d, 0x2, 0x2, 0x34a, 0x349, 0x3, 0x2, 0x2, 0x2, 0x34a, 
       0x34b, 0x3, 0x2, 0x2, 0x2, 0x34b, 0x34c, 0x3, 0x2, 0x2, 0x2, 0x34c, 
       0x374, 0x7, 0x7b, 0x2, 0x2, 0x34d, 0x34f, 0x6, 0x49, 0x10, 0x2, 0x34e, 
       0x350, 0x7, 0x6d, 0x2, 0x2, 0x34f, 0x34e, 0x3, 0x2, 0x2, 0x2, 0x34f, 
       0x350, 0x3, 0x2, 0x2, 0x2, 0x350, 0x351, 0x3, 0x2, 0x2, 0x2, 0x351, 
       0x374, 0x7, 0x7c, 0x2, 0x2, 0x352, 0x354, 0x6, 0x49, 0x11, 0x2, 0x353, 
       0x355, 0x7, 0x6d, 0x2, 0x2, 0x354, 0x353, 0x3, 0x2, 0x2, 0x2, 0x354, 
       0x355, 0x3, 0x2, 0x2, 0x2, 0x355, 0x356, 0x3, 0x2, 0x2, 0x2, 0x356, 
       0x374, 0x9, 0xd, 0x2, 0x2, 0x357, 0x359, 0x7, 0x6d, 0x2, 0x2, 0x358, 
       0x357, 0x3, 0x2, 0x2, 0x2, 0x358, 0x359, 0x3, 0x2, 0x2, 0x2, 0x359, 
       0x35a, 0x3, 0x2, 0x2, 0x2, 0x35a, 0x374, 0x7, 0x7a, 0x2, 0x2, 0x35b, 
       0x35d, 0x7, 0x6d, 0x2, 0x2, 0x35c, 0x35b, 0x3, 0x2, 0x2, 0x2, 0x35c, 
       0x35d, 0x3, 0x2, 0x2, 0x2, 0x35d, 0x35e, 0x3, 0x2, 0x2, 0x2, 0x35e, 
       0x374, 0x7, 0x77, 0x2, 0x2, 0x35f, 0x361, 0x7, 0x6d, 0x2, 0x2, 0x360, 
       0x35f, 0x3, 0x2, 0x2, 0x2, 0x360, 0x361, 0x3, 0x2, 0x2, 0x2, 0x361, 
       0x362, 0x3, 0x2, 0x2, 0x2, 0x362, 0x374, 0x7, 0x78, 0x2, 0x2, 0x363, 
       0x365, 0x7, 0x6d, 0x2, 0x2, 0x364, 0x363, 0x3, 0x2, 0x2, 0x2, 0x364, 
       0x365, 0x3, 0x2, 0x2, 0x2, 0x365, 0x366, 0x3, 0x2, 0x2, 0x2, 0x366, 
       0x374, 0x7, 0x79, 0x2, 0x2, 0x367, 0x369, 0x7, 0x6d, 0x2, 0x2, 0x368, 
       0x367, 0x3, 0x2, 0x2, 0x2, 0x368, 0x369, 0x3, 0x2, 0x2, 0x2, 0x369, 
       0x36a, 0x3, 0x2, 0x2, 0x2, 0x36a, 0x374, 0x7, 0x7e, 0x2, 0x2, 0x36b, 
       0x36d, 0x7, 0x6d, 0x2, 0x2, 0x36c, 0x36b, 0x3, 0x2, 0x2, 0x2, 0x36c, 
       0x36d, 0x3, 0x2, 0x2, 0x2, 0x36d, 0x36e, 0x3, 0x2, 0x2, 0x2, 0x36e, 
       0x374, 0x7, 0x7d, 0x2, 0x2, 0x36f, 0x371, 0x7, 0x6d, 0x2, 0x2, 0x370, 
       0x36f, 0x3, 0x2, 0x2, 0x2, 0x370, 0x371, 0x3, 0x2, 0x2, 0x2, 0x371, 
       0x372, 0x3, 0x2, 0x2, 0x2, 0x372, 0x374, 0x7, 0x7f, 0x2, 0x2, 0x373, 
       0x348, 0x3, 0x2, 0x2, 0x2, 0x373, 0x34d, 0x3, 0x2, 0x2, 0x2, 0x373, 
       0x352, 0x3, 0x2, 0x2, 0x2, 0x373, 0x358, 0x3, 0x2, 0x2, 0x2, 0x373, 
       0x35c, 0x3, 0x2, 0x2, 0x2, 0x373, 0x360, 0x3, 0x2, 0x2, 0x2, 0x373, 
       0x364, 0x3, 0x2, 0x2, 0x2, 0x373, 0x368, 0x3, 0x2, 0x2, 0x2, 0x373, 
       0x36c, 0x3, 0x2, 0x2, 0x2, 0x373, 0x370, 0x3, 0x2, 0x2, 0x2, 0x374, 
       0x91, 0x3, 0x2, 0x2, 0x2, 0x375, 0x381, 0x7, 0x35, 0x2, 0x2, 0x376, 
       0x377, 0x5, 0x32, 0x1a, 0x2, 0x377, 0x378, 0x7, 0x76, 0x2, 0x2, 0x378, 
       0x381, 0x3, 0x2, 0x2, 0x2, 0x379, 0x381, 0x5, 0x90, 0x49, 0x2, 0x37a, 
       0x381, 0x5, 0x94, 0x4b, 0x2, 0x37b, 0x37d, 0x7, 0x76, 0x2, 0x2, 0x37c, 
       0x37b, 0x3, 0x2, 0x2, 0x2, 0x37d, 0x37e, 0x3, 0x2, 0x2, 0x2, 0x37e, 
       0x37c, 0x3, 0x2, 0x2, 0x2, 0x37e, 0x37f, 0x3, 0x2, 0x2, 0x2, 0x37f, 
       0x381, 0x3, 0x2, 0x2, 0x2, 0x380, 0x375, 0x3, 0x2, 0x2, 0x2, 0x380, 
       0x376, 0x3, 0x2, 0x2, 0x2, 0x380, 0x379, 0x3, 0x2, 0x2, 0x2, 0x380, 
       0x37a, 0x3, 0x2, 0x2, 0x2, 0x380, 0x37c, 0x3, 0x2, 0x2, 0x2, 0x381, 
       0x93, 0x3, 0x2, 0x2, 0x2, 0x382, 0x383, 0x9, 0xe, 0x2, 0x2, 0x383, 
       0x95, 0x3, 0x2, 0x2, 0x2, 0x384, 0x385, 0x9, 0xf, 0x2, 0x2, 0x385, 
       0x97, 0x3, 0x2, 0x2, 0x2, 0x386, 0x387, 0x9, 0x10, 0x2, 0x2, 0x387, 
       0x99, 0x3, 0x2, 0x2, 0x2, 0x388, 0x389, 0x9, 0x11, 0x2, 0x2, 0x389, 
       0x9b, 0x3, 0x2, 0x2, 0x2, 0x68, 0xa0, 0xb1, 0xb4, 0xb9, 0xbb, 0xbf, 
       0xc9, 0xd5, 0xd8, 0xdc, 0xdf, 0xe2, 0xe5, 0xed, 0xf4, 0xfb, 0x102, 
       0x105, 0x119, 0x122, 0x125, 0x12e, 0x132, 0x135, 0x13b, 0x14c, 0x152, 
       0x156, 0x158, 0x15f, 0x167, 0x16c, 0x170, 0x172, 0x177, 0x17f, 0x18c, 
       0x196, 0x199, 0x1a0, 0x1af, 0x1b1, 0x1b9, 0x1bb, 0x1bf, 0x1c2, 0x1c5, 
       0x1ce, 0x1dd, 0x1e2, 0x1ee, 0x1f3, 0x1fb, 0x1fe, 0x202, 0x213, 0x21a, 
       0x224, 0x22b, 0x249, 0x25c, 0x27b, 0x27f, 0x282, 0x28a, 0x293, 0x299, 
       0x2a1, 0x2a6, 0x2b2, 0x2b7, 0x2ba, 0x2c0, 0x2c6, 0x2cb, 0x2d0, 0x2d6, 
       0x2eb, 0x2ed, 0x2f5, 0x2fa, 0x307, 0x30c, 0x31e, 0x329, 0x32c, 0x336, 
       0x33d, 0x345, 0x34a, 0x34f, 0x354, 0x358, 0x35c, 0x360, 0x364, 0x368, 
       0x36c, 0x370, 0x373, 0x37e, 0x380, 
  };

  _serializedATN.insert(_serializedATN.end(), serializedATNSegment0,
    serializedATNSegment0 + sizeof(serializedATNSegment0) / sizeof(serializedATNSegment0[0]));


  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

NebulaSQLParser::Initializer NebulaSQLParser::_init;
