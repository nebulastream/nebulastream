
// Generated from NebulaSQL.g4 by ANTLR 4.9.2


#include <Parsers/NebulaSQL/gen/NebulaSQLListener.h>
#include <Parsers/NebulaSQL/gen/NebulaSQLParser.h>

using namespace antlrcpp;
using namespace NES::Parsers;
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
    setState(168);
    statement();
    setState(172);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == NebulaSQLParser::T__0) {
      setState(169);
      match(NebulaSQLParser::T__0);
      setState(174);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(175);
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
    setState(177);
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
    setState(179);
    queryTerm(0);
    setState(180);
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
    setState(192);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::ORDER) {
      setState(182);
      match(NebulaSQLParser::ORDER);
      setState(183);
      match(NebulaSQLParser::BY);
      setState(184);
      dynamic_cast<QueryOrganizationContext *>(_localctx)->sortItemContext = sortItem();
      dynamic_cast<QueryOrganizationContext *>(_localctx)->order.push_back(dynamic_cast<QueryOrganizationContext *>(_localctx)->sortItemContext);
      setState(189);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NebulaSQLParser::T__1) {
        setState(185);
        match(NebulaSQLParser::T__1);
        setState(186);
        dynamic_cast<QueryOrganizationContext *>(_localctx)->sortItemContext = sortItem();
        dynamic_cast<QueryOrganizationContext *>(_localctx)->order.push_back(dynamic_cast<QueryOrganizationContext *>(_localctx)->sortItemContext);
        setState(191);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(199);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::LIMIT) {
      setState(194);
      match(NebulaSQLParser::LIMIT);
      setState(197);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case NebulaSQLParser::ALL: {
          setState(195);
          match(NebulaSQLParser::ALL);
          break;
        }

        case NebulaSQLParser::INTEGER_VALUE: {
          setState(196);
          dynamic_cast<QueryOrganizationContext *>(_localctx)->limit = match(NebulaSQLParser::INTEGER_VALUE);
          break;
        }

      default:
        throw NoViableAltException(this);
      }
    }
    setState(203);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::OFFSET) {
      setState(201);
      match(NebulaSQLParser::OFFSET);
      setState(202);
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

    setState(206);
    queryPrimary();
    _ctx->stop = _input->LT(-1);
    setState(213);
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
        setState(208);

        if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
        setState(209);
        dynamic_cast<SetOperationContext *>(_localctx)->setoperator = match(NebulaSQLParser::UNION);
        setState(210);
        dynamic_cast<SetOperationContext *>(_localctx)->right = queryTerm(2); 
      }
      setState(215);
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
    setState(225);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NebulaSQLParser::SELECT: {
        _localctx = dynamic_cast<QueryPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::QueryPrimaryDefaultContext>(_localctx));
        enterOuterAlt(_localctx, 1);
        setState(216);
        querySpecification();
        break;
      }

      case NebulaSQLParser::FROM: {
        _localctx = dynamic_cast<QueryPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::FromStmtContext>(_localctx));
        enterOuterAlt(_localctx, 2);
        setState(217);
        fromStatement();
        break;
      }

      case NebulaSQLParser::TABLE: {
        _localctx = dynamic_cast<QueryPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::TableContext>(_localctx));
        enterOuterAlt(_localctx, 3);
        setState(218);
        match(NebulaSQLParser::TABLE);
        setState(219);
        multipartIdentifier();
        break;
      }

      case NebulaSQLParser::VALUES: {
        _localctx = dynamic_cast<QueryPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::InlineTableDefault1Context>(_localctx));
        enterOuterAlt(_localctx, 4);
        setState(220);
        inlineTable();
        break;
      }

      case NebulaSQLParser::T__2: {
        _localctx = dynamic_cast<QueryPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::SubqueryContext>(_localctx));
        enterOuterAlt(_localctx, 5);
        setState(221);
        match(NebulaSQLParser::T__2);
        setState(222);
        query();
        setState(223);
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

NebulaSQLParser::SinkClauseContext* NebulaSQLParser::QuerySpecificationContext::sinkClause() {
  return getRuleContext<NebulaSQLParser::SinkClauseContext>(0);
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
    selectClause();
    setState(228);
    fromClause();
    setState(230);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 8, _ctx)) {
    case 1: {
      setState(229);
      whereClause();
      break;
    }

    default:
      break;
    }
    setState(233);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 9, _ctx)) {
    case 1: {
      setState(232);
      windowedAggregationClause();
      break;
    }

    default:
      break;
    }
    setState(236);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 10, _ctx)) {
    case 1: {
      setState(235);
      havingClause();
      break;
    }

    default:
      break;
    }
    setState(239);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 11, _ctx)) {
    case 1: {
      setState(238);
      sinkClause();
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
    setState(241);
    match(NebulaSQLParser::FROM);
    setState(242);
    relation();
    setState(247);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 12, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(243);
        match(NebulaSQLParser::T__1);
        setState(244);
        relation(); 
      }
      setState(249);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 12, _ctx);
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
    setState(250);
    relationPrimary();
    setState(254);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 13, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(251);
        joinRelation(); 
      }
      setState(256);
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
    setState(268);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NebulaSQLParser::INNER:
      case NebulaSQLParser::JOIN: {
        enterOuterAlt(_localctx, 1);
        setState(257);
        joinType();
        setState(258);
        match(NebulaSQLParser::JOIN);
        setState(259);
        dynamic_cast<JoinRelationContext *>(_localctx)->right = relationPrimary();
        setState(261);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 14, _ctx)) {
        case 1: {
          setState(260);
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
        setState(263);
        match(NebulaSQLParser::NATURAL);
        setState(264);
        joinType();
        setState(265);
        match(NebulaSQLParser::JOIN);
        setState(266);
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
    setState(271);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::INNER) {
      setState(270);
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
    setState(273);
    match(NebulaSQLParser::ON);
    setState(274);
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
    setState(291);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 17, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<RelationPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::TableNameContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(276);
      multipartIdentifier();
      setState(277);
      tableAlias();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<RelationPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::AliasedQueryContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(279);
      match(NebulaSQLParser::T__2);
      setState(280);
      query();
      setState(281);
      match(NebulaSQLParser::T__3);
      setState(282);
      tableAlias();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<RelationPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::AliasedRelationContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(284);
      match(NebulaSQLParser::T__2);
      setState(285);
      relation();
      setState(286);
      match(NebulaSQLParser::T__3);
      setState(287);
      tableAlias();
      break;
    }

    case 4: {
      _localctx = dynamic_cast<RelationPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::InlineTableDefault2Context>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(289);
      inlineTable();
      break;
    }

    case 5: {
      _localctx = dynamic_cast<RelationPrimaryContext *>(_tracker.createInstance<NebulaSQLParser::TableValuedFunctionContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(290);
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
    setState(293);
    dynamic_cast<FunctionTableContext *>(_localctx)->funcName = errorCapturingIdentifier();
    setState(294);
    match(NebulaSQLParser::T__2);
    setState(303);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 19, _ctx)) {
    case 1: {
      setState(295);
      expression();
      setState(300);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NebulaSQLParser::T__1) {
        setState(296);
        match(NebulaSQLParser::T__1);
        setState(297);
        expression();
        setState(302);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      break;
    }

    default:
      break;
    }
    setState(305);
    match(NebulaSQLParser::T__3);
    setState(306);
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
    setState(308);
    fromClause();
    setState(310); 
    _errHandler->sync(this);
    alt = 1;
    do {
      switch (alt) {
        case 1: {
              setState(309);
              fromStatementBody();
              break;
            }

      default:
        throw NoViableAltException(this);
      }
      setState(312); 
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 20, _ctx);
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
    setState(314);
    selectClause();
    setState(316);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 21, _ctx)) {
    case 1: {
      setState(315);
      whereClause();
      break;
    }

    default:
      break;
    }
    setState(319);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 22, _ctx)) {
    case 1: {
      setState(318);
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
    setState(321);
    match(NebulaSQLParser::SELECT);
    setState(325);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 23, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(322);
        dynamic_cast<SelectClauseContext *>(_localctx)->hintContext = hint();
        dynamic_cast<SelectClauseContext *>(_localctx)->hints.push_back(dynamic_cast<SelectClauseContext *>(_localctx)->hintContext); 
      }
      setState(327);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 23, _ctx);
    }
    setState(328);
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
    setState(330);
    match(NebulaSQLParser::WHERE);
    setState(331);
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
    setState(333);
    match(NebulaSQLParser::HAVING);
    setState(334);
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
    setState(336);
    match(NebulaSQLParser::VALUES);
    setState(337);
    expression();
    setState(342);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 24, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(338);
        match(NebulaSQLParser::T__1);
        setState(339);
        expression(); 
      }
      setState(344);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 24, _ctx);
    }
    setState(345);
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
    setState(354);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 27, _ctx)) {
    case 1: {
      setState(348);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 25, _ctx)) {
      case 1: {
        setState(347);
        match(NebulaSQLParser::AS);
        break;
      }

      default:
        break;
      }
      setState(350);
      strictIdentifier();
      setState(352);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 26, _ctx)) {
      case 1: {
        setState(351);
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
    setState(356);
    multipartIdentifier();
    setState(361);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == NebulaSQLParser::T__1) {
      setState(357);
      match(NebulaSQLParser::T__1);
      setState(358);
      multipartIdentifier();
      setState(363);
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
    setState(364);
    dynamic_cast<MultipartIdentifierContext *>(_localctx)->errorCapturingIdentifierContext = errorCapturingIdentifier();
    dynamic_cast<MultipartIdentifierContext *>(_localctx)->parts.push_back(dynamic_cast<MultipartIdentifierContext *>(_localctx)->errorCapturingIdentifierContext);
    setState(369);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 29, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(365);
        match(NebulaSQLParser::T__4);
        setState(366);
        dynamic_cast<MultipartIdentifierContext *>(_localctx)->errorCapturingIdentifierContext = errorCapturingIdentifier();
        dynamic_cast<MultipartIdentifierContext *>(_localctx)->parts.push_back(dynamic_cast<MultipartIdentifierContext *>(_localctx)->errorCapturingIdentifierContext); 
      }
      setState(371);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 29, _ctx);
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
    setState(372);
    expression();
    setState(380);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 32, _ctx)) {
    case 1: {
      setState(374);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx)) {
      case 1: {
        setState(373);
        match(NebulaSQLParser::AS);
        break;
      }

      default:
        break;
      }
      setState(378);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx)) {
      case 1: {
        setState(376);
        dynamic_cast<NamedExpressionContext *>(_localctx)->name = errorCapturingIdentifier();
        break;
      }

      case 2: {
        setState(377);
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
    setState(385);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 33, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(382);
      strictIdentifier();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(383);

      if (!(!SQL_standard_keyword_behavior)) throw FailedPredicateException(this, "!SQL_standard_keyword_behavior");
      setState(384);
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
    setState(393);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 34, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<StrictIdentifierContext *>(_tracker.createInstance<NebulaSQLParser::UnquotedIdentifierContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(387);
      match(NebulaSQLParser::IDENTIFIER);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<StrictIdentifierContext *>(_tracker.createInstance<NebulaSQLParser::QuotedIdentifierAlternativeContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(388);
      quotedIdentifier();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<StrictIdentifierContext *>(_tracker.createInstance<NebulaSQLParser::UnquotedIdentifierContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(389);

      if (!(SQL_standard_keyword_behavior)) throw FailedPredicateException(this, "SQL_standard_keyword_behavior");
      setState(390);
      ansiNonReserved();
      break;
    }

    case 4: {
      _localctx = dynamic_cast<StrictIdentifierContext *>(_tracker.createInstance<NebulaSQLParser::UnquotedIdentifierContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(391);

      if (!(!SQL_standard_keyword_behavior)) throw FailedPredicateException(this, "!SQL_standard_keyword_behavior");
      setState(392);
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
    setState(395);
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
    setState(397);
    match(NebulaSQLParser::T__2);
    setState(398);
    identifierSeq();
    setState(399);
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
    setState(401);
    dynamic_cast<IdentifierSeqContext *>(_localctx)->errorCapturingIdentifierContext = errorCapturingIdentifier();
    dynamic_cast<IdentifierSeqContext *>(_localctx)->ident.push_back(dynamic_cast<IdentifierSeqContext *>(_localctx)->errorCapturingIdentifierContext);
    setState(406);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == NebulaSQLParser::T__1) {
      setState(402);
      match(NebulaSQLParser::T__1);
      setState(403);
      dynamic_cast<IdentifierSeqContext *>(_localctx)->errorCapturingIdentifierContext = errorCapturingIdentifier();
      dynamic_cast<IdentifierSeqContext *>(_localctx)->ident.push_back(dynamic_cast<IdentifierSeqContext *>(_localctx)->errorCapturingIdentifierContext);
      setState(408);
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
    setState(409);
    identifier();
    setState(410);
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
    setState(419);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 37, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ErrorCapturingIdentifierExtraContext *>(_tracker.createInstance<NebulaSQLParser::ErrorIdentContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(414); 
      _errHandler->sync(this);
      alt = 1;
      do {
        switch (alt) {
          case 1: {
                setState(412);
                match(NebulaSQLParser::MINUS);
                setState(413);
                identifier();
                break;
              }

        default:
          throw NoViableAltException(this);
        }
        setState(416); 
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 36, _ctx);
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
    setState(421);
    namedExpression();
    setState(426);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 38, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(422);
        match(NebulaSQLParser::T__1);
        setState(423);
        namedExpression(); 
      }
      setState(428);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 38, _ctx);
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
    setState(429);
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
    setState(443);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 40, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<LogicalNotContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(432);
      match(NebulaSQLParser::NOT);
      setState(433);
      booleanExpression(5);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<ExistsContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(434);
      match(NebulaSQLParser::EXISTS);
      setState(435);
      match(NebulaSQLParser::T__2);
      setState(436);
      query();
      setState(437);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<PredicatedContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(439);
      valueExpression(0);
      setState(441);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 39, _ctx)) {
      case 1: {
        setState(440);
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
    setState(453);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 42, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(451);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 41, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<LogicalBinaryContext>(_tracker.createInstance<BooleanExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleBooleanExpression);
          setState(445);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(446);
          dynamic_cast<LogicalBinaryContext *>(_localctx)->op = match(NebulaSQLParser::AND);
          setState(447);
          dynamic_cast<LogicalBinaryContext *>(_localctx)->right = booleanExpression(3);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<LogicalBinaryContext>(_tracker.createInstance<BooleanExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleBooleanExpression);
          setState(448);

          if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(449);
          dynamic_cast<LogicalBinaryContext *>(_localctx)->op = match(NebulaSQLParser::OR);
          setState(450);
          dynamic_cast<LogicalBinaryContext *>(_localctx)->right = booleanExpression(2);
          break;
        }

        default:
          break;
        } 
      }
      setState(455);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 42, _ctx);
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

NebulaSQLParser::WindowClauseContext* NebulaSQLParser::WindowedAggregationClauseContext::windowClause() {
  return getRuleContext<NebulaSQLParser::WindowClauseContext>(0);
}

NebulaSQLParser::AggregationClauseContext* NebulaSQLParser::WindowedAggregationClauseContext::aggregationClause() {
  return getRuleContext<NebulaSQLParser::AggregationClauseContext>(0);
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
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::GROUP) {
      setState(456);
      aggregationClause();
    }
    setState(459);
    windowClause();
    setState(461);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 44, _ctx)) {
    case 1: {
      setState(460);
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
    setState(507);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 49, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(463);
      match(NebulaSQLParser::GROUP);
      setState(464);
      match(NebulaSQLParser::BY);
      setState(465);
      dynamic_cast<AggregationClauseContext *>(_localctx)->expressionContext = expression();
      dynamic_cast<AggregationClauseContext *>(_localctx)->groupingExpressions.push_back(dynamic_cast<AggregationClauseContext *>(_localctx)->expressionContext);
      setState(470);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 45, _ctx);
      while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
        if (alt == 1) {
          setState(466);
          match(NebulaSQLParser::T__1);
          setState(467);
          dynamic_cast<AggregationClauseContext *>(_localctx)->expressionContext = expression();
          dynamic_cast<AggregationClauseContext *>(_localctx)->groupingExpressions.push_back(dynamic_cast<AggregationClauseContext *>(_localctx)->expressionContext); 
        }
        setState(472);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 45, _ctx);
      }
      setState(490);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 47, _ctx)) {
      case 1: {
        setState(473);
        match(NebulaSQLParser::WITH);
        setState(474);
        dynamic_cast<AggregationClauseContext *>(_localctx)->kind = match(NebulaSQLParser::ROLLUP);
        break;
      }

      case 2: {
        setState(475);
        match(NebulaSQLParser::WITH);
        setState(476);
        dynamic_cast<AggregationClauseContext *>(_localctx)->kind = match(NebulaSQLParser::CUBE);
        break;
      }

      case 3: {
        setState(477);
        dynamic_cast<AggregationClauseContext *>(_localctx)->kind = match(NebulaSQLParser::GROUPING);
        setState(478);
        match(NebulaSQLParser::SETS);
        setState(479);
        match(NebulaSQLParser::T__2);
        setState(480);
        groupingSet();
        setState(485);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NebulaSQLParser::T__1) {
          setState(481);
          match(NebulaSQLParser::T__1);
          setState(482);
          groupingSet();
          setState(487);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(488);
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
      setState(492);
      match(NebulaSQLParser::GROUP);
      setState(493);
      match(NebulaSQLParser::BY);
      setState(494);
      dynamic_cast<AggregationClauseContext *>(_localctx)->kind = match(NebulaSQLParser::GROUPING);
      setState(495);
      match(NebulaSQLParser::SETS);
      setState(496);
      match(NebulaSQLParser::T__2);
      setState(497);
      groupingSet();
      setState(502);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NebulaSQLParser::T__1) {
        setState(498);
        match(NebulaSQLParser::T__1);
        setState(499);
        groupingSet();
        setState(504);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(505);
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
    setState(522);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 52, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(509);
      match(NebulaSQLParser::T__2);
      setState(518);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 51, _ctx)) {
      case 1: {
        setState(510);
        expression();
        setState(515);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NebulaSQLParser::T__1) {
          setState(511);
          match(NebulaSQLParser::T__1);
          setState(512);
          expression();
          setState(517);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

      default:
        break;
      }
      setState(520);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(521);
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
    setState(524);
    match(NebulaSQLParser::WINDOW);
    setState(525);
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
    setState(527);
    match(NebulaSQLParser::WATERMARK);
    setState(528);
    match(NebulaSQLParser::T__2);
    setState(529);
    watermarkParameters();
    setState(530);
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
    setState(532);
    dynamic_cast<WatermarkParametersContext *>(_localctx)->watermarkIdentifier = identifier();
    setState(533);
    match(NebulaSQLParser::T__1);
    setState(534);
    dynamic_cast<WatermarkParametersContext *>(_localctx)->watermark = match(NebulaSQLParser::INTEGER_VALUE);
    setState(535);
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
//----------------- ThresholdBasedWindowContext ------------------------------------------------------------------

NebulaSQLParser::ConditionWindowContext* NebulaSQLParser::ThresholdBasedWindowContext::conditionWindow() {
  return getRuleContext<NebulaSQLParser::ConditionWindowContext>(0);
}

NebulaSQLParser::ThresholdBasedWindowContext::ThresholdBasedWindowContext(WindowSpecContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::ThresholdBasedWindowContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterThresholdBasedWindow(this);
}
void NebulaSQLParser::ThresholdBasedWindowContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitThresholdBasedWindow(this);
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
    setState(540);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 53, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<WindowSpecContext *>(_tracker.createInstance<NebulaSQLParser::TimeBasedWindowContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(537);
      timeWindow();
      break;
    }

    case 2: {
      _localctx = dynamic_cast<WindowSpecContext *>(_tracker.createInstance<NebulaSQLParser::CountBasedWindowContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(538);
      countWindow();
      break;
    }

    case 3: {
      _localctx = dynamic_cast<WindowSpecContext *>(_tracker.createInstance<NebulaSQLParser::ThresholdBasedWindowContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(539);
      conditionWindow();
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
    setState(564);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NebulaSQLParser::TUMBLING: {
        _localctx = dynamic_cast<TimeWindowContext *>(_tracker.createInstance<NebulaSQLParser::TumblingWindowContext>(_localctx));
        enterOuterAlt(_localctx, 1);
        setState(542);
        match(NebulaSQLParser::TUMBLING);
        setState(543);
        match(NebulaSQLParser::T__2);
        setState(547);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NebulaSQLParser::IDENTIFIER) {
          setState(544);
          timestampParameter();
          setState(545);
          match(NebulaSQLParser::T__1);
        }
        setState(549);
        sizeParameter();
        setState(550);
        match(NebulaSQLParser::T__3);
        break;
      }

      case NebulaSQLParser::SLIDING: {
        _localctx = dynamic_cast<TimeWindowContext *>(_tracker.createInstance<NebulaSQLParser::SlidingWindowContext>(_localctx));
        enterOuterAlt(_localctx, 2);
        setState(552);
        match(NebulaSQLParser::SLIDING);
        setState(553);
        match(NebulaSQLParser::T__2);
        setState(557);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NebulaSQLParser::IDENTIFIER) {
          setState(554);
          timestampParameter();
          setState(555);
          match(NebulaSQLParser::T__1);
        }
        setState(559);
        sizeParameter();
        setState(560);
        match(NebulaSQLParser::T__1);
        setState(561);
        advancebyParameter();
        setState(562);
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
    setState(566);
    match(NebulaSQLParser::TUMBLING);
    setState(567);
    match(NebulaSQLParser::T__2);
    setState(568);
    match(NebulaSQLParser::INTEGER_VALUE);
    setState(569);
    match(NebulaSQLParser::T__3);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConditionWindowContext ------------------------------------------------------------------

NebulaSQLParser::ConditionWindowContext::ConditionWindowContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NebulaSQLParser::ConditionWindowContext::getRuleIndex() const {
  return NebulaSQLParser::RuleConditionWindow;
}

void NebulaSQLParser::ConditionWindowContext::copyFrom(ConditionWindowContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ThresholdWindowContext ------------------------------------------------------------------

tree::TerminalNode* NebulaSQLParser::ThresholdWindowContext::THRESHOLD() {
  return getToken(NebulaSQLParser::THRESHOLD, 0);
}

NebulaSQLParser::ConditionParameterContext* NebulaSQLParser::ThresholdWindowContext::conditionParameter() {
  return getRuleContext<NebulaSQLParser::ConditionParameterContext>(0);
}

NebulaSQLParser::ThresholdMinSizeParameterContext* NebulaSQLParser::ThresholdWindowContext::thresholdMinSizeParameter() {
  return getRuleContext<NebulaSQLParser::ThresholdMinSizeParameterContext>(0);
}

NebulaSQLParser::ThresholdWindowContext::ThresholdWindowContext(ConditionWindowContext *ctx) { copyFrom(ctx); }

void NebulaSQLParser::ThresholdWindowContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterThresholdWindow(this);
}
void NebulaSQLParser::ThresholdWindowContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitThresholdWindow(this);
}
NebulaSQLParser::ConditionWindowContext* NebulaSQLParser::conditionWindow() {
  ConditionWindowContext *_localctx = _tracker.createInstance<ConditionWindowContext>(_ctx, getState());
  enterRule(_localctx, 86, NebulaSQLParser::RuleConditionWindow);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    _localctx = dynamic_cast<ConditionWindowContext *>(_tracker.createInstance<NebulaSQLParser::ThresholdWindowContext>(_localctx));
    enterOuterAlt(_localctx, 1);
    setState(571);
    match(NebulaSQLParser::THRESHOLD);
    setState(572);
    match(NebulaSQLParser::T__2);
    setState(573);
    conditionParameter();
    setState(576);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::T__1) {
      setState(574);
      match(NebulaSQLParser::T__1);
      setState(575);
      thresholdMinSizeParameter();
    }
    setState(578);
    match(NebulaSQLParser::T__3);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConditionParameterContext ------------------------------------------------------------------

NebulaSQLParser::ConditionParameterContext::ConditionParameterContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::ExpressionContext* NebulaSQLParser::ConditionParameterContext::expression() {
  return getRuleContext<NebulaSQLParser::ExpressionContext>(0);
}


size_t NebulaSQLParser::ConditionParameterContext::getRuleIndex() const {
  return NebulaSQLParser::RuleConditionParameter;
}

void NebulaSQLParser::ConditionParameterContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConditionParameter(this);
}

void NebulaSQLParser::ConditionParameterContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConditionParameter(this);
}

NebulaSQLParser::ConditionParameterContext* NebulaSQLParser::conditionParameter() {
  ConditionParameterContext *_localctx = _tracker.createInstance<ConditionParameterContext>(_ctx, getState());
  enterRule(_localctx, 88, NebulaSQLParser::RuleConditionParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(580);
    expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ThresholdMinSizeParameterContext ------------------------------------------------------------------

NebulaSQLParser::ThresholdMinSizeParameterContext::ThresholdMinSizeParameterContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::ThresholdMinSizeParameterContext::INTEGER_VALUE() {
  return getToken(NebulaSQLParser::INTEGER_VALUE, 0);
}


size_t NebulaSQLParser::ThresholdMinSizeParameterContext::getRuleIndex() const {
  return NebulaSQLParser::RuleThresholdMinSizeParameter;
}

void NebulaSQLParser::ThresholdMinSizeParameterContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterThresholdMinSizeParameter(this);
}

void NebulaSQLParser::ThresholdMinSizeParameterContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitThresholdMinSizeParameter(this);
}

NebulaSQLParser::ThresholdMinSizeParameterContext* NebulaSQLParser::thresholdMinSizeParameter() {
  ThresholdMinSizeParameterContext *_localctx = _tracker.createInstance<ThresholdMinSizeParameterContext>(_ctx, getState());
  enterRule(_localctx, 90, NebulaSQLParser::RuleThresholdMinSizeParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(582);
    match(NebulaSQLParser::INTEGER_VALUE);
   
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
  enterRule(_localctx, 92, NebulaSQLParser::RuleSizeParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(584);
    match(NebulaSQLParser::SIZE);
    setState(585);
    match(NebulaSQLParser::INTEGER_VALUE);
    setState(586);
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
  enterRule(_localctx, 94, NebulaSQLParser::RuleAdvancebyParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(588);
    match(NebulaSQLParser::ADVANCE);
    setState(589);
    match(NebulaSQLParser::BY);
    setState(590);
    match(NebulaSQLParser::INTEGER_VALUE);
    setState(591);
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
  enterRule(_localctx, 96, NebulaSQLParser::RuleTimeUnit);
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
    setState(593);
    _la = _input->LA(1);
    if (!(((((_la - 84) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 84)) & ((1ULL << (NebulaSQLParser::MS - 84))
      | (1ULL << (NebulaSQLParser::SEC - 84))
      | (1ULL << (NebulaSQLParser::MIN - 84))
      | (1ULL << (NebulaSQLParser::HOUR - 84))
      | (1ULL << (NebulaSQLParser::DAY - 84)))) != 0))) {
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
  enterRule(_localctx, 98, NebulaSQLParser::RuleTimestampParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(595);
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

tree::TerminalNode* NebulaSQLParser::FunctionNameContext::MEDIAN() {
  return getToken(NebulaSQLParser::MEDIAN, 0);
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
  enterRule(_localctx, 100, NebulaSQLParser::RuleFunctionName);
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
    setState(597);
    _la = _input->LA(1);
    if (!(((((_la - 86) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 86)) & ((1ULL << (NebulaSQLParser::MIN - 86))
      | (1ULL << (NebulaSQLParser::MAX - 86))
      | (1ULL << (NebulaSQLParser::AVG - 86))
      | (1ULL << (NebulaSQLParser::SUM - 86))
      | (1ULL << (NebulaSQLParser::COUNT - 86))
      | (1ULL << (NebulaSQLParser::MEDIAN - 86)))) != 0))) {
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
  enterRule(_localctx, 102, NebulaSQLParser::RuleSinkClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(599);
    match(NebulaSQLParser::INTO);
    setState(600);
    sinkType();
    setState(602);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 58, _ctx)) {
    case 1: {
      setState(601);
      match(NebulaSQLParser::AS);
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

//----------------- SinkTypeContext ------------------------------------------------------------------

NebulaSQLParser::SinkTypeContext::SinkTypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NebulaSQLParser::SinkTypeZMQContext* NebulaSQLParser::SinkTypeContext::sinkTypeZMQ() {
  return getRuleContext<NebulaSQLParser::SinkTypeZMQContext>(0);
}

NebulaSQLParser::SinkTypeKafkaContext* NebulaSQLParser::SinkTypeContext::sinkTypeKafka() {
  return getRuleContext<NebulaSQLParser::SinkTypeKafkaContext>(0);
}

NebulaSQLParser::SinkTypeFileContext* NebulaSQLParser::SinkTypeContext::sinkTypeFile() {
  return getRuleContext<NebulaSQLParser::SinkTypeFileContext>(0);
}

NebulaSQLParser::SinkTypeMQTTContext* NebulaSQLParser::SinkTypeContext::sinkTypeMQTT() {
  return getRuleContext<NebulaSQLParser::SinkTypeMQTTContext>(0);
}

NebulaSQLParser::SinkTypeOPCContext* NebulaSQLParser::SinkTypeContext::sinkTypeOPC() {
  return getRuleContext<NebulaSQLParser::SinkTypeOPCContext>(0);
}

NebulaSQLParser::SinkTypePrintContext* NebulaSQLParser::SinkTypeContext::sinkTypePrint() {
  return getRuleContext<NebulaSQLParser::SinkTypePrintContext>(0);
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
  enterRule(_localctx, 104, NebulaSQLParser::RuleSinkType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(610);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NebulaSQLParser::ZMQ: {
        enterOuterAlt(_localctx, 1);
        setState(604);
        sinkTypeZMQ();
        break;
      }

      case NebulaSQLParser::KAFKA: {
        enterOuterAlt(_localctx, 2);
        setState(605);
        sinkTypeKafka();
        break;
      }

      case NebulaSQLParser::FILE: {
        enterOuterAlt(_localctx, 3);
        setState(606);
        sinkTypeFile();
        break;
      }

      case NebulaSQLParser::MQTT: {
        enterOuterAlt(_localctx, 4);
        setState(607);
        sinkTypeMQTT();
        break;
      }

      case NebulaSQLParser::OPC: {
        enterOuterAlt(_localctx, 5);
        setState(608);
        sinkTypeOPC();
        break;
      }

      case NebulaSQLParser::PRINT: {
        enterOuterAlt(_localctx, 6);
        setState(609);
        sinkTypePrint();
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
  enterRule(_localctx, 106, NebulaSQLParser::RuleSinkTypeZMQ);

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
    zmqKeyword();
    setState(613);
    match(NebulaSQLParser::T__2);
    setState(614);
    dynamic_cast<SinkTypeZMQContext *>(_localctx)->zmqStreamName = streamName();
    setState(615);
    match(NebulaSQLParser::T__1);
    setState(616);
    dynamic_cast<SinkTypeZMQContext *>(_localctx)->zmqHostLabel = host();
    setState(617);
    match(NebulaSQLParser::T__1);
    setState(618);
    dynamic_cast<SinkTypeZMQContext *>(_localctx)->zmqPort = port();
    setState(619);
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
  enterRule(_localctx, 108, NebulaSQLParser::RuleNullNotnull);
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
    setState(622);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::NOT) {
      setState(621);
      match(NebulaSQLParser::NOT);
    }
    setState(624);
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
  enterRule(_localctx, 110, NebulaSQLParser::RuleZmqKeyword);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(626);
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
  enterRule(_localctx, 112, NebulaSQLParser::RuleStreamName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(628);
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
  enterRule(_localctx, 114, NebulaSQLParser::RuleHost);
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
    setState(630);
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
  enterRule(_localctx, 116, NebulaSQLParser::RulePort);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(632);
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
  enterRule(_localctx, 118, NebulaSQLParser::RuleSinkTypeKafka);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(634);
    kafkaKeyword();
    setState(635);
    match(NebulaSQLParser::T__2);
    setState(636);
    dynamic_cast<SinkTypeKafkaContext *>(_localctx)->broker = kafkaBroker();
    setState(637);
    match(NebulaSQLParser::T__1);
    setState(638);
    dynamic_cast<SinkTypeKafkaContext *>(_localctx)->topic = kafkaTopic();
    setState(639);
    match(NebulaSQLParser::T__1);
    setState(640);
    dynamic_cast<SinkTypeKafkaContext *>(_localctx)->timeout = kafkaProducerTimout();
    setState(641);
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
  enterRule(_localctx, 120, NebulaSQLParser::RuleKafkaKeyword);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(643);
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
  enterRule(_localctx, 122, NebulaSQLParser::RuleKafkaBroker);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(645);
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
  enterRule(_localctx, 124, NebulaSQLParser::RuleKafkaTopic);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(647);
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
  enterRule(_localctx, 126, NebulaSQLParser::RuleKafkaProducerTimout);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(649);
    match(NebulaSQLParser::INTEGER_VALUE);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SinkTypeFileContext ------------------------------------------------------------------

NebulaSQLParser::SinkTypeFileContext::SinkTypeFileContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::SinkTypeFileContext::FILE() {
  return getToken(NebulaSQLParser::FILE, 0);
}

std::vector<tree::TerminalNode *> NebulaSQLParser::SinkTypeFileContext::STRING() {
  return getTokens(NebulaSQLParser::STRING);
}

tree::TerminalNode* NebulaSQLParser::SinkTypeFileContext::STRING(size_t i) {
  return getToken(NebulaSQLParser::STRING, i);
}

NebulaSQLParser::FileFormatContext* NebulaSQLParser::SinkTypeFileContext::fileFormat() {
  return getRuleContext<NebulaSQLParser::FileFormatContext>(0);
}


size_t NebulaSQLParser::SinkTypeFileContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSinkTypeFile;
}

void NebulaSQLParser::SinkTypeFileContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSinkTypeFile(this);
}

void NebulaSQLParser::SinkTypeFileContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSinkTypeFile(this);
}

NebulaSQLParser::SinkTypeFileContext* NebulaSQLParser::sinkTypeFile() {
  SinkTypeFileContext *_localctx = _tracker.createInstance<SinkTypeFileContext>(_ctx, getState());
  enterRule(_localctx, 128, NebulaSQLParser::RuleSinkTypeFile);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(651);
    match(NebulaSQLParser::FILE);
    setState(652);
    match(NebulaSQLParser::T__2);
    setState(653);
    dynamic_cast<SinkTypeFileContext *>(_localctx)->path = match(NebulaSQLParser::STRING);
    setState(654);
    match(NebulaSQLParser::T__1);
    setState(655);
    dynamic_cast<SinkTypeFileContext *>(_localctx)->format = fileFormat();
    setState(656);
    match(NebulaSQLParser::T__1);
    setState(657);
    dynamic_cast<SinkTypeFileContext *>(_localctx)->append = match(NebulaSQLParser::STRING);
    setState(658);
    match(NebulaSQLParser::T__3);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FileFormatContext ------------------------------------------------------------------

NebulaSQLParser::FileFormatContext::FileFormatContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::FileFormatContext::CSV_FORMAT() {
  return getToken(NebulaSQLParser::CSV_FORMAT, 0);
}

tree::TerminalNode* NebulaSQLParser::FileFormatContext::NES_FORMAT() {
  return getToken(NebulaSQLParser::NES_FORMAT, 0);
}

tree::TerminalNode* NebulaSQLParser::FileFormatContext::TEXT_FORMAT() {
  return getToken(NebulaSQLParser::TEXT_FORMAT, 0);
}


size_t NebulaSQLParser::FileFormatContext::getRuleIndex() const {
  return NebulaSQLParser::RuleFileFormat;
}

void NebulaSQLParser::FileFormatContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFileFormat(this);
}

void NebulaSQLParser::FileFormatContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFileFormat(this);
}

NebulaSQLParser::FileFormatContext* NebulaSQLParser::fileFormat() {
  FileFormatContext *_localctx = _tracker.createInstance<FileFormatContext>(_ctx, getState());
  enterRule(_localctx, 130, NebulaSQLParser::RuleFileFormat);
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
    setState(660);
    _la = _input->LA(1);
    if (!(((((_la - 103) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 103)) & ((1ULL << (NebulaSQLParser::CSV_FORMAT - 103))
      | (1ULL << (NebulaSQLParser::NES_FORMAT - 103))
      | (1ULL << (NebulaSQLParser::TEXT_FORMAT - 103)))) != 0))) {
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

//----------------- SinkTypeMQTTContext ------------------------------------------------------------------

NebulaSQLParser::SinkTypeMQTTContext::SinkTypeMQTTContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::SinkTypeMQTTContext::MQTT() {
  return getToken(NebulaSQLParser::MQTT, 0);
}

std::vector<tree::TerminalNode *> NebulaSQLParser::SinkTypeMQTTContext::STRING() {
  return getTokens(NebulaSQLParser::STRING);
}

tree::TerminalNode* NebulaSQLParser::SinkTypeMQTTContext::STRING(size_t i) {
  return getToken(NebulaSQLParser::STRING, i);
}

std::vector<tree::TerminalNode *> NebulaSQLParser::SinkTypeMQTTContext::INTEGER_VALUE() {
  return getTokens(NebulaSQLParser::INTEGER_VALUE);
}

tree::TerminalNode* NebulaSQLParser::SinkTypeMQTTContext::INTEGER_VALUE(size_t i) {
  return getToken(NebulaSQLParser::INTEGER_VALUE, i);
}

NebulaSQLParser::TimeUnitContext* NebulaSQLParser::SinkTypeMQTTContext::timeUnit() {
  return getRuleContext<NebulaSQLParser::TimeUnitContext>(0);
}

NebulaSQLParser::QosContext* NebulaSQLParser::SinkTypeMQTTContext::qos() {
  return getRuleContext<NebulaSQLParser::QosContext>(0);
}

tree::TerminalNode* NebulaSQLParser::SinkTypeMQTTContext::BOOLEAN_VALUE() {
  return getToken(NebulaSQLParser::BOOLEAN_VALUE, 0);
}


size_t NebulaSQLParser::SinkTypeMQTTContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSinkTypeMQTT;
}

void NebulaSQLParser::SinkTypeMQTTContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSinkTypeMQTT(this);
}

void NebulaSQLParser::SinkTypeMQTTContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSinkTypeMQTT(this);
}

NebulaSQLParser::SinkTypeMQTTContext* NebulaSQLParser::sinkTypeMQTT() {
  SinkTypeMQTTContext *_localctx = _tracker.createInstance<SinkTypeMQTTContext>(_ctx, getState());
  enterRule(_localctx, 132, NebulaSQLParser::RuleSinkTypeMQTT);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(662);
    match(NebulaSQLParser::MQTT);
    setState(663);
    match(NebulaSQLParser::T__2);
    setState(664);
    dynamic_cast<SinkTypeMQTTContext *>(_localctx)->mqttHostLabel = match(NebulaSQLParser::STRING);
    setState(665);
    match(NebulaSQLParser::T__1);
    setState(666);
    dynamic_cast<SinkTypeMQTTContext *>(_localctx)->topic = match(NebulaSQLParser::STRING);
    setState(667);
    match(NebulaSQLParser::T__1);
    setState(668);
    dynamic_cast<SinkTypeMQTTContext *>(_localctx)->user = match(NebulaSQLParser::STRING);
    setState(669);
    match(NebulaSQLParser::T__1);
    setState(670);
    dynamic_cast<SinkTypeMQTTContext *>(_localctx)->maxBufferedMSGs = match(NebulaSQLParser::INTEGER_VALUE);
    setState(671);
    match(NebulaSQLParser::T__1);
    setState(672);
    dynamic_cast<SinkTypeMQTTContext *>(_localctx)->mqttTimeUnitLabel = timeUnit();
    setState(673);
    match(NebulaSQLParser::T__1);
    setState(674);
    dynamic_cast<SinkTypeMQTTContext *>(_localctx)->messageDelay = match(NebulaSQLParser::INTEGER_VALUE);
    setState(675);
    match(NebulaSQLParser::T__1);
    setState(676);
    dynamic_cast<SinkTypeMQTTContext *>(_localctx)->qualityOfService = qos();
    setState(677);
    match(NebulaSQLParser::T__1);
    setState(678);
    dynamic_cast<SinkTypeMQTTContext *>(_localctx)->asynchronousClient = match(NebulaSQLParser::BOOLEAN_VALUE);
    setState(679);
    match(NebulaSQLParser::T__3);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QosContext ------------------------------------------------------------------

NebulaSQLParser::QosContext::QosContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::QosContext::AT_MOST_ONCE() {
  return getToken(NebulaSQLParser::AT_MOST_ONCE, 0);
}

tree::TerminalNode* NebulaSQLParser::QosContext::AT_LEAST_ONCE() {
  return getToken(NebulaSQLParser::AT_LEAST_ONCE, 0);
}


size_t NebulaSQLParser::QosContext::getRuleIndex() const {
  return NebulaSQLParser::RuleQos;
}

void NebulaSQLParser::QosContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterQos(this);
}

void NebulaSQLParser::QosContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitQos(this);
}

NebulaSQLParser::QosContext* NebulaSQLParser::qos() {
  QosContext *_localctx = _tracker.createInstance<QosContext>(_ctx, getState());
  enterRule(_localctx, 134, NebulaSQLParser::RuleQos);
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
    setState(681);
    _la = _input->LA(1);
    if (!(_la == NebulaSQLParser::AT_MOST_ONCE

    || _la == NebulaSQLParser::AT_LEAST_ONCE)) {
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

//----------------- SinkTypeOPCContext ------------------------------------------------------------------

NebulaSQLParser::SinkTypeOPCContext::SinkTypeOPCContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::SinkTypeOPCContext::OPC() {
  return getToken(NebulaSQLParser::OPC, 0);
}

std::vector<tree::TerminalNode *> NebulaSQLParser::SinkTypeOPCContext::STRING() {
  return getTokens(NebulaSQLParser::STRING);
}

tree::TerminalNode* NebulaSQLParser::SinkTypeOPCContext::STRING(size_t i) {
  return getToken(NebulaSQLParser::STRING, i);
}


size_t NebulaSQLParser::SinkTypeOPCContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSinkTypeOPC;
}

void NebulaSQLParser::SinkTypeOPCContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSinkTypeOPC(this);
}

void NebulaSQLParser::SinkTypeOPCContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSinkTypeOPC(this);
}

NebulaSQLParser::SinkTypeOPCContext* NebulaSQLParser::sinkTypeOPC() {
  SinkTypeOPCContext *_localctx = _tracker.createInstance<SinkTypeOPCContext>(_ctx, getState());
  enterRule(_localctx, 136, NebulaSQLParser::RuleSinkTypeOPC);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(683);
    match(NebulaSQLParser::OPC);
    setState(684);
    match(NebulaSQLParser::T__2);
    setState(685);
    dynamic_cast<SinkTypeOPCContext *>(_localctx)->url = match(NebulaSQLParser::STRING);
    setState(686);
    match(NebulaSQLParser::T__1);
    setState(687);
    dynamic_cast<SinkTypeOPCContext *>(_localctx)->nodeId = match(NebulaSQLParser::STRING);
    setState(688);
    match(NebulaSQLParser::T__1);
    setState(689);
    dynamic_cast<SinkTypeOPCContext *>(_localctx)->user = match(NebulaSQLParser::STRING);
    setState(690);
    match(NebulaSQLParser::T__1);
    setState(691);
    dynamic_cast<SinkTypeOPCContext *>(_localctx)->password = match(NebulaSQLParser::STRING);
    setState(692);
    match(NebulaSQLParser::T__3);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SinkTypePrintContext ------------------------------------------------------------------

NebulaSQLParser::SinkTypePrintContext::SinkTypePrintContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NebulaSQLParser::SinkTypePrintContext::PRINT() {
  return getToken(NebulaSQLParser::PRINT, 0);
}


size_t NebulaSQLParser::SinkTypePrintContext::getRuleIndex() const {
  return NebulaSQLParser::RuleSinkTypePrint;
}

void NebulaSQLParser::SinkTypePrintContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSinkTypePrint(this);
}

void NebulaSQLParser::SinkTypePrintContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NebulaSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSinkTypePrint(this);
}

NebulaSQLParser::SinkTypePrintContext* NebulaSQLParser::sinkTypePrint() {
  SinkTypePrintContext *_localctx = _tracker.createInstance<SinkTypePrintContext>(_ctx, getState());
  enterRule(_localctx, 138, NebulaSQLParser::RuleSinkTypePrint);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(694);
    match(NebulaSQLParser::PRINT);
   
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
  enterRule(_localctx, 140, NebulaSQLParser::RuleSortItem);
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
    setState(696);
    expression();
    setState(698);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::ASC

    || _la == NebulaSQLParser::DESC) {
      setState(697);
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
    setState(702);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NebulaSQLParser::NULLS) {
      setState(700);
      match(NebulaSQLParser::NULLS);
      setState(701);
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
  enterRule(_localctx, 142, NebulaSQLParser::RulePredicate);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(783);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 75, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(705);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(704);
        match(NebulaSQLParser::NOT);
      }
      setState(707);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::BETWEEN);
      setState(708);
      dynamic_cast<PredicateContext *>(_localctx)->lower = valueExpression(0);
      setState(709);
      match(NebulaSQLParser::AND);
      setState(710);
      dynamic_cast<PredicateContext *>(_localctx)->upper = valueExpression(0);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(713);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(712);
        match(NebulaSQLParser::NOT);
      }
      setState(715);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::IN);
      setState(716);
      match(NebulaSQLParser::T__2);
      setState(717);
      expression();
      setState(722);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NebulaSQLParser::T__1) {
        setState(718);
        match(NebulaSQLParser::T__1);
        setState(719);
        expression();
        setState(724);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(725);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(728);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(727);
        match(NebulaSQLParser::NOT);
      }
      setState(730);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::IN);
      setState(731);
      match(NebulaSQLParser::T__2);
      setState(732);
      query();
      setState(733);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(736);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(735);
        match(NebulaSQLParser::NOT);
      }
      setState(738);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::RLIKE);
      setState(739);
      dynamic_cast<PredicateContext *>(_localctx)->pattern = valueExpression(0);
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(741);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(740);
        match(NebulaSQLParser::NOT);
      }
      setState(743);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::LIKE);
      setState(744);
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
      setState(758);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 70, _ctx)) {
      case 1: {
        setState(745);
        match(NebulaSQLParser::T__2);
        setState(746);
        match(NebulaSQLParser::T__3);
        break;
      }

      case 2: {
        setState(747);
        match(NebulaSQLParser::T__2);
        setState(748);
        expression();
        setState(753);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NebulaSQLParser::T__1) {
          setState(749);
          match(NebulaSQLParser::T__1);
          setState(750);
          expression();
          setState(755);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(756);
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
      setState(761);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(760);
        match(NebulaSQLParser::NOT);
      }
      setState(763);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::LIKE);
      setState(764);
      dynamic_cast<PredicateContext *>(_localctx)->pattern = valueExpression(0);
      setState(767);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 72, _ctx)) {
      case 1: {
        setState(765);
        match(NebulaSQLParser::ESCAPE);
        setState(766);
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
      setState(769);
      match(NebulaSQLParser::IS);
      setState(770);
      nullNotnull();
      break;
    }

    case 8: {
      enterOuterAlt(_localctx, 8);
      setState(771);
      match(NebulaSQLParser::IS);
      setState(773);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(772);
        match(NebulaSQLParser::NOT);
      }
      setState(775);
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
      setState(776);
      match(NebulaSQLParser::IS);
      setState(778);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::NOT) {
        setState(777);
        match(NebulaSQLParser::NOT);
      }
      setState(780);
      dynamic_cast<PredicateContext *>(_localctx)->kind = match(NebulaSQLParser::DISTINCT);
      setState(781);
      match(NebulaSQLParser::FROM);
      setState(782);
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
  size_t startState = 144;
  enterRecursionRule(_localctx, 144, NebulaSQLParser::RuleValueExpression, precedence);

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
    setState(789);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 76, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<ValueExpressionDefaultContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(786);
      primaryExpression(0);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<ArithmeticUnaryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(787);
      dynamic_cast<ArithmeticUnaryContext *>(_localctx)->op = _input->LT(1);
      _la = _input->LA(1);
      if (!(((((_la - 117) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 117)) & ((1ULL << (NebulaSQLParser::PLUS - 117))
        | (1ULL << (NebulaSQLParser::MINUS - 117))
        | (1ULL << (NebulaSQLParser::TILDE - 117)))) != 0))) {
        dynamic_cast<ArithmeticUnaryContext *>(_localctx)->op = _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(788);
      valueExpression(7);
      break;
    }

    default:
      break;
    }
    _ctx->stop = _input->LT(-1);
    setState(812);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 78, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(810);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 77, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(_tracker.createInstance<ValueExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleValueExpression);
          setState(791);

          if (!(precpred(_ctx, 6))) throw FailedPredicateException(this, "precpred(_ctx, 6)");
          setState(792);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = _input->LT(1);
          _la = _input->LA(1);
          if (!(_la == NebulaSQLParser::DIV || ((((_la - 119) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 119)) & ((1ULL << (NebulaSQLParser::ASTERISK - 119))
            | (1ULL << (NebulaSQLParser::SLASH - 119))
            | (1ULL << (NebulaSQLParser::PERCENT - 119)))) != 0))) {
            dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(793);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->right = valueExpression(7);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(_tracker.createInstance<ValueExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleValueExpression);
          setState(794);

          if (!(precpred(_ctx, 5))) throw FailedPredicateException(this, "precpred(_ctx, 5)");
          setState(795);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = _input->LT(1);
          _la = _input->LA(1);
          if (!(((((_la - 117) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 117)) & ((1ULL << (NebulaSQLParser::PLUS - 117))
            | (1ULL << (NebulaSQLParser::MINUS - 117))
            | (1ULL << (NebulaSQLParser::CONCAT_PIPE - 117)))) != 0))) {
            dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(796);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->right = valueExpression(6);
          break;
        }

        case 3: {
          auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(_tracker.createInstance<ValueExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleValueExpression);
          setState(797);

          if (!(precpred(_ctx, 4))) throw FailedPredicateException(this, "precpred(_ctx, 4)");
          setState(798);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = match(NebulaSQLParser::AMPERSAND);
          setState(799);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->right = valueExpression(5);
          break;
        }

        case 4: {
          auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(_tracker.createInstance<ValueExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleValueExpression);
          setState(800);

          if (!(precpred(_ctx, 3))) throw FailedPredicateException(this, "precpred(_ctx, 3)");
          setState(801);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = match(NebulaSQLParser::HAT);
          setState(802);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->right = valueExpression(4);
          break;
        }

        case 5: {
          auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(_tracker.createInstance<ValueExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleValueExpression);
          setState(803);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(804);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->op = match(NebulaSQLParser::PIPE);
          setState(805);
          dynamic_cast<ArithmeticBinaryContext *>(_localctx)->right = valueExpression(3);
          break;
        }

        case 6: {
          auto newContext = _tracker.createInstance<ComparisonContext>(_tracker.createInstance<ValueExpressionContext>(parentContext, parentState));
          _localctx = newContext;
          newContext->left = previousContext;
          pushNewRecursionContext(newContext, startState, RuleValueExpression);
          setState(806);

          if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(807);
          comparisonOperator();
          setState(808);
          dynamic_cast<ComparisonContext *>(_localctx)->right = valueExpression(2);
          break;
        }

        default:
          break;
        } 
      }
      setState(814);
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
  enterRule(_localctx, 146, NebulaSQLParser::RuleComparisonOperator);
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
    setState(815);
    _la = _input->LA(1);
    if (!(((((_la - 109) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 109)) & ((1ULL << (NebulaSQLParser::EQ - 109))
      | (1ULL << (NebulaSQLParser::NSEQ - 109))
      | (1ULL << (NebulaSQLParser::NEQ - 109))
      | (1ULL << (NebulaSQLParser::NEQJ - 109))
      | (1ULL << (NebulaSQLParser::LT - 109))
      | (1ULL << (NebulaSQLParser::LTE - 109))
      | (1ULL << (NebulaSQLParser::GT - 109))
      | (1ULL << (NebulaSQLParser::GTE - 109)))) != 0))) {
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
  enterRule(_localctx, 148, NebulaSQLParser::RuleHint);

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
    setState(817);
    match(NebulaSQLParser::T__5);
    setState(818);
    dynamic_cast<HintContext *>(_localctx)->hintStatementContext = hintStatement();
    dynamic_cast<HintContext *>(_localctx)->hintStatements.push_back(dynamic_cast<HintContext *>(_localctx)->hintStatementContext);
    setState(825);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 80, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(820);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 79, _ctx)) {
        case 1: {
          setState(819);
          match(NebulaSQLParser::T__1);
          break;
        }

        default:
          break;
        }
        setState(822);
        dynamic_cast<HintContext *>(_localctx)->hintStatementContext = hintStatement();
        dynamic_cast<HintContext *>(_localctx)->hintStatements.push_back(dynamic_cast<HintContext *>(_localctx)->hintStatementContext); 
      }
      setState(827);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 80, _ctx);
    }
    setState(828);
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
  enterRule(_localctx, 150, NebulaSQLParser::RuleHintStatement);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(843);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 82, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(830);
      dynamic_cast<HintStatementContext *>(_localctx)->hintName = identifier();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(831);
      dynamic_cast<HintStatementContext *>(_localctx)->hintName = identifier();
      setState(832);
      match(NebulaSQLParser::T__2);
      setState(833);
      dynamic_cast<HintStatementContext *>(_localctx)->primaryExpressionContext = primaryExpression(0);
      dynamic_cast<HintStatementContext *>(_localctx)->parameters.push_back(dynamic_cast<HintStatementContext *>(_localctx)->primaryExpressionContext);
      setState(838);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NebulaSQLParser::T__1) {
        setState(834);
        match(NebulaSQLParser::T__1);
        setState(835);
        dynamic_cast<HintStatementContext *>(_localctx)->primaryExpressionContext = primaryExpression(0);
        dynamic_cast<HintStatementContext *>(_localctx)->parameters.push_back(dynamic_cast<HintStatementContext *>(_localctx)->primaryExpressionContext);
        setState(840);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(841);
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
  size_t startState = 152;
  enterRecursionRule(_localctx, 152, NebulaSQLParser::RulePrimaryExpression, precedence);

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
    setState(885);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 86, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<StarContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(846);
      match(NebulaSQLParser::ASTERISK);
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<StarContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(847);
      qualifiedName();
      setState(848);
      match(NebulaSQLParser::T__4);
      setState(849);
      match(NebulaSQLParser::ASTERISK);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<SubqueryExpressionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(851);
      match(NebulaSQLParser::T__2);
      setState(852);
      query();
      setState(853);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 4: {
      _localctx = _tracker.createInstance<RowConstructorContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(855);
      match(NebulaSQLParser::T__2);
      setState(856);
      namedExpression();
      setState(859); 
      _errHandler->sync(this);
      _la = _input->LA(1);
      do {
        setState(857);
        match(NebulaSQLParser::T__1);
        setState(858);
        namedExpression();
        setState(861); 
        _errHandler->sync(this);
        _la = _input->LA(1);
      } while (_la == NebulaSQLParser::T__1);
      setState(863);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 5: {
      _localctx = _tracker.createInstance<FunctionCallContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(865);
      functionName();
      setState(866);
      match(NebulaSQLParser::T__2);
      setState(875);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 85, _ctx)) {
      case 1: {
        setState(867);
        dynamic_cast<FunctionCallContext *>(_localctx)->expressionContext = expression();
        dynamic_cast<FunctionCallContext *>(_localctx)->argument.push_back(dynamic_cast<FunctionCallContext *>(_localctx)->expressionContext);
        setState(872);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NebulaSQLParser::T__1) {
          setState(868);
          match(NebulaSQLParser::T__1);
          setState(869);
          dynamic_cast<FunctionCallContext *>(_localctx)->expressionContext = expression();
          dynamic_cast<FunctionCallContext *>(_localctx)->argument.push_back(dynamic_cast<FunctionCallContext *>(_localctx)->expressionContext);
          setState(874);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

      default:
        break;
      }
      setState(877);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 6: {
      _localctx = _tracker.createInstance<ParenthesizedExpressionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(879);
      match(NebulaSQLParser::T__2);
      setState(880);
      expression();
      setState(881);
      match(NebulaSQLParser::T__3);
      break;
    }

    case 7: {
      _localctx = _tracker.createInstance<ConstantDefaultContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(883);
      constant();
      break;
    }

    case 8: {
      _localctx = _tracker.createInstance<ColumnReferenceContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(884);
      identifier();
      break;
    }

    default:
      break;
    }
    _ctx->stop = _input->LT(-1);
    setState(892);
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
        setState(887);

        if (!(precpred(_ctx, 7))) throw FailedPredicateException(this, "precpred(_ctx, 7)");
        setState(888);
        match(NebulaSQLParser::T__4);
        setState(889);
        dynamic_cast<DereferenceContext *>(_localctx)->fieldName = identifier(); 
      }
      setState(894);
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
  enterRule(_localctx, 154, NebulaSQLParser::RuleQualifiedName);

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
    setState(895);
    identifier();
    setState(900);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 88, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(896);
        match(NebulaSQLParser::T__4);
        setState(897);
        identifier(); 
      }
      setState(902);
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
  enterRule(_localctx, 156, NebulaSQLParser::RuleNumber);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(946);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 99, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::ExponentLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(903);

      if (!(!legacy_exponent_literal_as_decimal_enabled)) throw FailedPredicateException(this, "!legacy_exponent_literal_as_decimal_enabled");
      setState(905);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(904);
        match(NebulaSQLParser::MINUS);
      }
      setState(907);
      match(NebulaSQLParser::EXPONENT_VALUE);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::DecimalLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(908);

      if (!(!legacy_exponent_literal_as_decimal_enabled)) throw FailedPredicateException(this, "!legacy_exponent_literal_as_decimal_enabled");
      setState(910);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(909);
        match(NebulaSQLParser::MINUS);
      }
      setState(912);
      match(NebulaSQLParser::DECIMAL_VALUE);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::LegacyDecimalLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(913);

      if (!(legacy_exponent_literal_as_decimal_enabled)) throw FailedPredicateException(this, "legacy_exponent_literal_as_decimal_enabled");
      setState(915);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(914);
        match(NebulaSQLParser::MINUS);
      }
      setState(917);
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
      setState(919);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(918);
        match(NebulaSQLParser::MINUS);
      }
      setState(921);
      match(NebulaSQLParser::INTEGER_VALUE);
      break;
    }

    case 5: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::BigIntLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(923);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(922);
        match(NebulaSQLParser::MINUS);
      }
      setState(925);
      match(NebulaSQLParser::BIGINT_LITERAL);
      break;
    }

    case 6: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::SmallIntLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 6);
      setState(927);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(926);
        match(NebulaSQLParser::MINUS);
      }
      setState(929);
      match(NebulaSQLParser::SMALLINT_LITERAL);
      break;
    }

    case 7: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::TinyIntLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 7);
      setState(931);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(930);
        match(NebulaSQLParser::MINUS);
      }
      setState(933);
      match(NebulaSQLParser::TINYINT_LITERAL);
      break;
    }

    case 8: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::DoubleLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 8);
      setState(935);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(934);
        match(NebulaSQLParser::MINUS);
      }
      setState(937);
      match(NebulaSQLParser::DOUBLE_LITERAL);
      break;
    }

    case 9: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::FloatLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 9);
      setState(939);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(938);
        match(NebulaSQLParser::MINUS);
      }
      setState(941);
      match(NebulaSQLParser::FLOAT_LITERAL);
      break;
    }

    case 10: {
      _localctx = dynamic_cast<NumberContext *>(_tracker.createInstance<NebulaSQLParser::BigDecimalLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 10);
      setState(943);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NebulaSQLParser::MINUS) {
        setState(942);
        match(NebulaSQLParser::MINUS);
      }
      setState(945);
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
  enterRule(_localctx, 158, NebulaSQLParser::RuleConstant);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(959);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 101, _ctx)) {
    case 1: {
      _localctx = dynamic_cast<ConstantContext *>(_tracker.createInstance<NebulaSQLParser::NullLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 1);
      setState(948);
      match(NebulaSQLParser::NULLTOKEN);
      break;
    }

    case 2: {
      _localctx = dynamic_cast<ConstantContext *>(_tracker.createInstance<NebulaSQLParser::TypeConstructorContext>(_localctx));
      enterOuterAlt(_localctx, 2);
      setState(949);
      identifier();
      setState(950);
      match(NebulaSQLParser::STRING);
      break;
    }

    case 3: {
      _localctx = dynamic_cast<ConstantContext *>(_tracker.createInstance<NebulaSQLParser::NumericLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 3);
      setState(952);
      number();
      break;
    }

    case 4: {
      _localctx = dynamic_cast<ConstantContext *>(_tracker.createInstance<NebulaSQLParser::BooleanLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 4);
      setState(953);
      booleanValue();
      break;
    }

    case 5: {
      _localctx = dynamic_cast<ConstantContext *>(_tracker.createInstance<NebulaSQLParser::StringLiteralContext>(_localctx));
      enterOuterAlt(_localctx, 5);
      setState(955); 
      _errHandler->sync(this);
      alt = 1;
      do {
        switch (alt) {
          case 1: {
                setState(954);
                match(NebulaSQLParser::STRING);
                break;
              }

        default:
          throw NoViableAltException(this);
        }
        setState(957); 
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
  enterRule(_localctx, 160, NebulaSQLParser::RuleBooleanValue);
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
    setState(961);
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
  enterRule(_localctx, 162, NebulaSQLParser::RuleStrictNonReserved);
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
    setState(963);
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
  enterRule(_localctx, 164, NebulaSQLParser::RuleAnsiNonReserved);
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
    setState(965);
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
  enterRule(_localctx, 166, NebulaSQLParser::RuleNonReserved);
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
    setState(967);
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
    case 72: return valueExpressionSempred(dynamic_cast<ValueExpressionContext *>(context), predicateIndex);
    case 76: return primaryExpressionSempred(dynamic_cast<PrimaryExpressionContext *>(context), predicateIndex);
    case 78: return numberSempred(dynamic_cast<NumberContext *>(context), predicateIndex);

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::queryTermSempred(QueryTermContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 0: return precpred(_ctx, 1);

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::identifierSempred(IdentifierContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 1: return !SQL_standard_keyword_behavior;

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::strictIdentifierSempred(StrictIdentifierContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 2: return SQL_standard_keyword_behavior;
    case 3: return !SQL_standard_keyword_behavior;

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::booleanExpressionSempred(BooleanExpressionContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 4: return precpred(_ctx, 2);
    case 5: return precpred(_ctx, 1);

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::valueExpressionSempred(ValueExpressionContext *_localctx, size_t predicateIndex) {
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
  switch (predicateIndex) {
    case 12: return precpred(_ctx, 7);

  default:
    break;
  }
  return true;
}

bool NebulaSQLParser::numberSempred(NumberContext *_localctx, size_t predicateIndex) {
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
  "watermarkParameters", "windowSpec", "timeWindow", "countWindow", "conditionWindow", 
  "conditionParameter", "thresholdMinSizeParameter", "sizeParameter", "advancebyParameter", 
  "timeUnit", "timestampParameter", "functionName", "sinkClause", "sinkType", 
  "sinkTypeZMQ", "nullNotnull", "zmqKeyword", "streamName", "host", "port", 
  "sinkTypeKafka", "kafkaKeyword", "kafkaBroker", "kafkaTopic", "kafkaProducerTimout", 
  "sinkTypeFile", "fileFormat", "sinkTypeMQTT", "qos", "sinkTypeOPC", "sinkTypePrint", 
  "sortItem", "predicate", "valueExpression", "comparisonOperator", "hint", 
  "hintStatement", "primaryExpression", "qualifiedName", "number", "constant", 
  "booleanValue", "strictNonReserved", "ansiNonReserved", "nonReserved"
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
  "", "", "", "", "", "", "'FILE'", "'MQTT'", "'OPC'", "", "", "'CSV_FORMAT'", 
  "'NES_FORMAT'", "'TEXT_FORMAT'", "'AT_MOST_ONCE'", "'AT_LEAST_ONCE'", 
  "", "", "'<=>'", "'<>'", "'!='", "'<'", "", "'>'", "", "'+'", "'-'", "'*'", 
  "'/'", "'%'", "'~'", "'&'", "'|'", "'||'", "'^'"
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
  "SLIDING", "THRESHOLD", "SIZE", "ADVANCE", "MS", "SEC", "MIN", "HOUR", 
  "DAY", "MAX", "AVG", "SUM", "COUNT", "MEDIAN", "WATERMARK", "OFFSET", 
  "ZMQ", "KAFKA", "FILE", "MQTT", "OPC", "PRINT", "LOCALHOST", "CSV_FORMAT", 
  "NES_FORMAT", "TEXT_FORMAT", "AT_MOST_ONCE", "AT_LEAST_ONCE", "BOOLEAN_VALUE", 
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
       0x3, 0x91, 0x3cc, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
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
       0x9, 0x4d, 0x4, 0x4e, 0x9, 0x4e, 0x4, 0x4f, 0x9, 0x4f, 0x4, 0x50, 
       0x9, 0x50, 0x4, 0x51, 0x9, 0x51, 0x4, 0x52, 0x9, 0x52, 0x4, 0x53, 
       0x9, 0x53, 0x4, 0x54, 0x9, 0x54, 0x4, 0x55, 0x9, 0x55, 0x3, 0x2, 
       0x3, 0x2, 0x7, 0x2, 0xad, 0xa, 0x2, 0xc, 0x2, 0xe, 0x2, 0xb0, 0xb, 
       0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x3, 0x3, 0x3, 0x3, 0x4, 0x3, 0x4, 
       0x3, 0x4, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x7, 
       0x5, 0xbe, 0xa, 0x5, 0xc, 0x5, 0xe, 0x5, 0xc1, 0xb, 0x5, 0x5, 0x5, 
       0xc3, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0xc8, 0xa, 
       0x5, 0x5, 0x5, 0xca, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0xce, 
       0xa, 0x5, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 
       0x6, 0x7, 0x6, 0xd6, 0xa, 0x6, 0xc, 0x6, 0xe, 0x6, 0xd9, 0xb, 0x6, 
       0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 
       0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0xe4, 0xa, 0x7, 0x3, 0x8, 0x3, 
       0x8, 0x3, 0x8, 0x5, 0x8, 0xe9, 0xa, 0x8, 0x3, 0x8, 0x5, 0x8, 0xec, 
       0xa, 0x8, 0x3, 0x8, 0x5, 0x8, 0xef, 0xa, 0x8, 0x3, 0x8, 0x5, 0x8, 
       0xf2, 0xa, 0x8, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x7, 0x9, 
       0xf8, 0xa, 0x9, 0xc, 0x9, 0xe, 0x9, 0xfb, 0xb, 0x9, 0x3, 0xa, 0x3, 
       0xa, 0x7, 0xa, 0xff, 0xa, 0xa, 0xc, 0xa, 0xe, 0xa, 0x102, 0xb, 0xa, 
       0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x5, 0xb, 0x108, 0xa, 0xb, 
       0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x5, 0xb, 0x10f, 
       0xa, 0xb, 0x3, 0xc, 0x5, 0xc, 0x112, 0xa, 0xc, 0x3, 0xd, 0x3, 0xd, 
       0x3, 0xd, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 
       0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 
       0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x5, 0xe, 0x126, 0xa, 0xe, 0x3, 0xf, 
       0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x7, 0xf, 0x12d, 0xa, 0xf, 
       0xc, 0xf, 0xe, 0xf, 0x130, 0xb, 0xf, 0x5, 0xf, 0x132, 0xa, 0xf, 0x3, 
       0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0x10, 0x3, 0x10, 0x6, 0x10, 0x139, 
       0xa, 0x10, 0xd, 0x10, 0xe, 0x10, 0x13a, 0x3, 0x11, 0x3, 0x11, 0x5, 
       0x11, 0x13f, 0xa, 0x11, 0x3, 0x11, 0x5, 0x11, 0x142, 0xa, 0x11, 0x3, 
       0x12, 0x3, 0x12, 0x7, 0x12, 0x146, 0xa, 0x12, 0xc, 0x12, 0xe, 0x12, 
       0x149, 0xb, 0x12, 0x3, 0x12, 0x3, 0x12, 0x3, 0x13, 0x3, 0x13, 0x3, 
       0x13, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x15, 0x3, 0x15, 0x3, 
       0x15, 0x3, 0x15, 0x7, 0x15, 0x157, 0xa, 0x15, 0xc, 0x15, 0xe, 0x15, 
       0x15a, 0xb, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x16, 0x5, 0x16, 0x15f, 
       0xa, 0x16, 0x3, 0x16, 0x3, 0x16, 0x5, 0x16, 0x163, 0xa, 0x16, 0x5, 
       0x16, 0x165, 0xa, 0x16, 0x3, 0x17, 0x3, 0x17, 0x3, 0x17, 0x7, 0x17, 
       0x16a, 0xa, 0x17, 0xc, 0x17, 0xe, 0x17, 0x16d, 0xb, 0x17, 0x3, 0x18, 
       0x3, 0x18, 0x3, 0x18, 0x7, 0x18, 0x172, 0xa, 0x18, 0xc, 0x18, 0xe, 
       0x18, 0x175, 0xb, 0x18, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x179, 0xa, 
       0x19, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x17d, 0xa, 0x19, 0x5, 0x19, 
       0x17f, 0xa, 0x19, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x5, 0x1a, 0x184, 
       0xa, 0x1a, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 
       0x3, 0x1b, 0x5, 0x1b, 0x18c, 0xa, 0x1b, 0x3, 0x1c, 0x3, 0x1c, 0x3, 
       0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1e, 0x3, 0x1e, 0x3, 
       0x1e, 0x7, 0x1e, 0x197, 0xa, 0x1e, 0xc, 0x1e, 0xe, 0x1e, 0x19a, 0xb, 
       0x1e, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x20, 0x3, 0x20, 0x6, 
       0x20, 0x1a1, 0xa, 0x20, 0xd, 0x20, 0xe, 0x20, 0x1a2, 0x3, 0x20, 0x5, 
       0x20, 0x1a6, 0xa, 0x20, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x7, 0x21, 
       0x1ab, 0xa, 0x21, 0xc, 0x21, 0xe, 0x21, 0x1ae, 0xb, 0x21, 0x3, 0x22, 
       0x3, 0x22, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 
       0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 
       0x1bc, 0xa, 0x23, 0x5, 0x23, 0x1be, 0xa, 0x23, 0x3, 0x23, 0x3, 0x23, 
       0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x7, 0x23, 0x1c6, 0xa, 
       0x23, 0xc, 0x23, 0xe, 0x23, 0x1c9, 0xb, 0x23, 0x3, 0x24, 0x5, 0x24, 
       0x1cc, 0xa, 0x24, 0x3, 0x24, 0x3, 0x24, 0x5, 0x24, 0x1d0, 0xa, 0x24, 
       0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x7, 0x25, 
       0x1d7, 0xa, 0x25, 0xc, 0x25, 0xe, 0x25, 0x1da, 0xb, 0x25, 0x3, 0x25, 
       0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 
       0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x7, 0x25, 0x1e6, 0xa, 0x25, 0xc, 
       0x25, 0xe, 0x25, 0x1e9, 0xb, 0x25, 0x3, 0x25, 0x3, 0x25, 0x5, 0x25, 
       0x1ed, 0xa, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 
       0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x7, 0x25, 0x1f7, 0xa, 0x25, 
       0xc, 0x25, 0xe, 0x25, 0x1fa, 0xb, 0x25, 0x3, 0x25, 0x3, 0x25, 0x5, 
       0x25, 0x1fe, 0xa, 0x25, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 
       0x7, 0x26, 0x204, 0xa, 0x26, 0xc, 0x26, 0xe, 0x26, 0x207, 0xb, 0x26, 
       0x5, 0x26, 0x209, 0xa, 0x26, 0x3, 0x26, 0x3, 0x26, 0x5, 0x26, 0x20d, 
       0xa, 0x26, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x28, 0x3, 0x28, 
       0x3, 0x28, 0x3, 0x28, 0x3, 0x28, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 
       0x3, 0x29, 0x3, 0x29, 0x3, 0x2a, 0x3, 0x2a, 0x3, 0x2a, 0x5, 0x2a, 
       0x21f, 0xa, 0x2a, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 
       0x2b, 0x5, 0x2b, 0x226, 0xa, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 
       0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 
       0x230, 0xa, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 
       0x2b, 0x5, 0x2b, 0x237, 0xa, 0x2b, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 
       0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 
       0x3, 0x2d, 0x5, 0x2d, 0x243, 0xa, 0x2d, 0x3, 0x2d, 0x3, 0x2d, 0x3, 
       0x2e, 0x3, 0x2e, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x30, 0x3, 0x30, 0x3, 
       0x30, 0x3, 0x30, 0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x3, 
       0x31, 0x3, 0x32, 0x3, 0x32, 0x3, 0x33, 0x3, 0x33, 0x3, 0x34, 0x3, 
       0x34, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x5, 0x35, 0x25d, 0xa, 0x35, 
       0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 0x3, 0x36, 
       0x5, 0x36, 0x265, 0xa, 0x36, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 
       0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 
       0x38, 0x5, 0x38, 0x271, 0xa, 0x38, 0x3, 0x38, 0x3, 0x38, 0x3, 0x39, 
       0x3, 0x39, 0x3, 0x3a, 0x3, 0x3a, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3c, 
       0x3, 0x3c, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 
       0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3e, 0x3, 0x3e, 
       0x3, 0x3f, 0x3, 0x3f, 0x3, 0x40, 0x3, 0x40, 0x3, 0x41, 0x3, 0x41, 
       0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 
       0x3, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x43, 0x3, 0x43, 0x3, 0x44, 
       0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 
       0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 
       0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 
       0x3, 0x45, 0x3, 0x45, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 
       0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 
       0x3, 0x46, 0x3, 0x47, 0x3, 0x47, 0x3, 0x48, 0x3, 0x48, 0x5, 0x48, 
       0x2bd, 0xa, 0x48, 0x3, 0x48, 0x3, 0x48, 0x5, 0x48, 0x2c1, 0xa, 0x48, 
       0x3, 0x49, 0x5, 0x49, 0x2c4, 0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 
       0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x2cc, 0xa, 0x49, 
       0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x7, 0x49, 
       0x2d3, 0xa, 0x49, 0xc, 0x49, 0xe, 0x49, 0x2d6, 0xb, 0x49, 0x3, 0x49, 
       0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x2db, 0xa, 0x49, 0x3, 0x49, 0x3, 
       0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x2e3, 
       0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x2e8, 0xa, 
       0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 
       0x49, 0x3, 0x49, 0x3, 0x49, 0x7, 0x49, 0x2f2, 0xa, 0x49, 0xc, 0x49, 
       0xe, 0x49, 0x2f5, 0xb, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x2f9, 
       0xa, 0x49, 0x3, 0x49, 0x5, 0x49, 0x2fc, 0xa, 0x49, 0x3, 0x49, 0x3, 
       0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x302, 0xa, 0x49, 0x3, 0x49, 
       0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x308, 0xa, 0x49, 0x3, 
       0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x30d, 0xa, 0x49, 0x3, 0x49, 
       0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x312, 0xa, 0x49, 0x3, 0x4a, 0x3, 
       0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x5, 0x4a, 0x318, 0xa, 0x4a, 0x3, 0x4a, 
       0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 
       0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 
       0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 
       0x7, 0x4a, 0x32d, 0xa, 0x4a, 0xc, 0x4a, 0xe, 0x4a, 0x330, 0xb, 0x4a, 
       0x3, 0x4b, 0x3, 0x4b, 0x3, 0x4c, 0x3, 0x4c, 0x3, 0x4c, 0x5, 0x4c, 
       0x337, 0xa, 0x4c, 0x3, 0x4c, 0x7, 0x4c, 0x33a, 0xa, 0x4c, 0xc, 0x4c, 
       0xe, 0x4c, 0x33d, 0xb, 0x4c, 0x3, 0x4c, 0x3, 0x4c, 0x3, 0x4d, 0x3, 
       0x4d, 0x3, 0x4d, 0x3, 0x4d, 0x3, 0x4d, 0x3, 0x4d, 0x7, 0x4d, 0x347, 
       0xa, 0x4d, 0xc, 0x4d, 0xe, 0x4d, 0x34a, 0xb, 0x4d, 0x3, 0x4d, 0x3, 
       0x4d, 0x5, 0x4d, 0x34e, 0xa, 0x4d, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 
       0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 
       0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x6, 0x4e, 
       0x35e, 0xa, 0x4e, 0xd, 0x4e, 0xe, 0x4e, 0x35f, 0x3, 0x4e, 0x3, 0x4e, 
       0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x7, 0x4e, 
       0x369, 0xa, 0x4e, 0xc, 0x4e, 0xe, 0x4e, 0x36c, 0xb, 0x4e, 0x5, 0x4e, 
       0x36e, 0xa, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 
       0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x5, 0x4e, 0x378, 0xa, 0x4e, 
       0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x7, 0x4e, 0x37d, 0xa, 0x4e, 0xc, 
       0x4e, 0xe, 0x4e, 0x380, 0xb, 0x4e, 0x3, 0x4f, 0x3, 0x4f, 0x3, 0x4f, 
       0x7, 0x4f, 0x385, 0xa, 0x4f, 0xc, 0x4f, 0xe, 0x4f, 0x388, 0xb, 0x4f, 
       0x3, 0x50, 0x3, 0x50, 0x5, 0x50, 0x38c, 0xa, 0x50, 0x3, 0x50, 0x3, 
       0x50, 0x3, 0x50, 0x5, 0x50, 0x391, 0xa, 0x50, 0x3, 0x50, 0x3, 0x50, 
       0x3, 0x50, 0x5, 0x50, 0x396, 0xa, 0x50, 0x3, 0x50, 0x3, 0x50, 0x5, 
       0x50, 0x39a, 0xa, 0x50, 0x3, 0x50, 0x3, 0x50, 0x5, 0x50, 0x39e, 0xa, 
       0x50, 0x3, 0x50, 0x3, 0x50, 0x5, 0x50, 0x3a2, 0xa, 0x50, 0x3, 0x50, 
       0x3, 0x50, 0x5, 0x50, 0x3a6, 0xa, 0x50, 0x3, 0x50, 0x3, 0x50, 0x5, 
       0x50, 0x3aa, 0xa, 0x50, 0x3, 0x50, 0x3, 0x50, 0x5, 0x50, 0x3ae, 0xa, 
       0x50, 0x3, 0x50, 0x3, 0x50, 0x5, 0x50, 0x3b2, 0xa, 0x50, 0x3, 0x50, 
       0x5, 0x50, 0x3b5, 0xa, 0x50, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x3, 
       0x51, 0x3, 0x51, 0x3, 0x51, 0x3, 0x51, 0x6, 0x51, 0x3be, 0xa, 0x51, 
       0xd, 0x51, 0xe, 0x51, 0x3bf, 0x5, 0x51, 0x3c2, 0xa, 0x51, 0x3, 0x52, 
       0x3, 0x52, 0x3, 0x53, 0x3, 0x53, 0x3, 0x54, 0x3, 0x54, 0x3, 0x55, 
       0x3, 0x55, 0x3, 0x55, 0x2, 0x6, 0xa, 0x44, 0x92, 0x9a, 0x56, 0x2, 
       0x4, 0x6, 0x8, 0xa, 0xc, 0xe, 0x10, 0x12, 0x14, 0x16, 0x18, 0x1a, 
       0x1c, 0x1e, 0x20, 0x22, 0x24, 0x26, 0x28, 0x2a, 0x2c, 0x2e, 0x30, 
       0x32, 0x34, 0x36, 0x38, 0x3a, 0x3c, 0x3e, 0x40, 0x42, 0x44, 0x46, 
       0x48, 0x4a, 0x4c, 0x4e, 0x50, 0x52, 0x54, 0x56, 0x58, 0x5a, 0x5c, 
       0x5e, 0x60, 0x62, 0x64, 0x66, 0x68, 0x6a, 0x6c, 0x6e, 0x70, 0x72, 
       0x74, 0x76, 0x78, 0x7a, 0x7c, 0x7e, 0x80, 0x82, 0x84, 0x86, 0x88, 
       0x8a, 0x8c, 0x8e, 0x90, 0x92, 0x94, 0x96, 0x98, 0x9a, 0x9c, 0x9e, 
       0xa0, 0xa2, 0xa4, 0xa6, 0xa8, 0x2, 0x14, 0x3, 0x2, 0x56, 0x5a, 0x4, 
       0x2, 0x58, 0x58, 0x5b, 0x5f, 0x4, 0x2, 0x68, 0x68, 0x8f, 0x8f, 0x3, 
       0x2, 0x69, 0x6b, 0x3, 0x2, 0x6c, 0x6d, 0x4, 0x2, 0xf, 0xf, 0x16, 
       0x16, 0x4, 0x2, 0x1f, 0x1f, 0x2d, 0x2d, 0x5, 0x2, 0xb, 0xb, 0xd, 
       0xd, 0x42, 0x42, 0x5, 0x2, 0x1e, 0x1e, 0x46, 0x46, 0x49, 0x49, 0x4, 
       0x2, 0x77, 0x78, 0x7c, 0x7c, 0x4, 0x2, 0x18, 0x18, 0x79, 0x7b, 0x4, 
       0x2, 0x77, 0x78, 0x7f, 0x7f, 0x3, 0x2, 0x6f, 0x76, 0x3, 0x2, 0x86, 
       0x87, 0x4, 0x2, 0x1e, 0x1e, 0x46, 0x46, 0xb, 0x2, 0x22, 0x22, 0x28, 
       0x28, 0x2c, 0x2c, 0x2e, 0x2e, 0x33, 0x33, 0x38, 0x38, 0x3d, 0x3d, 
       0x48, 0x48, 0x4b, 0x4b, 0x13, 0x2, 0xf, 0x12, 0x14, 0x16, 0x18, 0x19, 
       0x1d, 0x1d, 0x1f, 0x1f, 0x24, 0x24, 0x29, 0x29, 0x2d, 0x2d, 0x2f, 
       0x30, 0x32, 0x32, 0x36, 0x36, 0x3b, 0x3b, 0x3e, 0x3f, 0x41, 0x41, 
       0x46, 0x47, 0x4c, 0x4c, 0x4f, 0x4f, 0x13, 0x2, 0xb, 0x12, 0x14, 0x19, 
       0x1c, 0x1f, 0x21, 0x21, 0x23, 0x25, 0x27, 0x27, 0x29, 0x2b, 0x2d, 
       0x2d, 0x2f, 0x30, 0x32, 0x32, 0x34, 0x36, 0x39, 0x3b, 0x3e, 0x42, 
       0x44, 0x44, 0x46, 0x47, 0x4c, 0x4c, 0x4e, 0x50, 0x2, 0x409, 0x2, 
       0xaa, 0x3, 0x2, 0x2, 0x2, 0x4, 0xb3, 0x3, 0x2, 0x2, 0x2, 0x6, 0xb5, 
       0x3, 0x2, 0x2, 0x2, 0x8, 0xc2, 0x3, 0x2, 0x2, 0x2, 0xa, 0xcf, 0x3, 
       0x2, 0x2, 0x2, 0xc, 0xe3, 0x3, 0x2, 0x2, 0x2, 0xe, 0xe5, 0x3, 0x2, 
       0x2, 0x2, 0x10, 0xf3, 0x3, 0x2, 0x2, 0x2, 0x12, 0xfc, 0x3, 0x2, 0x2, 
       0x2, 0x14, 0x10e, 0x3, 0x2, 0x2, 0x2, 0x16, 0x111, 0x3, 0x2, 0x2, 
       0x2, 0x18, 0x113, 0x3, 0x2, 0x2, 0x2, 0x1a, 0x125, 0x3, 0x2, 0x2, 
       0x2, 0x1c, 0x127, 0x3, 0x2, 0x2, 0x2, 0x1e, 0x136, 0x3, 0x2, 0x2, 
       0x2, 0x20, 0x13c, 0x3, 0x2, 0x2, 0x2, 0x22, 0x143, 0x3, 0x2, 0x2, 
       0x2, 0x24, 0x14c, 0x3, 0x2, 0x2, 0x2, 0x26, 0x14f, 0x3, 0x2, 0x2, 
       0x2, 0x28, 0x152, 0x3, 0x2, 0x2, 0x2, 0x2a, 0x164, 0x3, 0x2, 0x2, 
       0x2, 0x2c, 0x166, 0x3, 0x2, 0x2, 0x2, 0x2e, 0x16e, 0x3, 0x2, 0x2, 
       0x2, 0x30, 0x176, 0x3, 0x2, 0x2, 0x2, 0x32, 0x183, 0x3, 0x2, 0x2, 
       0x2, 0x34, 0x18b, 0x3, 0x2, 0x2, 0x2, 0x36, 0x18d, 0x3, 0x2, 0x2, 
       0x2, 0x38, 0x18f, 0x3, 0x2, 0x2, 0x2, 0x3a, 0x193, 0x3, 0x2, 0x2, 
       0x2, 0x3c, 0x19b, 0x3, 0x2, 0x2, 0x2, 0x3e, 0x1a5, 0x3, 0x2, 0x2, 
       0x2, 0x40, 0x1a7, 0x3, 0x2, 0x2, 0x2, 0x42, 0x1af, 0x3, 0x2, 0x2, 
       0x2, 0x44, 0x1bd, 0x3, 0x2, 0x2, 0x2, 0x46, 0x1cb, 0x3, 0x2, 0x2, 
       0x2, 0x48, 0x1fd, 0x3, 0x2, 0x2, 0x2, 0x4a, 0x20c, 0x3, 0x2, 0x2, 
       0x2, 0x4c, 0x20e, 0x3, 0x2, 0x2, 0x2, 0x4e, 0x211, 0x3, 0x2, 0x2, 
       0x2, 0x50, 0x216, 0x3, 0x2, 0x2, 0x2, 0x52, 0x21e, 0x3, 0x2, 0x2, 
       0x2, 0x54, 0x236, 0x3, 0x2, 0x2, 0x2, 0x56, 0x238, 0x3, 0x2, 0x2, 
       0x2, 0x58, 0x23d, 0x3, 0x2, 0x2, 0x2, 0x5a, 0x246, 0x3, 0x2, 0x2, 
       0x2, 0x5c, 0x248, 0x3, 0x2, 0x2, 0x2, 0x5e, 0x24a, 0x3, 0x2, 0x2, 
       0x2, 0x60, 0x24e, 0x3, 0x2, 0x2, 0x2, 0x62, 0x253, 0x3, 0x2, 0x2, 
       0x2, 0x64, 0x255, 0x3, 0x2, 0x2, 0x2, 0x66, 0x257, 0x3, 0x2, 0x2, 
       0x2, 0x68, 0x259, 0x3, 0x2, 0x2, 0x2, 0x6a, 0x264, 0x3, 0x2, 0x2, 
       0x2, 0x6c, 0x266, 0x3, 0x2, 0x2, 0x2, 0x6e, 0x270, 0x3, 0x2, 0x2, 
       0x2, 0x70, 0x274, 0x3, 0x2, 0x2, 0x2, 0x72, 0x276, 0x3, 0x2, 0x2, 
       0x2, 0x74, 0x278, 0x3, 0x2, 0x2, 0x2, 0x76, 0x27a, 0x3, 0x2, 0x2, 
       0x2, 0x78, 0x27c, 0x3, 0x2, 0x2, 0x2, 0x7a, 0x285, 0x3, 0x2, 0x2, 
       0x2, 0x7c, 0x287, 0x3, 0x2, 0x2, 0x2, 0x7e, 0x289, 0x3, 0x2, 0x2, 
       0x2, 0x80, 0x28b, 0x3, 0x2, 0x2, 0x2, 0x82, 0x28d, 0x3, 0x2, 0x2, 
       0x2, 0x84, 0x296, 0x3, 0x2, 0x2, 0x2, 0x86, 0x298, 0x3, 0x2, 0x2, 
       0x2, 0x88, 0x2ab, 0x3, 0x2, 0x2, 0x2, 0x8a, 0x2ad, 0x3, 0x2, 0x2, 
       0x2, 0x8c, 0x2b8, 0x3, 0x2, 0x2, 0x2, 0x8e, 0x2ba, 0x3, 0x2, 0x2, 
       0x2, 0x90, 0x311, 0x3, 0x2, 0x2, 0x2, 0x92, 0x317, 0x3, 0x2, 0x2, 
       0x2, 0x94, 0x331, 0x3, 0x2, 0x2, 0x2, 0x96, 0x333, 0x3, 0x2, 0x2, 
       0x2, 0x98, 0x34d, 0x3, 0x2, 0x2, 0x2, 0x9a, 0x377, 0x3, 0x2, 0x2, 
       0x2, 0x9c, 0x381, 0x3, 0x2, 0x2, 0x2, 0x9e, 0x3b4, 0x3, 0x2, 0x2, 
       0x2, 0xa0, 0x3c1, 0x3, 0x2, 0x2, 0x2, 0xa2, 0x3c3, 0x3, 0x2, 0x2, 
       0x2, 0xa4, 0x3c5, 0x3, 0x2, 0x2, 0x2, 0xa6, 0x3c7, 0x3, 0x2, 0x2, 
       0x2, 0xa8, 0x3c9, 0x3, 0x2, 0x2, 0x2, 0xaa, 0xae, 0x5, 0x4, 0x3, 
       0x2, 0xab, 0xad, 0x7, 0x3, 0x2, 0x2, 0xac, 0xab, 0x3, 0x2, 0x2, 0x2, 
       0xad, 0xb0, 0x3, 0x2, 0x2, 0x2, 0xae, 0xac, 0x3, 0x2, 0x2, 0x2, 0xae, 
       0xaf, 0x3, 0x2, 0x2, 0x2, 0xaf, 0xb1, 0x3, 0x2, 0x2, 0x2, 0xb0, 0xae, 
       0x3, 0x2, 0x2, 0x2, 0xb1, 0xb2, 0x7, 0x2, 0x2, 0x3, 0xb2, 0x3, 0x3, 
       0x2, 0x2, 0x2, 0xb3, 0xb4, 0x5, 0x6, 0x4, 0x2, 0xb4, 0x5, 0x3, 0x2, 
       0x2, 0x2, 0xb5, 0xb6, 0x5, 0xa, 0x6, 0x2, 0xb6, 0xb7, 0x5, 0x8, 0x5, 
       0x2, 0xb7, 0x7, 0x3, 0x2, 0x2, 0x2, 0xb8, 0xb9, 0x7, 0x3a, 0x2, 0x2, 
       0xb9, 0xba, 0x7, 0x12, 0x2, 0x2, 0xba, 0xbf, 0x5, 0x8e, 0x48, 0x2, 
       0xbb, 0xbc, 0x7, 0x4, 0x2, 0x2, 0xbc, 0xbe, 0x5, 0x8e, 0x48, 0x2, 
       0xbd, 0xbb, 0x3, 0x2, 0x2, 0x2, 0xbe, 0xc1, 0x3, 0x2, 0x2, 0x2, 0xbf, 
       0xbd, 0x3, 0x2, 0x2, 0x2, 0xbf, 0xc0, 0x3, 0x2, 0x2, 0x2, 0xc0, 0xc3, 
       0x3, 0x2, 0x2, 0x2, 0xc1, 0xbf, 0x3, 0x2, 0x2, 0x2, 0xc2, 0xb8, 0x3, 
       0x2, 0x2, 0x2, 0xc2, 0xc3, 0x3, 0x2, 0x2, 0x2, 0xc3, 0xc9, 0x3, 0x2, 
       0x2, 0x2, 0xc4, 0xc7, 0x7, 0x30, 0x2, 0x2, 0xc5, 0xc8, 0x7, 0xb, 
       0x2, 0x2, 0xc6, 0xc8, 0x7, 0x85, 0x2, 0x2, 0xc7, 0xc5, 0x3, 0x2, 
       0x2, 0x2, 0xc7, 0xc6, 0x3, 0x2, 0x2, 0x2, 0xc8, 0xca, 0x3, 0x2, 0x2, 
       0x2, 0xc9, 0xc4, 0x3, 0x2, 0x2, 0x2, 0xc9, 0xca, 0x3, 0x2, 0x2, 0x2, 
       0xca, 0xcd, 0x3, 0x2, 0x2, 0x2, 0xcb, 0xcc, 0x7, 0x61, 0x2, 0x2, 
       0xcc, 0xce, 0x7, 0x85, 0x2, 0x2, 0xcd, 0xcb, 0x3, 0x2, 0x2, 0x2, 
       0xcd, 0xce, 0x3, 0x2, 0x2, 0x2, 0xce, 0x9, 0x3, 0x2, 0x2, 0x2, 0xcf, 
       0xd0, 0x8, 0x6, 0x1, 0x2, 0xd0, 0xd1, 0x5, 0xc, 0x7, 0x2, 0xd1, 0xd7, 
       0x3, 0x2, 0x2, 0x2, 0xd2, 0xd3, 0xc, 0x3, 0x2, 0x2, 0xd3, 0xd4, 0x7, 
       0x48, 0x2, 0x2, 0xd4, 0xd6, 0x5, 0xa, 0x6, 0x4, 0xd5, 0xd2, 0x3, 
       0x2, 0x2, 0x2, 0xd6, 0xd9, 0x3, 0x2, 0x2, 0x2, 0xd7, 0xd5, 0x3, 0x2, 
       0x2, 0x2, 0xd7, 0xd8, 0x3, 0x2, 0x2, 0x2, 0xd8, 0xb, 0x3, 0x2, 0x2, 
       0x2, 0xd9, 0xd7, 0x3, 0x2, 0x2, 0x2, 0xda, 0xe4, 0x5, 0xe, 0x8, 0x2, 
       0xdb, 0xe4, 0x5, 0x1e, 0x10, 0x2, 0xdc, 0xdd, 0x7, 0x44, 0x2, 0x2, 
       0xdd, 0xe4, 0x5, 0x2e, 0x18, 0x2, 0xde, 0xe4, 0x5, 0x28, 0x15, 0x2, 
       0xdf, 0xe0, 0x7, 0x5, 0x2, 0x2, 0xe0, 0xe1, 0x5, 0x6, 0x4, 0x2, 0xe1, 
       0xe2, 0x7, 0x6, 0x2, 0x2, 0xe2, 0xe4, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xda, 
       0x3, 0x2, 0x2, 0x2, 0xe3, 0xdb, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xdc, 0x3, 
       0x2, 0x2, 0x2, 0xe3, 0xde, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xdf, 0x3, 0x2, 
       0x2, 0x2, 0xe4, 0xd, 0x3, 0x2, 0x2, 0x2, 0xe5, 0xe6, 0x5, 0x22, 0x12, 
       0x2, 0xe6, 0xe8, 0x5, 0x10, 0x9, 0x2, 0xe7, 0xe9, 0x5, 0x24, 0x13, 
       0x2, 0xe8, 0xe7, 0x3, 0x2, 0x2, 0x2, 0xe8, 0xe9, 0x3, 0x2, 0x2, 0x2, 
       0xe9, 0xeb, 0x3, 0x2, 0x2, 0x2, 0xea, 0xec, 0x5, 0x46, 0x24, 0x2, 
       0xeb, 0xea, 0x3, 0x2, 0x2, 0x2, 0xeb, 0xec, 0x3, 0x2, 0x2, 0x2, 0xec, 
       0xee, 0x3, 0x2, 0x2, 0x2, 0xed, 0xef, 0x5, 0x26, 0x14, 0x2, 0xee, 
       0xed, 0x3, 0x2, 0x2, 0x2, 0xee, 0xef, 0x3, 0x2, 0x2, 0x2, 0xef, 0xf1, 
       0x3, 0x2, 0x2, 0x2, 0xf0, 0xf2, 0x5, 0x68, 0x35, 0x2, 0xf1, 0xf0, 
       0x3, 0x2, 0x2, 0x2, 0xf1, 0xf2, 0x3, 0x2, 0x2, 0x2, 0xf2, 0xf, 0x3, 
       0x2, 0x2, 0x2, 0xf3, 0xf4, 0x7, 0x21, 0x2, 0x2, 0xf4, 0xf9, 0x5, 
       0x12, 0xa, 0x2, 0xf5, 0xf6, 0x7, 0x4, 0x2, 0x2, 0xf6, 0xf8, 0x5, 
       0x12, 0xa, 0x2, 0xf7, 0xf5, 0x3, 0x2, 0x2, 0x2, 0xf8, 0xfb, 0x3, 
       0x2, 0x2, 0x2, 0xf9, 0xf7, 0x3, 0x2, 0x2, 0x2, 0xf9, 0xfa, 0x3, 0x2, 
       0x2, 0x2, 0xfa, 0x11, 0x3, 0x2, 0x2, 0x2, 0xfb, 0xf9, 0x3, 0x2, 0x2, 
       0x2, 0xfc, 0x100, 0x5, 0x1a, 0xe, 0x2, 0xfd, 0xff, 0x5, 0x14, 0xb, 
       0x2, 0xfe, 0xfd, 0x3, 0x2, 0x2, 0x2, 0xff, 0x102, 0x3, 0x2, 0x2, 
       0x2, 0x100, 0xfe, 0x3, 0x2, 0x2, 0x2, 0x100, 0x101, 0x3, 0x2, 0x2, 
       0x2, 0x101, 0x13, 0x3, 0x2, 0x2, 0x2, 0x102, 0x100, 0x3, 0x2, 0x2, 
       0x2, 0x103, 0x104, 0x5, 0x16, 0xc, 0x2, 0x104, 0x105, 0x7, 0x2c, 
       0x2, 0x2, 0x105, 0x107, 0x5, 0x1a, 0xe, 0x2, 0x106, 0x108, 0x5, 0x18, 
       0xd, 0x2, 0x107, 0x106, 0x3, 0x2, 0x2, 0x2, 0x107, 0x108, 0x3, 0x2, 
       0x2, 0x2, 0x108, 0x10f, 0x3, 0x2, 0x2, 0x2, 0x109, 0x10a, 0x7, 0x33, 
       0x2, 0x2, 0x10a, 0x10b, 0x5, 0x16, 0xc, 0x2, 0x10b, 0x10c, 0x7, 0x2c, 
       0x2, 0x2, 0x10c, 0x10d, 0x5, 0x1a, 0xe, 0x2, 0x10d, 0x10f, 0x3, 0x2, 
       0x2, 0x2, 0x10e, 0x103, 0x3, 0x2, 0x2, 0x2, 0x10e, 0x109, 0x3, 0x2, 
       0x2, 0x2, 0x10f, 0x15, 0x3, 0x2, 0x2, 0x2, 0x110, 0x112, 0x7, 0x28, 
       0x2, 0x2, 0x111, 0x110, 0x3, 0x2, 0x2, 0x2, 0x111, 0x112, 0x3, 0x2, 
       0x2, 0x2, 0x112, 0x17, 0x3, 0x2, 0x2, 0x2, 0x113, 0x114, 0x7, 0x38, 
       0x2, 0x2, 0x114, 0x115, 0x5, 0x44, 0x23, 0x2, 0x115, 0x19, 0x3, 0x2, 
       0x2, 0x2, 0x116, 0x117, 0x5, 0x2e, 0x18, 0x2, 0x117, 0x118, 0x5, 
       0x2a, 0x16, 0x2, 0x118, 0x126, 0x3, 0x2, 0x2, 0x2, 0x119, 0x11a, 
       0x7, 0x5, 0x2, 0x2, 0x11a, 0x11b, 0x5, 0x6, 0x4, 0x2, 0x11b, 0x11c, 
       0x7, 0x6, 0x2, 0x2, 0x11c, 0x11d, 0x5, 0x2a, 0x16, 0x2, 0x11d, 0x126, 
       0x3, 0x2, 0x2, 0x2, 0x11e, 0x11f, 0x7, 0x5, 0x2, 0x2, 0x11f, 0x120, 
       0x5, 0x12, 0xa, 0x2, 0x120, 0x121, 0x7, 0x6, 0x2, 0x2, 0x121, 0x122, 
       0x5, 0x2a, 0x16, 0x2, 0x122, 0x126, 0x3, 0x2, 0x2, 0x2, 0x123, 0x126, 
       0x5, 0x28, 0x15, 0x2, 0x124, 0x126, 0x5, 0x1c, 0xf, 0x2, 0x125, 0x116, 
       0x3, 0x2, 0x2, 0x2, 0x125, 0x119, 0x3, 0x2, 0x2, 0x2, 0x125, 0x11e, 
       0x3, 0x2, 0x2, 0x2, 0x125, 0x123, 0x3, 0x2, 0x2, 0x2, 0x125, 0x124, 
       0x3, 0x2, 0x2, 0x2, 0x126, 0x1b, 0x3, 0x2, 0x2, 0x2, 0x127, 0x128, 
       0x5, 0x3c, 0x1f, 0x2, 0x128, 0x131, 0x7, 0x5, 0x2, 0x2, 0x129, 0x12e, 
       0x5, 0x42, 0x22, 0x2, 0x12a, 0x12b, 0x7, 0x4, 0x2, 0x2, 0x12b, 0x12d, 
       0x5, 0x42, 0x22, 0x2, 0x12c, 0x12a, 0x3, 0x2, 0x2, 0x2, 0x12d, 0x130, 
       0x3, 0x2, 0x2, 0x2, 0x12e, 0x12c, 0x3, 0x2, 0x2, 0x2, 0x12e, 0x12f, 
       0x3, 0x2, 0x2, 0x2, 0x12f, 0x132, 0x3, 0x2, 0x2, 0x2, 0x130, 0x12e, 
       0x3, 0x2, 0x2, 0x2, 0x131, 0x129, 0x3, 0x2, 0x2, 0x2, 0x131, 0x132, 
       0x3, 0x2, 0x2, 0x2, 0x132, 0x133, 0x3, 0x2, 0x2, 0x2, 0x133, 0x134, 
       0x7, 0x6, 0x2, 0x2, 0x134, 0x135, 0x5, 0x2a, 0x16, 0x2, 0x135, 0x1d, 
       0x3, 0x2, 0x2, 0x2, 0x136, 0x138, 0x5, 0x10, 0x9, 0x2, 0x137, 0x139, 
       0x5, 0x20, 0x11, 0x2, 0x138, 0x137, 0x3, 0x2, 0x2, 0x2, 0x139, 0x13a, 
       0x3, 0x2, 0x2, 0x2, 0x13a, 0x138, 0x3, 0x2, 0x2, 0x2, 0x13a, 0x13b, 
       0x3, 0x2, 0x2, 0x2, 0x13b, 0x1f, 0x3, 0x2, 0x2, 0x2, 0x13c, 0x13e, 
       0x5, 0x22, 0x12, 0x2, 0x13d, 0x13f, 0x5, 0x24, 0x13, 0x2, 0x13e, 
       0x13d, 0x3, 0x2, 0x2, 0x2, 0x13e, 0x13f, 0x3, 0x2, 0x2, 0x2, 0x13f, 
       0x141, 0x3, 0x2, 0x2, 0x2, 0x140, 0x142, 0x5, 0x48, 0x25, 0x2, 0x141, 
       0x140, 0x3, 0x2, 0x2, 0x2, 0x141, 0x142, 0x3, 0x2, 0x2, 0x2, 0x142, 
       0x21, 0x3, 0x2, 0x2, 0x2, 0x143, 0x147, 0x7, 0x40, 0x2, 0x2, 0x144, 
       0x146, 0x5, 0x96, 0x4c, 0x2, 0x145, 0x144, 0x3, 0x2, 0x2, 0x2, 0x146, 
       0x149, 0x3, 0x2, 0x2, 0x2, 0x147, 0x145, 0x3, 0x2, 0x2, 0x2, 0x147, 
       0x148, 0x3, 0x2, 0x2, 0x2, 0x148, 0x14a, 0x3, 0x2, 0x2, 0x2, 0x149, 
       0x147, 0x3, 0x2, 0x2, 0x2, 0x14a, 0x14b, 0x5, 0x40, 0x21, 0x2, 0x14b, 
       0x23, 0x3, 0x2, 0x2, 0x2, 0x14c, 0x14d, 0x7, 0x4e, 0x2, 0x2, 0x14d, 
       0x14e, 0x5, 0x44, 0x23, 0x2, 0x14e, 0x25, 0x3, 0x2, 0x2, 0x2, 0x14f, 
       0x150, 0x7, 0x25, 0x2, 0x2, 0x150, 0x151, 0x5, 0x44, 0x23, 0x2, 0x151, 
       0x27, 0x3, 0x2, 0x2, 0x2, 0x152, 0x153, 0x7, 0x4c, 0x2, 0x2, 0x153, 
       0x158, 0x5, 0x42, 0x22, 0x2, 0x154, 0x155, 0x7, 0x4, 0x2, 0x2, 0x155, 
       0x157, 0x5, 0x42, 0x22, 0x2, 0x156, 0x154, 0x3, 0x2, 0x2, 0x2, 0x157, 
       0x15a, 0x3, 0x2, 0x2, 0x2, 0x158, 0x156, 0x3, 0x2, 0x2, 0x2, 0x158, 
       0x159, 0x3, 0x2, 0x2, 0x2, 0x159, 0x15b, 0x3, 0x2, 0x2, 0x2, 0x15a, 
       0x158, 0x3, 0x2, 0x2, 0x2, 0x15b, 0x15c, 0x5, 0x2a, 0x16, 0x2, 0x15c, 
       0x29, 0x3, 0x2, 0x2, 0x2, 0x15d, 0x15f, 0x7, 0xe, 0x2, 0x2, 0x15e, 
       0x15d, 0x3, 0x2, 0x2, 0x2, 0x15e, 0x15f, 0x3, 0x2, 0x2, 0x2, 0x15f, 
       0x160, 0x3, 0x2, 0x2, 0x2, 0x160, 0x162, 0x5, 0x34, 0x1b, 0x2, 0x161, 
       0x163, 0x5, 0x38, 0x1d, 0x2, 0x162, 0x161, 0x3, 0x2, 0x2, 0x2, 0x162, 
       0x163, 0x3, 0x2, 0x2, 0x2, 0x163, 0x165, 0x3, 0x2, 0x2, 0x2, 0x164, 
       0x15e, 0x3, 0x2, 0x2, 0x2, 0x164, 0x165, 0x3, 0x2, 0x2, 0x2, 0x165, 
       0x2b, 0x3, 0x2, 0x2, 0x2, 0x166, 0x16b, 0x5, 0x2e, 0x18, 0x2, 0x167, 
       0x168, 0x7, 0x4, 0x2, 0x2, 0x168, 0x16a, 0x5, 0x2e, 0x18, 0x2, 0x169, 
       0x167, 0x3, 0x2, 0x2, 0x2, 0x16a, 0x16d, 0x3, 0x2, 0x2, 0x2, 0x16b, 
       0x169, 0x3, 0x2, 0x2, 0x2, 0x16b, 0x16c, 0x3, 0x2, 0x2, 0x2, 0x16c, 
       0x2d, 0x3, 0x2, 0x2, 0x2, 0x16d, 0x16b, 0x3, 0x2, 0x2, 0x2, 0x16e, 
       0x173, 0x5, 0x3c, 0x1f, 0x2, 0x16f, 0x170, 0x7, 0x7, 0x2, 0x2, 0x170, 
       0x172, 0x5, 0x3c, 0x1f, 0x2, 0x171, 0x16f, 0x3, 0x2, 0x2, 0x2, 0x172, 
       0x175, 0x3, 0x2, 0x2, 0x2, 0x173, 0x171, 0x3, 0x2, 0x2, 0x2, 0x173, 
       0x174, 0x3, 0x2, 0x2, 0x2, 0x174, 0x2f, 0x3, 0x2, 0x2, 0x2, 0x175, 
       0x173, 0x3, 0x2, 0x2, 0x2, 0x176, 0x17e, 0x5, 0x42, 0x22, 0x2, 0x177, 
       0x179, 0x7, 0xe, 0x2, 0x2, 0x178, 0x177, 0x3, 0x2, 0x2, 0x2, 0x178, 
       0x179, 0x3, 0x2, 0x2, 0x2, 0x179, 0x17c, 0x3, 0x2, 0x2, 0x2, 0x17a, 
       0x17d, 0x5, 0x3c, 0x1f, 0x2, 0x17b, 0x17d, 0x5, 0x38, 0x1d, 0x2, 
       0x17c, 0x17a, 0x3, 0x2, 0x2, 0x2, 0x17c, 0x17b, 0x3, 0x2, 0x2, 0x2, 
       0x17d, 0x17f, 0x3, 0x2, 0x2, 0x2, 0x17e, 0x178, 0x3, 0x2, 0x2, 0x2, 
       0x17e, 0x17f, 0x3, 0x2, 0x2, 0x2, 0x17f, 0x31, 0x3, 0x2, 0x2, 0x2, 
       0x180, 0x184, 0x5, 0x34, 0x1b, 0x2, 0x181, 0x182, 0x6, 0x1a, 0x3, 
       0x2, 0x182, 0x184, 0x5, 0xa4, 0x53, 0x2, 0x183, 0x180, 0x3, 0x2, 
       0x2, 0x2, 0x183, 0x181, 0x3, 0x2, 0x2, 0x2, 0x184, 0x33, 0x3, 0x2, 
       0x2, 0x2, 0x185, 0x18c, 0x7, 0x8b, 0x2, 0x2, 0x186, 0x18c, 0x5, 0x36, 
       0x1c, 0x2, 0x187, 0x188, 0x6, 0x1b, 0x4, 0x2, 0x188, 0x18c, 0x5, 
       0xa6, 0x54, 0x2, 0x189, 0x18a, 0x6, 0x1b, 0x5, 0x2, 0x18a, 0x18c, 
       0x5, 0xa8, 0x55, 0x2, 0x18b, 0x185, 0x3, 0x2, 0x2, 0x2, 0x18b, 0x186, 
       0x3, 0x2, 0x2, 0x2, 0x18b, 0x187, 0x3, 0x2, 0x2, 0x2, 0x18b, 0x189, 
       0x3, 0x2, 0x2, 0x2, 0x18c, 0x35, 0x3, 0x2, 0x2, 0x2, 0x18d, 0x18e, 
       0x7, 0xa, 0x2, 0x2, 0x18e, 0x37, 0x3, 0x2, 0x2, 0x2, 0x18f, 0x190, 
       0x7, 0x5, 0x2, 0x2, 0x190, 0x191, 0x5, 0x3a, 0x1e, 0x2, 0x191, 0x192, 
       0x7, 0x6, 0x2, 0x2, 0x192, 0x39, 0x3, 0x2, 0x2, 0x2, 0x193, 0x198, 
       0x5, 0x3c, 0x1f, 0x2, 0x194, 0x195, 0x7, 0x4, 0x2, 0x2, 0x195, 0x197, 
       0x5, 0x3c, 0x1f, 0x2, 0x196, 0x194, 0x3, 0x2, 0x2, 0x2, 0x197, 0x19a, 
       0x3, 0x2, 0x2, 0x2, 0x198, 0x196, 0x3, 0x2, 0x2, 0x2, 0x198, 0x199, 
       0x3, 0x2, 0x2, 0x2, 0x199, 0x3b, 0x3, 0x2, 0x2, 0x2, 0x19a, 0x198, 
       0x3, 0x2, 0x2, 0x2, 0x19b, 0x19c, 0x5, 0x32, 0x1a, 0x2, 0x19c, 0x19d, 
       0x5, 0x3e, 0x20, 0x2, 0x19d, 0x3d, 0x3, 0x2, 0x2, 0x2, 0x19e, 0x19f, 
       0x7, 0x78, 0x2, 0x2, 0x19f, 0x1a1, 0x5, 0x32, 0x1a, 0x2, 0x1a0, 0x19e, 
       0x3, 0x2, 0x2, 0x2, 0x1a1, 0x1a2, 0x3, 0x2, 0x2, 0x2, 0x1a2, 0x1a0, 
       0x3, 0x2, 0x2, 0x2, 0x1a2, 0x1a3, 0x3, 0x2, 0x2, 0x2, 0x1a3, 0x1a6, 
       0x3, 0x2, 0x2, 0x2, 0x1a4, 0x1a6, 0x3, 0x2, 0x2, 0x2, 0x1a5, 0x1a0, 
       0x3, 0x2, 0x2, 0x2, 0x1a5, 0x1a4, 0x3, 0x2, 0x2, 0x2, 0x1a6, 0x3f, 
       0x3, 0x2, 0x2, 0x2, 0x1a7, 0x1ac, 0x5, 0x30, 0x19, 0x2, 0x1a8, 0x1a9, 
       0x7, 0x4, 0x2, 0x2, 0x1a9, 0x1ab, 0x5, 0x30, 0x19, 0x2, 0x1aa, 0x1a8, 
       0x3, 0x2, 0x2, 0x2, 0x1ab, 0x1ae, 0x3, 0x2, 0x2, 0x2, 0x1ac, 0x1aa, 
       0x3, 0x2, 0x2, 0x2, 0x1ac, 0x1ad, 0x3, 0x2, 0x2, 0x2, 0x1ad, 0x41, 
       0x3, 0x2, 0x2, 0x2, 0x1ae, 0x1ac, 0x3, 0x2, 0x2, 0x2, 0x1af, 0x1b0, 
       0x5, 0x44, 0x23, 0x2, 0x1b0, 0x43, 0x3, 0x2, 0x2, 0x2, 0x1b1, 0x1b2, 
       0x8, 0x23, 0x1, 0x2, 0x1b2, 0x1b3, 0x7, 0x34, 0x2, 0x2, 0x1b3, 0x1be, 
       0x5, 0x44, 0x23, 0x7, 0x1b4, 0x1b5, 0x7, 0x1d, 0x2, 0x2, 0x1b5, 0x1b6, 
       0x7, 0x5, 0x2, 0x2, 0x1b6, 0x1b7, 0x5, 0x6, 0x4, 0x2, 0x1b7, 0x1b8, 
       0x7, 0x6, 0x2, 0x2, 0x1b8, 0x1be, 0x3, 0x2, 0x2, 0x2, 0x1b9, 0x1bb, 
       0x5, 0x92, 0x4a, 0x2, 0x1ba, 0x1bc, 0x5, 0x90, 0x49, 0x2, 0x1bb, 
       0x1ba, 0x3, 0x2, 0x2, 0x2, 0x1bb, 0x1bc, 0x3, 0x2, 0x2, 0x2, 0x1bc, 
       0x1be, 0x3, 0x2, 0x2, 0x2, 0x1bd, 0x1b1, 0x3, 0x2, 0x2, 0x2, 0x1bd, 
       0x1b4, 0x3, 0x2, 0x2, 0x2, 0x1bd, 0x1b9, 0x3, 0x2, 0x2, 0x2, 0x1be, 
       0x1c7, 0x3, 0x2, 0x2, 0x2, 0x1bf, 0x1c0, 0xc, 0x4, 0x2, 0x2, 0x1c0, 
       0x1c1, 0x7, 0xc, 0x2, 0x2, 0x1c1, 0x1c6, 0x5, 0x44, 0x23, 0x5, 0x1c2, 
       0x1c3, 0xc, 0x3, 0x2, 0x2, 0x1c3, 0x1c4, 0x7, 0x39, 0x2, 0x2, 0x1c4, 
       0x1c6, 0x5, 0x44, 0x23, 0x4, 0x1c5, 0x1bf, 0x3, 0x2, 0x2, 0x2, 0x1c5, 
       0x1c2, 0x3, 0x2, 0x2, 0x2, 0x1c6, 0x1c9, 0x3, 0x2, 0x2, 0x2, 0x1c7, 
       0x1c5, 0x3, 0x2, 0x2, 0x2, 0x1c7, 0x1c8, 0x3, 0x2, 0x2, 0x2, 0x1c8, 
       0x45, 0x3, 0x2, 0x2, 0x2, 0x1c9, 0x1c7, 0x3, 0x2, 0x2, 0x2, 0x1ca, 
       0x1cc, 0x5, 0x48, 0x25, 0x2, 0x1cb, 0x1ca, 0x3, 0x2, 0x2, 0x2, 0x1cb, 
       0x1cc, 0x3, 0x2, 0x2, 0x2, 0x1cc, 0x1cd, 0x3, 0x2, 0x2, 0x2, 0x1cd, 
       0x1cf, 0x5, 0x4c, 0x27, 0x2, 0x1ce, 0x1d0, 0x5, 0x4e, 0x28, 0x2, 
       0x1cf, 0x1ce, 0x3, 0x2, 0x2, 0x2, 0x1cf, 0x1d0, 0x3, 0x2, 0x2, 0x2, 
       0x1d0, 0x47, 0x3, 0x2, 0x2, 0x2, 0x1d1, 0x1d2, 0x7, 0x23, 0x2, 0x2, 
       0x1d2, 0x1d3, 0x7, 0x12, 0x2, 0x2, 0x1d3, 0x1d8, 0x5, 0x42, 0x22, 
       0x2, 0x1d4, 0x1d5, 0x7, 0x4, 0x2, 0x2, 0x1d5, 0x1d7, 0x5, 0x42, 0x22, 
       0x2, 0x1d6, 0x1d4, 0x3, 0x2, 0x2, 0x2, 0x1d7, 0x1da, 0x3, 0x2, 0x2, 
       0x2, 0x1d8, 0x1d6, 0x3, 0x2, 0x2, 0x2, 0x1d8, 0x1d9, 0x3, 0x2, 0x2, 
       0x2, 0x1d9, 0x1ec, 0x3, 0x2, 0x2, 0x2, 0x1da, 0x1d8, 0x3, 0x2, 0x2, 
       0x2, 0x1db, 0x1dc, 0x7, 0x50, 0x2, 0x2, 0x1dc, 0x1ed, 0x7, 0x3f, 
       0x2, 0x2, 0x1dd, 0x1de, 0x7, 0x50, 0x2, 0x2, 0x1de, 0x1ed, 0x7, 0x14, 
       0x2, 0x2, 0x1df, 0x1e0, 0x7, 0x24, 0x2, 0x2, 0x1e0, 0x1e1, 0x7, 0x41, 
       0x2, 0x2, 0x1e1, 0x1e2, 0x7, 0x5, 0x2, 0x2, 0x1e2, 0x1e7, 0x5, 0x4a, 
       0x26, 0x2, 0x1e3, 0x1e4, 0x7, 0x4, 0x2, 0x2, 0x1e4, 0x1e6, 0x5, 0x4a, 
       0x26, 0x2, 0x1e5, 0x1e3, 0x3, 0x2, 0x2, 0x2, 0x1e6, 0x1e9, 0x3, 0x2, 
       0x2, 0x2, 0x1e7, 0x1e5, 0x3, 0x2, 0x2, 0x2, 0x1e7, 0x1e8, 0x3, 0x2, 
       0x2, 0x2, 0x1e8, 0x1ea, 0x3, 0x2, 0x2, 0x2, 0x1e9, 0x1e7, 0x3, 0x2, 
       0x2, 0x2, 0x1ea, 0x1eb, 0x7, 0x6, 0x2, 0x2, 0x1eb, 0x1ed, 0x3, 0x2, 
       0x2, 0x2, 0x1ec, 0x1db, 0x3, 0x2, 0x2, 0x2, 0x1ec, 0x1dd, 0x3, 0x2, 
       0x2, 0x2, 0x1ec, 0x1df, 0x3, 0x2, 0x2, 0x2, 0x1ec, 0x1ed, 0x3, 0x2, 
       0x2, 0x2, 0x1ed, 0x1fe, 0x3, 0x2, 0x2, 0x2, 0x1ee, 0x1ef, 0x7, 0x23, 
       0x2, 0x2, 0x1ef, 0x1f0, 0x7, 0x12, 0x2, 0x2, 0x1f0, 0x1f1, 0x7, 0x24, 
       0x2, 0x2, 0x1f1, 0x1f2, 0x7, 0x41, 0x2, 0x2, 0x1f2, 0x1f3, 0x7, 0x5, 
       0x2, 0x2, 0x1f3, 0x1f8, 0x5, 0x4a, 0x26, 0x2, 0x1f4, 0x1f5, 0x7, 
       0x4, 0x2, 0x2, 0x1f5, 0x1f7, 0x5, 0x4a, 0x26, 0x2, 0x1f6, 0x1f4, 
       0x3, 0x2, 0x2, 0x2, 0x1f7, 0x1fa, 0x3, 0x2, 0x2, 0x2, 0x1f8, 0x1f6, 
       0x3, 0x2, 0x2, 0x2, 0x1f8, 0x1f9, 0x3, 0x2, 0x2, 0x2, 0x1f9, 0x1fb, 
       0x3, 0x2, 0x2, 0x2, 0x1fa, 0x1f8, 0x3, 0x2, 0x2, 0x2, 0x1fb, 0x1fc, 
       0x7, 0x6, 0x2, 0x2, 0x1fc, 0x1fe, 0x3, 0x2, 0x2, 0x2, 0x1fd, 0x1d1, 
       0x3, 0x2, 0x2, 0x2, 0x1fd, 0x1ee, 0x3, 0x2, 0x2, 0x2, 0x1fe, 0x49, 
       0x3, 0x2, 0x2, 0x2, 0x1ff, 0x208, 0x7, 0x5, 0x2, 0x2, 0x200, 0x205, 
       0x5, 0x42, 0x22, 0x2, 0x201, 0x202, 0x7, 0x4, 0x2, 0x2, 0x202, 0x204, 
       0x5, 0x42, 0x22, 0x2, 0x203, 0x201, 0x3, 0x2, 0x2, 0x2, 0x204, 0x207, 
       0x3, 0x2, 0x2, 0x2, 0x205, 0x203, 0x3, 0x2, 0x2, 0x2, 0x205, 0x206, 
       0x3, 0x2, 0x2, 0x2, 0x206, 0x209, 0x3, 0x2, 0x2, 0x2, 0x207, 0x205, 
       0x3, 0x2, 0x2, 0x2, 0x208, 0x200, 0x3, 0x2, 0x2, 0x2, 0x208, 0x209, 
       0x3, 0x2, 0x2, 0x2, 0x209, 0x20a, 0x3, 0x2, 0x2, 0x2, 0x20a, 0x20d, 
       0x7, 0x6, 0x2, 0x2, 0x20b, 0x20d, 0x5, 0x42, 0x22, 0x2, 0x20c, 0x1ff, 
       0x3, 0x2, 0x2, 0x2, 0x20c, 0x20b, 0x3, 0x2, 0x2, 0x2, 0x20d, 0x4b, 
       0x3, 0x2, 0x2, 0x2, 0x20e, 0x20f, 0x7, 0x4f, 0x2, 0x2, 0x20f, 0x210, 
       0x5, 0x52, 0x2a, 0x2, 0x210, 0x4d, 0x3, 0x2, 0x2, 0x2, 0x211, 0x212, 
       0x7, 0x60, 0x2, 0x2, 0x212, 0x213, 0x7, 0x5, 0x2, 0x2, 0x213, 0x214, 
       0x5, 0x50, 0x29, 0x2, 0x214, 0x215, 0x7, 0x6, 0x2, 0x2, 0x215, 0x4f, 
       0x3, 0x2, 0x2, 0x2, 0x216, 0x217, 0x5, 0x32, 0x1a, 0x2, 0x217, 0x218, 
       0x7, 0x4, 0x2, 0x2, 0x218, 0x219, 0x7, 0x85, 0x2, 0x2, 0x219, 0x21a, 
       0x5, 0x62, 0x32, 0x2, 0x21a, 0x51, 0x3, 0x2, 0x2, 0x2, 0x21b, 0x21f, 
       0x5, 0x54, 0x2b, 0x2, 0x21c, 0x21f, 0x5, 0x56, 0x2c, 0x2, 0x21d, 
       0x21f, 0x5, 0x58, 0x2d, 0x2, 0x21e, 0x21b, 0x3, 0x2, 0x2, 0x2, 0x21e, 
       0x21c, 0x3, 0x2, 0x2, 0x2, 0x21e, 0x21d, 0x3, 0x2, 0x2, 0x2, 0x21f, 
       0x53, 0x3, 0x2, 0x2, 0x2, 0x220, 0x221, 0x7, 0x51, 0x2, 0x2, 0x221, 
       0x225, 0x7, 0x5, 0x2, 0x2, 0x222, 0x223, 0x5, 0x64, 0x33, 0x2, 0x223, 
       0x224, 0x7, 0x4, 0x2, 0x2, 0x224, 0x226, 0x3, 0x2, 0x2, 0x2, 0x225, 
       0x222, 0x3, 0x2, 0x2, 0x2, 0x225, 0x226, 0x3, 0x2, 0x2, 0x2, 0x226, 
       0x227, 0x3, 0x2, 0x2, 0x2, 0x227, 0x228, 0x5, 0x5e, 0x30, 0x2, 0x228, 
       0x229, 0x7, 0x6, 0x2, 0x2, 0x229, 0x237, 0x3, 0x2, 0x2, 0x2, 0x22a, 
       0x22b, 0x7, 0x52, 0x2, 0x2, 0x22b, 0x22f, 0x7, 0x5, 0x2, 0x2, 0x22c, 
       0x22d, 0x5, 0x64, 0x33, 0x2, 0x22d, 0x22e, 0x7, 0x4, 0x2, 0x2, 0x22e, 
       0x230, 0x3, 0x2, 0x2, 0x2, 0x22f, 0x22c, 0x3, 0x2, 0x2, 0x2, 0x22f, 
       0x230, 0x3, 0x2, 0x2, 0x2, 0x230, 0x231, 0x3, 0x2, 0x2, 0x2, 0x231, 
       0x232, 0x5, 0x5e, 0x30, 0x2, 0x232, 0x233, 0x7, 0x4, 0x2, 0x2, 0x233, 
       0x234, 0x5, 0x60, 0x31, 0x2, 0x234, 0x235, 0x7, 0x6, 0x2, 0x2, 0x235, 
       0x237, 0x3, 0x2, 0x2, 0x2, 0x236, 0x220, 0x3, 0x2, 0x2, 0x2, 0x236, 
       0x22a, 0x3, 0x2, 0x2, 0x2, 0x237, 0x55, 0x3, 0x2, 0x2, 0x2, 0x238, 
       0x239, 0x7, 0x51, 0x2, 0x2, 0x239, 0x23a, 0x7, 0x5, 0x2, 0x2, 0x23a, 
       0x23b, 0x7, 0x85, 0x2, 0x2, 0x23b, 0x23c, 0x7, 0x6, 0x2, 0x2, 0x23c, 
       0x57, 0x3, 0x2, 0x2, 0x2, 0x23d, 0x23e, 0x7, 0x53, 0x2, 0x2, 0x23e, 
       0x23f, 0x7, 0x5, 0x2, 0x2, 0x23f, 0x242, 0x5, 0x5a, 0x2e, 0x2, 0x240, 
       0x241, 0x7, 0x4, 0x2, 0x2, 0x241, 0x243, 0x5, 0x5c, 0x2f, 0x2, 0x242, 
       0x240, 0x3, 0x2, 0x2, 0x2, 0x242, 0x243, 0x3, 0x2, 0x2, 0x2, 0x243, 
       0x244, 0x3, 0x2, 0x2, 0x2, 0x244, 0x245, 0x7, 0x6, 0x2, 0x2, 0x245, 
       0x59, 0x3, 0x2, 0x2, 0x2, 0x246, 0x247, 0x5, 0x42, 0x22, 0x2, 0x247, 
       0x5b, 0x3, 0x2, 0x2, 0x2, 0x248, 0x249, 0x7, 0x85, 0x2, 0x2, 0x249, 
       0x5d, 0x3, 0x2, 0x2, 0x2, 0x24a, 0x24b, 0x7, 0x54, 0x2, 0x2, 0x24b, 
       0x24c, 0x7, 0x85, 0x2, 0x2, 0x24c, 0x24d, 0x5, 0x62, 0x32, 0x2, 0x24d, 
       0x5f, 0x3, 0x2, 0x2, 0x2, 0x24e, 0x24f, 0x7, 0x55, 0x2, 0x2, 0x24f, 
       0x250, 0x7, 0x12, 0x2, 0x2, 0x250, 0x251, 0x7, 0x85, 0x2, 0x2, 0x251, 
       0x252, 0x5, 0x62, 0x32, 0x2, 0x252, 0x61, 0x3, 0x2, 0x2, 0x2, 0x253, 
       0x254, 0x9, 0x2, 0x2, 0x2, 0x254, 0x63, 0x3, 0x2, 0x2, 0x2, 0x255, 
       0x256, 0x7, 0x8b, 0x2, 0x2, 0x256, 0x65, 0x3, 0x2, 0x2, 0x2, 0x257, 
       0x258, 0x9, 0x3, 0x2, 0x2, 0x258, 0x67, 0x3, 0x2, 0x2, 0x2, 0x259, 
       0x25a, 0x7, 0x2a, 0x2, 0x2, 0x25a, 0x25c, 0x5, 0x6a, 0x36, 0x2, 0x25b, 
       0x25d, 0x7, 0xe, 0x2, 0x2, 0x25c, 0x25b, 0x3, 0x2, 0x2, 0x2, 0x25c, 
       0x25d, 0x3, 0x2, 0x2, 0x2, 0x25d, 0x69, 0x3, 0x2, 0x2, 0x2, 0x25e, 
       0x265, 0x5, 0x6c, 0x37, 0x2, 0x25f, 0x265, 0x5, 0x78, 0x3d, 0x2, 
       0x260, 0x265, 0x5, 0x82, 0x42, 0x2, 0x261, 0x265, 0x5, 0x86, 0x44, 
       0x2, 0x262, 0x265, 0x5, 0x8a, 0x46, 0x2, 0x263, 0x265, 0x5, 0x8c, 
       0x47, 0x2, 0x264, 0x25e, 0x3, 0x2, 0x2, 0x2, 0x264, 0x25f, 0x3, 0x2, 
       0x2, 0x2, 0x264, 0x260, 0x3, 0x2, 0x2, 0x2, 0x264, 0x261, 0x3, 0x2, 
       0x2, 0x2, 0x264, 0x262, 0x3, 0x2, 0x2, 0x2, 0x264, 0x263, 0x3, 0x2, 
       0x2, 0x2, 0x265, 0x6b, 0x3, 0x2, 0x2, 0x2, 0x266, 0x267, 0x5, 0x70, 
       0x39, 0x2, 0x267, 0x268, 0x7, 0x5, 0x2, 0x2, 0x268, 0x269, 0x5, 0x72, 
       0x3a, 0x2, 0x269, 0x26a, 0x7, 0x4, 0x2, 0x2, 0x26a, 0x26b, 0x5, 0x74, 
       0x3b, 0x2, 0x26b, 0x26c, 0x7, 0x4, 0x2, 0x2, 0x26c, 0x26d, 0x5, 0x76, 
       0x3c, 0x2, 0x26d, 0x26e, 0x7, 0x6, 0x2, 0x2, 0x26e, 0x6d, 0x3, 0x2, 
       0x2, 0x2, 0x26f, 0x271, 0x7, 0x34, 0x2, 0x2, 0x270, 0x26f, 0x3, 0x2, 
       0x2, 0x2, 0x270, 0x271, 0x3, 0x2, 0x2, 0x2, 0x271, 0x272, 0x3, 0x2, 
       0x2, 0x2, 0x272, 0x273, 0x7, 0x35, 0x2, 0x2, 0x273, 0x6f, 0x3, 0x2, 
       0x2, 0x2, 0x274, 0x275, 0x7, 0x62, 0x2, 0x2, 0x275, 0x71, 0x3, 0x2, 
       0x2, 0x2, 0x276, 0x277, 0x7, 0x8b, 0x2, 0x2, 0x277, 0x73, 0x3, 0x2, 
       0x2, 0x2, 0x278, 0x279, 0x9, 0x4, 0x2, 0x2, 0x279, 0x75, 0x3, 0x2, 
       0x2, 0x2, 0x27a, 0x27b, 0x7, 0x85, 0x2, 0x2, 0x27b, 0x77, 0x3, 0x2, 
       0x2, 0x2, 0x27c, 0x27d, 0x5, 0x7a, 0x3e, 0x2, 0x27d, 0x27e, 0x7, 
       0x5, 0x2, 0x2, 0x27e, 0x27f, 0x5, 0x7c, 0x3f, 0x2, 0x27f, 0x280, 
       0x7, 0x4, 0x2, 0x2, 0x280, 0x281, 0x5, 0x7e, 0x40, 0x2, 0x281, 0x282, 
       0x7, 0x4, 0x2, 0x2, 0x282, 0x283, 0x5, 0x80, 0x41, 0x2, 0x283, 0x284, 
       0x7, 0x6, 0x2, 0x2, 0x284, 0x79, 0x3, 0x2, 0x2, 0x2, 0x285, 0x286, 
       0x7, 0x63, 0x2, 0x2, 0x286, 0x7b, 0x3, 0x2, 0x2, 0x2, 0x287, 0x288, 
       0x7, 0x8b, 0x2, 0x2, 0x288, 0x7d, 0x3, 0x2, 0x2, 0x2, 0x289, 0x28a, 
       0x7, 0x8b, 0x2, 0x2, 0x28a, 0x7f, 0x3, 0x2, 0x2, 0x2, 0x28b, 0x28c, 
       0x7, 0x85, 0x2, 0x2, 0x28c, 0x81, 0x3, 0x2, 0x2, 0x2, 0x28d, 0x28e, 
       0x7, 0x64, 0x2, 0x2, 0x28e, 0x28f, 0x7, 0x5, 0x2, 0x2, 0x28f, 0x290, 
       0x7, 0x81, 0x2, 0x2, 0x290, 0x291, 0x7, 0x4, 0x2, 0x2, 0x291, 0x292, 
       0x5, 0x84, 0x43, 0x2, 0x292, 0x293, 0x7, 0x4, 0x2, 0x2, 0x293, 0x294, 
       0x7, 0x81, 0x2, 0x2, 0x294, 0x295, 0x7, 0x6, 0x2, 0x2, 0x295, 0x83, 
       0x3, 0x2, 0x2, 0x2, 0x296, 0x297, 0x9, 0x5, 0x2, 0x2, 0x297, 0x85, 
       0x3, 0x2, 0x2, 0x2, 0x298, 0x299, 0x7, 0x65, 0x2, 0x2, 0x299, 0x29a, 
       0x7, 0x5, 0x2, 0x2, 0x29a, 0x29b, 0x7, 0x81, 0x2, 0x2, 0x29b, 0x29c, 
       0x7, 0x4, 0x2, 0x2, 0x29c, 0x29d, 0x7, 0x81, 0x2, 0x2, 0x29d, 0x29e, 
       0x7, 0x4, 0x2, 0x2, 0x29e, 0x29f, 0x7, 0x81, 0x2, 0x2, 0x29f, 0x2a0, 
       0x7, 0x4, 0x2, 0x2, 0x2a0, 0x2a1, 0x7, 0x85, 0x2, 0x2, 0x2a1, 0x2a2, 
       0x7, 0x4, 0x2, 0x2, 0x2a2, 0x2a3, 0x5, 0x62, 0x32, 0x2, 0x2a3, 0x2a4, 
       0x7, 0x4, 0x2, 0x2, 0x2a4, 0x2a5, 0x7, 0x85, 0x2, 0x2, 0x2a5, 0x2a6, 
       0x7, 0x4, 0x2, 0x2, 0x2a6, 0x2a7, 0x5, 0x88, 0x45, 0x2, 0x2a7, 0x2a8, 
       0x7, 0x4, 0x2, 0x2, 0x2a8, 0x2a9, 0x7, 0x6e, 0x2, 0x2, 0x2a9, 0x2aa, 
       0x7, 0x6, 0x2, 0x2, 0x2aa, 0x87, 0x3, 0x2, 0x2, 0x2, 0x2ab, 0x2ac, 
       0x9, 0x6, 0x2, 0x2, 0x2ac, 0x89, 0x3, 0x2, 0x2, 0x2, 0x2ad, 0x2ae, 
       0x7, 0x66, 0x2, 0x2, 0x2ae, 0x2af, 0x7, 0x5, 0x2, 0x2, 0x2af, 0x2b0, 
       0x7, 0x81, 0x2, 0x2, 0x2b0, 0x2b1, 0x7, 0x4, 0x2, 0x2, 0x2b1, 0x2b2, 
       0x7, 0x81, 0x2, 0x2, 0x2b2, 0x2b3, 0x7, 0x4, 0x2, 0x2, 0x2b3, 0x2b4, 
       0x7, 0x81, 0x2, 0x2, 0x2b4, 0x2b5, 0x7, 0x4, 0x2, 0x2, 0x2b5, 0x2b6, 
       0x7, 0x81, 0x2, 0x2, 0x2b6, 0x2b7, 0x7, 0x6, 0x2, 0x2, 0x2b7, 0x8b, 
       0x3, 0x2, 0x2, 0x2, 0x2b8, 0x2b9, 0x7, 0x67, 0x2, 0x2, 0x2b9, 0x8d, 
       0x3, 0x2, 0x2, 0x2, 0x2ba, 0x2bc, 0x5, 0x42, 0x22, 0x2, 0x2bb, 0x2bd, 
       0x9, 0x7, 0x2, 0x2, 0x2bc, 0x2bb, 0x3, 0x2, 0x2, 0x2, 0x2bc, 0x2bd, 
       0x3, 0x2, 0x2, 0x2, 0x2bd, 0x2c0, 0x3, 0x2, 0x2, 0x2, 0x2be, 0x2bf, 
       0x7, 0x36, 0x2, 0x2, 0x2bf, 0x2c1, 0x9, 0x8, 0x2, 0x2, 0x2c0, 0x2be, 
       0x3, 0x2, 0x2, 0x2, 0x2c0, 0x2c1, 0x3, 0x2, 0x2, 0x2, 0x2c1, 0x8f, 
       0x3, 0x2, 0x2, 0x2, 0x2c2, 0x2c4, 0x7, 0x34, 0x2, 0x2, 0x2c3, 0x2c2, 
       0x3, 0x2, 0x2, 0x2, 0x2c3, 0x2c4, 0x3, 0x2, 0x2, 0x2, 0x2c4, 0x2c5, 
       0x3, 0x2, 0x2, 0x2, 0x2c5, 0x2c6, 0x7, 0x11, 0x2, 0x2, 0x2c6, 0x2c7, 
       0x5, 0x92, 0x4a, 0x2, 0x2c7, 0x2c8, 0x7, 0xc, 0x2, 0x2, 0x2c8, 0x2c9, 
       0x5, 0x92, 0x4a, 0x2, 0x2c9, 0x312, 0x3, 0x2, 0x2, 0x2, 0x2ca, 0x2cc, 
       0x7, 0x34, 0x2, 0x2, 0x2cb, 0x2ca, 0x3, 0x2, 0x2, 0x2, 0x2cb, 0x2cc, 
       0x3, 0x2, 0x2, 0x2, 0x2cc, 0x2cd, 0x3, 0x2, 0x2, 0x2, 0x2cd, 0x2ce, 
       0x7, 0x27, 0x2, 0x2, 0x2ce, 0x2cf, 0x7, 0x5, 0x2, 0x2, 0x2cf, 0x2d4, 
       0x5, 0x42, 0x22, 0x2, 0x2d0, 0x2d1, 0x7, 0x4, 0x2, 0x2, 0x2d1, 0x2d3, 
       0x5, 0x42, 0x22, 0x2, 0x2d2, 0x2d0, 0x3, 0x2, 0x2, 0x2, 0x2d3, 0x2d6, 
       0x3, 0x2, 0x2, 0x2, 0x2d4, 0x2d2, 0x3, 0x2, 0x2, 0x2, 0x2d4, 0x2d5, 
       0x3, 0x2, 0x2, 0x2, 0x2d5, 0x2d7, 0x3, 0x2, 0x2, 0x2, 0x2d6, 0x2d4, 
       0x3, 0x2, 0x2, 0x2, 0x2d7, 0x2d8, 0x7, 0x6, 0x2, 0x2, 0x2d8, 0x312, 
       0x3, 0x2, 0x2, 0x2, 0x2d9, 0x2db, 0x7, 0x34, 0x2, 0x2, 0x2da, 0x2d9, 
       0x3, 0x2, 0x2, 0x2, 0x2da, 0x2db, 0x3, 0x2, 0x2, 0x2, 0x2db, 0x2dc, 
       0x3, 0x2, 0x2, 0x2, 0x2dc, 0x2dd, 0x7, 0x27, 0x2, 0x2, 0x2dd, 0x2de, 
       0x7, 0x5, 0x2, 0x2, 0x2de, 0x2df, 0x5, 0x6, 0x4, 0x2, 0x2df, 0x2e0, 
       0x7, 0x6, 0x2, 0x2, 0x2e0, 0x312, 0x3, 0x2, 0x2, 0x2, 0x2e1, 0x2e3, 
       0x7, 0x34, 0x2, 0x2, 0x2e2, 0x2e1, 0x3, 0x2, 0x2, 0x2, 0x2e2, 0x2e3, 
       0x3, 0x2, 0x2, 0x2, 0x2e3, 0x2e4, 0x3, 0x2, 0x2, 0x2, 0x2e4, 0x2e5, 
       0x7, 0x3e, 0x2, 0x2, 0x2e5, 0x312, 0x5, 0x92, 0x4a, 0x2, 0x2e6, 0x2e8, 
       0x7, 0x34, 0x2, 0x2, 0x2e7, 0x2e6, 0x3, 0x2, 0x2, 0x2, 0x2e7, 0x2e8, 
       0x3, 0x2, 0x2, 0x2, 0x2e8, 0x2e9, 0x3, 0x2, 0x2, 0x2, 0x2e9, 0x2ea, 
       0x7, 0x2f, 0x2, 0x2, 0x2ea, 0x2f8, 0x9, 0x9, 0x2, 0x2, 0x2eb, 0x2ec, 
       0x7, 0x5, 0x2, 0x2, 0x2ec, 0x2f9, 0x7, 0x6, 0x2, 0x2, 0x2ed, 0x2ee, 
       0x7, 0x5, 0x2, 0x2, 0x2ee, 0x2f3, 0x5, 0x42, 0x22, 0x2, 0x2ef, 0x2f0, 
       0x7, 0x4, 0x2, 0x2, 0x2f0, 0x2f2, 0x5, 0x42, 0x22, 0x2, 0x2f1, 0x2ef, 
       0x3, 0x2, 0x2, 0x2, 0x2f2, 0x2f5, 0x3, 0x2, 0x2, 0x2, 0x2f3, 0x2f1, 
       0x3, 0x2, 0x2, 0x2, 0x2f3, 0x2f4, 0x3, 0x2, 0x2, 0x2, 0x2f4, 0x2f6, 
       0x3, 0x2, 0x2, 0x2, 0x2f5, 0x2f3, 0x3, 0x2, 0x2, 0x2, 0x2f6, 0x2f7, 
       0x7, 0x6, 0x2, 0x2, 0x2f7, 0x2f9, 0x3, 0x2, 0x2, 0x2, 0x2f8, 0x2eb, 
       0x3, 0x2, 0x2, 0x2, 0x2f8, 0x2ed, 0x3, 0x2, 0x2, 0x2, 0x2f9, 0x312, 
       0x3, 0x2, 0x2, 0x2, 0x2fa, 0x2fc, 0x7, 0x34, 0x2, 0x2, 0x2fb, 0x2fa, 
       0x3, 0x2, 0x2, 0x2, 0x2fb, 0x2fc, 0x3, 0x2, 0x2, 0x2, 0x2fc, 0x2fd, 
       0x3, 0x2, 0x2, 0x2, 0x2fd, 0x2fe, 0x7, 0x2f, 0x2, 0x2, 0x2fe, 0x301, 
       0x5, 0x92, 0x4a, 0x2, 0x2ff, 0x300, 0x7, 0x1c, 0x2, 0x2, 0x300, 0x302, 
       0x7, 0x81, 0x2, 0x2, 0x301, 0x2ff, 0x3, 0x2, 0x2, 0x2, 0x301, 0x302, 
       0x3, 0x2, 0x2, 0x2, 0x302, 0x312, 0x3, 0x2, 0x2, 0x2, 0x303, 0x304, 
       0x7, 0x2b, 0x2, 0x2, 0x304, 0x312, 0x5, 0x6e, 0x38, 0x2, 0x305, 0x307, 
       0x7, 0x2b, 0x2, 0x2, 0x306, 0x308, 0x7, 0x34, 0x2, 0x2, 0x307, 0x306, 
       0x3, 0x2, 0x2, 0x2, 0x307, 0x308, 0x3, 0x2, 0x2, 0x2, 0x308, 0x309, 
       0x3, 0x2, 0x2, 0x2, 0x309, 0x312, 0x9, 0xa, 0x2, 0x2, 0x30a, 0x30c, 
       0x7, 0x2b, 0x2, 0x2, 0x30b, 0x30d, 0x7, 0x34, 0x2, 0x2, 0x30c, 0x30b, 
       0x3, 0x2, 0x2, 0x2, 0x30c, 0x30d, 0x3, 0x2, 0x2, 0x2, 0x30d, 0x30e, 
       0x3, 0x2, 0x2, 0x2, 0x30e, 0x30f, 0x7, 0x17, 0x2, 0x2, 0x30f, 0x310, 
       0x7, 0x21, 0x2, 0x2, 0x310, 0x312, 0x5, 0x92, 0x4a, 0x2, 0x311, 0x2c3, 
       0x3, 0x2, 0x2, 0x2, 0x311, 0x2cb, 0x3, 0x2, 0x2, 0x2, 0x311, 0x2da, 
       0x3, 0x2, 0x2, 0x2, 0x311, 0x2e2, 0x3, 0x2, 0x2, 0x2, 0x311, 0x2e7, 
       0x3, 0x2, 0x2, 0x2, 0x311, 0x2fb, 0x3, 0x2, 0x2, 0x2, 0x311, 0x303, 
       0x3, 0x2, 0x2, 0x2, 0x311, 0x305, 0x3, 0x2, 0x2, 0x2, 0x311, 0x30a, 
       0x3, 0x2, 0x2, 0x2, 0x312, 0x91, 0x3, 0x2, 0x2, 0x2, 0x313, 0x314, 
       0x8, 0x4a, 0x1, 0x2, 0x314, 0x318, 0x5, 0x9a, 0x4e, 0x2, 0x315, 0x316, 
       0x9, 0xb, 0x2, 0x2, 0x316, 0x318, 0x5, 0x92, 0x4a, 0x9, 0x317, 0x313, 
       0x3, 0x2, 0x2, 0x2, 0x317, 0x315, 0x3, 0x2, 0x2, 0x2, 0x318, 0x32e, 
       0x3, 0x2, 0x2, 0x2, 0x319, 0x31a, 0xc, 0x8, 0x2, 0x2, 0x31a, 0x31b, 
       0x9, 0xc, 0x2, 0x2, 0x31b, 0x32d, 0x5, 0x92, 0x4a, 0x9, 0x31c, 0x31d, 
       0xc, 0x7, 0x2, 0x2, 0x31d, 0x31e, 0x9, 0xd, 0x2, 0x2, 0x31e, 0x32d, 
       0x5, 0x92, 0x4a, 0x8, 0x31f, 0x320, 0xc, 0x6, 0x2, 0x2, 0x320, 0x321, 
       0x7, 0x7d, 0x2, 0x2, 0x321, 0x32d, 0x5, 0x92, 0x4a, 0x7, 0x322, 0x323, 
       0xc, 0x5, 0x2, 0x2, 0x323, 0x324, 0x7, 0x80, 0x2, 0x2, 0x324, 0x32d, 
       0x5, 0x92, 0x4a, 0x6, 0x325, 0x326, 0xc, 0x4, 0x2, 0x2, 0x326, 0x327, 
       0x7, 0x7e, 0x2, 0x2, 0x327, 0x32d, 0x5, 0x92, 0x4a, 0x5, 0x328, 0x329, 
       0xc, 0x3, 0x2, 0x2, 0x329, 0x32a, 0x5, 0x94, 0x4b, 0x2, 0x32a, 0x32b, 
       0x5, 0x92, 0x4a, 0x4, 0x32b, 0x32d, 0x3, 0x2, 0x2, 0x2, 0x32c, 0x319, 
       0x3, 0x2, 0x2, 0x2, 0x32c, 0x31c, 0x3, 0x2, 0x2, 0x2, 0x32c, 0x31f, 
       0x3, 0x2, 0x2, 0x2, 0x32c, 0x322, 0x3, 0x2, 0x2, 0x2, 0x32c, 0x325, 
       0x3, 0x2, 0x2, 0x2, 0x32c, 0x328, 0x3, 0x2, 0x2, 0x2, 0x32d, 0x330, 
       0x3, 0x2, 0x2, 0x2, 0x32e, 0x32c, 0x3, 0x2, 0x2, 0x2, 0x32e, 0x32f, 
       0x3, 0x2, 0x2, 0x2, 0x32f, 0x93, 0x3, 0x2, 0x2, 0x2, 0x330, 0x32e, 
       0x3, 0x2, 0x2, 0x2, 0x331, 0x332, 0x9, 0xe, 0x2, 0x2, 0x332, 0x95, 
       0x3, 0x2, 0x2, 0x2, 0x333, 0x334, 0x7, 0x8, 0x2, 0x2, 0x334, 0x33b, 
       0x5, 0x98, 0x4d, 0x2, 0x335, 0x337, 0x7, 0x4, 0x2, 0x2, 0x336, 0x335, 
       0x3, 0x2, 0x2, 0x2, 0x336, 0x337, 0x3, 0x2, 0x2, 0x2, 0x337, 0x338, 
       0x3, 0x2, 0x2, 0x2, 0x338, 0x33a, 0x5, 0x98, 0x4d, 0x2, 0x339, 0x336, 
       0x3, 0x2, 0x2, 0x2, 0x33a, 0x33d, 0x3, 0x2, 0x2, 0x2, 0x33b, 0x339, 
       0x3, 0x2, 0x2, 0x2, 0x33b, 0x33c, 0x3, 0x2, 0x2, 0x2, 0x33c, 0x33e, 
       0x3, 0x2, 0x2, 0x2, 0x33d, 0x33b, 0x3, 0x2, 0x2, 0x2, 0x33e, 0x33f, 
       0x7, 0x9, 0x2, 0x2, 0x33f, 0x97, 0x3, 0x2, 0x2, 0x2, 0x340, 0x34e, 
       0x5, 0x32, 0x1a, 0x2, 0x341, 0x342, 0x5, 0x32, 0x1a, 0x2, 0x342, 
       0x343, 0x7, 0x5, 0x2, 0x2, 0x343, 0x348, 0x5, 0x9a, 0x4e, 0x2, 0x344, 
       0x345, 0x7, 0x4, 0x2, 0x2, 0x345, 0x347, 0x5, 0x9a, 0x4e, 0x2, 0x346, 
       0x344, 0x3, 0x2, 0x2, 0x2, 0x347, 0x34a, 0x3, 0x2, 0x2, 0x2, 0x348, 
       0x346, 0x3, 0x2, 0x2, 0x2, 0x348, 0x349, 0x3, 0x2, 0x2, 0x2, 0x349, 
       0x34b, 0x3, 0x2, 0x2, 0x2, 0x34a, 0x348, 0x3, 0x2, 0x2, 0x2, 0x34b, 
       0x34c, 0x7, 0x6, 0x2, 0x2, 0x34c, 0x34e, 0x3, 0x2, 0x2, 0x2, 0x34d, 
       0x340, 0x3, 0x2, 0x2, 0x2, 0x34d, 0x341, 0x3, 0x2, 0x2, 0x2, 0x34e, 
       0x99, 0x3, 0x2, 0x2, 0x2, 0x34f, 0x350, 0x8, 0x4e, 0x1, 0x2, 0x350, 
       0x378, 0x7, 0x79, 0x2, 0x2, 0x351, 0x352, 0x5, 0x9c, 0x4f, 0x2, 0x352, 
       0x353, 0x7, 0x7, 0x2, 0x2, 0x353, 0x354, 0x7, 0x79, 0x2, 0x2, 0x354, 
       0x378, 0x3, 0x2, 0x2, 0x2, 0x355, 0x356, 0x7, 0x5, 0x2, 0x2, 0x356, 
       0x357, 0x5, 0x6, 0x4, 0x2, 0x357, 0x358, 0x7, 0x6, 0x2, 0x2, 0x358, 
       0x378, 0x3, 0x2, 0x2, 0x2, 0x359, 0x35a, 0x7, 0x5, 0x2, 0x2, 0x35a, 
       0x35d, 0x5, 0x30, 0x19, 0x2, 0x35b, 0x35c, 0x7, 0x4, 0x2, 0x2, 0x35c, 
       0x35e, 0x5, 0x30, 0x19, 0x2, 0x35d, 0x35b, 0x3, 0x2, 0x2, 0x2, 0x35e, 
       0x35f, 0x3, 0x2, 0x2, 0x2, 0x35f, 0x35d, 0x3, 0x2, 0x2, 0x2, 0x35f, 
       0x360, 0x3, 0x2, 0x2, 0x2, 0x360, 0x361, 0x3, 0x2, 0x2, 0x2, 0x361, 
       0x362, 0x7, 0x6, 0x2, 0x2, 0x362, 0x378, 0x3, 0x2, 0x2, 0x2, 0x363, 
       0x364, 0x5, 0x66, 0x34, 0x2, 0x364, 0x36d, 0x7, 0x5, 0x2, 0x2, 0x365, 
       0x36a, 0x5, 0x42, 0x22, 0x2, 0x366, 0x367, 0x7, 0x4, 0x2, 0x2, 0x367, 
       0x369, 0x5, 0x42, 0x22, 0x2, 0x368, 0x366, 0x3, 0x2, 0x2, 0x2, 0x369, 
       0x36c, 0x3, 0x2, 0x2, 0x2, 0x36a, 0x368, 0x3, 0x2, 0x2, 0x2, 0x36a, 
       0x36b, 0x3, 0x2, 0x2, 0x2, 0x36b, 0x36e, 0x3, 0x2, 0x2, 0x2, 0x36c, 
       0x36a, 0x3, 0x2, 0x2, 0x2, 0x36d, 0x365, 0x3, 0x2, 0x2, 0x2, 0x36d, 
       0x36e, 0x3, 0x2, 0x2, 0x2, 0x36e, 0x36f, 0x3, 0x2, 0x2, 0x2, 0x36f, 
       0x370, 0x7, 0x6, 0x2, 0x2, 0x370, 0x378, 0x3, 0x2, 0x2, 0x2, 0x371, 
       0x372, 0x7, 0x5, 0x2, 0x2, 0x372, 0x373, 0x5, 0x42, 0x22, 0x2, 0x373, 
       0x374, 0x7, 0x6, 0x2, 0x2, 0x374, 0x378, 0x3, 0x2, 0x2, 0x2, 0x375, 
       0x378, 0x5, 0xa0, 0x51, 0x2, 0x376, 0x378, 0x5, 0x32, 0x1a, 0x2, 
       0x377, 0x34f, 0x3, 0x2, 0x2, 0x2, 0x377, 0x351, 0x3, 0x2, 0x2, 0x2, 
       0x377, 0x355, 0x3, 0x2, 0x2, 0x2, 0x377, 0x359, 0x3, 0x2, 0x2, 0x2, 
       0x377, 0x363, 0x3, 0x2, 0x2, 0x2, 0x377, 0x371, 0x3, 0x2, 0x2, 0x2, 
       0x377, 0x375, 0x3, 0x2, 0x2, 0x2, 0x377, 0x376, 0x3, 0x2, 0x2, 0x2, 
       0x378, 0x37e, 0x3, 0x2, 0x2, 0x2, 0x379, 0x37a, 0xc, 0x9, 0x2, 0x2, 
       0x37a, 0x37b, 0x7, 0x7, 0x2, 0x2, 0x37b, 0x37d, 0x5, 0x32, 0x1a, 
       0x2, 0x37c, 0x379, 0x3, 0x2, 0x2, 0x2, 0x37d, 0x380, 0x3, 0x2, 0x2, 
       0x2, 0x37e, 0x37c, 0x3, 0x2, 0x2, 0x2, 0x37e, 0x37f, 0x3, 0x2, 0x2, 
       0x2, 0x37f, 0x9b, 0x3, 0x2, 0x2, 0x2, 0x380, 0x37e, 0x3, 0x2, 0x2, 
       0x2, 0x381, 0x386, 0x5, 0x32, 0x1a, 0x2, 0x382, 0x383, 0x7, 0x7, 
       0x2, 0x2, 0x383, 0x385, 0x5, 0x32, 0x1a, 0x2, 0x384, 0x382, 0x3, 
       0x2, 0x2, 0x2, 0x385, 0x388, 0x3, 0x2, 0x2, 0x2, 0x386, 0x384, 0x3, 
       0x2, 0x2, 0x2, 0x386, 0x387, 0x3, 0x2, 0x2, 0x2, 0x387, 0x9d, 0x3, 
       0x2, 0x2, 0x2, 0x388, 0x386, 0x3, 0x2, 0x2, 0x2, 0x389, 0x38b, 0x6, 
       0x50, 0xf, 0x2, 0x38a, 0x38c, 0x7, 0x78, 0x2, 0x2, 0x38b, 0x38a, 
       0x3, 0x2, 0x2, 0x2, 0x38b, 0x38c, 0x3, 0x2, 0x2, 0x2, 0x38c, 0x38d, 
       0x3, 0x2, 0x2, 0x2, 0x38d, 0x3b5, 0x7, 0x86, 0x2, 0x2, 0x38e, 0x390, 
       0x6, 0x50, 0x10, 0x2, 0x38f, 0x391, 0x7, 0x78, 0x2, 0x2, 0x390, 0x38f, 
       0x3, 0x2, 0x2, 0x2, 0x390, 0x391, 0x3, 0x2, 0x2, 0x2, 0x391, 0x392, 
       0x3, 0x2, 0x2, 0x2, 0x392, 0x3b5, 0x7, 0x87, 0x2, 0x2, 0x393, 0x395, 
       0x6, 0x50, 0x11, 0x2, 0x394, 0x396, 0x7, 0x78, 0x2, 0x2, 0x395, 0x394, 
       0x3, 0x2, 0x2, 0x2, 0x395, 0x396, 0x3, 0x2, 0x2, 0x2, 0x396, 0x397, 
       0x3, 0x2, 0x2, 0x2, 0x397, 0x3b5, 0x9, 0xf, 0x2, 0x2, 0x398, 0x39a, 
       0x7, 0x78, 0x2, 0x2, 0x399, 0x398, 0x3, 0x2, 0x2, 0x2, 0x399, 0x39a, 
       0x3, 0x2, 0x2, 0x2, 0x39a, 0x39b, 0x3, 0x2, 0x2, 0x2, 0x39b, 0x3b5, 
       0x7, 0x85, 0x2, 0x2, 0x39c, 0x39e, 0x7, 0x78, 0x2, 0x2, 0x39d, 0x39c, 
       0x3, 0x2, 0x2, 0x2, 0x39d, 0x39e, 0x3, 0x2, 0x2, 0x2, 0x39e, 0x39f, 
       0x3, 0x2, 0x2, 0x2, 0x39f, 0x3b5, 0x7, 0x82, 0x2, 0x2, 0x3a0, 0x3a2, 
       0x7, 0x78, 0x2, 0x2, 0x3a1, 0x3a0, 0x3, 0x2, 0x2, 0x2, 0x3a1, 0x3a2, 
       0x3, 0x2, 0x2, 0x2, 0x3a2, 0x3a3, 0x3, 0x2, 0x2, 0x2, 0x3a3, 0x3b5, 
       0x7, 0x83, 0x2, 0x2, 0x3a4, 0x3a6, 0x7, 0x78, 0x2, 0x2, 0x3a5, 0x3a4, 
       0x3, 0x2, 0x2, 0x2, 0x3a5, 0x3a6, 0x3, 0x2, 0x2, 0x2, 0x3a6, 0x3a7, 
       0x3, 0x2, 0x2, 0x2, 0x3a7, 0x3b5, 0x7, 0x84, 0x2, 0x2, 0x3a8, 0x3aa, 
       0x7, 0x78, 0x2, 0x2, 0x3a9, 0x3a8, 0x3, 0x2, 0x2, 0x2, 0x3a9, 0x3aa, 
       0x3, 0x2, 0x2, 0x2, 0x3aa, 0x3ab, 0x3, 0x2, 0x2, 0x2, 0x3ab, 0x3b5, 
       0x7, 0x89, 0x2, 0x2, 0x3ac, 0x3ae, 0x7, 0x78, 0x2, 0x2, 0x3ad, 0x3ac, 
       0x3, 0x2, 0x2, 0x2, 0x3ad, 0x3ae, 0x3, 0x2, 0x2, 0x2, 0x3ae, 0x3af, 
       0x3, 0x2, 0x2, 0x2, 0x3af, 0x3b5, 0x7, 0x88, 0x2, 0x2, 0x3b0, 0x3b2, 
       0x7, 0x78, 0x2, 0x2, 0x3b1, 0x3b0, 0x3, 0x2, 0x2, 0x2, 0x3b1, 0x3b2, 
       0x3, 0x2, 0x2, 0x2, 0x3b2, 0x3b3, 0x3, 0x2, 0x2, 0x2, 0x3b3, 0x3b5, 
       0x7, 0x8a, 0x2, 0x2, 0x3b4, 0x389, 0x3, 0x2, 0x2, 0x2, 0x3b4, 0x38e, 
       0x3, 0x2, 0x2, 0x2, 0x3b4, 0x393, 0x3, 0x2, 0x2, 0x2, 0x3b4, 0x399, 
       0x3, 0x2, 0x2, 0x2, 0x3b4, 0x39d, 0x3, 0x2, 0x2, 0x2, 0x3b4, 0x3a1, 
       0x3, 0x2, 0x2, 0x2, 0x3b4, 0x3a5, 0x3, 0x2, 0x2, 0x2, 0x3b4, 0x3a9, 
       0x3, 0x2, 0x2, 0x2, 0x3b4, 0x3ad, 0x3, 0x2, 0x2, 0x2, 0x3b4, 0x3b1, 
       0x3, 0x2, 0x2, 0x2, 0x3b5, 0x9f, 0x3, 0x2, 0x2, 0x2, 0x3b6, 0x3c2, 
       0x7, 0x35, 0x2, 0x2, 0x3b7, 0x3b8, 0x5, 0x32, 0x1a, 0x2, 0x3b8, 0x3b9, 
       0x7, 0x81, 0x2, 0x2, 0x3b9, 0x3c2, 0x3, 0x2, 0x2, 0x2, 0x3ba, 0x3c2, 
       0x5, 0x9e, 0x50, 0x2, 0x3bb, 0x3c2, 0x5, 0xa2, 0x52, 0x2, 0x3bc, 
       0x3be, 0x7, 0x81, 0x2, 0x2, 0x3bd, 0x3bc, 0x3, 0x2, 0x2, 0x2, 0x3be, 
       0x3bf, 0x3, 0x2, 0x2, 0x2, 0x3bf, 0x3bd, 0x3, 0x2, 0x2, 0x2, 0x3bf, 
       0x3c0, 0x3, 0x2, 0x2, 0x2, 0x3c0, 0x3c2, 0x3, 0x2, 0x2, 0x2, 0x3c1, 
       0x3b6, 0x3, 0x2, 0x2, 0x2, 0x3c1, 0x3b7, 0x3, 0x2, 0x2, 0x2, 0x3c1, 
       0x3ba, 0x3, 0x2, 0x2, 0x2, 0x3c1, 0x3bb, 0x3, 0x2, 0x2, 0x2, 0x3c1, 
       0x3bd, 0x3, 0x2, 0x2, 0x2, 0x3c2, 0xa1, 0x3, 0x2, 0x2, 0x2, 0x3c3, 
       0x3c4, 0x9, 0x10, 0x2, 0x2, 0x3c4, 0xa3, 0x3, 0x2, 0x2, 0x2, 0x3c5, 
       0x3c6, 0x9, 0x11, 0x2, 0x2, 0x3c6, 0xa5, 0x3, 0x2, 0x2, 0x2, 0x3c7, 
       0x3c8, 0x9, 0x12, 0x2, 0x2, 0x3c8, 0xa7, 0x3, 0x2, 0x2, 0x2, 0x3c9, 
       0x3ca, 0x9, 0x13, 0x2, 0x2, 0x3ca, 0xa9, 0x3, 0x2, 0x2, 0x2, 0x68, 
       0xae, 0xbf, 0xc2, 0xc7, 0xc9, 0xcd, 0xd7, 0xe3, 0xe8, 0xeb, 0xee, 
       0xf1, 0xf9, 0x100, 0x107, 0x10e, 0x111, 0x125, 0x12e, 0x131, 0x13a, 
       0x13e, 0x141, 0x147, 0x158, 0x15e, 0x162, 0x164, 0x16b, 0x173, 0x178, 
       0x17c, 0x17e, 0x183, 0x18b, 0x198, 0x1a2, 0x1a5, 0x1ac, 0x1bb, 0x1bd, 
       0x1c5, 0x1c7, 0x1cb, 0x1cf, 0x1d8, 0x1e7, 0x1ec, 0x1f8, 0x1fd, 0x205, 
       0x208, 0x20c, 0x21e, 0x225, 0x22f, 0x236, 0x242, 0x25c, 0x264, 0x270, 
       0x2bc, 0x2c0, 0x2c3, 0x2cb, 0x2d4, 0x2da, 0x2e2, 0x2e7, 0x2f3, 0x2f8, 
       0x2fb, 0x301, 0x307, 0x30c, 0x311, 0x317, 0x32c, 0x32e, 0x336, 0x33b, 
       0x348, 0x34d, 0x35f, 0x36a, 0x36d, 0x377, 0x37e, 0x386, 0x38b, 0x390, 
       0x395, 0x399, 0x39d, 0x3a1, 0x3a5, 0x3a9, 0x3ad, 0x3b1, 0x3b4, 0x3bf, 
       0x3c1, 
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
