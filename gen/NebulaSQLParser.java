// Generated from /home/rudi/dima/nebulastream-public/nes-nebuli/parser/NebulaSQL.g4 by ANTLR 4.13.2
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue", "this-escape"})
public class NebulaSQLParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, BACKQUOTED_IDENTIFIER=8, 
		ALL=9, AND=10, ANY=11, AS=12, ASC=13, AT=14, BETWEEN=15, BY=16, COMMENT=17, 
		CUBE=18, DELETE=19, DESC=20, DISTINCT=21, DIV=22, DROP=23, ELSE=24, END=25, 
		ESCAPE=26, EXISTS=27, FALSE=28, FIRST=29, FOR=30, FROM=31, FULL=32, GROUP=33, 
		GROUPING=34, HAVING=35, IF=36, IN=37, INNER=38, INSERT=39, INTO=40, IS=41, 
		JOIN=42, LAST=43, LEFT=44, LIKE=45, LIMIT=46, LIST=47, MERGE=48, NATURAL=49, 
		NOT=50, NULLTOKEN=51, NULLS=52, OF=53, ON=54, OR=55, ORDER=56, QUERY=57, 
		RECOVER=58, RIGHT=59, RLIKE=60, ROLLUP=61, SELECT=62, SETS=63, SOME=64, 
		START=65, TABLE=66, TO=67, TRUE=68, TYPE=69, UNION=70, UNKNOWN=71, USE=72, 
		USING=73, VALUES=74, WHEN=75, WHERE=76, WINDOW=77, WITH=78, TUMBLING=79, 
		SLIDING=80, THRESHOLD=81, SIZE=82, ADVANCE=83, MS=84, SEC=85, MIN=86, 
		HOUR=87, DAY=88, MAX=89, AVG=90, SUM=91, COUNT=92, MEDIAN=93, WATERMARK=94, 
		OFFSET=95, FILE=96, PRINT=97, LOCALHOST=98, CSV_FORMAT=99, AT_MOST_ONCE=100, 
		AT_LEAST_ONCE=101, BOOLEAN_VALUE=102, EQ=103, NSEQ=104, NEQ=105, NEQJ=106, 
		LT=107, LTE=108, GT=109, GTE=110, PLUS=111, MINUS=112, ASTERISK=113, SLASH=114, 
		PERCENT=115, TILDE=116, AMPERSAND=117, PIPE=118, CONCAT_PIPE=119, HAT=120, 
		STRING=121, BIGINT_LITERAL=122, SMALLINT_LITERAL=123, TINYINT_LITERAL=124, 
		INTEGER_VALUE=125, EXPONENT_VALUE=126, DECIMAL_VALUE=127, FLOAT_LITERAL=128, 
		DOUBLE_LITERAL=129, BIGDECIMAL_LITERAL=130, IDENTIFIER=131, SIMPLE_COMMENT=132, 
		BRACKETED_COMMENT=133, WS=134, FOUR_OCTETS=135, OCTET=136, UNRECOGNIZED=137;
	public static final int
		RULE_singleStatement = 0, RULE_statement = 1, RULE_query = 2, RULE_queryOrganization = 3, 
		RULE_queryTerm = 4, RULE_queryPrimary = 5, RULE_querySpecification = 6, 
		RULE_fromClause = 7, RULE_relation = 8, RULE_joinRelation = 9, RULE_joinType = 10, 
		RULE_joinCriteria = 11, RULE_relationPrimary = 12, RULE_functionTable = 13, 
		RULE_fromStatement = 14, RULE_fromStatementBody = 15, RULE_selectClause = 16, 
		RULE_whereClause = 17, RULE_havingClause = 18, RULE_inlineTable = 19, 
		RULE_tableAlias = 20, RULE_multipartIdentifierList = 21, RULE_multipartIdentifier = 22, 
		RULE_namedExpression = 23, RULE_identifier = 24, RULE_strictIdentifier = 25, 
		RULE_quotedIdentifier = 26, RULE_identifierList = 27, RULE_identifierSeq = 28, 
		RULE_errorCapturingIdentifier = 29, RULE_errorCapturingIdentifierExtra = 30, 
		RULE_namedExpressionSeq = 31, RULE_expression = 32, RULE_booleanExpression = 33, 
		RULE_windowedAggregationClause = 34, RULE_aggregationClause = 35, RULE_groupingSet = 36, 
		RULE_windowClause = 37, RULE_watermarkClause = 38, RULE_watermarkParameters = 39, 
		RULE_windowSpec = 40, RULE_timeWindow = 41, RULE_countWindow = 42, RULE_conditionWindow = 43, 
		RULE_conditionParameter = 44, RULE_thresholdMinSizeParameter = 45, RULE_sizeParameter = 46, 
		RULE_advancebyParameter = 47, RULE_timeUnit = 48, RULE_timestampParameter = 49, 
		RULE_functionName = 50, RULE_sinkClause = 51, RULE_sinkType = 52, RULE_nullNotnull = 53, 
		RULE_streamName = 54, RULE_sinkTypeFile = 55, RULE_sinkTypePrint = 56, 
		RULE_fileFormat = 57, RULE_sortItem = 58, RULE_predicate = 59, RULE_valueExpression = 60, 
		RULE_comparisonOperator = 61, RULE_hint = 62, RULE_hintStatement = 63, 
		RULE_primaryExpression = 64, RULE_qualifiedName = 65, RULE_number = 66, 
		RULE_constant = 67, RULE_booleanValue = 68, RULE_strictNonReserved = 69, 
		RULE_ansiNonReserved = 70, RULE_nonReserved = 71;
	private static String[] makeRuleNames() {
		return new String[] {
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
			"nullNotnull", "streamName", "sinkTypeFile", "sinkTypePrint", "fileFormat", 
			"sortItem", "predicate", "valueExpression", "comparisonOperator", "hint", 
			"hintStatement", "primaryExpression", "qualifiedName", "number", "constant", 
			"booleanValue", "strictNonReserved", "ansiNonReserved", "nonReserved"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "';'", "','", "'('", "')'", "'.'", "'/*+'", "'*/'", null, null, 
			null, "'ANY'", null, null, "'AT'", null, null, "'COMMENT'", "'CUBE'", 
			"'DELETE'", null, "'DISTINCT'", "'DIV'", "'DROP'", "'ELSE'", "'END'", 
			"'ESCAPE'", "'EXISTS'", "'FALSE'", "'FIRST'", "'FOR'", null, "'FULL'", 
			null, "'GROUPING'", null, "'IF'", null, null, null, null, null, null, 
			"'LAST'", "'LEFT'", "'LIKE'", null, "'LIST'", null, "'NATURAL'", null, 
			"'NULL'", "'NULLS'", "'OF'", null, null, null, "'QUERY'", "'RECOVER'", 
			"'RIGHT'", null, "'ROLLUP'", null, "'SETS'", "'SOME'", "'START'", "'TABLE'", 
			"'TO'", "'TRUE'", "'TYPE'", null, "'UNKNOWN'", "'USE'", "'USING'", "'VALUES'", 
			"'WHEN'", null, null, "'WITH'", null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, "'File'", 
			"'Print'", null, "'CSV_FORMAT'", "'AT_MOST_ONCE'", "'AT_LEAST_ONCE'", 
			null, null, "'<=>'", "'<>'", "'!='", "'<'", null, "'>'", null, "'+'", 
			"'-'", "'*'", "'/'", "'%'", "'~'", "'&'", "'|'", "'||'", "'^'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, "BACKQUOTED_IDENTIFIER", 
			"ALL", "AND", "ANY", "AS", "ASC", "AT", "BETWEEN", "BY", "COMMENT", "CUBE", 
			"DELETE", "DESC", "DISTINCT", "DIV", "DROP", "ELSE", "END", "ESCAPE", 
			"EXISTS", "FALSE", "FIRST", "FOR", "FROM", "FULL", "GROUP", "GROUPING", 
			"HAVING", "IF", "IN", "INNER", "INSERT", "INTO", "IS", "JOIN", "LAST", 
			"LEFT", "LIKE", "LIMIT", "LIST", "MERGE", "NATURAL", "NOT", "NULLTOKEN", 
			"NULLS", "OF", "ON", "OR", "ORDER", "QUERY", "RECOVER", "RIGHT", "RLIKE", 
			"ROLLUP", "SELECT", "SETS", "SOME", "START", "TABLE", "TO", "TRUE", "TYPE", 
			"UNION", "UNKNOWN", "USE", "USING", "VALUES", "WHEN", "WHERE", "WINDOW", 
			"WITH", "TUMBLING", "SLIDING", "THRESHOLD", "SIZE", "ADVANCE", "MS", 
			"SEC", "MIN", "HOUR", "DAY", "MAX", "AVG", "SUM", "COUNT", "MEDIAN", 
			"WATERMARK", "OFFSET", "FILE", "PRINT", "LOCALHOST", "CSV_FORMAT", "AT_MOST_ONCE", 
			"AT_LEAST_ONCE", "BOOLEAN_VALUE", "EQ", "NSEQ", "NEQ", "NEQJ", "LT", 
			"LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
			"TILDE", "AMPERSAND", "PIPE", "CONCAT_PIPE", "HAT", "STRING", "BIGINT_LITERAL", 
			"SMALLINT_LITERAL", "TINYINT_LITERAL", "INTEGER_VALUE", "EXPONENT_VALUE", 
			"DECIMAL_VALUE", "FLOAT_LITERAL", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL", 
			"IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", "FOUR_OCTETS", 
			"OCTET", "UNRECOGNIZED"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "NebulaSQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }


	      bool SQL_standard_keyword_behavior = false;
	      bool legacy_exponent_literal_as_decimal_enabled = false;

	public NebulaSQLParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(NebulaSQLParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(144);
			statement();
			setState(148);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0) {
				{
				{
				setState(145);
				match(T__0);
				}
				}
				setState(150);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(151);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StatementContext extends ParserRuleContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(153);
			query();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryContext extends ParserRuleContext {
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_query);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(155);
			queryTerm(0);
			setState(156);
			queryOrganization();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryOrganizationContext extends ParserRuleContext {
		public SortItemContext sortItem;
		public List<SortItemContext> order = new ArrayList<SortItemContext>();
		public Token limit;
		public Token offset;
		public TerminalNode ORDER() { return getToken(NebulaSQLParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(NebulaSQLParser.BY, 0); }
		public TerminalNode LIMIT() { return getToken(NebulaSQLParser.LIMIT, 0); }
		public TerminalNode OFFSET() { return getToken(NebulaSQLParser.OFFSET, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(NebulaSQLParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(NebulaSQLParser.INTEGER_VALUE, i);
		}
		public TerminalNode ALL() { return getToken(NebulaSQLParser.ALL, 0); }
		public QueryOrganizationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryOrganization; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterQueryOrganization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitQueryOrganization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitQueryOrganization(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryOrganizationContext queryOrganization() throws RecognitionException {
		QueryOrganizationContext _localctx = new QueryOrganizationContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_queryOrganization);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(168);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(158);
				match(ORDER);
				setState(159);
				match(BY);
				setState(160);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(165);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(161);
					match(T__1);
					setState(162);
					((QueryOrganizationContext)_localctx).sortItem = sortItem();
					((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
					}
					}
					setState(167);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(175);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(170);
				match(LIMIT);
				setState(173);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ALL:
					{
					setState(171);
					match(ALL);
					}
					break;
				case INTEGER_VALUE:
					{
					setState(172);
					((QueryOrganizationContext)_localctx).limit = match(INTEGER_VALUE);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
			}

			setState(179);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OFFSET) {
				{
				setState(177);
				match(OFFSET);
				setState(178);
				((QueryOrganizationContext)_localctx).offset = match(INTEGER_VALUE);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PrimaryQueryContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public PrimaryQueryContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterPrimaryQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitPrimaryQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitPrimaryQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetOperationContext extends QueryTermContext {
		public QueryTermContext left;
		public Token setoperator;
		public QueryTermContext right;
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode UNION() { return getToken(NebulaSQLParser.UNION, 0); }
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSetOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSetOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSetOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		return queryTerm(0);
	}

	private QueryTermContext queryTerm(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
		QueryTermContext _prevctx = _localctx;
		int _startState = 8;
		enterRecursionRule(_localctx, 8, RULE_queryTerm, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new PrimaryQueryContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(182);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(189);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
					((SetOperationContext)_localctx).left = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
					setState(184);
					if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
					setState(185);
					((SetOperationContext)_localctx).setoperator = match(UNION);
					setState(186);
					((SetOperationContext)_localctx).right = queryTerm(2);
					}
					} 
				}
				setState(191);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitQueryPrimaryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitQueryPrimaryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableDefault1Context extends QueryPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault1Context(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterInlineTableDefault1(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitInlineTableDefault1(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitInlineTableDefault1(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FromStmtContext extends QueryPrimaryContext {
		public FromStatementContext fromStatement() {
			return getRuleContext(FromStatementContext.class,0);
		}
		public FromStmtContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterFromStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitFromStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitFromStmt(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(NebulaSQLParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_queryPrimary);
		try {
			setState(201);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(192);
				querySpecification();
				}
				break;
			case FROM:
				_localctx = new FromStmtContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(193);
				fromStatement();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(194);
				match(TABLE);
				setState(195);
				multipartIdentifier();
				}
				break;
			case VALUES:
				_localctx = new InlineTableDefault1Context(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(196);
				inlineTable();
				}
				break;
			case T__2:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(197);
				match(T__2);
				setState(198);
				query();
				setState(199);
				match(T__3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QuerySpecificationContext extends ParserRuleContext {
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public WindowedAggregationClauseContext windowedAggregationClause() {
			return getRuleContext(WindowedAggregationClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public SinkClauseContext sinkClause() {
			return getRuleContext(SinkClauseContext.class,0);
		}
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitQuerySpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_querySpecification);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(203);
			selectClause();
			setState(204);
			fromClause();
			setState(206);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
			case 1:
				{
				setState(205);
				whereClause();
				}
				break;
			}
			setState(209);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
			case 1:
				{
				setState(208);
				windowedAggregationClause();
				}
				break;
			}
			setState(212);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
			case 1:
				{
				setState(211);
				havingClause();
				}
				break;
			}
			setState(215);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
			case 1:
				{
				setState(214);
				sinkClause();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FromClauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(NebulaSQLParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public FromClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterFromClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitFromClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitFromClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_fromClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(217);
			match(FROM);
			setState(218);
			relation();
			setState(223);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,12,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(219);
					match(T__1);
					setState(220);
					relation();
					}
					} 
				}
				setState(225);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,12,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public List<JoinRelationContext> joinRelation() {
			return getRuleContexts(JoinRelationContext.class);
		}
		public JoinRelationContext joinRelation(int i) {
			return getRuleContext(JoinRelationContext.class,i);
		}
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		RelationContext _localctx = new RelationContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_relation);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(226);
			relationPrimary();
			setState(230);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(227);
					joinRelation();
					}
					} 
				}
				setState(232);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinRelationContext extends ParserRuleContext {
		public RelationPrimaryContext right;
		public TerminalNode JOIN() { return getToken(NebulaSQLParser.JOIN, 0); }
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public TerminalNode NATURAL() { return getToken(NebulaSQLParser.NATURAL, 0); }
		public JoinRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterJoinRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitJoinRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitJoinRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinRelationContext joinRelation() throws RecognitionException {
		JoinRelationContext _localctx = new JoinRelationContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_joinRelation);
		try {
			setState(244);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INNER:
			case JOIN:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(233);
				joinType();
				}
				setState(234);
				match(JOIN);
				setState(235);
				((JoinRelationContext)_localctx).right = relationPrimary();
				setState(237);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
				case 1:
					{
					setState(236);
					joinCriteria();
					}
					break;
				}
				}
				break;
			case NATURAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(239);
				match(NATURAL);
				setState(240);
				joinType();
				setState(241);
				match(JOIN);
				setState(242);
				((JoinRelationContext)_localctx).right = relationPrimary();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(NebulaSQLParser.INNER, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitJoinType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitJoinType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_joinType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(247);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INNER) {
				{
				setState(246);
				match(INNER);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(NebulaSQLParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitJoinCriteria(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitJoinCriteria(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_joinCriteria);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(249);
			match(ON);
			setState(250);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableValuedFunctionContext extends RelationPrimaryContext {
		public FunctionTableContext functionTable() {
			return getRuleContext(FunctionTableContext.class,0);
		}
		public TableValuedFunctionContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterTableValuedFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitTableValuedFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitTableValuedFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableDefault2Context extends RelationPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault2Context(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterInlineTableDefault2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitInlineTableDefault2(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitInlineTableDefault2(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AliasedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public AliasedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitAliasedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitAliasedRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AliasedQueryContext extends RelationPrimaryContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public AliasedQueryContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterAliasedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitAliasedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitAliasedQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableNameContext extends RelationPrimaryContext {
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitTableName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_relationPrimary);
		try {
			setState(267);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(252);
				multipartIdentifier();
				setState(253);
				tableAlias();
				}
				break;
			case 2:
				_localctx = new AliasedQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(255);
				match(T__2);
				setState(256);
				query();
				setState(257);
				match(T__3);
				setState(258);
				tableAlias();
				}
				break;
			case 3:
				_localctx = new AliasedRelationContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(260);
				match(T__2);
				setState(261);
				relation();
				setState(262);
				match(T__3);
				setState(263);
				tableAlias();
				}
				break;
			case 4:
				_localctx = new InlineTableDefault2Context(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(265);
				inlineTable();
				}
				break;
			case 5:
				_localctx = new TableValuedFunctionContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(266);
				functionTable();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FunctionTableContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext funcName;
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public FunctionTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterFunctionTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitFunctionTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitFunctionTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionTableContext functionTable() throws RecognitionException {
		FunctionTableContext _localctx = new FunctionTableContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_functionTable);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(269);
			((FunctionTableContext)_localctx).funcName = errorCapturingIdentifier();
			setState(270);
			match(T__2);
			setState(279);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
			case 1:
				{
				setState(271);
				expression();
				setState(276);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(272);
					match(T__1);
					setState(273);
					expression();
					}
					}
					setState(278);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			}
			setState(281);
			match(T__3);
			setState(282);
			tableAlias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FromStatementContext extends ParserRuleContext {
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<FromStatementBodyContext> fromStatementBody() {
			return getRuleContexts(FromStatementBodyContext.class);
		}
		public FromStatementBodyContext fromStatementBody(int i) {
			return getRuleContext(FromStatementBodyContext.class,i);
		}
		public FromStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterFromStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitFromStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitFromStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromStatementContext fromStatement() throws RecognitionException {
		FromStatementContext _localctx = new FromStatementContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_fromStatement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(284);
			fromClause();
			setState(286); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(285);
					fromStatementBody();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(288); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,20,_ctx);
			} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FromStatementBodyContext extends ParserRuleContext {
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public AggregationClauseContext aggregationClause() {
			return getRuleContext(AggregationClauseContext.class,0);
		}
		public FromStatementBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromStatementBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterFromStatementBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitFromStatementBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitFromStatementBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromStatementBodyContext fromStatementBody() throws RecognitionException {
		FromStatementBodyContext _localctx = new FromStatementBodyContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_fromStatementBody);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(290);
			selectClause();
			setState(292);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
			case 1:
				{
				setState(291);
				whereClause();
				}
				break;
			}
			setState(295);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
			case 1:
				{
				setState(294);
				aggregationClause();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SelectClauseContext extends ParserRuleContext {
		public HintContext hint;
		public List<HintContext> hints = new ArrayList<HintContext>();
		public TerminalNode SELECT() { return getToken(NebulaSQLParser.SELECT, 0); }
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public List<HintContext> hint() {
			return getRuleContexts(HintContext.class);
		}
		public HintContext hint(int i) {
			return getRuleContext(HintContext.class,i);
		}
		public SelectClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSelectClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSelectClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSelectClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectClauseContext selectClause() throws RecognitionException {
		SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_selectClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(297);
			match(SELECT);
			setState(301);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,23,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(298);
					((SelectClauseContext)_localctx).hint = hint();
					((SelectClauseContext)_localctx).hints.add(((SelectClauseContext)_localctx).hint);
					}
					} 
				}
				setState(303);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,23,_ctx);
			}
			setState(304);
			namedExpressionSeq();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WhereClauseContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(NebulaSQLParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public WhereClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whereClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterWhereClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitWhereClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitWhereClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhereClauseContext whereClause() throws RecognitionException {
		WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_whereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(306);
			match(WHERE);
			setState(307);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class HavingClauseContext extends ParserRuleContext {
		public TerminalNode HAVING() { return getToken(NebulaSQLParser.HAVING, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public HavingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_havingClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterHavingClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitHavingClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitHavingClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HavingClauseContext havingClause() throws RecognitionException {
		HavingClauseContext _localctx = new HavingClauseContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_havingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(309);
			match(HAVING);
			setState(310);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableContext extends ParserRuleContext {
		public TerminalNode VALUES() { return getToken(NebulaSQLParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public InlineTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inlineTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterInlineTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitInlineTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitInlineTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InlineTableContext inlineTable() throws RecognitionException {
		InlineTableContext _localctx = new InlineTableContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_inlineTable);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(312);
			match(VALUES);
			setState(313);
			expression();
			setState(318);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(314);
					match(T__1);
					setState(315);
					expression();
					}
					} 
				}
				setState(320);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
			}
			setState(321);
			tableAlias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableAliasContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(NebulaSQLParser.AS, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TableAliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableAlias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterTableAlias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitTableAlias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitTableAlias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableAliasContext tableAlias() throws RecognitionException {
		TableAliasContext _localctx = new TableAliasContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_tableAlias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(330);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
			case 1:
				{
				setState(324);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
				case 1:
					{
					setState(323);
					match(AS);
					}
					break;
				}
				setState(326);
				strictIdentifier();
				setState(328);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
				case 1:
					{
					setState(327);
					identifierList();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultipartIdentifierListContext extends ParserRuleContext {
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public MultipartIdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multipartIdentifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterMultipartIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitMultipartIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitMultipartIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultipartIdentifierListContext multipartIdentifierList() throws RecognitionException {
		MultipartIdentifierListContext _localctx = new MultipartIdentifierListContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_multipartIdentifierList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(332);
			multipartIdentifier();
			setState(337);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(333);
				match(T__1);
				setState(334);
				multipartIdentifier();
				}
				}
				setState(339);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultipartIdentifierContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext errorCapturingIdentifier;
		public List<ErrorCapturingIdentifierContext> parts = new ArrayList<ErrorCapturingIdentifierContext>();
		public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
			return getRuleContexts(ErrorCapturingIdentifierContext.class);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
			return getRuleContext(ErrorCapturingIdentifierContext.class,i);
		}
		public MultipartIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multipartIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterMultipartIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitMultipartIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitMultipartIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultipartIdentifierContext multipartIdentifier() throws RecognitionException {
		MultipartIdentifierContext _localctx = new MultipartIdentifierContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_multipartIdentifier);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(340);
			((MultipartIdentifierContext)_localctx).errorCapturingIdentifier = errorCapturingIdentifier();
			((MultipartIdentifierContext)_localctx).parts.add(((MultipartIdentifierContext)_localctx).errorCapturingIdentifier);
			setState(345);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,29,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(341);
					match(T__4);
					setState(342);
					((MultipartIdentifierContext)_localctx).errorCapturingIdentifier = errorCapturingIdentifier();
					((MultipartIdentifierContext)_localctx).parts.add(((MultipartIdentifierContext)_localctx).errorCapturingIdentifier);
					}
					} 
				}
				setState(347);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,29,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedExpressionContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext name;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode AS() { return getToken(NebulaSQLParser.AS, 0); }
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public NamedExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterNamedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitNamedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitNamedExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedExpressionContext namedExpression() throws RecognitionException {
		NamedExpressionContext _localctx = new NamedExpressionContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_namedExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(348);
			expression();
			setState(356);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
			case 1:
				{
				setState(350);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
				case 1:
					{
					setState(349);
					match(AS);
					}
					break;
				}
				setState(354);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
				case 1:
					{
					setState(352);
					((NamedExpressionContext)_localctx).name = errorCapturingIdentifier();
					}
					break;
				case 2:
					{
					setState(353);
					identifierList();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public StrictNonReservedContext strictNonReserved() {
			return getRuleContext(StrictNonReservedContext.class,0);
		}
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_identifier);
		try {
			setState(361);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(358);
				strictIdentifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(359);
				if (!(!SQL_standard_keyword_behavior)) throw new FailedPredicateException(this, "!SQL_standard_keyword_behavior");
				setState(360);
				strictNonReserved();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StrictIdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictIdentifier; }
	 
		public StrictIdentifierContext() { }
		public void copyFrom(StrictIdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuotedIdentifierAlternativeContext extends StrictIdentifierContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public QuotedIdentifierAlternativeContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterQuotedIdentifierAlternative(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitQuotedIdentifierAlternative(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitQuotedIdentifierAlternative(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnquotedIdentifierContext extends StrictIdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(NebulaSQLParser.IDENTIFIER, 0); }
		public AnsiNonReservedContext ansiNonReserved() {
			return getRuleContext(AnsiNonReservedContext.class,0);
		}
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StrictIdentifierContext strictIdentifier() throws RecognitionException {
		StrictIdentifierContext _localctx = new StrictIdentifierContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_strictIdentifier);
		try {
			setState(369);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
			case 1:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(363);
				match(IDENTIFIER);
				}
				break;
			case 2:
				_localctx = new QuotedIdentifierAlternativeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(364);
				quotedIdentifier();
				}
				break;
			case 3:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(365);
				if (!(SQL_standard_keyword_behavior)) throw new FailedPredicateException(this, "SQL_standard_keyword_behavior");
				setState(366);
				ansiNonReserved();
				}
				break;
			case 4:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(367);
				if (!(!SQL_standard_keyword_behavior)) throw new FailedPredicateException(this, "!SQL_standard_keyword_behavior");
				setState(368);
				nonReserved();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(NebulaSQLParser.BACKQUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(371);
			match(BACKQUOTED_IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierListContext extends ParserRuleContext {
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public IdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierListContext identifierList() throws RecognitionException {
		IdentifierListContext _localctx = new IdentifierListContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_identifierList);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(373);
			match(T__2);
			setState(374);
			identifierSeq();
			setState(375);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierSeqContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext errorCapturingIdentifier;
		public List<ErrorCapturingIdentifierContext> ident = new ArrayList<ErrorCapturingIdentifierContext>();
		public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
			return getRuleContexts(ErrorCapturingIdentifierContext.class);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
			return getRuleContext(ErrorCapturingIdentifierContext.class,i);
		}
		public IdentifierSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterIdentifierSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitIdentifierSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitIdentifierSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierSeqContext identifierSeq() throws RecognitionException {
		IdentifierSeqContext _localctx = new IdentifierSeqContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_identifierSeq);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(377);
			((IdentifierSeqContext)_localctx).errorCapturingIdentifier = errorCapturingIdentifier();
			((IdentifierSeqContext)_localctx).ident.add(((IdentifierSeqContext)_localctx).errorCapturingIdentifier);
			setState(382);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(378);
				match(T__1);
				setState(379);
				((IdentifierSeqContext)_localctx).errorCapturingIdentifier = errorCapturingIdentifier();
				((IdentifierSeqContext)_localctx).ident.add(((IdentifierSeqContext)_localctx).errorCapturingIdentifier);
				}
				}
				setState(384);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ErrorCapturingIdentifierContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ErrorCapturingIdentifierExtraContext errorCapturingIdentifierExtra() {
			return getRuleContext(ErrorCapturingIdentifierExtraContext.class,0);
		}
		public ErrorCapturingIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorCapturingIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterErrorCapturingIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitErrorCapturingIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitErrorCapturingIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ErrorCapturingIdentifierContext errorCapturingIdentifier() throws RecognitionException {
		ErrorCapturingIdentifierContext _localctx = new ErrorCapturingIdentifierContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_errorCapturingIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(385);
			identifier();
			setState(386);
			errorCapturingIdentifierExtra();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ErrorCapturingIdentifierExtraContext extends ParserRuleContext {
		public ErrorCapturingIdentifierExtraContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorCapturingIdentifierExtra; }
	 
		public ErrorCapturingIdentifierExtraContext() { }
		public void copyFrom(ErrorCapturingIdentifierExtraContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ErrorIdentContext extends ErrorCapturingIdentifierExtraContext {
		public List<TerminalNode> MINUS() { return getTokens(NebulaSQLParser.MINUS); }
		public TerminalNode MINUS(int i) {
			return getToken(NebulaSQLParser.MINUS, i);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ErrorIdentContext(ErrorCapturingIdentifierExtraContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterErrorIdent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitErrorIdent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitErrorIdent(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RealIdentContext extends ErrorCapturingIdentifierExtraContext {
		public RealIdentContext(ErrorCapturingIdentifierExtraContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterRealIdent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitRealIdent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitRealIdent(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ErrorCapturingIdentifierExtraContext errorCapturingIdentifierExtra() throws RecognitionException {
		ErrorCapturingIdentifierExtraContext _localctx = new ErrorCapturingIdentifierExtraContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_errorCapturingIdentifierExtra);
		try {
			int _alt;
			setState(395);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
			case 1:
				_localctx = new ErrorIdentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(390); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(388);
						match(MINUS);
						setState(389);
						identifier();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(392); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,36,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				break;
			case 2:
				_localctx = new RealIdentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedExpressionSeqContext extends ParserRuleContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public NamedExpressionSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpressionSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterNamedExpressionSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitNamedExpressionSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitNamedExpressionSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedExpressionSeqContext namedExpressionSeq() throws RecognitionException {
		NamedExpressionSeqContext _localctx = new NamedExpressionSeqContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_namedExpressionSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(397);
			namedExpression();
			setState(402);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,38,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(398);
					match(T__1);
					setState(399);
					namedExpression();
					}
					} 
				}
				setState(404);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,38,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(405);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(NebulaSQLParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitLogicalNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PredicatedContext extends BooleanExpressionContext {
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitPredicated(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitPredicated(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExistsContext extends BooleanExpressionContext {
		public TerminalNode EXISTS() { return getToken(NebulaSQLParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitExists(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalBinaryContext extends BooleanExpressionContext {
		public BooleanExpressionContext left;
		public Token op;
		public BooleanExpressionContext right;
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(NebulaSQLParser.AND, 0); }
		public TerminalNode OR() { return getToken(NebulaSQLParser.OR, 0); }
		public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterLogicalBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitLogicalBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitLogicalBinary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 66;
		enterRecursionRule(_localctx, 66, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(419);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,40,_ctx) ) {
			case 1:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(408);
				match(NOT);
				setState(409);
				booleanExpression(5);
				}
				break;
			case 2:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(410);
				match(EXISTS);
				setState(411);
				match(T__2);
				setState(412);
				query();
				setState(413);
				match(T__3);
				}
				break;
			case 3:
				{
				_localctx = new PredicatedContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(415);
				valueExpression(0);
				setState(417);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
				case 1:
					{
					setState(416);
					predicate();
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(429);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,42,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(427);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
					case 1:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(421);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(422);
						((LogicalBinaryContext)_localctx).op = match(AND);
						setState(423);
						((LogicalBinaryContext)_localctx).right = booleanExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(424);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(425);
						((LogicalBinaryContext)_localctx).op = match(OR);
						setState(426);
						((LogicalBinaryContext)_localctx).right = booleanExpression(2);
						}
						break;
					}
					} 
				}
				setState(431);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,42,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowedAggregationClauseContext extends ParserRuleContext {
		public WindowClauseContext windowClause() {
			return getRuleContext(WindowClauseContext.class,0);
		}
		public AggregationClauseContext aggregationClause() {
			return getRuleContext(AggregationClauseContext.class,0);
		}
		public WatermarkClauseContext watermarkClause() {
			return getRuleContext(WatermarkClauseContext.class,0);
		}
		public WindowedAggregationClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowedAggregationClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterWindowedAggregationClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitWindowedAggregationClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitWindowedAggregationClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowedAggregationClauseContext windowedAggregationClause() throws RecognitionException {
		WindowedAggregationClauseContext _localctx = new WindowedAggregationClauseContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_windowedAggregationClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(433);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==GROUP) {
				{
				setState(432);
				aggregationClause();
				}
			}

			setState(435);
			windowClause();
			setState(437);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,44,_ctx) ) {
			case 1:
				{
				setState(436);
				watermarkClause();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AggregationClauseContext extends ParserRuleContext {
		public ExpressionContext expression;
		public List<ExpressionContext> groupingExpressions = new ArrayList<ExpressionContext>();
		public Token kind;
		public TerminalNode GROUP() { return getToken(NebulaSQLParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(NebulaSQLParser.BY, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode WITH() { return getToken(NebulaSQLParser.WITH, 0); }
		public TerminalNode SETS() { return getToken(NebulaSQLParser.SETS, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public TerminalNode ROLLUP() { return getToken(NebulaSQLParser.ROLLUP, 0); }
		public TerminalNode CUBE() { return getToken(NebulaSQLParser.CUBE, 0); }
		public TerminalNode GROUPING() { return getToken(NebulaSQLParser.GROUPING, 0); }
		public AggregationClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregationClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterAggregationClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitAggregationClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitAggregationClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggregationClauseContext aggregationClause() throws RecognitionException {
		AggregationClauseContext _localctx = new AggregationClauseContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_aggregationClause);
		int _la;
		try {
			int _alt;
			setState(483);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(439);
				match(GROUP);
				setState(440);
				match(BY);
				setState(441);
				((AggregationClauseContext)_localctx).expression = expression();
				((AggregationClauseContext)_localctx).groupingExpressions.add(((AggregationClauseContext)_localctx).expression);
				setState(446);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,45,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(442);
						match(T__1);
						setState(443);
						((AggregationClauseContext)_localctx).expression = expression();
						((AggregationClauseContext)_localctx).groupingExpressions.add(((AggregationClauseContext)_localctx).expression);
						}
						} 
					}
					setState(448);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,45,_ctx);
				}
				setState(466);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,47,_ctx) ) {
				case 1:
					{
					setState(449);
					match(WITH);
					setState(450);
					((AggregationClauseContext)_localctx).kind = match(ROLLUP);
					}
					break;
				case 2:
					{
					setState(451);
					match(WITH);
					setState(452);
					((AggregationClauseContext)_localctx).kind = match(CUBE);
					}
					break;
				case 3:
					{
					setState(453);
					((AggregationClauseContext)_localctx).kind = match(GROUPING);
					setState(454);
					match(SETS);
					setState(455);
					match(T__2);
					setState(456);
					groupingSet();
					setState(461);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(457);
						match(T__1);
						setState(458);
						groupingSet();
						}
						}
						setState(463);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(464);
					match(T__3);
					}
					break;
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(468);
				match(GROUP);
				setState(469);
				match(BY);
				setState(470);
				((AggregationClauseContext)_localctx).kind = match(GROUPING);
				setState(471);
				match(SETS);
				setState(472);
				match(T__2);
				setState(473);
				groupingSet();
				setState(478);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(474);
					match(T__1);
					setState(475);
					groupingSet();
					}
					}
					setState(480);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(481);
				match(T__3);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupingSetContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_groupingSet);
		int _la;
		try {
			setState(498);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(485);
				match(T__2);
				setState(494);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,51,_ctx) ) {
				case 1:
					{
					setState(486);
					expression();
					setState(491);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(487);
						match(T__1);
						setState(488);
						expression();
						}
						}
						setState(493);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				}
				setState(496);
				match(T__3);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(497);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowClauseContext extends ParserRuleContext {
		public TerminalNode WINDOW() { return getToken(NebulaSQLParser.WINDOW, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public WindowClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterWindowClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitWindowClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitWindowClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowClauseContext windowClause() throws RecognitionException {
		WindowClauseContext _localctx = new WindowClauseContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_windowClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(500);
			match(WINDOW);
			setState(501);
			windowSpec();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WatermarkClauseContext extends ParserRuleContext {
		public TerminalNode WATERMARK() { return getToken(NebulaSQLParser.WATERMARK, 0); }
		public WatermarkParametersContext watermarkParameters() {
			return getRuleContext(WatermarkParametersContext.class,0);
		}
		public WatermarkClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_watermarkClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterWatermarkClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitWatermarkClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitWatermarkClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WatermarkClauseContext watermarkClause() throws RecognitionException {
		WatermarkClauseContext _localctx = new WatermarkClauseContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_watermarkClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(503);
			match(WATERMARK);
			setState(504);
			match(T__2);
			setState(505);
			watermarkParameters();
			setState(506);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WatermarkParametersContext extends ParserRuleContext {
		public IdentifierContext watermarkIdentifier;
		public Token watermark;
		public TimeUnitContext watermarkTimeUnit;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode INTEGER_VALUE() { return getToken(NebulaSQLParser.INTEGER_VALUE, 0); }
		public TimeUnitContext timeUnit() {
			return getRuleContext(TimeUnitContext.class,0);
		}
		public WatermarkParametersContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_watermarkParameters; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterWatermarkParameters(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitWatermarkParameters(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitWatermarkParameters(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WatermarkParametersContext watermarkParameters() throws RecognitionException {
		WatermarkParametersContext _localctx = new WatermarkParametersContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_watermarkParameters);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(508);
			((WatermarkParametersContext)_localctx).watermarkIdentifier = identifier();
			setState(509);
			match(T__1);
			setState(510);
			((WatermarkParametersContext)_localctx).watermark = match(INTEGER_VALUE);
			setState(511);
			((WatermarkParametersContext)_localctx).watermarkTimeUnit = timeUnit();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowSpecContext extends ParserRuleContext {
		public WindowSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowSpec; }
	 
		public WindowSpecContext() { }
		public void copyFrom(WindowSpecContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimeBasedWindowContext extends WindowSpecContext {
		public TimeWindowContext timeWindow() {
			return getRuleContext(TimeWindowContext.class,0);
		}
		public TimeBasedWindowContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterTimeBasedWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitTimeBasedWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitTimeBasedWindow(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CountBasedWindowContext extends WindowSpecContext {
		public CountWindowContext countWindow() {
			return getRuleContext(CountWindowContext.class,0);
		}
		public CountBasedWindowContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterCountBasedWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitCountBasedWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitCountBasedWindow(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ThresholdBasedWindowContext extends WindowSpecContext {
		public ConditionWindowContext conditionWindow() {
			return getRuleContext(ConditionWindowContext.class,0);
		}
		public ThresholdBasedWindowContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterThresholdBasedWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitThresholdBasedWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitThresholdBasedWindow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowSpecContext windowSpec() throws RecognitionException {
		WindowSpecContext _localctx = new WindowSpecContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_windowSpec);
		try {
			setState(516);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,53,_ctx) ) {
			case 1:
				_localctx = new TimeBasedWindowContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(513);
				timeWindow();
				}
				break;
			case 2:
				_localctx = new CountBasedWindowContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(514);
				countWindow();
				}
				break;
			case 3:
				_localctx = new ThresholdBasedWindowContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(515);
				conditionWindow();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TimeWindowContext extends ParserRuleContext {
		public TimeWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeWindow; }
	 
		public TimeWindowContext() { }
		public void copyFrom(TimeWindowContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TumblingWindowContext extends TimeWindowContext {
		public TerminalNode TUMBLING() { return getToken(NebulaSQLParser.TUMBLING, 0); }
		public SizeParameterContext sizeParameter() {
			return getRuleContext(SizeParameterContext.class,0);
		}
		public TimestampParameterContext timestampParameter() {
			return getRuleContext(TimestampParameterContext.class,0);
		}
		public TumblingWindowContext(TimeWindowContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterTumblingWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitTumblingWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitTumblingWindow(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SlidingWindowContext extends TimeWindowContext {
		public TerminalNode SLIDING() { return getToken(NebulaSQLParser.SLIDING, 0); }
		public SizeParameterContext sizeParameter() {
			return getRuleContext(SizeParameterContext.class,0);
		}
		public AdvancebyParameterContext advancebyParameter() {
			return getRuleContext(AdvancebyParameterContext.class,0);
		}
		public TimestampParameterContext timestampParameter() {
			return getRuleContext(TimestampParameterContext.class,0);
		}
		public SlidingWindowContext(TimeWindowContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSlidingWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSlidingWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSlidingWindow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TimeWindowContext timeWindow() throws RecognitionException {
		TimeWindowContext _localctx = new TimeWindowContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_timeWindow);
		int _la;
		try {
			setState(540);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case TUMBLING:
				_localctx = new TumblingWindowContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(518);
				match(TUMBLING);
				setState(519);
				match(T__2);
				setState(523);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(520);
					timestampParameter();
					setState(521);
					match(T__1);
					}
				}

				setState(525);
				sizeParameter();
				setState(526);
				match(T__3);
				}
				break;
			case SLIDING:
				_localctx = new SlidingWindowContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(528);
				match(SLIDING);
				setState(529);
				match(T__2);
				setState(533);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(530);
					timestampParameter();
					setState(531);
					match(T__1);
					}
				}

				setState(535);
				sizeParameter();
				setState(536);
				match(T__1);
				setState(537);
				advancebyParameter();
				setState(538);
				match(T__3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CountWindowContext extends ParserRuleContext {
		public CountWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_countWindow; }
	 
		public CountWindowContext() { }
		public void copyFrom(CountWindowContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CountBasedTumblingContext extends CountWindowContext {
		public TerminalNode TUMBLING() { return getToken(NebulaSQLParser.TUMBLING, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(NebulaSQLParser.INTEGER_VALUE, 0); }
		public CountBasedTumblingContext(CountWindowContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterCountBasedTumbling(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitCountBasedTumbling(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitCountBasedTumbling(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CountWindowContext countWindow() throws RecognitionException {
		CountWindowContext _localctx = new CountWindowContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_countWindow);
		try {
			_localctx = new CountBasedTumblingContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(542);
			match(TUMBLING);
			setState(543);
			match(T__2);
			setState(544);
			match(INTEGER_VALUE);
			setState(545);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConditionWindowContext extends ParserRuleContext {
		public ConditionWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_conditionWindow; }
	 
		public ConditionWindowContext() { }
		public void copyFrom(ConditionWindowContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ThresholdWindowContext extends ConditionWindowContext {
		public TerminalNode THRESHOLD() { return getToken(NebulaSQLParser.THRESHOLD, 0); }
		public ConditionParameterContext conditionParameter() {
			return getRuleContext(ConditionParameterContext.class,0);
		}
		public ThresholdMinSizeParameterContext thresholdMinSizeParameter() {
			return getRuleContext(ThresholdMinSizeParameterContext.class,0);
		}
		public ThresholdWindowContext(ConditionWindowContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterThresholdWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitThresholdWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitThresholdWindow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConditionWindowContext conditionWindow() throws RecognitionException {
		ConditionWindowContext _localctx = new ConditionWindowContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_conditionWindow);
		int _la;
		try {
			_localctx = new ThresholdWindowContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(547);
			match(THRESHOLD);
			setState(548);
			match(T__2);
			setState(549);
			conditionParameter();
			setState(552);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(550);
				match(T__1);
				setState(551);
				thresholdMinSizeParameter();
				}
			}

			setState(554);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConditionParameterContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ConditionParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_conditionParameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterConditionParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitConditionParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitConditionParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConditionParameterContext conditionParameter() throws RecognitionException {
		ConditionParameterContext _localctx = new ConditionParameterContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_conditionParameter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(556);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ThresholdMinSizeParameterContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(NebulaSQLParser.INTEGER_VALUE, 0); }
		public ThresholdMinSizeParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_thresholdMinSizeParameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterThresholdMinSizeParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitThresholdMinSizeParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitThresholdMinSizeParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ThresholdMinSizeParameterContext thresholdMinSizeParameter() throws RecognitionException {
		ThresholdMinSizeParameterContext _localctx = new ThresholdMinSizeParameterContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_thresholdMinSizeParameter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(558);
			match(INTEGER_VALUE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SizeParameterContext extends ParserRuleContext {
		public TerminalNode SIZE() { return getToken(NebulaSQLParser.SIZE, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(NebulaSQLParser.INTEGER_VALUE, 0); }
		public TimeUnitContext timeUnit() {
			return getRuleContext(TimeUnitContext.class,0);
		}
		public SizeParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sizeParameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSizeParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSizeParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSizeParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SizeParameterContext sizeParameter() throws RecognitionException {
		SizeParameterContext _localctx = new SizeParameterContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_sizeParameter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(560);
			match(SIZE);
			setState(561);
			match(INTEGER_VALUE);
			setState(562);
			timeUnit();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AdvancebyParameterContext extends ParserRuleContext {
		public TerminalNode ADVANCE() { return getToken(NebulaSQLParser.ADVANCE, 0); }
		public TerminalNode BY() { return getToken(NebulaSQLParser.BY, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(NebulaSQLParser.INTEGER_VALUE, 0); }
		public TimeUnitContext timeUnit() {
			return getRuleContext(TimeUnitContext.class,0);
		}
		public AdvancebyParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_advancebyParameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterAdvancebyParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitAdvancebyParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitAdvancebyParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AdvancebyParameterContext advancebyParameter() throws RecognitionException {
		AdvancebyParameterContext _localctx = new AdvancebyParameterContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_advancebyParameter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(564);
			match(ADVANCE);
			setState(565);
			match(BY);
			setState(566);
			match(INTEGER_VALUE);
			setState(567);
			timeUnit();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TimeUnitContext extends ParserRuleContext {
		public TerminalNode MS() { return getToken(NebulaSQLParser.MS, 0); }
		public TerminalNode SEC() { return getToken(NebulaSQLParser.SEC, 0); }
		public TerminalNode MIN() { return getToken(NebulaSQLParser.MIN, 0); }
		public TerminalNode HOUR() { return getToken(NebulaSQLParser.HOUR, 0); }
		public TerminalNode DAY() { return getToken(NebulaSQLParser.DAY, 0); }
		public TimeUnitContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeUnit; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterTimeUnit(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitTimeUnit(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitTimeUnit(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TimeUnitContext timeUnit() throws RecognitionException {
		TimeUnitContext _localctx = new TimeUnitContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_timeUnit);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(569);
			_la = _input.LA(1);
			if ( !(((((_la - 84)) & ~0x3f) == 0 && ((1L << (_la - 84)) & 31L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TimestampParameterContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(NebulaSQLParser.IDENTIFIER, 0); }
		public TimestampParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timestampParameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterTimestampParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitTimestampParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitTimestampParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TimestampParameterContext timestampParameter() throws RecognitionException {
		TimestampParameterContext _localctx = new TimestampParameterContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_timestampParameter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(571);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FunctionNameContext extends ParserRuleContext {
		public TerminalNode AVG() { return getToken(NebulaSQLParser.AVG, 0); }
		public TerminalNode MAX() { return getToken(NebulaSQLParser.MAX, 0); }
		public TerminalNode MIN() { return getToken(NebulaSQLParser.MIN, 0); }
		public TerminalNode SUM() { return getToken(NebulaSQLParser.SUM, 0); }
		public TerminalNode COUNT() { return getToken(NebulaSQLParser.COUNT, 0); }
		public TerminalNode MEDIAN() { return getToken(NebulaSQLParser.MEDIAN, 0); }
		public FunctionNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterFunctionName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitFunctionName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitFunctionName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionNameContext functionName() throws RecognitionException {
		FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_functionName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(573);
			_la = _input.LA(1);
			if ( !(((((_la - 86)) & ~0x3f) == 0 && ((1L << (_la - 86)) & 249L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SinkClauseContext extends ParserRuleContext {
		public TerminalNode INTO() { return getToken(NebulaSQLParser.INTO, 0); }
		public SinkTypeContext sinkType() {
			return getRuleContext(SinkTypeContext.class,0);
		}
		public TerminalNode AS() { return getToken(NebulaSQLParser.AS, 0); }
		public SinkClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sinkClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSinkClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSinkClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSinkClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SinkClauseContext sinkClause() throws RecognitionException {
		SinkClauseContext _localctx = new SinkClauseContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_sinkClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(575);
			match(INTO);
			setState(576);
			sinkType();
			setState(578);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,58,_ctx) ) {
			case 1:
				{
				setState(577);
				match(AS);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SinkTypeContext extends ParserRuleContext {
		public SinkTypeFileContext sinkTypeFile() {
			return getRuleContext(SinkTypeFileContext.class,0);
		}
		public SinkTypePrintContext sinkTypePrint() {
			return getRuleContext(SinkTypePrintContext.class,0);
		}
		public SinkTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sinkType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSinkType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSinkType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSinkType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SinkTypeContext sinkType() throws RecognitionException {
		SinkTypeContext _localctx = new SinkTypeContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_sinkType);
		try {
			setState(582);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FILE:
				enterOuterAlt(_localctx, 1);
				{
				setState(580);
				sinkTypeFile();
				}
				break;
			case PRINT:
				enterOuterAlt(_localctx, 2);
				{
				setState(581);
				sinkTypePrint();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NullNotnullContext extends ParserRuleContext {
		public TerminalNode NULLTOKEN() { return getToken(NebulaSQLParser.NULLTOKEN, 0); }
		public TerminalNode NOT() { return getToken(NebulaSQLParser.NOT, 0); }
		public NullNotnullContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nullNotnull; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterNullNotnull(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitNullNotnull(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitNullNotnull(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NullNotnullContext nullNotnull() throws RecognitionException {
		NullNotnullContext _localctx = new NullNotnullContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_nullNotnull);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(585);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(584);
				match(NOT);
				}
			}

			setState(587);
			match(NULLTOKEN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StreamNameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(NebulaSQLParser.IDENTIFIER, 0); }
		public StreamNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_streamName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterStreamName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitStreamName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitStreamName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StreamNameContext streamName() throws RecognitionException {
		StreamNameContext _localctx = new StreamNameContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_streamName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(589);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SinkTypeFileContext extends ParserRuleContext {
		public Token path;
		public FileFormatContext format;
		public Token append;
		public TerminalNode FILE() { return getToken(NebulaSQLParser.FILE, 0); }
		public List<TerminalNode> STRING() { return getTokens(NebulaSQLParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(NebulaSQLParser.STRING, i);
		}
		public FileFormatContext fileFormat() {
			return getRuleContext(FileFormatContext.class,0);
		}
		public SinkTypeFileContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sinkTypeFile; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSinkTypeFile(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSinkTypeFile(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSinkTypeFile(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SinkTypeFileContext sinkTypeFile() throws RecognitionException {
		SinkTypeFileContext _localctx = new SinkTypeFileContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_sinkTypeFile);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(591);
			match(FILE);
			setState(592);
			match(T__2);
			setState(593);
			((SinkTypeFileContext)_localctx).path = match(STRING);
			setState(594);
			match(T__1);
			setState(595);
			((SinkTypeFileContext)_localctx).format = fileFormat();
			setState(596);
			match(T__1);
			setState(597);
			((SinkTypeFileContext)_localctx).append = match(STRING);
			setState(598);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SinkTypePrintContext extends ParserRuleContext {
		public TerminalNode PRINT() { return getToken(NebulaSQLParser.PRINT, 0); }
		public SinkTypePrintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sinkTypePrint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSinkTypePrint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSinkTypePrint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSinkTypePrint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SinkTypePrintContext sinkTypePrint() throws RecognitionException {
		SinkTypePrintContext _localctx = new SinkTypePrintContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_sinkTypePrint);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(600);
			match(PRINT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FileFormatContext extends ParserRuleContext {
		public TerminalNode CSV_FORMAT() { return getToken(NebulaSQLParser.CSV_FORMAT, 0); }
		public FileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fileFormat; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FileFormatContext fileFormat() throws RecognitionException {
		FileFormatContext _localctx = new FileFormatContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_fileFormat);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(602);
			match(CSV_FORMAT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrder;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(NebulaSQLParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(NebulaSQLParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(NebulaSQLParser.DESC, 0); }
		public TerminalNode LAST() { return getToken(NebulaSQLParser.LAST, 0); }
		public TerminalNode FIRST() { return getToken(NebulaSQLParser.FIRST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSortItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(604);
			expression();
			setState(606);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(605);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(610);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(608);
				match(NULLS);
				setState(609);
				((SortItemContext)_localctx).nullOrder = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrder = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PredicateContext extends ParserRuleContext {
		public Token kind;
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public ValueExpressionContext pattern;
		public Token quantifier;
		public Token escapeChar;
		public ValueExpressionContext right;
		public TerminalNode AND() { return getToken(NebulaSQLParser.AND, 0); }
		public TerminalNode BETWEEN() { return getToken(NebulaSQLParser.BETWEEN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(NebulaSQLParser.NOT, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode IN() { return getToken(NebulaSQLParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RLIKE() { return getToken(NebulaSQLParser.RLIKE, 0); }
		public TerminalNode LIKE() { return getToken(NebulaSQLParser.LIKE, 0); }
		public TerminalNode ANY() { return getToken(NebulaSQLParser.ANY, 0); }
		public TerminalNode SOME() { return getToken(NebulaSQLParser.SOME, 0); }
		public TerminalNode ALL() { return getToken(NebulaSQLParser.ALL, 0); }
		public TerminalNode ESCAPE() { return getToken(NebulaSQLParser.ESCAPE, 0); }
		public TerminalNode STRING() { return getToken(NebulaSQLParser.STRING, 0); }
		public TerminalNode IS() { return getToken(NebulaSQLParser.IS, 0); }
		public NullNotnullContext nullNotnull() {
			return getRuleContext(NullNotnullContext.class,0);
		}
		public TerminalNode TRUE() { return getToken(NebulaSQLParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(NebulaSQLParser.FALSE, 0); }
		public TerminalNode UNKNOWN() { return getToken(NebulaSQLParser.UNKNOWN, 0); }
		public TerminalNode FROM() { return getToken(NebulaSQLParser.FROM, 0); }
		public TerminalNode DISTINCT() { return getToken(NebulaSQLParser.DISTINCT, 0); }
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_predicate);
		int _la;
		try {
			setState(691);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(613);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(612);
					match(NOT);
					}
				}

				setState(615);
				((PredicateContext)_localctx).kind = match(BETWEEN);
				setState(616);
				((PredicateContext)_localctx).lower = valueExpression(0);
				setState(617);
				match(AND);
				setState(618);
				((PredicateContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(621);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(620);
					match(NOT);
					}
				}

				setState(623);
				((PredicateContext)_localctx).kind = match(IN);
				setState(624);
				match(T__2);
				setState(625);
				expression();
				setState(630);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(626);
					match(T__1);
					setState(627);
					expression();
					}
					}
					setState(632);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(633);
				match(T__3);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(636);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(635);
					match(NOT);
					}
				}

				setState(638);
				((PredicateContext)_localctx).kind = match(IN);
				setState(639);
				match(T__2);
				setState(640);
				query();
				setState(641);
				match(T__3);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(644);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(643);
					match(NOT);
					}
				}

				setState(646);
				((PredicateContext)_localctx).kind = match(RLIKE);
				setState(647);
				((PredicateContext)_localctx).pattern = valueExpression(0);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(649);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(648);
					match(NOT);
					}
				}

				setState(651);
				((PredicateContext)_localctx).kind = match(LIKE);
				setState(652);
				((PredicateContext)_localctx).quantifier = _input.LT(1);
				_la = _input.LA(1);
				if ( !(((((_la - 9)) & ~0x3f) == 0 && ((1L << (_la - 9)) & 36028797018963973L) != 0)) ) {
					((PredicateContext)_localctx).quantifier = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(666);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
				case 1:
					{
					setState(653);
					match(T__2);
					setState(654);
					match(T__3);
					}
					break;
				case 2:
					{
					setState(655);
					match(T__2);
					setState(656);
					expression();
					setState(661);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(657);
						match(T__1);
						setState(658);
						expression();
						}
						}
						setState(663);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(664);
					match(T__3);
					}
					break;
				}
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(669);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(668);
					match(NOT);
					}
				}

				setState(671);
				((PredicateContext)_localctx).kind = match(LIKE);
				setState(672);
				((PredicateContext)_localctx).pattern = valueExpression(0);
				setState(675);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
				case 1:
					{
					setState(673);
					match(ESCAPE);
					setState(674);
					((PredicateContext)_localctx).escapeChar = match(STRING);
					}
					break;
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(677);
				match(IS);
				setState(678);
				nullNotnull();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(679);
				match(IS);
				setState(681);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(680);
					match(NOT);
					}
				}

				setState(683);
				((PredicateContext)_localctx).kind = _input.LT(1);
				_la = _input.LA(1);
				if ( !(((((_la - 28)) & ~0x3f) == 0 && ((1L << (_la - 28)) & 9895604649985L) != 0)) ) {
					((PredicateContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(684);
				match(IS);
				setState(686);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(685);
					match(NOT);
					}
				}

				setState(688);
				((PredicateContext)_localctx).kind = match(DISTINCT);
				setState(689);
				match(FROM);
				setState(690);
				((PredicateContext)_localctx).right = valueExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitValueExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ComparisonContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitComparison(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token op;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(NebulaSQLParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(NebulaSQLParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(NebulaSQLParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(NebulaSQLParser.DIV, 0); }
		public TerminalNode PLUS() { return getToken(NebulaSQLParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(NebulaSQLParser.MINUS, 0); }
		public TerminalNode CONCAT_PIPE() { return getToken(NebulaSQLParser.CONCAT_PIPE, 0); }
		public TerminalNode AMPERSAND() { return getToken(NebulaSQLParser.AMPERSAND, 0); }
		public TerminalNode HAT() { return getToken(NebulaSQLParser.HAT, 0); }
		public TerminalNode PIPE() { return getToken(NebulaSQLParser.PIPE, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitArithmeticBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitArithmeticBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token op;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(NebulaSQLParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(NebulaSQLParser.PLUS, 0); }
		public TerminalNode TILDE() { return getToken(NebulaSQLParser.TILDE, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitArithmeticUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitArithmeticUnary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 120;
		enterRecursionRule(_localctx, 120, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(697);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
			case 1:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(694);
				primaryExpression(0);
				}
				break;
			case 2:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(695);
				((ArithmeticUnaryContext)_localctx).op = _input.LT(1);
				_la = _input.LA(1);
				if ( !(((((_la - 111)) & ~0x3f) == 0 && ((1L << (_la - 111)) & 35L) != 0)) ) {
					((ArithmeticUnaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(696);
				valueExpression(7);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(720);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,78,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(718);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(699);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(700);
						((ArithmeticBinaryContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==DIV || ((((_la - 113)) & ~0x3f) == 0 && ((1L << (_la - 113)) & 7L) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(701);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(7);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(702);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(703);
						((ArithmeticBinaryContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 111)) & ~0x3f) == 0 && ((1L << (_la - 111)) & 259L) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(704);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(6);
						}
						break;
					case 3:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(705);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(706);
						((ArithmeticBinaryContext)_localctx).op = match(AMPERSAND);
						setState(707);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(5);
						}
						break;
					case 4:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(708);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(709);
						((ArithmeticBinaryContext)_localctx).op = match(HAT);
						setState(710);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 5:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(711);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(712);
						((ArithmeticBinaryContext)_localctx).op = match(PIPE);
						setState(713);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 6:
						{
						_localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
						((ComparisonContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(714);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(715);
						comparisonOperator();
						setState(716);
						((ComparisonContext)_localctx).right = valueExpression(2);
						}
						break;
					}
					} 
				}
				setState(722);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,78,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(NebulaSQLParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(NebulaSQLParser.NEQ, 0); }
		public TerminalNode NEQJ() { return getToken(NebulaSQLParser.NEQJ, 0); }
		public TerminalNode LT() { return getToken(NebulaSQLParser.LT, 0); }
		public TerminalNode LTE() { return getToken(NebulaSQLParser.LTE, 0); }
		public TerminalNode GT() { return getToken(NebulaSQLParser.GT, 0); }
		public TerminalNode GTE() { return getToken(NebulaSQLParser.GTE, 0); }
		public TerminalNode NSEQ() { return getToken(NebulaSQLParser.NSEQ, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(723);
			_la = _input.LA(1);
			if ( !(((((_la - 103)) & ~0x3f) == 0 && ((1L << (_la - 103)) & 255L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class HintContext extends ParserRuleContext {
		public HintStatementContext hintStatement;
		public List<HintStatementContext> hintStatements = new ArrayList<HintStatementContext>();
		public List<HintStatementContext> hintStatement() {
			return getRuleContexts(HintStatementContext.class);
		}
		public HintStatementContext hintStatement(int i) {
			return getRuleContext(HintStatementContext.class,i);
		}
		public HintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterHint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitHint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitHint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HintContext hint() throws RecognitionException {
		HintContext _localctx = new HintContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_hint);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(725);
			match(T__5);
			setState(726);
			((HintContext)_localctx).hintStatement = hintStatement();
			((HintContext)_localctx).hintStatements.add(((HintContext)_localctx).hintStatement);
			setState(733);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,80,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(728);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,79,_ctx) ) {
					case 1:
						{
						setState(727);
						match(T__1);
						}
						break;
					}
					setState(730);
					((HintContext)_localctx).hintStatement = hintStatement();
					((HintContext)_localctx).hintStatements.add(((HintContext)_localctx).hintStatement);
					}
					} 
				}
				setState(735);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,80,_ctx);
			}
			setState(736);
			match(T__6);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class HintStatementContext extends ParserRuleContext {
		public IdentifierContext hintName;
		public PrimaryExpressionContext primaryExpression;
		public List<PrimaryExpressionContext> parameters = new ArrayList<PrimaryExpressionContext>();
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<PrimaryExpressionContext> primaryExpression() {
			return getRuleContexts(PrimaryExpressionContext.class);
		}
		public PrimaryExpressionContext primaryExpression(int i) {
			return getRuleContext(PrimaryExpressionContext.class,i);
		}
		public HintStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hintStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterHintStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitHintStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitHintStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HintStatementContext hintStatement() throws RecognitionException {
		HintStatementContext _localctx = new HintStatementContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_hintStatement);
		int _la;
		try {
			setState(751);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(738);
				((HintStatementContext)_localctx).hintName = identifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(739);
				((HintStatementContext)_localctx).hintName = identifier();
				setState(740);
				match(T__2);
				setState(741);
				((HintStatementContext)_localctx).primaryExpression = primaryExpression(0);
				((HintStatementContext)_localctx).parameters.add(((HintStatementContext)_localctx).primaryExpression);
				setState(746);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(742);
					match(T__1);
					setState(743);
					((HintStatementContext)_localctx).primaryExpression = primaryExpression(0);
					((HintStatementContext)_localctx).parameters.add(((HintStatementContext)_localctx).primaryExpression);
					}
					}
					setState(748);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(749);
				match(T__3);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitDereference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitDereference(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ConstantDefaultContext extends PrimaryExpressionContext {
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ConstantDefaultContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterConstantDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitConstantDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitConstantDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitColumnReference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitColumnReference(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterRowConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitRowConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitRowConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitParenthesizedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StarContext extends PrimaryExpressionContext {
		public TerminalNode ASTERISK() { return getToken(NebulaSQLParser.ASTERISK, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public StarContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterStar(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitStar(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitStar(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public ExpressionContext expression;
		public List<ExpressionContext> argument = new ArrayList<ExpressionContext>();
		public FunctionNameContext functionName() {
			return getRuleContext(FunctionNameContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSubqueryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSubqueryExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 128;
		enterRecursionRule(_localctx, 128, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(793);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,86,_ctx) ) {
			case 1:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(754);
				match(ASTERISK);
				}
				break;
			case 2:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(755);
				qualifiedName();
				setState(756);
				match(T__4);
				setState(757);
				match(ASTERISK);
				}
				break;
			case 3:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(759);
				match(T__2);
				setState(760);
				query();
				setState(761);
				match(T__3);
				}
				break;
			case 4:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(763);
				match(T__2);
				setState(764);
				namedExpression();
				setState(767); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(765);
					match(T__1);
					setState(766);
					namedExpression();
					}
					}
					setState(769); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__1 );
				setState(771);
				match(T__3);
				}
				break;
			case 5:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(773);
				functionName();
				setState(774);
				match(T__2);
				setState(783);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
				case 1:
					{
					setState(775);
					((FunctionCallContext)_localctx).expression = expression();
					((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
					setState(780);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(776);
						match(T__1);
						setState(777);
						((FunctionCallContext)_localctx).expression = expression();
						((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
						}
						}
						setState(782);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				}
				setState(785);
				match(T__3);
				}
				break;
			case 6:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(787);
				match(T__2);
				setState(788);
				expression();
				setState(789);
				match(T__3);
				}
				break;
			case 7:
				{
				_localctx = new ConstantDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(791);
				constant();
				}
				break;
			case 8:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(792);
				identifier();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(800);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,87,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
					((DereferenceContext)_localctx).base = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
					setState(795);
					if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
					setState(796);
					match(T__4);
					setState(797);
					((DereferenceContext)_localctx).fieldName = identifier();
					}
					} 
				}
				setState(802);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,87,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitQualifiedName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(803);
			identifier();
			setState(808);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,88,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(804);
					match(T__4);
					setState(805);
					identifier();
					}
					} 
				}
				setState(810);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,88,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(NebulaSQLParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(NebulaSQLParser.MINUS, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BigIntLiteralContext extends NumberContext {
		public TerminalNode BIGINT_LITERAL() { return getToken(NebulaSQLParser.BIGINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(NebulaSQLParser.MINUS, 0); }
		public BigIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterBigIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitBigIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitBigIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TinyIntLiteralContext extends NumberContext {
		public TerminalNode TINYINT_LITERAL() { return getToken(NebulaSQLParser.TINYINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(NebulaSQLParser.MINUS, 0); }
		public TinyIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterTinyIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitTinyIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitTinyIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LegacyDecimalLiteralContext extends NumberContext {
		public TerminalNode EXPONENT_VALUE() { return getToken(NebulaSQLParser.EXPONENT_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(NebulaSQLParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(NebulaSQLParser.MINUS, 0); }
		public LegacyDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterLegacyDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitLegacyDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitLegacyDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BigDecimalLiteralContext extends NumberContext {
		public TerminalNode BIGDECIMAL_LITERAL() { return getToken(NebulaSQLParser.BIGDECIMAL_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(NebulaSQLParser.MINUS, 0); }
		public BigDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterBigDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitBigDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitBigDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExponentLiteralContext extends NumberContext {
		public TerminalNode EXPONENT_VALUE() { return getToken(NebulaSQLParser.EXPONENT_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(NebulaSQLParser.MINUS, 0); }
		public ExponentLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterExponentLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitExponentLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitExponentLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_LITERAL() { return getToken(NebulaSQLParser.DOUBLE_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(NebulaSQLParser.MINUS, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitDoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitDoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(NebulaSQLParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(NebulaSQLParser.MINUS, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FloatLiteralContext extends NumberContext {
		public TerminalNode FLOAT_LITERAL() { return getToken(NebulaSQLParser.FLOAT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(NebulaSQLParser.MINUS, 0); }
		public FloatLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterFloatLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitFloatLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitFloatLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SmallIntLiteralContext extends NumberContext {
		public TerminalNode SMALLINT_LITERAL() { return getToken(NebulaSQLParser.SMALLINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(NebulaSQLParser.MINUS, 0); }
		public SmallIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterSmallIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitSmallIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitSmallIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_number);
		int _la;
		try {
			setState(854);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,99,_ctx) ) {
			case 1:
				_localctx = new ExponentLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(811);
				if (!(!legacy_exponent_literal_as_decimal_enabled)) throw new FailedPredicateException(this, "!legacy_exponent_literal_as_decimal_enabled");
				setState(813);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(812);
					match(MINUS);
					}
				}

				setState(815);
				match(EXPONENT_VALUE);
				}
				break;
			case 2:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(816);
				if (!(!legacy_exponent_literal_as_decimal_enabled)) throw new FailedPredicateException(this, "!legacy_exponent_literal_as_decimal_enabled");
				setState(818);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(817);
					match(MINUS);
					}
				}

				setState(820);
				match(DECIMAL_VALUE);
				}
				break;
			case 3:
				_localctx = new LegacyDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(821);
				if (!(legacy_exponent_literal_as_decimal_enabled)) throw new FailedPredicateException(this, "legacy_exponent_literal_as_decimal_enabled");
				setState(823);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(822);
					match(MINUS);
					}
				}

				setState(825);
				_la = _input.LA(1);
				if ( !(_la==EXPONENT_VALUE || _la==DECIMAL_VALUE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 4:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(827);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(826);
					match(MINUS);
					}
				}

				setState(829);
				match(INTEGER_VALUE);
				}
				break;
			case 5:
				_localctx = new BigIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(831);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(830);
					match(MINUS);
					}
				}

				setState(833);
				match(BIGINT_LITERAL);
				}
				break;
			case 6:
				_localctx = new SmallIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(835);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(834);
					match(MINUS);
					}
				}

				setState(837);
				match(SMALLINT_LITERAL);
				}
				break;
			case 7:
				_localctx = new TinyIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(839);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(838);
					match(MINUS);
					}
				}

				setState(841);
				match(TINYINT_LITERAL);
				}
				break;
			case 8:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(843);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(842);
					match(MINUS);
					}
				}

				setState(845);
				match(DOUBLE_LITERAL);
				}
				break;
			case 9:
				_localctx = new FloatLiteralContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(847);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(846);
					match(MINUS);
					}
				}

				setState(849);
				match(FLOAT_LITERAL);
				}
				break;
			case 10:
				_localctx = new BigDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(851);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(850);
					match(MINUS);
					}
				}

				setState(853);
				match(BIGDECIMAL_LITERAL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConstantContext extends ParserRuleContext {
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
	 
		public ConstantContext() { }
		public void copyFrom(ConstantContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NullLiteralContext extends ConstantContext {
		public TerminalNode NULLTOKEN() { return getToken(NebulaSQLParser.NULLTOKEN, 0); }
		public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitNullLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitNullLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StringLiteralContext extends ConstantContext {
		public List<TerminalNode> STRING() { return getTokens(NebulaSQLParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(NebulaSQLParser.STRING, i);
		}
		public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TypeConstructorContext extends ConstantContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(NebulaSQLParser.STRING, 0); }
		public TypeConstructorContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitTypeConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitTypeConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NumericLiteralContext extends ConstantContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitNumericLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BooleanLiteralContext extends ConstantContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_constant);
		try {
			int _alt;
			setState(867);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,101,_ctx) ) {
			case 1:
				_localctx = new NullLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(856);
				match(NULLTOKEN);
				}
				break;
			case 2:
				_localctx = new TypeConstructorContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(857);
				identifier();
				setState(858);
				match(STRING);
				}
				break;
			case 3:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(860);
				number();
				}
				break;
			case 4:
				_localctx = new BooleanLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(861);
				booleanValue();
				}
				break;
			case 5:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(863); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(862);
						match(STRING);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(865); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,100,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(NebulaSQLParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(NebulaSQLParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(869);
			_la = _input.LA(1);
			if ( !(_la==FALSE || _la==TRUE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StrictNonReservedContext extends ParserRuleContext {
		public TerminalNode FULL() { return getToken(NebulaSQLParser.FULL, 0); }
		public TerminalNode INNER() { return getToken(NebulaSQLParser.INNER, 0); }
		public TerminalNode JOIN() { return getToken(NebulaSQLParser.JOIN, 0); }
		public TerminalNode LEFT() { return getToken(NebulaSQLParser.LEFT, 0); }
		public TerminalNode NATURAL() { return getToken(NebulaSQLParser.NATURAL, 0); }
		public TerminalNode ON() { return getToken(NebulaSQLParser.ON, 0); }
		public TerminalNode RIGHT() { return getToken(NebulaSQLParser.RIGHT, 0); }
		public TerminalNode UNION() { return getToken(NebulaSQLParser.UNION, 0); }
		public TerminalNode USING() { return getToken(NebulaSQLParser.USING, 0); }
		public StrictNonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictNonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterStrictNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitStrictNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitStrictNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StrictNonReservedContext strictNonReserved() throws RecognitionException {
		StrictNonReservedContext _localctx = new StrictNonReservedContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_strictNonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(871);
			_la = _input.LA(1);
			if ( !(((((_la - 32)) & ~0x3f) == 0 && ((1L << (_la - 32)) & 2474039710785L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AnsiNonReservedContext extends ParserRuleContext {
		public TerminalNode ASC() { return getToken(NebulaSQLParser.ASC, 0); }
		public TerminalNode AT() { return getToken(NebulaSQLParser.AT, 0); }
		public TerminalNode BETWEEN() { return getToken(NebulaSQLParser.BETWEEN, 0); }
		public TerminalNode BY() { return getToken(NebulaSQLParser.BY, 0); }
		public TerminalNode CUBE() { return getToken(NebulaSQLParser.CUBE, 0); }
		public TerminalNode DELETE() { return getToken(NebulaSQLParser.DELETE, 0); }
		public TerminalNode DESC() { return getToken(NebulaSQLParser.DESC, 0); }
		public TerminalNode DIV() { return getToken(NebulaSQLParser.DIV, 0); }
		public TerminalNode DROP() { return getToken(NebulaSQLParser.DROP, 0); }
		public TerminalNode EXISTS() { return getToken(NebulaSQLParser.EXISTS, 0); }
		public TerminalNode FIRST() { return getToken(NebulaSQLParser.FIRST, 0); }
		public TerminalNode GROUPING() { return getToken(NebulaSQLParser.GROUPING, 0); }
		public TerminalNode INSERT() { return getToken(NebulaSQLParser.INSERT, 0); }
		public TerminalNode LAST() { return getToken(NebulaSQLParser.LAST, 0); }
		public TerminalNode LIKE() { return getToken(NebulaSQLParser.LIKE, 0); }
		public TerminalNode LIMIT() { return getToken(NebulaSQLParser.LIMIT, 0); }
		public TerminalNode MERGE() { return getToken(NebulaSQLParser.MERGE, 0); }
		public TerminalNode NULLS() { return getToken(NebulaSQLParser.NULLS, 0); }
		public TerminalNode QUERY() { return getToken(NebulaSQLParser.QUERY, 0); }
		public TerminalNode RLIKE() { return getToken(NebulaSQLParser.RLIKE, 0); }
		public TerminalNode ROLLUP() { return getToken(NebulaSQLParser.ROLLUP, 0); }
		public TerminalNode SETS() { return getToken(NebulaSQLParser.SETS, 0); }
		public TerminalNode TRUE() { return getToken(NebulaSQLParser.TRUE, 0); }
		public TerminalNode TYPE() { return getToken(NebulaSQLParser.TYPE, 0); }
		public TerminalNode VALUES() { return getToken(NebulaSQLParser.VALUES, 0); }
		public TerminalNode WINDOW() { return getToken(NebulaSQLParser.WINDOW, 0); }
		public AnsiNonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ansiNonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterAnsiNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitAnsiNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitAnsiNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AnsiNonReservedContext ansiNonReserved() throws RecognitionException {
		AnsiNonReservedContext _localctx = new AnsiNonReservedContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_ansiNonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(873);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & -5615592343523696640L) != 0) || ((((_la - 68)) & ~0x3f) == 0 && ((1L << (_la - 68)) & 579L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode ASC() { return getToken(NebulaSQLParser.ASC, 0); }
		public TerminalNode AT() { return getToken(NebulaSQLParser.AT, 0); }
		public TerminalNode BETWEEN() { return getToken(NebulaSQLParser.BETWEEN, 0); }
		public TerminalNode CUBE() { return getToken(NebulaSQLParser.CUBE, 0); }
		public TerminalNode BY() { return getToken(NebulaSQLParser.BY, 0); }
		public TerminalNode DELETE() { return getToken(NebulaSQLParser.DELETE, 0); }
		public TerminalNode DESC() { return getToken(NebulaSQLParser.DESC, 0); }
		public TerminalNode DIV() { return getToken(NebulaSQLParser.DIV, 0); }
		public TerminalNode DROP() { return getToken(NebulaSQLParser.DROP, 0); }
		public TerminalNode EXISTS() { return getToken(NebulaSQLParser.EXISTS, 0); }
		public TerminalNode FIRST() { return getToken(NebulaSQLParser.FIRST, 0); }
		public TerminalNode GROUPING() { return getToken(NebulaSQLParser.GROUPING, 0); }
		public TerminalNode INSERT() { return getToken(NebulaSQLParser.INSERT, 0); }
		public TerminalNode LAST() { return getToken(NebulaSQLParser.LAST, 0); }
		public TerminalNode LIKE() { return getToken(NebulaSQLParser.LIKE, 0); }
		public TerminalNode LIMIT() { return getToken(NebulaSQLParser.LIMIT, 0); }
		public TerminalNode MERGE() { return getToken(NebulaSQLParser.MERGE, 0); }
		public TerminalNode NULLS() { return getToken(NebulaSQLParser.NULLS, 0); }
		public TerminalNode QUERY() { return getToken(NebulaSQLParser.QUERY, 0); }
		public TerminalNode RLIKE() { return getToken(NebulaSQLParser.RLIKE, 0); }
		public TerminalNode ROLLUP() { return getToken(NebulaSQLParser.ROLLUP, 0); }
		public TerminalNode SETS() { return getToken(NebulaSQLParser.SETS, 0); }
		public TerminalNode TRUE() { return getToken(NebulaSQLParser.TRUE, 0); }
		public TerminalNode TYPE() { return getToken(NebulaSQLParser.TYPE, 0); }
		public TerminalNode VALUES() { return getToken(NebulaSQLParser.VALUES, 0); }
		public TerminalNode WINDOW() { return getToken(NebulaSQLParser.WINDOW, 0); }
		public TerminalNode ALL() { return getToken(NebulaSQLParser.ALL, 0); }
		public TerminalNode AND() { return getToken(NebulaSQLParser.AND, 0); }
		public TerminalNode ANY() { return getToken(NebulaSQLParser.ANY, 0); }
		public TerminalNode AS() { return getToken(NebulaSQLParser.AS, 0); }
		public TerminalNode DISTINCT() { return getToken(NebulaSQLParser.DISTINCT, 0); }
		public TerminalNode ESCAPE() { return getToken(NebulaSQLParser.ESCAPE, 0); }
		public TerminalNode FALSE() { return getToken(NebulaSQLParser.FALSE, 0); }
		public TerminalNode FROM() { return getToken(NebulaSQLParser.FROM, 0); }
		public TerminalNode GROUP() { return getToken(NebulaSQLParser.GROUP, 0); }
		public TerminalNode HAVING() { return getToken(NebulaSQLParser.HAVING, 0); }
		public TerminalNode IN() { return getToken(NebulaSQLParser.IN, 0); }
		public TerminalNode INTO() { return getToken(NebulaSQLParser.INTO, 0); }
		public TerminalNode IS() { return getToken(NebulaSQLParser.IS, 0); }
		public TerminalNode NOT() { return getToken(NebulaSQLParser.NOT, 0); }
		public TerminalNode NULLTOKEN() { return getToken(NebulaSQLParser.NULLTOKEN, 0); }
		public TerminalNode OR() { return getToken(NebulaSQLParser.OR, 0); }
		public TerminalNode ORDER() { return getToken(NebulaSQLParser.ORDER, 0); }
		public TerminalNode SELECT() { return getToken(NebulaSQLParser.SELECT, 0); }
		public TerminalNode SOME() { return getToken(NebulaSQLParser.SOME, 0); }
		public TerminalNode TABLE() { return getToken(NebulaSQLParser.TABLE, 0); }
		public TerminalNode WHERE() { return getToken(NebulaSQLParser.WHERE, 0); }
		public TerminalNode WITH() { return getToken(NebulaSQLParser.WITH, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof NebulaSQLListener ) ((NebulaSQLListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof NebulaSQLVisitor ) return ((NebulaSQLVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(875);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & -892438752910246400L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 29749L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 4:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 24:
			return identifier_sempred((IdentifierContext)_localctx, predIndex);
		case 25:
			return strictIdentifier_sempred((StrictIdentifierContext)_localctx, predIndex);
		case 33:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 60:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 64:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		case 66:
			return number_sempred((NumberContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean identifier_sempred(IdentifierContext _localctx, int predIndex) {
		switch (predIndex) {
		case 1:
			return !SQL_standard_keyword_behavior;
		}
		return true;
	}
	private boolean strictIdentifier_sempred(StrictIdentifierContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return SQL_standard_keyword_behavior;
		case 3:
			return !SQL_standard_keyword_behavior;
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 4:
			return precpred(_ctx, 2);
		case 5:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 6:
			return precpred(_ctx, 6);
		case 7:
			return precpred(_ctx, 5);
		case 8:
			return precpred(_ctx, 4);
		case 9:
			return precpred(_ctx, 3);
		case 10:
			return precpred(_ctx, 2);
		case 11:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 12:
			return precpred(_ctx, 7);
		}
		return true;
	}
	private boolean number_sempred(NumberContext _localctx, int predIndex) {
		switch (predIndex) {
		case 13:
			return !legacy_exponent_literal_as_decimal_enabled;
		case 14:
			return !legacy_exponent_literal_as_decimal_enabled;
		case 15:
			return legacy_exponent_literal_as_decimal_enabled;
		}
		return true;
	}

	public static final String _serializedATN =
		"\u0004\u0001\u0089\u036e\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
		"\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
		"\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
		"\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
		"\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
		"\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007"+
		"\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007"+
		"\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007"+
		"\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007"+
		"\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007"+
		"\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007"+
		"\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007"+
		"\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007"+
		",\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u0007"+
		"1\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u0007"+
		"6\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007"+
		";\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007"+
		"@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007"+
		"E\u0002F\u0007F\u0002G\u0007G\u0001\u0000\u0001\u0000\u0005\u0000\u0093"+
		"\b\u0000\n\u0000\f\u0000\u0096\t\u0000\u0001\u0000\u0001\u0000\u0001\u0001"+
		"\u0001\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0005\u0003\u00a4\b\u0003\n\u0003"+
		"\f\u0003\u00a7\t\u0003\u0003\u0003\u00a9\b\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0003\u0003\u00ae\b\u0003\u0003\u0003\u00b0\b\u0003\u0001"+
		"\u0003\u0001\u0003\u0003\u0003\u00b4\b\u0003\u0001\u0004\u0001\u0004\u0001"+
		"\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0005\u0004\u00bc\b\u0004\n"+
		"\u0004\f\u0004\u00bf\t\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003"+
		"\u0005\u00ca\b\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u00cf"+
		"\b\u0006\u0001\u0006\u0003\u0006\u00d2\b\u0006\u0001\u0006\u0003\u0006"+
		"\u00d5\b\u0006\u0001\u0006\u0003\u0006\u00d8\b\u0006\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0005\u0007\u00de\b\u0007\n\u0007\f\u0007"+
		"\u00e1\t\u0007\u0001\b\u0001\b\u0005\b\u00e5\b\b\n\b\f\b\u00e8\t\b\u0001"+
		"\t\u0001\t\u0001\t\u0001\t\u0003\t\u00ee\b\t\u0001\t\u0001\t\u0001\t\u0001"+
		"\t\u0001\t\u0003\t\u00f5\b\t\u0001\n\u0003\n\u00f8\b\n\u0001\u000b\u0001"+
		"\u000b\u0001\u000b\u0001\f\u0001\f\u0001\f\u0001\f\u0001\f\u0001\f\u0001"+
		"\f\u0001\f\u0001\f\u0001\f\u0001\f\u0001\f\u0001\f\u0001\f\u0001\f\u0003"+
		"\f\u010c\b\f\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0005\r\u0113\b\r"+
		"\n\r\f\r\u0116\t\r\u0003\r\u0118\b\r\u0001\r\u0001\r\u0001\r\u0001\u000e"+
		"\u0001\u000e\u0004\u000e\u011f\b\u000e\u000b\u000e\f\u000e\u0120\u0001"+
		"\u000f\u0001\u000f\u0003\u000f\u0125\b\u000f\u0001\u000f\u0003\u000f\u0128"+
		"\b\u000f\u0001\u0010\u0001\u0010\u0005\u0010\u012c\b\u0010\n\u0010\f\u0010"+
		"\u012f\t\u0010\u0001\u0010\u0001\u0010\u0001\u0011\u0001\u0011\u0001\u0011"+
		"\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0013\u0001\u0013\u0001\u0013"+
		"\u0001\u0013\u0005\u0013\u013d\b\u0013\n\u0013\f\u0013\u0140\t\u0013\u0001"+
		"\u0013\u0001\u0013\u0001\u0014\u0003\u0014\u0145\b\u0014\u0001\u0014\u0001"+
		"\u0014\u0003\u0014\u0149\b\u0014\u0003\u0014\u014b\b\u0014\u0001\u0015"+
		"\u0001\u0015\u0001\u0015\u0005\u0015\u0150\b\u0015\n\u0015\f\u0015\u0153"+
		"\t\u0015\u0001\u0016\u0001\u0016\u0001\u0016\u0005\u0016\u0158\b\u0016"+
		"\n\u0016\f\u0016\u015b\t\u0016\u0001\u0017\u0001\u0017\u0003\u0017\u015f"+
		"\b\u0017\u0001\u0017\u0001\u0017\u0003\u0017\u0163\b\u0017\u0003\u0017"+
		"\u0165\b\u0017\u0001\u0018\u0001\u0018\u0001\u0018\u0003\u0018\u016a\b"+
		"\u0018\u0001\u0019\u0001\u0019\u0001\u0019\u0001\u0019\u0001\u0019\u0001"+
		"\u0019\u0003\u0019\u0172\b\u0019\u0001\u001a\u0001\u001a\u0001\u001b\u0001"+
		"\u001b\u0001\u001b\u0001\u001b\u0001\u001c\u0001\u001c\u0001\u001c\u0005"+
		"\u001c\u017d\b\u001c\n\u001c\f\u001c\u0180\t\u001c\u0001\u001d\u0001\u001d"+
		"\u0001\u001d\u0001\u001e\u0001\u001e\u0004\u001e\u0187\b\u001e\u000b\u001e"+
		"\f\u001e\u0188\u0001\u001e\u0003\u001e\u018c\b\u001e\u0001\u001f\u0001"+
		"\u001f\u0001\u001f\u0005\u001f\u0191\b\u001f\n\u001f\f\u001f\u0194\t\u001f"+
		"\u0001 \u0001 \u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001"+
		"!\u0001!\u0001!\u0003!\u01a2\b!\u0003!\u01a4\b!\u0001!\u0001!\u0001!\u0001"+
		"!\u0001!\u0001!\u0005!\u01ac\b!\n!\f!\u01af\t!\u0001\"\u0003\"\u01b2\b"+
		"\"\u0001\"\u0001\"\u0003\"\u01b6\b\"\u0001#\u0001#\u0001#\u0001#\u0001"+
		"#\u0005#\u01bd\b#\n#\f#\u01c0\t#\u0001#\u0001#\u0001#\u0001#\u0001#\u0001"+
		"#\u0001#\u0001#\u0001#\u0001#\u0005#\u01cc\b#\n#\f#\u01cf\t#\u0001#\u0001"+
		"#\u0003#\u01d3\b#\u0001#\u0001#\u0001#\u0001#\u0001#\u0001#\u0001#\u0001"+
		"#\u0005#\u01dd\b#\n#\f#\u01e0\t#\u0001#\u0001#\u0003#\u01e4\b#\u0001$"+
		"\u0001$\u0001$\u0001$\u0005$\u01ea\b$\n$\f$\u01ed\t$\u0003$\u01ef\b$\u0001"+
		"$\u0001$\u0003$\u01f3\b$\u0001%\u0001%\u0001%\u0001&\u0001&\u0001&\u0001"+
		"&\u0001&\u0001\'\u0001\'\u0001\'\u0001\'\u0001\'\u0001(\u0001(\u0001("+
		"\u0003(\u0205\b(\u0001)\u0001)\u0001)\u0001)\u0001)\u0003)\u020c\b)\u0001"+
		")\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0003)\u0216\b)\u0001"+
		")\u0001)\u0001)\u0001)\u0001)\u0003)\u021d\b)\u0001*\u0001*\u0001*\u0001"+
		"*\u0001*\u0001+\u0001+\u0001+\u0001+\u0001+\u0003+\u0229\b+\u0001+\u0001"+
		"+\u0001,\u0001,\u0001-\u0001-\u0001.\u0001.\u0001.\u0001.\u0001/\u0001"+
		"/\u0001/\u0001/\u0001/\u00010\u00010\u00011\u00011\u00012\u00012\u0001"+
		"3\u00013\u00013\u00033\u0243\b3\u00014\u00014\u00034\u0247\b4\u00015\u0003"+
		"5\u024a\b5\u00015\u00015\u00016\u00016\u00017\u00017\u00017\u00017\u0001"+
		"7\u00017\u00017\u00017\u00017\u00018\u00018\u00019\u00019\u0001:\u0001"+
		":\u0003:\u025f\b:\u0001:\u0001:\u0003:\u0263\b:\u0001;\u0003;\u0266\b"+
		";\u0001;\u0001;\u0001;\u0001;\u0001;\u0001;\u0003;\u026e\b;\u0001;\u0001"+
		";\u0001;\u0001;\u0001;\u0005;\u0275\b;\n;\f;\u0278\t;\u0001;\u0001;\u0001"+
		";\u0003;\u027d\b;\u0001;\u0001;\u0001;\u0001;\u0001;\u0001;\u0003;\u0285"+
		"\b;\u0001;\u0001;\u0001;\u0003;\u028a\b;\u0001;\u0001;\u0001;\u0001;\u0001"+
		";\u0001;\u0001;\u0001;\u0005;\u0294\b;\n;\f;\u0297\t;\u0001;\u0001;\u0003"+
		";\u029b\b;\u0001;\u0003;\u029e\b;\u0001;\u0001;\u0001;\u0001;\u0003;\u02a4"+
		"\b;\u0001;\u0001;\u0001;\u0001;\u0003;\u02aa\b;\u0001;\u0001;\u0001;\u0003"+
		";\u02af\b;\u0001;\u0001;\u0001;\u0003;\u02b4\b;\u0001<\u0001<\u0001<\u0001"+
		"<\u0003<\u02ba\b<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001"+
		"<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001"+
		"<\u0001<\u0005<\u02cf\b<\n<\f<\u02d2\t<\u0001=\u0001=\u0001>\u0001>\u0001"+
		">\u0003>\u02d9\b>\u0001>\u0005>\u02dc\b>\n>\f>\u02df\t>\u0001>\u0001>"+
		"\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0005?\u02e9\b?\n?\f?\u02ec"+
		"\t?\u0001?\u0001?\u0003?\u02f0\b?\u0001@\u0001@\u0001@\u0001@\u0001@\u0001"+
		"@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0004@\u0300"+
		"\b@\u000b@\f@\u0301\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0005"+
		"@\u030b\b@\n@\f@\u030e\t@\u0003@\u0310\b@\u0001@\u0001@\u0001@\u0001@"+
		"\u0001@\u0001@\u0001@\u0001@\u0003@\u031a\b@\u0001@\u0001@\u0001@\u0005"+
		"@\u031f\b@\n@\f@\u0322\t@\u0001A\u0001A\u0001A\u0005A\u0327\bA\nA\fA\u032a"+
		"\tA\u0001B\u0001B\u0003B\u032e\bB\u0001B\u0001B\u0001B\u0003B\u0333\b"+
		"B\u0001B\u0001B\u0001B\u0003B\u0338\bB\u0001B\u0001B\u0003B\u033c\bB\u0001"+
		"B\u0001B\u0003B\u0340\bB\u0001B\u0001B\u0003B\u0344\bB\u0001B\u0001B\u0003"+
		"B\u0348\bB\u0001B\u0001B\u0003B\u034c\bB\u0001B\u0001B\u0003B\u0350\b"+
		"B\u0001B\u0001B\u0003B\u0354\bB\u0001B\u0003B\u0357\bB\u0001C\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0004C\u0360\bC\u000bC\fC\u0361\u0003C\u0364"+
		"\bC\u0001D\u0001D\u0001E\u0001E\u0001F\u0001F\u0001G\u0001G\u0001G\u0000"+
		"\u0004\bBx\u0080H\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014"+
		"\u0016\u0018\u001a\u001c\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\^`bdfh"+
		"jlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0000\u000f"+
		"\u0001\u0000TX\u0002\u0000VVY]\u0002\u0000\r\r\u0014\u0014\u0002\u0000"+
		"\u001d\u001d++\u0003\u0000\t\t\u000b\u000b@@\u0003\u0000\u001c\u001cD"+
		"DGG\u0002\u0000optt\u0002\u0000\u0016\u0016qs\u0002\u0000opww\u0001\u0000"+
		"gn\u0001\u0000~\u007f\u0002\u0000\u001c\u001cDD\t\u0000  &&**,,1166;;"+
		"FFII\u0011\u0000\r\u0010\u0012\u0014\u0016\u0017\u001b\u001b\u001d\u001d"+
		"\"\"\'\'++-.004499<=??DEJJMM\u0011\u0000\t\u0010\u0012\u0017\u001a\u001d"+
		"\u001f\u001f!#%%\')++-.002479<@BBDEJJLN\u03b3\u0000\u0090\u0001\u0000"+
		"\u0000\u0000\u0002\u0099\u0001\u0000\u0000\u0000\u0004\u009b\u0001\u0000"+
		"\u0000\u0000\u0006\u00a8\u0001\u0000\u0000\u0000\b\u00b5\u0001\u0000\u0000"+
		"\u0000\n\u00c9\u0001\u0000\u0000\u0000\f\u00cb\u0001\u0000\u0000\u0000"+
		"\u000e\u00d9\u0001\u0000\u0000\u0000\u0010\u00e2\u0001\u0000\u0000\u0000"+
		"\u0012\u00f4\u0001\u0000\u0000\u0000\u0014\u00f7\u0001\u0000\u0000\u0000"+
		"\u0016\u00f9\u0001\u0000\u0000\u0000\u0018\u010b\u0001\u0000\u0000\u0000"+
		"\u001a\u010d\u0001\u0000\u0000\u0000\u001c\u011c\u0001\u0000\u0000\u0000"+
		"\u001e\u0122\u0001\u0000\u0000\u0000 \u0129\u0001\u0000\u0000\u0000\""+
		"\u0132\u0001\u0000\u0000\u0000$\u0135\u0001\u0000\u0000\u0000&\u0138\u0001"+
		"\u0000\u0000\u0000(\u014a\u0001\u0000\u0000\u0000*\u014c\u0001\u0000\u0000"+
		"\u0000,\u0154\u0001\u0000\u0000\u0000.\u015c\u0001\u0000\u0000\u00000"+
		"\u0169\u0001\u0000\u0000\u00002\u0171\u0001\u0000\u0000\u00004\u0173\u0001"+
		"\u0000\u0000\u00006\u0175\u0001\u0000\u0000\u00008\u0179\u0001\u0000\u0000"+
		"\u0000:\u0181\u0001\u0000\u0000\u0000<\u018b\u0001\u0000\u0000\u0000>"+
		"\u018d\u0001\u0000\u0000\u0000@\u0195\u0001\u0000\u0000\u0000B\u01a3\u0001"+
		"\u0000\u0000\u0000D\u01b1\u0001\u0000\u0000\u0000F\u01e3\u0001\u0000\u0000"+
		"\u0000H\u01f2\u0001\u0000\u0000\u0000J\u01f4\u0001\u0000\u0000\u0000L"+
		"\u01f7\u0001\u0000\u0000\u0000N\u01fc\u0001\u0000\u0000\u0000P\u0204\u0001"+
		"\u0000\u0000\u0000R\u021c\u0001\u0000\u0000\u0000T\u021e\u0001\u0000\u0000"+
		"\u0000V\u0223\u0001\u0000\u0000\u0000X\u022c\u0001\u0000\u0000\u0000Z"+
		"\u022e\u0001\u0000\u0000\u0000\\\u0230\u0001\u0000\u0000\u0000^\u0234"+
		"\u0001\u0000\u0000\u0000`\u0239\u0001\u0000\u0000\u0000b\u023b\u0001\u0000"+
		"\u0000\u0000d\u023d\u0001\u0000\u0000\u0000f\u023f\u0001\u0000\u0000\u0000"+
		"h\u0246\u0001\u0000\u0000\u0000j\u0249\u0001\u0000\u0000\u0000l\u024d"+
		"\u0001\u0000\u0000\u0000n\u024f\u0001\u0000\u0000\u0000p\u0258\u0001\u0000"+
		"\u0000\u0000r\u025a\u0001\u0000\u0000\u0000t\u025c\u0001\u0000\u0000\u0000"+
		"v\u02b3\u0001\u0000\u0000\u0000x\u02b9\u0001\u0000\u0000\u0000z\u02d3"+
		"\u0001\u0000\u0000\u0000|\u02d5\u0001\u0000\u0000\u0000~\u02ef\u0001\u0000"+
		"\u0000\u0000\u0080\u0319\u0001\u0000\u0000\u0000\u0082\u0323\u0001\u0000"+
		"\u0000\u0000\u0084\u0356\u0001\u0000\u0000\u0000\u0086\u0363\u0001\u0000"+
		"\u0000\u0000\u0088\u0365\u0001\u0000\u0000\u0000\u008a\u0367\u0001\u0000"+
		"\u0000\u0000\u008c\u0369\u0001\u0000\u0000\u0000\u008e\u036b\u0001\u0000"+
		"\u0000\u0000\u0090\u0094\u0003\u0002\u0001\u0000\u0091\u0093\u0005\u0001"+
		"\u0000\u0000\u0092\u0091\u0001\u0000\u0000\u0000\u0093\u0096\u0001\u0000"+
		"\u0000\u0000\u0094\u0092\u0001\u0000\u0000\u0000\u0094\u0095\u0001\u0000"+
		"\u0000\u0000\u0095\u0097\u0001\u0000\u0000\u0000\u0096\u0094\u0001\u0000"+
		"\u0000\u0000\u0097\u0098\u0005\u0000\u0000\u0001\u0098\u0001\u0001\u0000"+
		"\u0000\u0000\u0099\u009a\u0003\u0004\u0002\u0000\u009a\u0003\u0001\u0000"+
		"\u0000\u0000\u009b\u009c\u0003\b\u0004\u0000\u009c\u009d\u0003\u0006\u0003"+
		"\u0000\u009d\u0005\u0001\u0000\u0000\u0000\u009e\u009f\u00058\u0000\u0000"+
		"\u009f\u00a0\u0005\u0010\u0000\u0000\u00a0\u00a5\u0003t:\u0000\u00a1\u00a2"+
		"\u0005\u0002\u0000\u0000\u00a2\u00a4\u0003t:\u0000\u00a3\u00a1\u0001\u0000"+
		"\u0000\u0000\u00a4\u00a7\u0001\u0000\u0000\u0000\u00a5\u00a3\u0001\u0000"+
		"\u0000\u0000\u00a5\u00a6\u0001\u0000\u0000\u0000\u00a6\u00a9\u0001\u0000"+
		"\u0000\u0000\u00a7\u00a5\u0001\u0000\u0000\u0000\u00a8\u009e\u0001\u0000"+
		"\u0000\u0000\u00a8\u00a9\u0001\u0000\u0000\u0000\u00a9\u00af\u0001\u0000"+
		"\u0000\u0000\u00aa\u00ad\u0005.\u0000\u0000\u00ab\u00ae\u0005\t\u0000"+
		"\u0000\u00ac\u00ae\u0005}\u0000\u0000\u00ad\u00ab\u0001\u0000\u0000\u0000"+
		"\u00ad\u00ac\u0001\u0000\u0000\u0000\u00ae\u00b0\u0001\u0000\u0000\u0000"+
		"\u00af\u00aa\u0001\u0000\u0000\u0000\u00af\u00b0\u0001\u0000\u0000\u0000"+
		"\u00b0\u00b3\u0001\u0000\u0000\u0000\u00b1\u00b2\u0005_\u0000\u0000\u00b2"+
		"\u00b4\u0005}\u0000\u0000\u00b3\u00b1\u0001\u0000\u0000\u0000\u00b3\u00b4"+
		"\u0001\u0000\u0000\u0000\u00b4\u0007\u0001\u0000\u0000\u0000\u00b5\u00b6"+
		"\u0006\u0004\uffff\uffff\u0000\u00b6\u00b7\u0003\n\u0005\u0000\u00b7\u00bd"+
		"\u0001\u0000\u0000\u0000\u00b8\u00b9\n\u0001\u0000\u0000\u00b9\u00ba\u0005"+
		"F\u0000\u0000\u00ba\u00bc\u0003\b\u0004\u0002\u00bb\u00b8\u0001\u0000"+
		"\u0000\u0000\u00bc\u00bf\u0001\u0000\u0000\u0000\u00bd\u00bb\u0001\u0000"+
		"\u0000\u0000\u00bd\u00be\u0001\u0000\u0000\u0000\u00be\t\u0001\u0000\u0000"+
		"\u0000\u00bf\u00bd\u0001\u0000\u0000\u0000\u00c0\u00ca\u0003\f\u0006\u0000"+
		"\u00c1\u00ca\u0003\u001c\u000e\u0000\u00c2\u00c3\u0005B\u0000\u0000\u00c3"+
		"\u00ca\u0003,\u0016\u0000\u00c4\u00ca\u0003&\u0013\u0000\u00c5\u00c6\u0005"+
		"\u0003\u0000\u0000\u00c6\u00c7\u0003\u0004\u0002\u0000\u00c7\u00c8\u0005"+
		"\u0004\u0000\u0000\u00c8\u00ca\u0001\u0000\u0000\u0000\u00c9\u00c0\u0001"+
		"\u0000\u0000\u0000\u00c9\u00c1\u0001\u0000\u0000\u0000\u00c9\u00c2\u0001"+
		"\u0000\u0000\u0000\u00c9\u00c4\u0001\u0000\u0000\u0000\u00c9\u00c5\u0001"+
		"\u0000\u0000\u0000\u00ca\u000b\u0001\u0000\u0000\u0000\u00cb\u00cc\u0003"+
		" \u0010\u0000\u00cc\u00ce\u0003\u000e\u0007\u0000\u00cd\u00cf\u0003\""+
		"\u0011\u0000\u00ce\u00cd\u0001\u0000\u0000\u0000\u00ce\u00cf\u0001\u0000"+
		"\u0000\u0000\u00cf\u00d1\u0001\u0000\u0000\u0000\u00d0\u00d2\u0003D\""+
		"\u0000\u00d1\u00d0\u0001\u0000\u0000\u0000\u00d1\u00d2\u0001\u0000\u0000"+
		"\u0000\u00d2\u00d4\u0001\u0000\u0000\u0000\u00d3\u00d5\u0003$\u0012\u0000"+
		"\u00d4\u00d3\u0001\u0000\u0000\u0000\u00d4\u00d5\u0001\u0000\u0000\u0000"+
		"\u00d5\u00d7\u0001\u0000\u0000\u0000\u00d6\u00d8\u0003f3\u0000\u00d7\u00d6"+
		"\u0001\u0000\u0000\u0000\u00d7\u00d8\u0001\u0000\u0000\u0000\u00d8\r\u0001"+
		"\u0000\u0000\u0000\u00d9\u00da\u0005\u001f\u0000\u0000\u00da\u00df\u0003"+
		"\u0010\b\u0000\u00db\u00dc\u0005\u0002\u0000\u0000\u00dc\u00de\u0003\u0010"+
		"\b\u0000\u00dd\u00db\u0001\u0000\u0000\u0000\u00de\u00e1\u0001\u0000\u0000"+
		"\u0000\u00df\u00dd\u0001\u0000\u0000\u0000\u00df\u00e0\u0001\u0000\u0000"+
		"\u0000\u00e0\u000f\u0001\u0000\u0000\u0000\u00e1\u00df\u0001\u0000\u0000"+
		"\u0000\u00e2\u00e6\u0003\u0018\f\u0000\u00e3\u00e5\u0003\u0012\t\u0000"+
		"\u00e4\u00e3\u0001\u0000\u0000\u0000\u00e5\u00e8\u0001\u0000\u0000\u0000"+
		"\u00e6\u00e4\u0001\u0000\u0000\u0000\u00e6\u00e7\u0001\u0000\u0000\u0000"+
		"\u00e7\u0011\u0001\u0000\u0000\u0000\u00e8\u00e6\u0001\u0000\u0000\u0000"+
		"\u00e9\u00ea\u0003\u0014\n\u0000\u00ea\u00eb\u0005*\u0000\u0000\u00eb"+
		"\u00ed\u0003\u0018\f\u0000\u00ec\u00ee\u0003\u0016\u000b\u0000\u00ed\u00ec"+
		"\u0001\u0000\u0000\u0000\u00ed\u00ee\u0001\u0000\u0000\u0000\u00ee\u00f5"+
		"\u0001\u0000\u0000\u0000\u00ef\u00f0\u00051\u0000\u0000\u00f0\u00f1\u0003"+
		"\u0014\n\u0000\u00f1\u00f2\u0005*\u0000\u0000\u00f2\u00f3\u0003\u0018"+
		"\f\u0000\u00f3\u00f5\u0001\u0000\u0000\u0000\u00f4\u00e9\u0001\u0000\u0000"+
		"\u0000\u00f4\u00ef\u0001\u0000\u0000\u0000\u00f5\u0013\u0001\u0000\u0000"+
		"\u0000\u00f6\u00f8\u0005&\u0000\u0000\u00f7\u00f6\u0001\u0000\u0000\u0000"+
		"\u00f7\u00f8\u0001\u0000\u0000\u0000\u00f8\u0015\u0001\u0000\u0000\u0000"+
		"\u00f9\u00fa\u00056\u0000\u0000\u00fa\u00fb\u0003B!\u0000\u00fb\u0017"+
		"\u0001\u0000\u0000\u0000\u00fc\u00fd\u0003,\u0016\u0000\u00fd\u00fe\u0003"+
		"(\u0014\u0000\u00fe\u010c\u0001\u0000\u0000\u0000\u00ff\u0100\u0005\u0003"+
		"\u0000\u0000\u0100\u0101\u0003\u0004\u0002\u0000\u0101\u0102\u0005\u0004"+
		"\u0000\u0000\u0102\u0103\u0003(\u0014\u0000\u0103\u010c\u0001\u0000\u0000"+
		"\u0000\u0104\u0105\u0005\u0003\u0000\u0000\u0105\u0106\u0003\u0010\b\u0000"+
		"\u0106\u0107\u0005\u0004\u0000\u0000\u0107\u0108\u0003(\u0014\u0000\u0108"+
		"\u010c\u0001\u0000\u0000\u0000\u0109\u010c\u0003&\u0013\u0000\u010a\u010c"+
		"\u0003\u001a\r\u0000\u010b\u00fc\u0001\u0000\u0000\u0000\u010b\u00ff\u0001"+
		"\u0000\u0000\u0000\u010b\u0104\u0001\u0000\u0000\u0000\u010b\u0109\u0001"+
		"\u0000\u0000\u0000\u010b\u010a\u0001\u0000\u0000\u0000\u010c\u0019\u0001"+
		"\u0000\u0000\u0000\u010d\u010e\u0003:\u001d\u0000\u010e\u0117\u0005\u0003"+
		"\u0000\u0000\u010f\u0114\u0003@ \u0000\u0110\u0111\u0005\u0002\u0000\u0000"+
		"\u0111\u0113\u0003@ \u0000\u0112\u0110\u0001\u0000\u0000\u0000\u0113\u0116"+
		"\u0001\u0000\u0000\u0000\u0114\u0112\u0001\u0000\u0000\u0000\u0114\u0115"+
		"\u0001\u0000\u0000\u0000\u0115\u0118\u0001\u0000\u0000\u0000\u0116\u0114"+
		"\u0001\u0000\u0000\u0000\u0117\u010f\u0001\u0000\u0000\u0000\u0117\u0118"+
		"\u0001\u0000\u0000\u0000\u0118\u0119\u0001\u0000\u0000\u0000\u0119\u011a"+
		"\u0005\u0004\u0000\u0000\u011a\u011b\u0003(\u0014\u0000\u011b\u001b\u0001"+
		"\u0000\u0000\u0000\u011c\u011e\u0003\u000e\u0007\u0000\u011d\u011f\u0003"+
		"\u001e\u000f\u0000\u011e\u011d\u0001\u0000\u0000\u0000\u011f\u0120\u0001"+
		"\u0000\u0000\u0000\u0120\u011e\u0001\u0000\u0000\u0000\u0120\u0121\u0001"+
		"\u0000\u0000\u0000\u0121\u001d\u0001\u0000\u0000\u0000\u0122\u0124\u0003"+
		" \u0010\u0000\u0123\u0125\u0003\"\u0011\u0000\u0124\u0123\u0001\u0000"+
		"\u0000\u0000\u0124\u0125\u0001\u0000\u0000\u0000\u0125\u0127\u0001\u0000"+
		"\u0000\u0000\u0126\u0128\u0003F#\u0000\u0127\u0126\u0001\u0000\u0000\u0000"+
		"\u0127\u0128\u0001\u0000\u0000\u0000\u0128\u001f\u0001\u0000\u0000\u0000"+
		"\u0129\u012d\u0005>\u0000\u0000\u012a\u012c\u0003|>\u0000\u012b\u012a"+
		"\u0001\u0000\u0000\u0000\u012c\u012f\u0001\u0000\u0000\u0000\u012d\u012b"+
		"\u0001\u0000\u0000\u0000\u012d\u012e\u0001\u0000\u0000\u0000\u012e\u0130"+
		"\u0001\u0000\u0000\u0000\u012f\u012d\u0001\u0000\u0000\u0000\u0130\u0131"+
		"\u0003>\u001f\u0000\u0131!\u0001\u0000\u0000\u0000\u0132\u0133\u0005L"+
		"\u0000\u0000\u0133\u0134\u0003B!\u0000\u0134#\u0001\u0000\u0000\u0000"+
		"\u0135\u0136\u0005#\u0000\u0000\u0136\u0137\u0003B!\u0000\u0137%\u0001"+
		"\u0000\u0000\u0000\u0138\u0139\u0005J\u0000\u0000\u0139\u013e\u0003@ "+
		"\u0000\u013a\u013b\u0005\u0002\u0000\u0000\u013b\u013d\u0003@ \u0000\u013c"+
		"\u013a\u0001\u0000\u0000\u0000\u013d\u0140\u0001\u0000\u0000\u0000\u013e"+
		"\u013c\u0001\u0000\u0000\u0000\u013e\u013f\u0001\u0000\u0000\u0000\u013f"+
		"\u0141\u0001\u0000\u0000\u0000\u0140\u013e\u0001\u0000\u0000\u0000\u0141"+
		"\u0142\u0003(\u0014\u0000\u0142\'\u0001\u0000\u0000\u0000\u0143\u0145"+
		"\u0005\f\u0000\u0000\u0144\u0143\u0001\u0000\u0000\u0000\u0144\u0145\u0001"+
		"\u0000\u0000\u0000\u0145\u0146\u0001\u0000\u0000\u0000\u0146\u0148\u0003"+
		"2\u0019\u0000\u0147\u0149\u00036\u001b\u0000\u0148\u0147\u0001\u0000\u0000"+
		"\u0000\u0148\u0149\u0001\u0000\u0000\u0000\u0149\u014b\u0001\u0000\u0000"+
		"\u0000\u014a\u0144\u0001\u0000\u0000\u0000\u014a\u014b\u0001\u0000\u0000"+
		"\u0000\u014b)\u0001\u0000\u0000\u0000\u014c\u0151\u0003,\u0016\u0000\u014d"+
		"\u014e\u0005\u0002\u0000\u0000\u014e\u0150\u0003,\u0016\u0000\u014f\u014d"+
		"\u0001\u0000\u0000\u0000\u0150\u0153\u0001\u0000\u0000\u0000\u0151\u014f"+
		"\u0001\u0000\u0000\u0000\u0151\u0152\u0001\u0000\u0000\u0000\u0152+\u0001"+
		"\u0000\u0000\u0000\u0153\u0151\u0001\u0000\u0000\u0000\u0154\u0159\u0003"+
		":\u001d\u0000\u0155\u0156\u0005\u0005\u0000\u0000\u0156\u0158\u0003:\u001d"+
		"\u0000\u0157\u0155\u0001\u0000\u0000\u0000\u0158\u015b\u0001\u0000\u0000"+
		"\u0000\u0159\u0157\u0001\u0000\u0000\u0000\u0159\u015a\u0001\u0000\u0000"+
		"\u0000\u015a-\u0001\u0000\u0000\u0000\u015b\u0159\u0001\u0000\u0000\u0000"+
		"\u015c\u0164\u0003@ \u0000\u015d\u015f\u0005\f\u0000\u0000\u015e\u015d"+
		"\u0001\u0000\u0000\u0000\u015e\u015f\u0001\u0000\u0000\u0000\u015f\u0162"+
		"\u0001\u0000\u0000\u0000\u0160\u0163\u0003:\u001d\u0000\u0161\u0163\u0003"+
		"6\u001b\u0000\u0162\u0160\u0001\u0000\u0000\u0000\u0162\u0161\u0001\u0000"+
		"\u0000\u0000\u0163\u0165\u0001\u0000\u0000\u0000\u0164\u015e\u0001\u0000"+
		"\u0000\u0000\u0164\u0165\u0001\u0000\u0000\u0000\u0165/\u0001\u0000\u0000"+
		"\u0000\u0166\u016a\u00032\u0019\u0000\u0167\u0168\u0004\u0018\u0001\u0000"+
		"\u0168\u016a\u0003\u008aE\u0000\u0169\u0166\u0001\u0000\u0000\u0000\u0169"+
		"\u0167\u0001\u0000\u0000\u0000\u016a1\u0001\u0000\u0000\u0000\u016b\u0172"+
		"\u0005\u0083\u0000\u0000\u016c\u0172\u00034\u001a\u0000\u016d\u016e\u0004"+
		"\u0019\u0002\u0000\u016e\u0172\u0003\u008cF\u0000\u016f\u0170\u0004\u0019"+
		"\u0003\u0000\u0170\u0172\u0003\u008eG\u0000\u0171\u016b\u0001\u0000\u0000"+
		"\u0000\u0171\u016c\u0001\u0000\u0000\u0000\u0171\u016d\u0001\u0000\u0000"+
		"\u0000\u0171\u016f\u0001\u0000\u0000\u0000\u01723\u0001\u0000\u0000\u0000"+
		"\u0173\u0174\u0005\b\u0000\u0000\u01745\u0001\u0000\u0000\u0000\u0175"+
		"\u0176\u0005\u0003\u0000\u0000\u0176\u0177\u00038\u001c\u0000\u0177\u0178"+
		"\u0005\u0004\u0000\u0000\u01787\u0001\u0000\u0000\u0000\u0179\u017e\u0003"+
		":\u001d\u0000\u017a\u017b\u0005\u0002\u0000\u0000\u017b\u017d\u0003:\u001d"+
		"\u0000\u017c\u017a\u0001\u0000\u0000\u0000\u017d\u0180\u0001\u0000\u0000"+
		"\u0000\u017e\u017c\u0001\u0000\u0000\u0000\u017e\u017f\u0001\u0000\u0000"+
		"\u0000\u017f9\u0001\u0000\u0000\u0000\u0180\u017e\u0001\u0000\u0000\u0000"+
		"\u0181\u0182\u00030\u0018\u0000\u0182\u0183\u0003<\u001e\u0000\u0183;"+
		"\u0001\u0000\u0000\u0000\u0184\u0185\u0005p\u0000\u0000\u0185\u0187\u0003"+
		"0\u0018\u0000\u0186\u0184\u0001\u0000\u0000\u0000\u0187\u0188\u0001\u0000"+
		"\u0000\u0000\u0188\u0186\u0001\u0000\u0000\u0000\u0188\u0189\u0001\u0000"+
		"\u0000\u0000\u0189\u018c\u0001\u0000\u0000\u0000\u018a\u018c\u0001\u0000"+
		"\u0000\u0000\u018b\u0186\u0001\u0000\u0000\u0000\u018b\u018a\u0001\u0000"+
		"\u0000\u0000\u018c=\u0001\u0000\u0000\u0000\u018d\u0192\u0003.\u0017\u0000"+
		"\u018e\u018f\u0005\u0002\u0000\u0000\u018f\u0191\u0003.\u0017\u0000\u0190"+
		"\u018e\u0001\u0000\u0000\u0000\u0191\u0194\u0001\u0000\u0000\u0000\u0192"+
		"\u0190\u0001\u0000\u0000\u0000\u0192\u0193\u0001\u0000\u0000\u0000\u0193"+
		"?\u0001\u0000\u0000\u0000\u0194\u0192\u0001\u0000\u0000\u0000\u0195\u0196"+
		"\u0003B!\u0000\u0196A\u0001\u0000\u0000\u0000\u0197\u0198\u0006!\uffff"+
		"\uffff\u0000\u0198\u0199\u00052\u0000\u0000\u0199\u01a4\u0003B!\u0005"+
		"\u019a\u019b\u0005\u001b\u0000\u0000\u019b\u019c\u0005\u0003\u0000\u0000"+
		"\u019c\u019d\u0003\u0004\u0002\u0000\u019d\u019e\u0005\u0004\u0000\u0000"+
		"\u019e\u01a4\u0001\u0000\u0000\u0000\u019f\u01a1\u0003x<\u0000\u01a0\u01a2"+
		"\u0003v;\u0000\u01a1\u01a0\u0001\u0000\u0000\u0000\u01a1\u01a2\u0001\u0000"+
		"\u0000\u0000\u01a2\u01a4\u0001\u0000\u0000\u0000\u01a3\u0197\u0001\u0000"+
		"\u0000\u0000\u01a3\u019a\u0001\u0000\u0000\u0000\u01a3\u019f\u0001\u0000"+
		"\u0000\u0000\u01a4\u01ad\u0001\u0000\u0000\u0000\u01a5\u01a6\n\u0002\u0000"+
		"\u0000\u01a6\u01a7\u0005\n\u0000\u0000\u01a7\u01ac\u0003B!\u0003\u01a8"+
		"\u01a9\n\u0001\u0000\u0000\u01a9\u01aa\u00057\u0000\u0000\u01aa\u01ac"+
		"\u0003B!\u0002\u01ab\u01a5\u0001\u0000\u0000\u0000\u01ab\u01a8\u0001\u0000"+
		"\u0000\u0000\u01ac\u01af\u0001\u0000\u0000\u0000\u01ad\u01ab\u0001\u0000"+
		"\u0000\u0000\u01ad\u01ae\u0001\u0000\u0000\u0000\u01aeC\u0001\u0000\u0000"+
		"\u0000\u01af\u01ad\u0001\u0000\u0000\u0000\u01b0\u01b2\u0003F#\u0000\u01b1"+
		"\u01b0\u0001\u0000\u0000\u0000\u01b1\u01b2\u0001\u0000\u0000\u0000\u01b2"+
		"\u01b3\u0001\u0000\u0000\u0000\u01b3\u01b5\u0003J%\u0000\u01b4\u01b6\u0003"+
		"L&\u0000\u01b5\u01b4\u0001\u0000\u0000\u0000\u01b5\u01b6\u0001\u0000\u0000"+
		"\u0000\u01b6E\u0001\u0000\u0000\u0000\u01b7\u01b8\u0005!\u0000\u0000\u01b8"+
		"\u01b9\u0005\u0010\u0000\u0000\u01b9\u01be\u0003@ \u0000\u01ba\u01bb\u0005"+
		"\u0002\u0000\u0000\u01bb\u01bd\u0003@ \u0000\u01bc\u01ba\u0001\u0000\u0000"+
		"\u0000\u01bd\u01c0\u0001\u0000\u0000\u0000\u01be\u01bc\u0001\u0000\u0000"+
		"\u0000\u01be\u01bf\u0001\u0000\u0000\u0000\u01bf\u01d2\u0001\u0000\u0000"+
		"\u0000\u01c0\u01be\u0001\u0000\u0000\u0000\u01c1\u01c2\u0005N\u0000\u0000"+
		"\u01c2\u01d3\u0005=\u0000\u0000\u01c3\u01c4\u0005N\u0000\u0000\u01c4\u01d3"+
		"\u0005\u0012\u0000\u0000\u01c5\u01c6\u0005\"\u0000\u0000\u01c6\u01c7\u0005"+
		"?\u0000\u0000\u01c7\u01c8\u0005\u0003\u0000\u0000\u01c8\u01cd\u0003H$"+
		"\u0000\u01c9\u01ca\u0005\u0002\u0000\u0000\u01ca\u01cc\u0003H$\u0000\u01cb"+
		"\u01c9\u0001\u0000\u0000\u0000\u01cc\u01cf\u0001\u0000\u0000\u0000\u01cd"+
		"\u01cb\u0001\u0000\u0000\u0000\u01cd\u01ce\u0001\u0000\u0000\u0000\u01ce"+
		"\u01d0\u0001\u0000\u0000\u0000\u01cf\u01cd\u0001\u0000\u0000\u0000\u01d0"+
		"\u01d1\u0005\u0004\u0000\u0000\u01d1\u01d3\u0001\u0000\u0000\u0000\u01d2"+
		"\u01c1\u0001\u0000\u0000\u0000\u01d2\u01c3\u0001\u0000\u0000\u0000\u01d2"+
		"\u01c5\u0001\u0000\u0000\u0000\u01d2\u01d3\u0001\u0000\u0000\u0000\u01d3"+
		"\u01e4\u0001\u0000\u0000\u0000\u01d4\u01d5\u0005!\u0000\u0000\u01d5\u01d6"+
		"\u0005\u0010\u0000\u0000\u01d6\u01d7\u0005\"\u0000\u0000\u01d7\u01d8\u0005"+
		"?\u0000\u0000\u01d8\u01d9\u0005\u0003\u0000\u0000\u01d9\u01de\u0003H$"+
		"\u0000\u01da\u01db\u0005\u0002\u0000\u0000\u01db\u01dd\u0003H$\u0000\u01dc"+
		"\u01da\u0001\u0000\u0000\u0000\u01dd\u01e0\u0001\u0000\u0000\u0000\u01de"+
		"\u01dc\u0001\u0000\u0000\u0000\u01de\u01df\u0001\u0000\u0000\u0000\u01df"+
		"\u01e1\u0001\u0000\u0000\u0000\u01e0\u01de\u0001\u0000\u0000\u0000\u01e1"+
		"\u01e2\u0005\u0004\u0000\u0000\u01e2\u01e4\u0001\u0000\u0000\u0000\u01e3"+
		"\u01b7\u0001\u0000\u0000\u0000\u01e3\u01d4\u0001\u0000\u0000\u0000\u01e4"+
		"G\u0001\u0000\u0000\u0000\u01e5\u01ee\u0005\u0003\u0000\u0000\u01e6\u01eb"+
		"\u0003@ \u0000\u01e7\u01e8\u0005\u0002\u0000\u0000\u01e8\u01ea\u0003@"+
		" \u0000\u01e9\u01e7\u0001\u0000\u0000\u0000\u01ea\u01ed\u0001\u0000\u0000"+
		"\u0000\u01eb\u01e9\u0001\u0000\u0000\u0000\u01eb\u01ec\u0001\u0000\u0000"+
		"\u0000\u01ec\u01ef\u0001\u0000\u0000\u0000\u01ed\u01eb\u0001\u0000\u0000"+
		"\u0000\u01ee\u01e6\u0001\u0000\u0000\u0000\u01ee\u01ef\u0001\u0000\u0000"+
		"\u0000\u01ef\u01f0\u0001\u0000\u0000\u0000\u01f0\u01f3\u0005\u0004\u0000"+
		"\u0000\u01f1\u01f3\u0003@ \u0000\u01f2\u01e5\u0001\u0000\u0000\u0000\u01f2"+
		"\u01f1\u0001\u0000\u0000\u0000\u01f3I\u0001\u0000\u0000\u0000\u01f4\u01f5"+
		"\u0005M\u0000\u0000\u01f5\u01f6\u0003P(\u0000\u01f6K\u0001\u0000\u0000"+
		"\u0000\u01f7\u01f8\u0005^\u0000\u0000\u01f8\u01f9\u0005\u0003\u0000\u0000"+
		"\u01f9\u01fa\u0003N\'\u0000\u01fa\u01fb\u0005\u0004\u0000\u0000\u01fb"+
		"M\u0001\u0000\u0000\u0000\u01fc\u01fd\u00030\u0018\u0000\u01fd\u01fe\u0005"+
		"\u0002\u0000\u0000\u01fe\u01ff\u0005}\u0000\u0000\u01ff\u0200\u0003`0"+
		"\u0000\u0200O\u0001\u0000\u0000\u0000\u0201\u0205\u0003R)\u0000\u0202"+
		"\u0205\u0003T*\u0000\u0203\u0205\u0003V+\u0000\u0204\u0201\u0001\u0000"+
		"\u0000\u0000\u0204\u0202\u0001\u0000\u0000\u0000\u0204\u0203\u0001\u0000"+
		"\u0000\u0000\u0205Q\u0001\u0000\u0000\u0000\u0206\u0207\u0005O\u0000\u0000"+
		"\u0207\u020b\u0005\u0003\u0000\u0000\u0208\u0209\u0003b1\u0000\u0209\u020a"+
		"\u0005\u0002\u0000\u0000\u020a\u020c\u0001\u0000\u0000\u0000\u020b\u0208"+
		"\u0001\u0000\u0000\u0000\u020b\u020c\u0001\u0000\u0000\u0000\u020c\u020d"+
		"\u0001\u0000\u0000\u0000\u020d\u020e\u0003\\.\u0000\u020e\u020f\u0005"+
		"\u0004\u0000\u0000\u020f\u021d\u0001\u0000\u0000\u0000\u0210\u0211\u0005"+
		"P\u0000\u0000\u0211\u0215\u0005\u0003\u0000\u0000\u0212\u0213\u0003b1"+
		"\u0000\u0213\u0214\u0005\u0002\u0000\u0000\u0214\u0216\u0001\u0000\u0000"+
		"\u0000\u0215\u0212\u0001\u0000\u0000\u0000\u0215\u0216\u0001\u0000\u0000"+
		"\u0000\u0216\u0217\u0001\u0000\u0000\u0000\u0217\u0218\u0003\\.\u0000"+
		"\u0218\u0219\u0005\u0002\u0000\u0000\u0219\u021a\u0003^/\u0000\u021a\u021b"+
		"\u0005\u0004\u0000\u0000\u021b\u021d\u0001\u0000\u0000\u0000\u021c\u0206"+
		"\u0001\u0000\u0000\u0000\u021c\u0210\u0001\u0000\u0000\u0000\u021dS\u0001"+
		"\u0000\u0000\u0000\u021e\u021f\u0005O\u0000\u0000\u021f\u0220\u0005\u0003"+
		"\u0000\u0000\u0220\u0221\u0005}\u0000\u0000\u0221\u0222\u0005\u0004\u0000"+
		"\u0000\u0222U\u0001\u0000\u0000\u0000\u0223\u0224\u0005Q\u0000\u0000\u0224"+
		"\u0225\u0005\u0003\u0000\u0000\u0225\u0228\u0003X,\u0000\u0226\u0227\u0005"+
		"\u0002\u0000\u0000\u0227\u0229\u0003Z-\u0000\u0228\u0226\u0001\u0000\u0000"+
		"\u0000\u0228\u0229\u0001\u0000\u0000\u0000\u0229\u022a\u0001\u0000\u0000"+
		"\u0000\u022a\u022b\u0005\u0004\u0000\u0000\u022bW\u0001\u0000\u0000\u0000"+
		"\u022c\u022d\u0003@ \u0000\u022dY\u0001\u0000\u0000\u0000\u022e\u022f"+
		"\u0005}\u0000\u0000\u022f[\u0001\u0000\u0000\u0000\u0230\u0231\u0005R"+
		"\u0000\u0000\u0231\u0232\u0005}\u0000\u0000\u0232\u0233\u0003`0\u0000"+
		"\u0233]\u0001\u0000\u0000\u0000\u0234\u0235\u0005S\u0000\u0000\u0235\u0236"+
		"\u0005\u0010\u0000\u0000\u0236\u0237\u0005}\u0000\u0000\u0237\u0238\u0003"+
		"`0\u0000\u0238_\u0001\u0000\u0000\u0000\u0239\u023a\u0007\u0000\u0000"+
		"\u0000\u023aa\u0001\u0000\u0000\u0000\u023b\u023c\u0005\u0083\u0000\u0000"+
		"\u023cc\u0001\u0000\u0000\u0000\u023d\u023e\u0007\u0001\u0000\u0000\u023e"+
		"e\u0001\u0000\u0000\u0000\u023f\u0240\u0005(\u0000\u0000\u0240\u0242\u0003"+
		"h4\u0000\u0241\u0243\u0005\f\u0000\u0000\u0242\u0241\u0001\u0000\u0000"+
		"\u0000\u0242\u0243\u0001\u0000\u0000\u0000\u0243g\u0001\u0000\u0000\u0000"+
		"\u0244\u0247\u0003n7\u0000\u0245\u0247\u0003p8\u0000\u0246\u0244\u0001"+
		"\u0000\u0000\u0000\u0246\u0245\u0001\u0000\u0000\u0000\u0247i\u0001\u0000"+
		"\u0000\u0000\u0248\u024a\u00052\u0000\u0000\u0249\u0248\u0001\u0000\u0000"+
		"\u0000\u0249\u024a\u0001\u0000\u0000\u0000\u024a\u024b\u0001\u0000\u0000"+
		"\u0000\u024b\u024c\u00053\u0000\u0000\u024ck\u0001\u0000\u0000\u0000\u024d"+
		"\u024e\u0005\u0083\u0000\u0000\u024em\u0001\u0000\u0000\u0000\u024f\u0250"+
		"\u0005`\u0000\u0000\u0250\u0251\u0005\u0003\u0000\u0000\u0251\u0252\u0005"+
		"y\u0000\u0000\u0252\u0253\u0005\u0002\u0000\u0000\u0253\u0254\u0003r9"+
		"\u0000\u0254\u0255\u0005\u0002\u0000\u0000\u0255\u0256\u0005y\u0000\u0000"+
		"\u0256\u0257\u0005\u0004\u0000\u0000\u0257o\u0001\u0000\u0000\u0000\u0258"+
		"\u0259\u0005a\u0000\u0000\u0259q\u0001\u0000\u0000\u0000\u025a\u025b\u0005"+
		"c\u0000\u0000\u025bs\u0001\u0000\u0000\u0000\u025c\u025e\u0003@ \u0000"+
		"\u025d\u025f\u0007\u0002\u0000\u0000\u025e\u025d\u0001\u0000\u0000\u0000"+
		"\u025e\u025f\u0001\u0000\u0000\u0000\u025f\u0262\u0001\u0000\u0000\u0000"+
		"\u0260\u0261\u00054\u0000\u0000\u0261\u0263\u0007\u0003\u0000\u0000\u0262"+
		"\u0260\u0001\u0000\u0000\u0000\u0262\u0263\u0001\u0000\u0000\u0000\u0263"+
		"u\u0001\u0000\u0000\u0000\u0264\u0266\u00052\u0000\u0000\u0265\u0264\u0001"+
		"\u0000\u0000\u0000\u0265\u0266\u0001\u0000\u0000\u0000\u0266\u0267\u0001"+
		"\u0000\u0000\u0000\u0267\u0268\u0005\u000f\u0000\u0000\u0268\u0269\u0003"+
		"x<\u0000\u0269\u026a\u0005\n\u0000\u0000\u026a\u026b\u0003x<\u0000\u026b"+
		"\u02b4\u0001\u0000\u0000\u0000\u026c\u026e\u00052\u0000\u0000\u026d\u026c"+
		"\u0001\u0000\u0000\u0000\u026d\u026e\u0001\u0000\u0000\u0000\u026e\u026f"+
		"\u0001\u0000\u0000\u0000\u026f\u0270\u0005%\u0000\u0000\u0270\u0271\u0005"+
		"\u0003\u0000\u0000\u0271\u0276\u0003@ \u0000\u0272\u0273\u0005\u0002\u0000"+
		"\u0000\u0273\u0275\u0003@ \u0000\u0274\u0272\u0001\u0000\u0000\u0000\u0275"+
		"\u0278\u0001\u0000\u0000\u0000\u0276\u0274\u0001\u0000\u0000\u0000\u0276"+
		"\u0277\u0001\u0000\u0000\u0000\u0277\u0279\u0001\u0000\u0000\u0000\u0278"+
		"\u0276\u0001\u0000\u0000\u0000\u0279\u027a\u0005\u0004\u0000\u0000\u027a"+
		"\u02b4\u0001\u0000\u0000\u0000\u027b\u027d\u00052\u0000\u0000\u027c\u027b"+
		"\u0001\u0000\u0000\u0000\u027c\u027d\u0001\u0000\u0000\u0000\u027d\u027e"+
		"\u0001\u0000\u0000\u0000\u027e\u027f\u0005%\u0000\u0000\u027f\u0280\u0005"+
		"\u0003\u0000\u0000\u0280\u0281\u0003\u0004\u0002\u0000\u0281\u0282\u0005"+
		"\u0004\u0000\u0000\u0282\u02b4\u0001\u0000\u0000\u0000\u0283\u0285\u0005"+
		"2\u0000\u0000\u0284\u0283\u0001\u0000\u0000\u0000\u0284\u0285\u0001\u0000"+
		"\u0000\u0000\u0285\u0286\u0001\u0000\u0000\u0000\u0286\u0287\u0005<\u0000"+
		"\u0000\u0287\u02b4\u0003x<\u0000\u0288\u028a\u00052\u0000\u0000\u0289"+
		"\u0288\u0001\u0000\u0000\u0000\u0289\u028a\u0001\u0000\u0000\u0000\u028a"+
		"\u028b\u0001\u0000\u0000\u0000\u028b\u028c\u0005-\u0000\u0000\u028c\u029a"+
		"\u0007\u0004\u0000\u0000\u028d\u028e\u0005\u0003\u0000\u0000\u028e\u029b"+
		"\u0005\u0004\u0000\u0000\u028f\u0290\u0005\u0003\u0000\u0000\u0290\u0295"+
		"\u0003@ \u0000\u0291\u0292\u0005\u0002\u0000\u0000\u0292\u0294\u0003@"+
		" \u0000\u0293\u0291\u0001\u0000\u0000\u0000\u0294\u0297\u0001\u0000\u0000"+
		"\u0000\u0295\u0293\u0001\u0000\u0000\u0000\u0295\u0296\u0001\u0000\u0000"+
		"\u0000\u0296\u0298\u0001\u0000\u0000\u0000\u0297\u0295\u0001\u0000\u0000"+
		"\u0000\u0298\u0299\u0005\u0004\u0000\u0000\u0299\u029b\u0001\u0000\u0000"+
		"\u0000\u029a\u028d\u0001\u0000\u0000\u0000\u029a\u028f\u0001\u0000\u0000"+
		"\u0000\u029b\u02b4\u0001\u0000\u0000\u0000\u029c\u029e\u00052\u0000\u0000"+
		"\u029d\u029c\u0001\u0000\u0000\u0000\u029d\u029e\u0001\u0000\u0000\u0000"+
		"\u029e\u029f\u0001\u0000\u0000\u0000\u029f\u02a0\u0005-\u0000\u0000\u02a0"+
		"\u02a3\u0003x<\u0000\u02a1\u02a2\u0005\u001a\u0000\u0000\u02a2\u02a4\u0005"+
		"y\u0000\u0000\u02a3\u02a1\u0001\u0000\u0000\u0000\u02a3\u02a4\u0001\u0000"+
		"\u0000\u0000\u02a4\u02b4\u0001\u0000\u0000\u0000\u02a5\u02a6\u0005)\u0000"+
		"\u0000\u02a6\u02b4\u0003j5\u0000\u02a7\u02a9\u0005)\u0000\u0000\u02a8"+
		"\u02aa\u00052\u0000\u0000\u02a9\u02a8\u0001\u0000\u0000\u0000\u02a9\u02aa"+
		"\u0001\u0000\u0000\u0000\u02aa\u02ab\u0001\u0000\u0000\u0000\u02ab\u02b4"+
		"\u0007\u0005\u0000\u0000\u02ac\u02ae\u0005)\u0000\u0000\u02ad\u02af\u0005"+
		"2\u0000\u0000\u02ae\u02ad\u0001\u0000\u0000\u0000\u02ae\u02af\u0001\u0000"+
		"\u0000\u0000\u02af\u02b0\u0001\u0000\u0000\u0000\u02b0\u02b1\u0005\u0015"+
		"\u0000\u0000\u02b1\u02b2\u0005\u001f\u0000\u0000\u02b2\u02b4\u0003x<\u0000"+
		"\u02b3\u0265\u0001\u0000\u0000\u0000\u02b3\u026d\u0001\u0000\u0000\u0000"+
		"\u02b3\u027c\u0001\u0000\u0000\u0000\u02b3\u0284\u0001\u0000\u0000\u0000"+
		"\u02b3\u0289\u0001\u0000\u0000\u0000\u02b3\u029d\u0001\u0000\u0000\u0000"+
		"\u02b3\u02a5\u0001\u0000\u0000\u0000\u02b3\u02a7\u0001\u0000\u0000\u0000"+
		"\u02b3\u02ac\u0001\u0000\u0000\u0000\u02b4w\u0001\u0000\u0000\u0000\u02b5"+
		"\u02b6\u0006<\uffff\uffff\u0000\u02b6\u02ba\u0003\u0080@\u0000\u02b7\u02b8"+
		"\u0007\u0006\u0000\u0000\u02b8\u02ba\u0003x<\u0007\u02b9\u02b5\u0001\u0000"+
		"\u0000\u0000\u02b9\u02b7\u0001\u0000\u0000\u0000\u02ba\u02d0\u0001\u0000"+
		"\u0000\u0000\u02bb\u02bc\n\u0006\u0000\u0000\u02bc\u02bd\u0007\u0007\u0000"+
		"\u0000\u02bd\u02cf\u0003x<\u0007\u02be\u02bf\n\u0005\u0000\u0000\u02bf"+
		"\u02c0\u0007\b\u0000\u0000\u02c0\u02cf\u0003x<\u0006\u02c1\u02c2\n\u0004"+
		"\u0000\u0000\u02c2\u02c3\u0005u\u0000\u0000\u02c3\u02cf\u0003x<\u0005"+
		"\u02c4\u02c5\n\u0003\u0000\u0000\u02c5\u02c6\u0005x\u0000\u0000\u02c6"+
		"\u02cf\u0003x<\u0004\u02c7\u02c8\n\u0002\u0000\u0000\u02c8\u02c9\u0005"+
		"v\u0000\u0000\u02c9\u02cf\u0003x<\u0003\u02ca\u02cb\n\u0001\u0000\u0000"+
		"\u02cb\u02cc\u0003z=\u0000\u02cc\u02cd\u0003x<\u0002\u02cd\u02cf\u0001"+
		"\u0000\u0000\u0000\u02ce\u02bb\u0001\u0000\u0000\u0000\u02ce\u02be\u0001"+
		"\u0000\u0000\u0000\u02ce\u02c1\u0001\u0000\u0000\u0000\u02ce\u02c4\u0001"+
		"\u0000\u0000\u0000\u02ce\u02c7\u0001\u0000\u0000\u0000\u02ce\u02ca\u0001"+
		"\u0000\u0000\u0000\u02cf\u02d2\u0001\u0000\u0000\u0000\u02d0\u02ce\u0001"+
		"\u0000\u0000\u0000\u02d0\u02d1\u0001\u0000\u0000\u0000\u02d1y\u0001\u0000"+
		"\u0000\u0000\u02d2\u02d0\u0001\u0000\u0000\u0000\u02d3\u02d4\u0007\t\u0000"+
		"\u0000\u02d4{\u0001\u0000\u0000\u0000\u02d5\u02d6\u0005\u0006\u0000\u0000"+
		"\u02d6\u02dd\u0003~?\u0000\u02d7\u02d9\u0005\u0002\u0000\u0000\u02d8\u02d7"+
		"\u0001\u0000\u0000\u0000\u02d8\u02d9\u0001\u0000\u0000\u0000\u02d9\u02da"+
		"\u0001\u0000\u0000\u0000\u02da\u02dc\u0003~?\u0000\u02db\u02d8\u0001\u0000"+
		"\u0000\u0000\u02dc\u02df\u0001\u0000\u0000\u0000\u02dd\u02db\u0001\u0000"+
		"\u0000\u0000\u02dd\u02de\u0001\u0000\u0000\u0000\u02de\u02e0\u0001\u0000"+
		"\u0000\u0000\u02df\u02dd\u0001\u0000\u0000\u0000\u02e0\u02e1\u0005\u0007"+
		"\u0000\u0000\u02e1}\u0001\u0000\u0000\u0000\u02e2\u02f0\u00030\u0018\u0000"+
		"\u02e3\u02e4\u00030\u0018\u0000\u02e4\u02e5\u0005\u0003\u0000\u0000\u02e5"+
		"\u02ea\u0003\u0080@\u0000\u02e6\u02e7\u0005\u0002\u0000\u0000\u02e7\u02e9"+
		"\u0003\u0080@\u0000\u02e8\u02e6\u0001\u0000\u0000\u0000\u02e9\u02ec\u0001"+
		"\u0000\u0000\u0000\u02ea\u02e8\u0001\u0000\u0000\u0000\u02ea\u02eb\u0001"+
		"\u0000\u0000\u0000\u02eb\u02ed\u0001\u0000\u0000\u0000\u02ec\u02ea\u0001"+
		"\u0000\u0000\u0000\u02ed\u02ee\u0005\u0004\u0000\u0000\u02ee\u02f0\u0001"+
		"\u0000\u0000\u0000\u02ef\u02e2\u0001\u0000\u0000\u0000\u02ef\u02e3\u0001"+
		"\u0000\u0000\u0000\u02f0\u007f\u0001\u0000\u0000\u0000\u02f1\u02f2\u0006"+
		"@\uffff\uffff\u0000\u02f2\u031a\u0005q\u0000\u0000\u02f3\u02f4\u0003\u0082"+
		"A\u0000\u02f4\u02f5\u0005\u0005\u0000\u0000\u02f5\u02f6\u0005q\u0000\u0000"+
		"\u02f6\u031a\u0001\u0000\u0000\u0000\u02f7\u02f8\u0005\u0003\u0000\u0000"+
		"\u02f8\u02f9\u0003\u0004\u0002\u0000\u02f9\u02fa\u0005\u0004\u0000\u0000"+
		"\u02fa\u031a\u0001\u0000\u0000\u0000\u02fb\u02fc\u0005\u0003\u0000\u0000"+
		"\u02fc\u02ff\u0003.\u0017\u0000\u02fd\u02fe\u0005\u0002\u0000\u0000\u02fe"+
		"\u0300\u0003.\u0017\u0000\u02ff\u02fd\u0001\u0000\u0000\u0000\u0300\u0301"+
		"\u0001\u0000\u0000\u0000\u0301\u02ff\u0001\u0000\u0000\u0000\u0301\u0302"+
		"\u0001\u0000\u0000\u0000\u0302\u0303\u0001\u0000\u0000\u0000\u0303\u0304"+
		"\u0005\u0004\u0000\u0000\u0304\u031a\u0001\u0000\u0000\u0000\u0305\u0306"+
		"\u0003d2\u0000\u0306\u030f\u0005\u0003\u0000\u0000\u0307\u030c\u0003@"+
		" \u0000\u0308\u0309\u0005\u0002\u0000\u0000\u0309\u030b\u0003@ \u0000"+
		"\u030a\u0308\u0001\u0000\u0000\u0000\u030b\u030e\u0001\u0000\u0000\u0000"+
		"\u030c\u030a\u0001\u0000\u0000\u0000\u030c\u030d\u0001\u0000\u0000\u0000"+
		"\u030d\u0310\u0001\u0000\u0000\u0000\u030e\u030c\u0001\u0000\u0000\u0000"+
		"\u030f\u0307\u0001\u0000\u0000\u0000\u030f\u0310\u0001\u0000\u0000\u0000"+
		"\u0310\u0311\u0001\u0000\u0000\u0000\u0311\u0312\u0005\u0004\u0000\u0000"+
		"\u0312\u031a\u0001\u0000\u0000\u0000\u0313\u0314\u0005\u0003\u0000\u0000"+
		"\u0314\u0315\u0003@ \u0000\u0315\u0316\u0005\u0004\u0000\u0000\u0316\u031a"+
		"\u0001\u0000\u0000\u0000\u0317\u031a\u0003\u0086C\u0000\u0318\u031a\u0003"+
		"0\u0018\u0000\u0319\u02f1\u0001\u0000\u0000\u0000\u0319\u02f3\u0001\u0000"+
		"\u0000\u0000\u0319\u02f7\u0001\u0000\u0000\u0000\u0319\u02fb\u0001\u0000"+
		"\u0000\u0000\u0319\u0305\u0001\u0000\u0000\u0000\u0319\u0313\u0001\u0000"+
		"\u0000\u0000\u0319\u0317\u0001\u0000\u0000\u0000\u0319\u0318\u0001\u0000"+
		"\u0000\u0000\u031a\u0320\u0001\u0000\u0000\u0000\u031b\u031c\n\u0007\u0000"+
		"\u0000\u031c\u031d\u0005\u0005\u0000\u0000\u031d\u031f\u00030\u0018\u0000"+
		"\u031e\u031b\u0001\u0000\u0000\u0000\u031f\u0322\u0001\u0000\u0000\u0000"+
		"\u0320\u031e\u0001\u0000\u0000\u0000\u0320\u0321\u0001\u0000\u0000\u0000"+
		"\u0321\u0081\u0001\u0000\u0000\u0000\u0322\u0320\u0001\u0000\u0000\u0000"+
		"\u0323\u0328\u00030\u0018\u0000\u0324\u0325\u0005\u0005\u0000\u0000\u0325"+
		"\u0327\u00030\u0018\u0000\u0326\u0324\u0001\u0000\u0000\u0000\u0327\u032a"+
		"\u0001\u0000\u0000\u0000\u0328\u0326\u0001\u0000\u0000\u0000\u0328\u0329"+
		"\u0001\u0000\u0000\u0000\u0329\u0083\u0001\u0000\u0000\u0000\u032a\u0328"+
		"\u0001\u0000\u0000\u0000\u032b\u032d\u0004B\r\u0000\u032c\u032e\u0005"+
		"p\u0000\u0000\u032d\u032c\u0001\u0000\u0000\u0000\u032d\u032e\u0001\u0000"+
		"\u0000\u0000\u032e\u032f\u0001\u0000\u0000\u0000\u032f\u0357\u0005~\u0000"+
		"\u0000\u0330\u0332\u0004B\u000e\u0000\u0331\u0333\u0005p\u0000\u0000\u0332"+
		"\u0331\u0001\u0000\u0000\u0000\u0332\u0333\u0001\u0000\u0000\u0000\u0333"+
		"\u0334\u0001\u0000\u0000\u0000\u0334\u0357\u0005\u007f\u0000\u0000\u0335"+
		"\u0337\u0004B\u000f\u0000\u0336\u0338\u0005p\u0000\u0000\u0337\u0336\u0001"+
		"\u0000\u0000\u0000\u0337\u0338\u0001\u0000\u0000\u0000\u0338\u0339\u0001"+
		"\u0000\u0000\u0000\u0339\u0357\u0007\n\u0000\u0000\u033a\u033c\u0005p"+
		"\u0000\u0000\u033b\u033a\u0001\u0000\u0000\u0000\u033b\u033c\u0001\u0000"+
		"\u0000\u0000\u033c\u033d\u0001\u0000\u0000\u0000\u033d\u0357\u0005}\u0000"+
		"\u0000\u033e\u0340\u0005p\u0000\u0000\u033f\u033e\u0001\u0000\u0000\u0000"+
		"\u033f\u0340\u0001\u0000\u0000\u0000\u0340\u0341\u0001\u0000\u0000\u0000"+
		"\u0341\u0357\u0005z\u0000\u0000\u0342\u0344\u0005p\u0000\u0000\u0343\u0342"+
		"\u0001\u0000\u0000\u0000\u0343\u0344\u0001\u0000\u0000\u0000\u0344\u0345"+
		"\u0001\u0000\u0000\u0000\u0345\u0357\u0005{\u0000\u0000\u0346\u0348\u0005"+
		"p\u0000\u0000\u0347\u0346\u0001\u0000\u0000\u0000\u0347\u0348\u0001\u0000"+
		"\u0000\u0000\u0348\u0349\u0001\u0000\u0000\u0000\u0349\u0357\u0005|\u0000"+
		"\u0000\u034a\u034c\u0005p\u0000\u0000\u034b\u034a\u0001\u0000\u0000\u0000"+
		"\u034b\u034c\u0001\u0000\u0000\u0000\u034c\u034d\u0001\u0000\u0000\u0000"+
		"\u034d\u0357\u0005\u0081\u0000\u0000\u034e\u0350\u0005p\u0000\u0000\u034f"+
		"\u034e\u0001\u0000\u0000\u0000\u034f\u0350\u0001\u0000\u0000\u0000\u0350"+
		"\u0351\u0001\u0000\u0000\u0000\u0351\u0357\u0005\u0080\u0000\u0000\u0352"+
		"\u0354\u0005p\u0000\u0000\u0353\u0352\u0001\u0000\u0000\u0000\u0353\u0354"+
		"\u0001\u0000\u0000\u0000\u0354\u0355\u0001\u0000\u0000\u0000\u0355\u0357"+
		"\u0005\u0082\u0000\u0000\u0356\u032b\u0001\u0000\u0000\u0000\u0356\u0330"+
		"\u0001\u0000\u0000\u0000\u0356\u0335\u0001\u0000\u0000\u0000\u0356\u033b"+
		"\u0001\u0000\u0000\u0000\u0356\u033f\u0001\u0000\u0000\u0000\u0356\u0343"+
		"\u0001\u0000\u0000\u0000\u0356\u0347\u0001\u0000\u0000\u0000\u0356\u034b"+
		"\u0001\u0000\u0000\u0000\u0356\u034f\u0001\u0000\u0000\u0000\u0356\u0353"+
		"\u0001\u0000\u0000\u0000\u0357\u0085\u0001\u0000\u0000\u0000\u0358\u0364"+
		"\u00053\u0000\u0000\u0359\u035a\u00030\u0018\u0000\u035a\u035b\u0005y"+
		"\u0000\u0000\u035b\u0364\u0001\u0000\u0000\u0000\u035c\u0364\u0003\u0084"+
		"B\u0000\u035d\u0364\u0003\u0088D\u0000\u035e\u0360\u0005y\u0000\u0000"+
		"\u035f\u035e\u0001\u0000\u0000\u0000\u0360\u0361\u0001\u0000\u0000\u0000"+
		"\u0361\u035f\u0001\u0000\u0000\u0000\u0361\u0362\u0001\u0000\u0000\u0000"+
		"\u0362\u0364\u0001\u0000\u0000\u0000\u0363\u0358\u0001\u0000\u0000\u0000"+
		"\u0363\u0359\u0001\u0000\u0000\u0000\u0363\u035c\u0001\u0000\u0000\u0000"+
		"\u0363\u035d\u0001\u0000\u0000\u0000\u0363\u035f\u0001\u0000\u0000\u0000"+
		"\u0364\u0087\u0001\u0000\u0000\u0000\u0365\u0366\u0007\u000b\u0000\u0000"+
		"\u0366\u0089\u0001\u0000\u0000\u0000\u0367\u0368\u0007\f\u0000\u0000\u0368"+
		"\u008b\u0001\u0000\u0000\u0000\u0369\u036a\u0007\r\u0000\u0000\u036a\u008d"+
		"\u0001\u0000\u0000\u0000\u036b\u036c\u0007\u000e\u0000\u0000\u036c\u008f"+
		"\u0001\u0000\u0000\u0000f\u0094\u00a5\u00a8\u00ad\u00af\u00b3\u00bd\u00c9"+
		"\u00ce\u00d1\u00d4\u00d7\u00df\u00e6\u00ed\u00f4\u00f7\u010b\u0114\u0117"+
		"\u0120\u0124\u0127\u012d\u013e\u0144\u0148\u014a\u0151\u0159\u015e\u0162"+
		"\u0164\u0169\u0171\u017e\u0188\u018b\u0192\u01a1\u01a3\u01ab\u01ad\u01b1"+
		"\u01b5\u01be\u01cd\u01d2\u01de\u01e3\u01eb\u01ee\u01f2\u0204\u020b\u0215"+
		"\u021c\u0228\u0242\u0246\u0249\u025e\u0262\u0265\u026d\u0276\u027c\u0284"+
		"\u0289\u0295\u029a\u029d\u02a3\u02a9\u02ae\u02b3\u02b9\u02ce\u02d0\u02d8"+
		"\u02dd\u02ea\u02ef\u0301\u030c\u030f\u0319\u0320\u0328\u032d\u0332\u0337"+
		"\u033b\u033f\u0343\u0347\u034b\u034f\u0353\u0356\u0361\u0363";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}
