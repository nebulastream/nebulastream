
// Generated from NebulaSQL.g4 by ANTLR 4.9.2

#pragma once


#include "antlr4-runtime.h"


namespace NES::Parsers {


class  NebulaSQLParser : public antlr4::Parser {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    BACKQUOTED_IDENTIFIER = 8, ALL = 9, AND = 10, ANY = 11, AS = 12, ASC = 13, 
    AT = 14, BETWEEN = 15, BY = 16, COMMENT = 17, CUBE = 18, DELETE = 19, 
    DESC = 20, DISTINCT = 21, DIV = 22, DROP = 23, ELSE = 24, END = 25, 
    ESCAPE = 26, EXISTS = 27, FALSE = 28, FIRST = 29, FOR = 30, FROM = 31, 
    FULL = 32, GROUP = 33, GROUPING = 34, HAVING = 35, IF = 36, IN = 37, 
    INNER = 38, INSERT = 39, INTO = 40, IS = 41, JOIN = 42, LAST = 43, LEFT = 44, 
    LIKE = 45, LIMIT = 46, LIST = 47, MERGE = 48, NATURAL = 49, NOT = 50, 
    NULLTOKEN = 51, NULLS = 52, OF = 53, ON = 54, OR = 55, ORDER = 56, QUERY = 57, 
    RECOVER = 58, RIGHT = 59, RLIKE = 60, ROLLUP = 61, SELECT = 62, SETS = 63, 
    SOME = 64, START = 65, TABLE = 66, TO = 67, TRUE = 68, TYPE = 69, UNION = 70, 
    UNKNOWN = 71, USE = 72, USING = 73, VALUES = 74, WHEN = 75, WHERE = 76, 
    WINDOW = 77, WITH = 78, TUMBLING = 79, SLIDING = 80, THRESHOLD = 81, 
    SIZE = 82, ADVANCE = 83, MS = 84, SEC = 85, MIN = 86, HOUR = 87, DAY = 88, 
    MAX = 89, AVG = 90, SUM = 91, COUNT = 92, MEDIAN = 93, WATERMARK = 94, 
    OFFSET = 95, ZMQ = 96, KAFKA = 97, FILE = 98, MQTT = 99, OPC = 100, 
    PRINT = 101, LOCALHOST = 102, CSV_FORMAT = 103, NES_FORMAT = 104, TEXT_FORMAT = 105, 
    AT_MOST_ONCE = 106, AT_LEAST_ONCE = 107, BOOLEAN_VALUE = 108, EQ = 109, 
    NSEQ = 110, NEQ = 111, NEQJ = 112, LT = 113, LTE = 114, GT = 115, GTE = 116, 
    PLUS = 117, MINUS = 118, ASTERISK = 119, SLASH = 120, PERCENT = 121, 
    TILDE = 122, AMPERSAND = 123, PIPE = 124, CONCAT_PIPE = 125, HAT = 126, 
    STRING = 127, BIGINT_LITERAL = 128, SMALLINT_LITERAL = 129, TINYINT_LITERAL = 130, 
    INTEGER_VALUE = 131, EXPONENT_VALUE = 132, DECIMAL_VALUE = 133, FLOAT_LITERAL = 134, 
    DOUBLE_LITERAL = 135, BIGDECIMAL_LITERAL = 136, IDENTIFIER = 137, SIMPLE_COMMENT = 138, 
    BRACKETED_COMMENT = 139, WS = 140, FOUR_OCTETS = 141, OCTET = 142, UNRECOGNIZED = 143
  };

  enum {
    RuleSingleStatement = 0, RuleStatement = 1, RuleQuery = 2, RuleQueryOrganization = 3, 
    RuleQueryTerm = 4, RuleQueryPrimary = 5, RuleQuerySpecification = 6, 
    RuleFromClause = 7, RuleRelation = 8, RuleJoinRelation = 9, RuleJoinType = 10, 
    RuleJoinCriteria = 11, RuleRelationPrimary = 12, RuleFunctionTable = 13, 
    RuleFromStatement = 14, RuleFromStatementBody = 15, RuleSelectClause = 16, 
    RuleWhereClause = 17, RuleHavingClause = 18, RuleInlineTable = 19, RuleTableAlias = 20, 
    RuleMultipartIdentifierList = 21, RuleMultipartIdentifier = 22, RuleNamedExpression = 23, 
    RuleIdentifier = 24, RuleStrictIdentifier = 25, RuleQuotedIdentifier = 26, 
    RuleIdentifierList = 27, RuleIdentifierSeq = 28, RuleErrorCapturingIdentifier = 29, 
    RuleErrorCapturingIdentifierExtra = 30, RuleNamedExpressionSeq = 31, 
    RuleExpression = 32, RuleBooleanExpression = 33, RuleWindowedAggregationClause = 34, 
    RuleAggregationClause = 35, RuleGroupingSet = 36, RuleWindowClause = 37, 
    RuleWatermarkClause = 38, RuleWatermarkParameters = 39, RuleWindowSpec = 40, 
    RuleTimeWindow = 41, RuleCountWindow = 42, RuleConditionWindow = 43, 
    RuleConditionParameter = 44, RuleThresholdMinSizeParameter = 45, RuleSizeParameter = 46, 
    RuleAdvancebyParameter = 47, RuleTimeUnit = 48, RuleTimestampParameter = 49, 
    RuleFunctionName = 50, RuleSinkClause = 51, RuleSinkType = 52, RuleSinkTypeZMQ = 53, 
    RuleNullNotnull = 54, RuleZmqKeyword = 55, RuleStreamName = 56, RuleHost = 57, 
    RulePort = 58, RuleSinkTypeKafka = 59, RuleKafkaKeyword = 60, RuleKafkaBroker = 61, 
    RuleKafkaTopic = 62, RuleKafkaProducerTimout = 63, RuleSinkTypeFile = 64, 
    RuleFileFormat = 65, RuleSinkTypeMQTT = 66, RuleQos = 67, RuleSinkTypeOPC = 68, 
    RuleSinkTypePrint = 69, RuleSortItem = 70, RulePredicate = 71, RuleValueExpression = 72, 
    RuleComparisonOperator = 73, RuleHint = 74, RuleHintStatement = 75, 
    RulePrimaryExpression = 76, RuleQualifiedName = 77, RuleNumber = 78, 
    RuleConstant = 79, RuleBooleanValue = 80, RuleStrictNonReserved = 81, 
    RuleAnsiNonReserved = 82, RuleNonReserved = 83
  };

  explicit NebulaSQLParser(antlr4::TokenStream *input);
  ~NebulaSQLParser();

  virtual std::string getGrammarFileName() const override;
  virtual const antlr4::atn::ATN& getATN() const override { return _atn; };
  virtual const std::vector<std::string>& getTokenNames() const override { return _tokenNames; }; // deprecated: use vocabulary instead.
  virtual const std::vector<std::string>& getRuleNames() const override;
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;


    /**
     * When true, the behavior of keywords follows ANSI SQL standard.
     */
    bool SQL_standard_keyword_behavior = false;

      /**
       * When false, a literal with an exponent would be converted into
       * double type rather than decimal type.
       */
    bool legacy_exponent_literal_as_decimal_enabled = false;


  class SingleStatementContext;
  class StatementContext;
  class QueryContext;
  class QueryOrganizationContext;
  class QueryTermContext;
  class QueryPrimaryContext;
  class QuerySpecificationContext;
  class FromClauseContext;
  class RelationContext;
  class JoinRelationContext;
  class JoinTypeContext;
  class JoinCriteriaContext;
  class RelationPrimaryContext;
  class FunctionTableContext;
  class FromStatementContext;
  class FromStatementBodyContext;
  class SelectClauseContext;
  class WhereClauseContext;
  class HavingClauseContext;
  class InlineTableContext;
  class TableAliasContext;
  class MultipartIdentifierListContext;
  class MultipartIdentifierContext;
  class NamedExpressionContext;
  class IdentifierContext;
  class StrictIdentifierContext;
  class QuotedIdentifierContext;
  class IdentifierListContext;
  class IdentifierSeqContext;
  class ErrorCapturingIdentifierContext;
  class ErrorCapturingIdentifierExtraContext;
  class NamedExpressionSeqContext;
  class ExpressionContext;
  class BooleanExpressionContext;
  class WindowedAggregationClauseContext;
  class AggregationClauseContext;
  class GroupingSetContext;
  class WindowClauseContext;
  class WatermarkClauseContext;
  class WatermarkParametersContext;
  class WindowSpecContext;
  class TimeWindowContext;
  class CountWindowContext;
  class ConditionWindowContext;
  class ConditionParameterContext;
  class ThresholdMinSizeParameterContext;
  class SizeParameterContext;
  class AdvancebyParameterContext;
  class TimeUnitContext;
  class TimestampParameterContext;
  class FunctionNameContext;
  class SinkClauseContext;
  class SinkTypeContext;
  class SinkTypeZMQContext;
  class NullNotnullContext;
  class ZmqKeywordContext;
  class StreamNameContext;
  class HostContext;
  class PortContext;
  class SinkTypeKafkaContext;
  class KafkaKeywordContext;
  class KafkaBrokerContext;
  class KafkaTopicContext;
  class KafkaProducerTimoutContext;
  class SinkTypeFileContext;
  class FileFormatContext;
  class SinkTypeMQTTContext;
  class QosContext;
  class SinkTypeOPCContext;
  class SinkTypePrintContext;
  class SortItemContext;
  class PredicateContext;
  class ValueExpressionContext;
  class ComparisonOperatorContext;
  class HintContext;
  class HintStatementContext;
  class PrimaryExpressionContext;
  class QualifiedNameContext;
  class NumberContext;
  class ConstantContext;
  class BooleanValueContext;
  class StrictNonReservedContext;
  class AnsiNonReservedContext;
  class NonReservedContext; 

  class  SingleStatementContext : public antlr4::ParserRuleContext {
  public:
    SingleStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StatementContext *statement();
    antlr4::tree::TerminalNode *EOF();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SingleStatementContext* singleStatement();

  class  StatementContext : public antlr4::ParserRuleContext {
  public:
    StatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryContext *query();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  StatementContext* statement();

  class  QueryContext : public antlr4::ParserRuleContext {
  public:
    QueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryTermContext *queryTerm();
    QueryOrganizationContext *queryOrganization();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QueryContext* query();

  class  QueryOrganizationContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::SortItemContext *sortItemContext = nullptr;
    std::vector<SortItemContext *> order;
    antlr4::Token *limit = nullptr;
    antlr4::Token *offset = nullptr;
    QueryOrganizationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ORDER();
    antlr4::tree::TerminalNode *BY();
    antlr4::tree::TerminalNode *LIMIT();
    antlr4::tree::TerminalNode *OFFSET();
    std::vector<SortItemContext *> sortItem();
    SortItemContext* sortItem(size_t i);
    std::vector<antlr4::tree::TerminalNode *> INTEGER_VALUE();
    antlr4::tree::TerminalNode* INTEGER_VALUE(size_t i);
    antlr4::tree::TerminalNode *ALL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QueryOrganizationContext* queryOrganization();

  class  QueryTermContext : public antlr4::ParserRuleContext {
  public:
    QueryTermContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    QueryTermContext() = default;
    void copyFrom(QueryTermContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  PrimaryQueryContext : public QueryTermContext {
  public:
    PrimaryQueryContext(QueryTermContext *ctx);

    QueryPrimaryContext *queryPrimary();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SetOperationContext : public QueryTermContext {
  public:
    SetOperationContext(QueryTermContext *ctx);

    NebulaSQLParser::QueryTermContext *left = nullptr;
    antlr4::Token *setoperator = nullptr;
    NebulaSQLParser::QueryTermContext *right = nullptr;
    std::vector<QueryTermContext *> queryTerm();
    QueryTermContext* queryTerm(size_t i);
    antlr4::tree::TerminalNode *UNION();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  QueryTermContext* queryTerm();
  QueryTermContext* queryTerm(int precedence);
  class  QueryPrimaryContext : public antlr4::ParserRuleContext {
  public:
    QueryPrimaryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    QueryPrimaryContext() = default;
    void copyFrom(QueryPrimaryContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  SubqueryContext : public QueryPrimaryContext {
  public:
    SubqueryContext(QueryPrimaryContext *ctx);

    QueryContext *query();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  QueryPrimaryDefaultContext : public QueryPrimaryContext {
  public:
    QueryPrimaryDefaultContext(QueryPrimaryContext *ctx);

    QuerySpecificationContext *querySpecification();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  InlineTableDefault1Context : public QueryPrimaryContext {
  public:
    InlineTableDefault1Context(QueryPrimaryContext *ctx);

    InlineTableContext *inlineTable();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  FromStmtContext : public QueryPrimaryContext {
  public:
    FromStmtContext(QueryPrimaryContext *ctx);

    FromStatementContext *fromStatement();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  TableContext : public QueryPrimaryContext {
  public:
    TableContext(QueryPrimaryContext *ctx);

    antlr4::tree::TerminalNode *TABLE();
    MultipartIdentifierContext *multipartIdentifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  QueryPrimaryContext* queryPrimary();

  class  QuerySpecificationContext : public antlr4::ParserRuleContext {
  public:
    QuerySpecificationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SelectClauseContext *selectClause();
    FromClauseContext *fromClause();
    WhereClauseContext *whereClause();
    WindowedAggregationClauseContext *windowedAggregationClause();
    HavingClauseContext *havingClause();
    SinkClauseContext *sinkClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QuerySpecificationContext* querySpecification();

  class  FromClauseContext : public antlr4::ParserRuleContext {
  public:
    FromClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FROM();
    std::vector<RelationContext *> relation();
    RelationContext* relation(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FromClauseContext* fromClause();

  class  RelationContext : public antlr4::ParserRuleContext {
  public:
    RelationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RelationPrimaryContext *relationPrimary();
    std::vector<JoinRelationContext *> joinRelation();
    JoinRelationContext* joinRelation(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  RelationContext* relation();

  class  JoinRelationContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::RelationPrimaryContext *right = nullptr;
    JoinRelationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *JOIN();
    RelationPrimaryContext *relationPrimary();
    JoinTypeContext *joinType();
    JoinCriteriaContext *joinCriteria();
    antlr4::tree::TerminalNode *NATURAL();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  JoinRelationContext* joinRelation();

  class  JoinTypeContext : public antlr4::ParserRuleContext {
  public:
    JoinTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INNER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  JoinTypeContext* joinType();

  class  JoinCriteriaContext : public antlr4::ParserRuleContext {
  public:
    JoinCriteriaContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ON();
    BooleanExpressionContext *booleanExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  JoinCriteriaContext* joinCriteria();

  class  RelationPrimaryContext : public antlr4::ParserRuleContext {
  public:
    RelationPrimaryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    RelationPrimaryContext() = default;
    void copyFrom(RelationPrimaryContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  TableValuedFunctionContext : public RelationPrimaryContext {
  public:
    TableValuedFunctionContext(RelationPrimaryContext *ctx);

    FunctionTableContext *functionTable();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  InlineTableDefault2Context : public RelationPrimaryContext {
  public:
    InlineTableDefault2Context(RelationPrimaryContext *ctx);

    InlineTableContext *inlineTable();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  AliasedRelationContext : public RelationPrimaryContext {
  public:
    AliasedRelationContext(RelationPrimaryContext *ctx);

    RelationContext *relation();
    TableAliasContext *tableAlias();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  AliasedQueryContext : public RelationPrimaryContext {
  public:
    AliasedQueryContext(RelationPrimaryContext *ctx);

    QueryContext *query();
    TableAliasContext *tableAlias();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  TableNameContext : public RelationPrimaryContext {
  public:
    TableNameContext(RelationPrimaryContext *ctx);

    MultipartIdentifierContext *multipartIdentifier();
    TableAliasContext *tableAlias();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  RelationPrimaryContext* relationPrimary();

  class  FunctionTableContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::ErrorCapturingIdentifierContext *funcName = nullptr;
    FunctionTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableAliasContext *tableAlias();
    ErrorCapturingIdentifierContext *errorCapturingIdentifier();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FunctionTableContext* functionTable();

  class  FromStatementContext : public antlr4::ParserRuleContext {
  public:
    FromStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    FromClauseContext *fromClause();
    std::vector<FromStatementBodyContext *> fromStatementBody();
    FromStatementBodyContext* fromStatementBody(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FromStatementContext* fromStatement();

  class  FromStatementBodyContext : public antlr4::ParserRuleContext {
  public:
    FromStatementBodyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SelectClauseContext *selectClause();
    WhereClauseContext *whereClause();
    AggregationClauseContext *aggregationClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FromStatementBodyContext* fromStatementBody();

  class  SelectClauseContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::HintContext *hintContext = nullptr;
    std::vector<HintContext *> hints;
    SelectClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SELECT();
    NamedExpressionSeqContext *namedExpressionSeq();
    std::vector<HintContext *> hint();
    HintContext* hint(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SelectClauseContext* selectClause();

  class  WhereClauseContext : public antlr4::ParserRuleContext {
  public:
    WhereClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WHERE();
    BooleanExpressionContext *booleanExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WhereClauseContext* whereClause();

  class  HavingClauseContext : public antlr4::ParserRuleContext {
  public:
    HavingClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *HAVING();
    BooleanExpressionContext *booleanExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  HavingClauseContext* havingClause();

  class  InlineTableContext : public antlr4::ParserRuleContext {
  public:
    InlineTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *VALUES();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    TableAliasContext *tableAlias();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InlineTableContext* inlineTable();

  class  TableAliasContext : public antlr4::ParserRuleContext {
  public:
    TableAliasContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StrictIdentifierContext *strictIdentifier();
    antlr4::tree::TerminalNode *AS();
    IdentifierListContext *identifierList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableAliasContext* tableAlias();

  class  MultipartIdentifierListContext : public antlr4::ParserRuleContext {
  public:
    MultipartIdentifierListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<MultipartIdentifierContext *> multipartIdentifier();
    MultipartIdentifierContext* multipartIdentifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  MultipartIdentifierListContext* multipartIdentifierList();

  class  MultipartIdentifierContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::ErrorCapturingIdentifierContext *errorCapturingIdentifierContext = nullptr;
    std::vector<ErrorCapturingIdentifierContext *> parts;
    MultipartIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ErrorCapturingIdentifierContext *> errorCapturingIdentifier();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  MultipartIdentifierContext* multipartIdentifier();

  class  NamedExpressionContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::ErrorCapturingIdentifierContext *name = nullptr;
    NamedExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext *expression();
    IdentifierListContext *identifierList();
    antlr4::tree::TerminalNode *AS();
    ErrorCapturingIdentifierContext *errorCapturingIdentifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NamedExpressionContext* namedExpression();

  class  IdentifierContext : public antlr4::ParserRuleContext {
  public:
    IdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StrictIdentifierContext *strictIdentifier();
    StrictNonReservedContext *strictNonReserved();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierContext* identifier();

  class  StrictIdentifierContext : public antlr4::ParserRuleContext {
  public:
    StrictIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    StrictIdentifierContext() = default;
    void copyFrom(StrictIdentifierContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  QuotedIdentifierAlternativeContext : public StrictIdentifierContext {
  public:
    QuotedIdentifierAlternativeContext(StrictIdentifierContext *ctx);

    QuotedIdentifierContext *quotedIdentifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  UnquotedIdentifierContext : public StrictIdentifierContext {
  public:
    UnquotedIdentifierContext(StrictIdentifierContext *ctx);

    antlr4::tree::TerminalNode *IDENTIFIER();
    AnsiNonReservedContext *ansiNonReserved();
    NonReservedContext *nonReserved();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  StrictIdentifierContext* strictIdentifier();

  class  QuotedIdentifierContext : public antlr4::ParserRuleContext {
  public:
    QuotedIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BACKQUOTED_IDENTIFIER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QuotedIdentifierContext* quotedIdentifier();

  class  IdentifierListContext : public antlr4::ParserRuleContext {
  public:
    IdentifierListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierSeqContext *identifierSeq();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierListContext* identifierList();

  class  IdentifierSeqContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::ErrorCapturingIdentifierContext *errorCapturingIdentifierContext = nullptr;
    std::vector<ErrorCapturingIdentifierContext *> ident;
    IdentifierSeqContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ErrorCapturingIdentifierContext *> errorCapturingIdentifier();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierSeqContext* identifierSeq();

  class  ErrorCapturingIdentifierContext : public antlr4::ParserRuleContext {
  public:
    ErrorCapturingIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    ErrorCapturingIdentifierExtraContext *errorCapturingIdentifierExtra();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ErrorCapturingIdentifierContext* errorCapturingIdentifier();

  class  ErrorCapturingIdentifierExtraContext : public antlr4::ParserRuleContext {
  public:
    ErrorCapturingIdentifierExtraContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    ErrorCapturingIdentifierExtraContext() = default;
    void copyFrom(ErrorCapturingIdentifierExtraContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  ErrorIdentContext : public ErrorCapturingIdentifierExtraContext {
  public:
    ErrorIdentContext(ErrorCapturingIdentifierExtraContext *ctx);

    std::vector<antlr4::tree::TerminalNode *> MINUS();
    antlr4::tree::TerminalNode* MINUS(size_t i);
    std::vector<IdentifierContext *> identifier();
    IdentifierContext* identifier(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  RealIdentContext : public ErrorCapturingIdentifierExtraContext {
  public:
    RealIdentContext(ErrorCapturingIdentifierExtraContext *ctx);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  ErrorCapturingIdentifierExtraContext* errorCapturingIdentifierExtra();

  class  NamedExpressionSeqContext : public antlr4::ParserRuleContext {
  public:
    NamedExpressionSeqContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NamedExpressionContext *> namedExpression();
    NamedExpressionContext* namedExpression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NamedExpressionSeqContext* namedExpressionSeq();

  class  ExpressionContext : public antlr4::ParserRuleContext {
  public:
    ExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    BooleanExpressionContext *booleanExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ExpressionContext* expression();

  class  BooleanExpressionContext : public antlr4::ParserRuleContext {
  public:
    BooleanExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    BooleanExpressionContext() = default;
    void copyFrom(BooleanExpressionContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  LogicalNotContext : public BooleanExpressionContext {
  public:
    LogicalNotContext(BooleanExpressionContext *ctx);

    antlr4::tree::TerminalNode *NOT();
    BooleanExpressionContext *booleanExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  PredicatedContext : public BooleanExpressionContext {
  public:
    PredicatedContext(BooleanExpressionContext *ctx);

    ValueExpressionContext *valueExpression();
    PredicateContext *predicate();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ExistsContext : public BooleanExpressionContext {
  public:
    ExistsContext(BooleanExpressionContext *ctx);

    antlr4::tree::TerminalNode *EXISTS();
    QueryContext *query();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  LogicalBinaryContext : public BooleanExpressionContext {
  public:
    LogicalBinaryContext(BooleanExpressionContext *ctx);

    NebulaSQLParser::BooleanExpressionContext *left = nullptr;
    antlr4::Token *op = nullptr;
    NebulaSQLParser::BooleanExpressionContext *right = nullptr;
    std::vector<BooleanExpressionContext *> booleanExpression();
    BooleanExpressionContext* booleanExpression(size_t i);
    antlr4::tree::TerminalNode *AND();
    antlr4::tree::TerminalNode *OR();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  BooleanExpressionContext* booleanExpression();
  BooleanExpressionContext* booleanExpression(int precedence);
  class  WindowedAggregationClauseContext : public antlr4::ParserRuleContext {
  public:
    WindowedAggregationClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    WindowClauseContext *windowClause();
    AggregationClauseContext *aggregationClause();
    WatermarkClauseContext *watermarkClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowedAggregationClauseContext* windowedAggregationClause();

  class  AggregationClauseContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::ExpressionContext *expressionContext = nullptr;
    std::vector<ExpressionContext *> groupingExpressions;
    antlr4::Token *kind = nullptr;
    AggregationClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GROUP();
    antlr4::tree::TerminalNode *BY();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *SETS();
    std::vector<GroupingSetContext *> groupingSet();
    GroupingSetContext* groupingSet(size_t i);
    antlr4::tree::TerminalNode *ROLLUP();
    antlr4::tree::TerminalNode *CUBE();
    antlr4::tree::TerminalNode *GROUPING();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AggregationClauseContext* aggregationClause();

  class  GroupingSetContext : public antlr4::ParserRuleContext {
  public:
    GroupingSetContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  GroupingSetContext* groupingSet();

  class  WindowClauseContext : public antlr4::ParserRuleContext {
  public:
    WindowClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WINDOW();
    WindowSpecContext *windowSpec();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowClauseContext* windowClause();

  class  WatermarkClauseContext : public antlr4::ParserRuleContext {
  public:
    WatermarkClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WATERMARK();
    WatermarkParametersContext *watermarkParameters();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WatermarkClauseContext* watermarkClause();

  class  WatermarkParametersContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::IdentifierContext *watermarkIdentifier = nullptr;
    antlr4::Token *watermark = nullptr;
    NebulaSQLParser::TimeUnitContext *watermarkTimeUnit = nullptr;
    WatermarkParametersContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *INTEGER_VALUE();
    TimeUnitContext *timeUnit();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WatermarkParametersContext* watermarkParameters();

  class  WindowSpecContext : public antlr4::ParserRuleContext {
  public:
    WindowSpecContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    WindowSpecContext() = default;
    void copyFrom(WindowSpecContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  TimeBasedWindowContext : public WindowSpecContext {
  public:
    TimeBasedWindowContext(WindowSpecContext *ctx);

    TimeWindowContext *timeWindow();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  CountBasedWindowContext : public WindowSpecContext {
  public:
    CountBasedWindowContext(WindowSpecContext *ctx);

    CountWindowContext *countWindow();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ThresholdBasedWindowContext : public WindowSpecContext {
  public:
    ThresholdBasedWindowContext(WindowSpecContext *ctx);

    ConditionWindowContext *conditionWindow();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  WindowSpecContext* windowSpec();

  class  TimeWindowContext : public antlr4::ParserRuleContext {
  public:
    TimeWindowContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    TimeWindowContext() = default;
    void copyFrom(TimeWindowContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  TumblingWindowContext : public TimeWindowContext {
  public:
    TumblingWindowContext(TimeWindowContext *ctx);

    antlr4::tree::TerminalNode *TUMBLING();
    SizeParameterContext *sizeParameter();
    TimestampParameterContext *timestampParameter();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SlidingWindowContext : public TimeWindowContext {
  public:
    SlidingWindowContext(TimeWindowContext *ctx);

    antlr4::tree::TerminalNode *SLIDING();
    SizeParameterContext *sizeParameter();
    AdvancebyParameterContext *advancebyParameter();
    TimestampParameterContext *timestampParameter();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  TimeWindowContext* timeWindow();

  class  CountWindowContext : public antlr4::ParserRuleContext {
  public:
    CountWindowContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    CountWindowContext() = default;
    void copyFrom(CountWindowContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  CountBasedTumblingContext : public CountWindowContext {
  public:
    CountBasedTumblingContext(CountWindowContext *ctx);

    antlr4::tree::TerminalNode *TUMBLING();
    antlr4::tree::TerminalNode *INTEGER_VALUE();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  CountWindowContext* countWindow();

  class  ConditionWindowContext : public antlr4::ParserRuleContext {
  public:
    ConditionWindowContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    ConditionWindowContext() = default;
    void copyFrom(ConditionWindowContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  ThresholdWindowContext : public ConditionWindowContext {
  public:
    ThresholdWindowContext(ConditionWindowContext *ctx);

    antlr4::tree::TerminalNode *THRESHOLD();
    ConditionParameterContext *conditionParameter();
    ThresholdMinSizeParameterContext *thresholdMinSizeParameter();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  ConditionWindowContext* conditionWindow();

  class  ConditionParameterContext : public antlr4::ParserRuleContext {
  public:
    ConditionParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext *expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ConditionParameterContext* conditionParameter();

  class  ThresholdMinSizeParameterContext : public antlr4::ParserRuleContext {
  public:
    ThresholdMinSizeParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INTEGER_VALUE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ThresholdMinSizeParameterContext* thresholdMinSizeParameter();

  class  SizeParameterContext : public antlr4::ParserRuleContext {
  public:
    SizeParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SIZE();
    antlr4::tree::TerminalNode *INTEGER_VALUE();
    TimeUnitContext *timeUnit();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SizeParameterContext* sizeParameter();

  class  AdvancebyParameterContext : public antlr4::ParserRuleContext {
  public:
    AdvancebyParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ADVANCE();
    antlr4::tree::TerminalNode *BY();
    antlr4::tree::TerminalNode *INTEGER_VALUE();
    TimeUnitContext *timeUnit();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AdvancebyParameterContext* advancebyParameter();

  class  TimeUnitContext : public antlr4::ParserRuleContext {
  public:
    TimeUnitContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MS();
    antlr4::tree::TerminalNode *SEC();
    antlr4::tree::TerminalNode *MIN();
    antlr4::tree::TerminalNode *HOUR();
    antlr4::tree::TerminalNode *DAY();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TimeUnitContext* timeUnit();

  class  TimestampParameterContext : public antlr4::ParserRuleContext {
  public:
    TimestampParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IDENTIFIER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TimestampParameterContext* timestampParameter();

  class  FunctionNameContext : public antlr4::ParserRuleContext {
  public:
    FunctionNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AVG();
    antlr4::tree::TerminalNode *MAX();
    antlr4::tree::TerminalNode *MIN();
    antlr4::tree::TerminalNode *SUM();
    antlr4::tree::TerminalNode *COUNT();
    antlr4::tree::TerminalNode *MEDIAN();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FunctionNameContext* functionName();

  class  SinkClauseContext : public antlr4::ParserRuleContext {
  public:
    SinkClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INTO();
    SinkTypeContext *sinkType();
    antlr4::tree::TerminalNode *AS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SinkClauseContext* sinkClause();

  class  SinkTypeContext : public antlr4::ParserRuleContext {
  public:
    SinkTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SinkTypeZMQContext *sinkTypeZMQ();
    SinkTypeKafkaContext *sinkTypeKafka();
    SinkTypeFileContext *sinkTypeFile();
    SinkTypeMQTTContext *sinkTypeMQTT();
    SinkTypeOPCContext *sinkTypeOPC();
    SinkTypePrintContext *sinkTypePrint();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SinkTypeContext* sinkType();

  class  SinkTypeZMQContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::StreamNameContext *zmqStreamName = nullptr;
    NebulaSQLParser::HostContext *zmqHostLabel = nullptr;
    NebulaSQLParser::PortContext *zmqPort = nullptr;
    SinkTypeZMQContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ZmqKeywordContext *zmqKeyword();
    StreamNameContext *streamName();
    HostContext *host();
    PortContext *port();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SinkTypeZMQContext* sinkTypeZMQ();

  class  NullNotnullContext : public antlr4::ParserRuleContext {
  public:
    NullNotnullContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NULLTOKEN();
    antlr4::tree::TerminalNode *NOT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NullNotnullContext* nullNotnull();

  class  ZmqKeywordContext : public antlr4::ParserRuleContext {
  public:
    ZmqKeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ZMQ();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ZmqKeywordContext* zmqKeyword();

  class  StreamNameContext : public antlr4::ParserRuleContext {
  public:
    StreamNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IDENTIFIER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  StreamNameContext* streamName();

  class  HostContext : public antlr4::ParserRuleContext {
  public:
    HostContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FOUR_OCTETS();
    antlr4::tree::TerminalNode *LOCALHOST();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  HostContext* host();

  class  PortContext : public antlr4::ParserRuleContext {
  public:
    PortContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INTEGER_VALUE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PortContext* port();

  class  SinkTypeKafkaContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::KafkaBrokerContext *broker = nullptr;
    NebulaSQLParser::KafkaTopicContext *topic = nullptr;
    NebulaSQLParser::KafkaProducerTimoutContext *timeout = nullptr;
    SinkTypeKafkaContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    KafkaKeywordContext *kafkaKeyword();
    KafkaBrokerContext *kafkaBroker();
    KafkaTopicContext *kafkaTopic();
    KafkaProducerTimoutContext *kafkaProducerTimout();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SinkTypeKafkaContext* sinkTypeKafka();

  class  KafkaKeywordContext : public antlr4::ParserRuleContext {
  public:
    KafkaKeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *KAFKA();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KafkaKeywordContext* kafkaKeyword();

  class  KafkaBrokerContext : public antlr4::ParserRuleContext {
  public:
    KafkaBrokerContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IDENTIFIER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KafkaBrokerContext* kafkaBroker();

  class  KafkaTopicContext : public antlr4::ParserRuleContext {
  public:
    KafkaTopicContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IDENTIFIER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KafkaTopicContext* kafkaTopic();

  class  KafkaProducerTimoutContext : public antlr4::ParserRuleContext {
  public:
    KafkaProducerTimoutContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INTEGER_VALUE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  KafkaProducerTimoutContext* kafkaProducerTimout();

  class  SinkTypeFileContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *path = nullptr;
    NebulaSQLParser::FileFormatContext *format = nullptr;
    antlr4::Token *append = nullptr;
    SinkTypeFileContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FILE();
    std::vector<antlr4::tree::TerminalNode *> STRING();
    antlr4::tree::TerminalNode* STRING(size_t i);
    FileFormatContext *fileFormat();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SinkTypeFileContext* sinkTypeFile();

  class  FileFormatContext : public antlr4::ParserRuleContext {
  public:
    FileFormatContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CSV_FORMAT();
    antlr4::tree::TerminalNode *NES_FORMAT();
    antlr4::tree::TerminalNode *TEXT_FORMAT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FileFormatContext* fileFormat();

  class  SinkTypeMQTTContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *mqttHostLabel = nullptr;
    antlr4::Token *topic = nullptr;
    antlr4::Token *user = nullptr;
    antlr4::Token *maxBufferedMSGs = nullptr;
    NebulaSQLParser::TimeUnitContext *mqttTimeUnitLabel = nullptr;
    antlr4::Token *messageDelay = nullptr;
    NebulaSQLParser::QosContext *qualityOfService = nullptr;
    antlr4::Token *asynchronousClient = nullptr;
    SinkTypeMQTTContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MQTT();
    std::vector<antlr4::tree::TerminalNode *> STRING();
    antlr4::tree::TerminalNode* STRING(size_t i);
    std::vector<antlr4::tree::TerminalNode *> INTEGER_VALUE();
    antlr4::tree::TerminalNode* INTEGER_VALUE(size_t i);
    TimeUnitContext *timeUnit();
    QosContext *qos();
    antlr4::tree::TerminalNode *BOOLEAN_VALUE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SinkTypeMQTTContext* sinkTypeMQTT();

  class  QosContext : public antlr4::ParserRuleContext {
  public:
    QosContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AT_MOST_ONCE();
    antlr4::tree::TerminalNode *AT_LEAST_ONCE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QosContext* qos();

  class  SinkTypeOPCContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *url = nullptr;
    antlr4::Token *nodeId = nullptr;
    antlr4::Token *user = nullptr;
    antlr4::Token *password = nullptr;
    SinkTypeOPCContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OPC();
    std::vector<antlr4::tree::TerminalNode *> STRING();
    antlr4::tree::TerminalNode* STRING(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SinkTypeOPCContext* sinkTypeOPC();

  class  SinkTypePrintContext : public antlr4::ParserRuleContext {
  public:
    SinkTypePrintContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PRINT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SinkTypePrintContext* sinkTypePrint();

  class  SortItemContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *ordering = nullptr;
    antlr4::Token *nullOrder = nullptr;
    SortItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext *expression();
    antlr4::tree::TerminalNode *NULLS();
    antlr4::tree::TerminalNode *ASC();
    antlr4::tree::TerminalNode *DESC();
    antlr4::tree::TerminalNode *LAST();
    antlr4::tree::TerminalNode *FIRST();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SortItemContext* sortItem();

  class  PredicateContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *kind = nullptr;
    NebulaSQLParser::ValueExpressionContext *lower = nullptr;
    NebulaSQLParser::ValueExpressionContext *upper = nullptr;
    NebulaSQLParser::ValueExpressionContext *pattern = nullptr;
    antlr4::Token *quantifier = nullptr;
    antlr4::Token *escapeChar = nullptr;
    NebulaSQLParser::ValueExpressionContext *right = nullptr;
    PredicateContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AND();
    antlr4::tree::TerminalNode *BETWEEN();
    std::vector<ValueExpressionContext *> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode *NOT();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode *IN();
    QueryContext *query();
    antlr4::tree::TerminalNode *RLIKE();
    antlr4::tree::TerminalNode *LIKE();
    antlr4::tree::TerminalNode *ANY();
    antlr4::tree::TerminalNode *SOME();
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *ESCAPE();
    antlr4::tree::TerminalNode *STRING();
    antlr4::tree::TerminalNode *IS();
    NullNotnullContext *nullNotnull();
    antlr4::tree::TerminalNode *TRUE();
    antlr4::tree::TerminalNode *FALSE();
    antlr4::tree::TerminalNode *UNKNOWN();
    antlr4::tree::TerminalNode *FROM();
    antlr4::tree::TerminalNode *DISTINCT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  PredicateContext* predicate();

  class  ValueExpressionContext : public antlr4::ParserRuleContext {
  public:
    ValueExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    ValueExpressionContext() = default;
    void copyFrom(ValueExpressionContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  ValueExpressionDefaultContext : public ValueExpressionContext {
  public:
    ValueExpressionDefaultContext(ValueExpressionContext *ctx);

    PrimaryExpressionContext *primaryExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ComparisonContext : public ValueExpressionContext {
  public:
    ComparisonContext(ValueExpressionContext *ctx);

    NebulaSQLParser::ValueExpressionContext *left = nullptr;
    NebulaSQLParser::ValueExpressionContext *right = nullptr;
    ComparisonOperatorContext *comparisonOperator();
    std::vector<ValueExpressionContext *> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ArithmeticBinaryContext : public ValueExpressionContext {
  public:
    ArithmeticBinaryContext(ValueExpressionContext *ctx);

    NebulaSQLParser::ValueExpressionContext *left = nullptr;
    antlr4::Token *op = nullptr;
    NebulaSQLParser::ValueExpressionContext *right = nullptr;
    std::vector<ValueExpressionContext *> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode *ASTERISK();
    antlr4::tree::TerminalNode *SLASH();
    antlr4::tree::TerminalNode *PERCENT();
    antlr4::tree::TerminalNode *DIV();
    antlr4::tree::TerminalNode *PLUS();
    antlr4::tree::TerminalNode *MINUS();
    antlr4::tree::TerminalNode *CONCAT_PIPE();
    antlr4::tree::TerminalNode *AMPERSAND();
    antlr4::tree::TerminalNode *HAT();
    antlr4::tree::TerminalNode *PIPE();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ArithmeticUnaryContext : public ValueExpressionContext {
  public:
    ArithmeticUnaryContext(ValueExpressionContext *ctx);

    antlr4::Token *op = nullptr;
    ValueExpressionContext *valueExpression();
    antlr4::tree::TerminalNode *MINUS();
    antlr4::tree::TerminalNode *PLUS();
    antlr4::tree::TerminalNode *TILDE();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  ValueExpressionContext* valueExpression();
  ValueExpressionContext* valueExpression(int precedence);
  class  ComparisonOperatorContext : public antlr4::ParserRuleContext {
  public:
    ComparisonOperatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EQ();
    antlr4::tree::TerminalNode *NEQ();
    antlr4::tree::TerminalNode *NEQJ();
    antlr4::tree::TerminalNode *LT();
    antlr4::tree::TerminalNode *LTE();
    antlr4::tree::TerminalNode *GT();
    antlr4::tree::TerminalNode *GTE();
    antlr4::tree::TerminalNode *NSEQ();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ComparisonOperatorContext* comparisonOperator();

  class  HintContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::HintStatementContext *hintStatementContext = nullptr;
    std::vector<HintStatementContext *> hintStatements;
    HintContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<HintStatementContext *> hintStatement();
    HintStatementContext* hintStatement(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  HintContext* hint();

  class  HintStatementContext : public antlr4::ParserRuleContext {
  public:
    NebulaSQLParser::IdentifierContext *hintName = nullptr;
    NebulaSQLParser::PrimaryExpressionContext *primaryExpressionContext = nullptr;
    std::vector<PrimaryExpressionContext *> parameters;
    HintStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    std::vector<PrimaryExpressionContext *> primaryExpression();
    PrimaryExpressionContext* primaryExpression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  HintStatementContext* hintStatement();

  class  PrimaryExpressionContext : public antlr4::ParserRuleContext {
  public:
    PrimaryExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    PrimaryExpressionContext() = default;
    void copyFrom(PrimaryExpressionContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  DereferenceContext : public PrimaryExpressionContext {
  public:
    DereferenceContext(PrimaryExpressionContext *ctx);

    NebulaSQLParser::PrimaryExpressionContext *base = nullptr;
    NebulaSQLParser::IdentifierContext *fieldName = nullptr;
    PrimaryExpressionContext *primaryExpression();
    IdentifierContext *identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ConstantDefaultContext : public PrimaryExpressionContext {
  public:
    ConstantDefaultContext(PrimaryExpressionContext *ctx);

    ConstantContext *constant();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ColumnReferenceContext : public PrimaryExpressionContext {
  public:
    ColumnReferenceContext(PrimaryExpressionContext *ctx);

    IdentifierContext *identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  RowConstructorContext : public PrimaryExpressionContext {
  public:
    RowConstructorContext(PrimaryExpressionContext *ctx);

    std::vector<NamedExpressionContext *> namedExpression();
    NamedExpressionContext* namedExpression(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ParenthesizedExpressionContext : public PrimaryExpressionContext {
  public:
    ParenthesizedExpressionContext(PrimaryExpressionContext *ctx);

    ExpressionContext *expression();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  StarContext : public PrimaryExpressionContext {
  public:
    StarContext(PrimaryExpressionContext *ctx);

    antlr4::tree::TerminalNode *ASTERISK();
    QualifiedNameContext *qualifiedName();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  FunctionCallContext : public PrimaryExpressionContext {
  public:
    FunctionCallContext(PrimaryExpressionContext *ctx);

    NebulaSQLParser::ExpressionContext *expressionContext = nullptr;
    std::vector<ExpressionContext *> argument;
    FunctionNameContext *functionName();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SubqueryExpressionContext : public PrimaryExpressionContext {
  public:
    SubqueryExpressionContext(PrimaryExpressionContext *ctx);

    QueryContext *query();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  PrimaryExpressionContext* primaryExpression();
  PrimaryExpressionContext* primaryExpression(int precedence);
  class  QualifiedNameContext : public antlr4::ParserRuleContext {
  public:
    QualifiedNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext *> identifier();
    IdentifierContext* identifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QualifiedNameContext* qualifiedName();

  class  NumberContext : public antlr4::ParserRuleContext {
  public:
    NumberContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    NumberContext() = default;
    void copyFrom(NumberContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  DecimalLiteralContext : public NumberContext {
  public:
    DecimalLiteralContext(NumberContext *ctx);

    antlr4::tree::TerminalNode *DECIMAL_VALUE();
    antlr4::tree::TerminalNode *MINUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  BigIntLiteralContext : public NumberContext {
  public:
    BigIntLiteralContext(NumberContext *ctx);

    antlr4::tree::TerminalNode *BIGINT_LITERAL();
    antlr4::tree::TerminalNode *MINUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  TinyIntLiteralContext : public NumberContext {
  public:
    TinyIntLiteralContext(NumberContext *ctx);

    antlr4::tree::TerminalNode *TINYINT_LITERAL();
    antlr4::tree::TerminalNode *MINUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  LegacyDecimalLiteralContext : public NumberContext {
  public:
    LegacyDecimalLiteralContext(NumberContext *ctx);

    antlr4::tree::TerminalNode *EXPONENT_VALUE();
    antlr4::tree::TerminalNode *DECIMAL_VALUE();
    antlr4::tree::TerminalNode *MINUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  BigDecimalLiteralContext : public NumberContext {
  public:
    BigDecimalLiteralContext(NumberContext *ctx);

    antlr4::tree::TerminalNode *BIGDECIMAL_LITERAL();
    antlr4::tree::TerminalNode *MINUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ExponentLiteralContext : public NumberContext {
  public:
    ExponentLiteralContext(NumberContext *ctx);

    antlr4::tree::TerminalNode *EXPONENT_VALUE();
    antlr4::tree::TerminalNode *MINUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  DoubleLiteralContext : public NumberContext {
  public:
    DoubleLiteralContext(NumberContext *ctx);

    antlr4::tree::TerminalNode *DOUBLE_LITERAL();
    antlr4::tree::TerminalNode *MINUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  IntegerLiteralContext : public NumberContext {
  public:
    IntegerLiteralContext(NumberContext *ctx);

    antlr4::tree::TerminalNode *INTEGER_VALUE();
    antlr4::tree::TerminalNode *MINUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  FloatLiteralContext : public NumberContext {
  public:
    FloatLiteralContext(NumberContext *ctx);

    antlr4::tree::TerminalNode *FLOAT_LITERAL();
    antlr4::tree::TerminalNode *MINUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  SmallIntLiteralContext : public NumberContext {
  public:
    SmallIntLiteralContext(NumberContext *ctx);

    antlr4::tree::TerminalNode *SMALLINT_LITERAL();
    antlr4::tree::TerminalNode *MINUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  NumberContext* number();

  class  ConstantContext : public antlr4::ParserRuleContext {
  public:
    ConstantContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    ConstantContext() = default;
    void copyFrom(ConstantContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  NullLiteralContext : public ConstantContext {
  public:
    NullLiteralContext(ConstantContext *ctx);

    antlr4::tree::TerminalNode *NULLTOKEN();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  StringLiteralContext : public ConstantContext {
  public:
    StringLiteralContext(ConstantContext *ctx);

    std::vector<antlr4::tree::TerminalNode *> STRING();
    antlr4::tree::TerminalNode* STRING(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  TypeConstructorContext : public ConstantContext {
  public:
    TypeConstructorContext(ConstantContext *ctx);

    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *STRING();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  NumericLiteralContext : public ConstantContext {
  public:
    NumericLiteralContext(ConstantContext *ctx);

    NumberContext *number();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  BooleanLiteralContext : public ConstantContext {
  public:
    BooleanLiteralContext(ConstantContext *ctx);

    BooleanValueContext *booleanValue();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  ConstantContext* constant();

  class  BooleanValueContext : public antlr4::ParserRuleContext {
  public:
    BooleanValueContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TRUE();
    antlr4::tree::TerminalNode *FALSE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  BooleanValueContext* booleanValue();

  class  StrictNonReservedContext : public antlr4::ParserRuleContext {
  public:
    StrictNonReservedContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FULL();
    antlr4::tree::TerminalNode *INNER();
    antlr4::tree::TerminalNode *JOIN();
    antlr4::tree::TerminalNode *LEFT();
    antlr4::tree::TerminalNode *NATURAL();
    antlr4::tree::TerminalNode *ON();
    antlr4::tree::TerminalNode *RIGHT();
    antlr4::tree::TerminalNode *UNION();
    antlr4::tree::TerminalNode *USING();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  StrictNonReservedContext* strictNonReserved();

  class  AnsiNonReservedContext : public antlr4::ParserRuleContext {
  public:
    AnsiNonReservedContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ASC();
    antlr4::tree::TerminalNode *AT();
    antlr4::tree::TerminalNode *BETWEEN();
    antlr4::tree::TerminalNode *BY();
    antlr4::tree::TerminalNode *CUBE();
    antlr4::tree::TerminalNode *DELETE();
    antlr4::tree::TerminalNode *DESC();
    antlr4::tree::TerminalNode *DIV();
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *EXISTS();
    antlr4::tree::TerminalNode *FIRST();
    antlr4::tree::TerminalNode *GROUPING();
    antlr4::tree::TerminalNode *INSERT();
    antlr4::tree::TerminalNode *LAST();
    antlr4::tree::TerminalNode *LIKE();
    antlr4::tree::TerminalNode *LIMIT();
    antlr4::tree::TerminalNode *MERGE();
    antlr4::tree::TerminalNode *NULLS();
    antlr4::tree::TerminalNode *QUERY();
    antlr4::tree::TerminalNode *RLIKE();
    antlr4::tree::TerminalNode *ROLLUP();
    antlr4::tree::TerminalNode *SETS();
    antlr4::tree::TerminalNode *TRUE();
    antlr4::tree::TerminalNode *TYPE();
    antlr4::tree::TerminalNode *VALUES();
    antlr4::tree::TerminalNode *WINDOW();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  AnsiNonReservedContext* ansiNonReserved();

  class  NonReservedContext : public antlr4::ParserRuleContext {
  public:
    NonReservedContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ASC();
    antlr4::tree::TerminalNode *AT();
    antlr4::tree::TerminalNode *BETWEEN();
    antlr4::tree::TerminalNode *CUBE();
    antlr4::tree::TerminalNode *BY();
    antlr4::tree::TerminalNode *DELETE();
    antlr4::tree::TerminalNode *DESC();
    antlr4::tree::TerminalNode *DIV();
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *EXISTS();
    antlr4::tree::TerminalNode *FIRST();
    antlr4::tree::TerminalNode *GROUPING();
    antlr4::tree::TerminalNode *INSERT();
    antlr4::tree::TerminalNode *LAST();
    antlr4::tree::TerminalNode *LIKE();
    antlr4::tree::TerminalNode *LIMIT();
    antlr4::tree::TerminalNode *MERGE();
    antlr4::tree::TerminalNode *NULLS();
    antlr4::tree::TerminalNode *QUERY();
    antlr4::tree::TerminalNode *RLIKE();
    antlr4::tree::TerminalNode *ROLLUP();
    antlr4::tree::TerminalNode *SETS();
    antlr4::tree::TerminalNode *TRUE();
    antlr4::tree::TerminalNode *TYPE();
    antlr4::tree::TerminalNode *VALUES();
    antlr4::tree::TerminalNode *WINDOW();
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *AND();
    antlr4::tree::TerminalNode *ANY();
    antlr4::tree::TerminalNode *AS();
    antlr4::tree::TerminalNode *DISTINCT();
    antlr4::tree::TerminalNode *ESCAPE();
    antlr4::tree::TerminalNode *FALSE();
    antlr4::tree::TerminalNode *FROM();
    antlr4::tree::TerminalNode *GROUP();
    antlr4::tree::TerminalNode *HAVING();
    antlr4::tree::TerminalNode *IN();
    antlr4::tree::TerminalNode *INTO();
    antlr4::tree::TerminalNode *IS();
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *NULLTOKEN();
    antlr4::tree::TerminalNode *OR();
    antlr4::tree::TerminalNode *ORDER();
    antlr4::tree::TerminalNode *SELECT();
    antlr4::tree::TerminalNode *SOME();
    antlr4::tree::TerminalNode *TABLE();
    antlr4::tree::TerminalNode *WHERE();
    antlr4::tree::TerminalNode *WITH();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NonReservedContext* nonReserved();


  virtual bool sempred(antlr4::RuleContext *_localctx, size_t ruleIndex, size_t predicateIndex) override;
  bool queryTermSempred(QueryTermContext *_localctx, size_t predicateIndex);
  bool identifierSempred(IdentifierContext *_localctx, size_t predicateIndex);
  bool strictIdentifierSempred(StrictIdentifierContext *_localctx, size_t predicateIndex);
  bool booleanExpressionSempred(BooleanExpressionContext *_localctx, size_t predicateIndex);
  bool valueExpressionSempred(ValueExpressionContext *_localctx, size_t predicateIndex);
  bool primaryExpressionSempred(PrimaryExpressionContext *_localctx, size_t predicateIndex);
  bool numberSempred(NumberContext *_localctx, size_t predicateIndex);

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

}  // namespace NES::Parsers
