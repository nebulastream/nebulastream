
// Generated from /IdeaProjects/nebulastream2/nes-sql-parser/AntlrSQL.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"


#include <Util/DisableWarningsPragma.hpp>
DISABLE_WARNING_PUSH
DISABLE_WARNING(-Wlogical-op-parentheses)
DISABLE_WARNING(-Wunused-parameter)




class  AntlrSQLParser : public antlr4::Parser {
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
    RECOVER = 58, RIGHT = 59, RLIKE = 60, ROLLUP = 61, SCHEMA = 62, SELECT = 63, 
    SETS = 64, SOME = 65, START = 66, TABLE = 67, TO = 68, TRUE = 69, TYPE = 70, 
    UNION = 71, UNKNOWN = 72, USE = 73, USING = 74, VALUES = 75, WHEN = 76, 
    WHERE = 77, WINDOW = 78, WITH = 79, SET = 80, TUMBLING = 81, SLIDING = 82, 
    THRESHOLD = 83, SIZE = 84, ADVANCE = 85, MS = 86, SEC = 87, MINUTE = 88, 
    HOUR = 89, DAY = 90, MIN = 91, MAX = 92, AVG = 93, SUM = 94, COUNT = 95, 
    MEDIAN = 96, WATERMARK = 97, OFFSET = 98, CSV_FORMAT = 99, AT_MOST_ONCE = 100, 
    AT_LEAST_ONCE = 101, JSON = 102, TEXT = 103, EXPLAIN = 104, BOOLEAN_VALUE = 105, 
    EQ = 106, NSEQ = 107, NEQ = 108, NEQJ = 109, LT = 110, LTE = 111, GT = 112, 
    GTE = 113, PLUS = 114, MINUS = 115, ASTERISK = 116, SLASH = 117, PERCENT = 118, 
    TILDE = 119, AMPERSAND = 120, PIPE = 121, CONCAT_PIPE = 122, HAT = 123, 
    STRING = 124, INTEGER_VALUE = 125, FLOAT_LITERAL = 126, WS = 127, SINKS = 128, 
    SOURCES = 129, QUERIES = 130, DATA_TYPE = 131, INTEGER_UNSIGNED_TYPE = 132, 
    INTEGER_SIGNED_TYPE = 133, INTEGER_BASES_TYPES = 134, TINY_INT_TYPE = 135, 
    SMALL_INT_TYPE = 136, NORMAL_INT_TYPE = 137, BIG_INT_TYPE = 138, FLOATING_POINT_TYPE = 139, 
    CHAR_TYPE = 140, VARSIZED_TYPE = 141, BOOLEAN_TYPE = 142, UNSIGNED_TYPE_QUALIFIER = 143, 
    SHOW = 144, FORMAT = 145, CREATE = 146, SOURCE = 147, LOGICAL = 148, 
    PHYSICAL = 149, WORKER = 150, SINK = 151, SIMPLE_COMMENT = 152, BRACKETED_COMMENT = 153, 
    IDENTIFIER = 154, UNRECOGNIZED = 155
  };

  enum {
    RuleSingleStatement = 0, RuleTerminatedStatement = 1, RuleMultipleStatements = 2, 
    RuleStatement = 3, RuleExplainStatement = 4, RuleCreateStatement = 5, 
    RuleCreateDefinition = 6, RuleCreateLogicalSourceDefinition = 7, RuleCreatePhysicalSourceDefinition = 8, 
    RuleOptionsClause = 9, RuleCreateSinkDefinition = 10, RuleCreateWorkerDefinition = 11, 
    RuleSchemaDefinition = 12, RuleColumnDefinition = 13, RuleTypeDefinition = 14, 
    RuleNullableDefinition = 15, RuleFromQuery = 16, RuleDropStatement = 17, 
    RuleDropSubject = 18, RuleDropQuery = 19, RuleDropSource = 20, RuleDropLogicalSourceSubject = 21, 
    RuleDropPhysicalSourceSubject = 22, RuleDropWorker = 23, RuleDropSink = 24, 
    RuleDropFilter = 25, RuleShowStatement = 26, RuleShowFormat = 27, RuleShowSubject = 28, 
    RuleShowFilter = 29, RuleQueryWithOptions = 30, RuleQuery = 31, RuleQueryOrganization = 32, 
    RuleQueryTerm = 33, RuleQueryPrimary = 34, RuleQuerySpecification = 35, 
    RuleFromClause = 36, RuleRelation = 37, RuleJoinRelation = 38, RuleJoinType = 39, 
    RuleJoinCriteria = 40, RuleRelationPrimary = 41, RuleInlineSource = 42, 
    RuleSchema = 43, RuleFromStatement = 44, RuleFromStatementBody = 45, 
    RuleSelectClause = 46, RuleWhereClause = 47, RuleHavingClause = 48, 
    RuleInlineTable = 49, RuleTableAlias = 50, RuleMultipartIdentifier = 51, 
    RuleNamedConfigExpression = 52, RuleNamedExpression = 53, RuleIdentifier = 54, 
    RuleStrictIdentifier = 55, RuleQuotedIdentifier = 56, RuleIdentifierChain = 57, 
    RuleIdentifierList = 58, RuleIdentifierSeq = 59, RuleErrorCapturingIdentifier = 60, 
    RuleErrorCapturingIdentifierExtra = 61, RuleNamedConfigExpressionSeq = 62, 
    RuleNamedExpressionSeq = 63, RuleExpression = 64, RuleBooleanExpression = 65, 
    RuleWindowedAggregationClause = 66, RuleGroupByClause = 67, RuleGroupingSet = 68, 
    RuleWindowClause = 69, RuleWatermarkClause = 70, RuleWatermarkParameters = 71, 
    RuleWindowSpec = 72, RuleTimeWindow = 73, RuleCountWindow = 74, RuleConditionWindow = 75, 
    RuleConditionParameter = 76, RuleThresholdMinSizeParameter = 77, RuleSizeParameter = 78, 
    RuleAdvancebyParameter = 79, RuleTimeUnit = 80, RuleTimestampParameter = 81, 
    RuleFunctionName = 82, RuleSinkClause = 83, RuleSink = 84, RuleInlineSink = 85, 
    RuleNullNotnull = 86, RuleStreamName = 87, RuleFileFormat = 88, RuleSortItem = 89, 
    RulePredicate = 90, RuleValueExpression = 91, RuleComparisonOperator = 92, 
    RuleHint = 93, RuleHintStatement = 94, RulePrimaryExpression = 95, RuleQualifiedName = 96, 
    RuleNumber = 97, RuleUnsignedIntegerLiteral = 98, RuleSignedIntegerLiteral = 99, 
    RuleConstant = 100, RuleBooleanValue = 101
  };

  explicit AntlrSQLParser(antlr4::TokenStream *input);

  AntlrSQLParser(antlr4::TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options);

  ~AntlrSQLParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;


        bool SQL_standard_keyword_behavior = false;
        bool legacy_exponent_literal_as_decimal_enabled = false;


  class SingleStatementContext;
  class TerminatedStatementContext;
  class MultipleStatementsContext;
  class StatementContext;
  class ExplainStatementContext;
  class CreateStatementContext;
  class CreateDefinitionContext;
  class CreateLogicalSourceDefinitionContext;
  class CreatePhysicalSourceDefinitionContext;
  class OptionsClauseContext;
  class CreateSinkDefinitionContext;
  class CreateWorkerDefinitionContext;
  class SchemaDefinitionContext;
  class ColumnDefinitionContext;
  class TypeDefinitionContext;
  class NullableDefinitionContext;
  class FromQueryContext;
  class DropStatementContext;
  class DropSubjectContext;
  class DropQueryContext;
  class DropSourceContext;
  class DropLogicalSourceSubjectContext;
  class DropPhysicalSourceSubjectContext;
  class DropWorkerContext;
  class DropSinkContext;
  class DropFilterContext;
  class ShowStatementContext;
  class ShowFormatContext;
  class ShowSubjectContext;
  class ShowFilterContext;
  class QueryWithOptionsContext;
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
  class InlineSourceContext;
  class SchemaContext;
  class FromStatementContext;
  class FromStatementBodyContext;
  class SelectClauseContext;
  class WhereClauseContext;
  class HavingClauseContext;
  class InlineTableContext;
  class TableAliasContext;
  class MultipartIdentifierContext;
  class NamedConfigExpressionContext;
  class NamedExpressionContext;
  class IdentifierContext;
  class StrictIdentifierContext;
  class QuotedIdentifierContext;
  class IdentifierChainContext;
  class IdentifierListContext;
  class IdentifierSeqContext;
  class ErrorCapturingIdentifierContext;
  class ErrorCapturingIdentifierExtraContext;
  class NamedConfigExpressionSeqContext;
  class NamedExpressionSeqContext;
  class ExpressionContext;
  class BooleanExpressionContext;
  class WindowedAggregationClauseContext;
  class GroupByClauseContext;
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
  class SinkContext;
  class InlineSinkContext;
  class NullNotnullContext;
  class StreamNameContext;
  class FileFormatContext;
  class SortItemContext;
  class PredicateContext;
  class ValueExpressionContext;
  class ComparisonOperatorContext;
  class HintContext;
  class HintStatementContext;
  class PrimaryExpressionContext;
  class QualifiedNameContext;
  class NumberContext;
  class UnsignedIntegerLiteralContext;
  class SignedIntegerLiteralContext;
  class ConstantContext;
  class BooleanValueContext; 

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

  class  TerminatedStatementContext : public antlr4::ParserRuleContext {
  public:
    TerminatedStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StatementContext *statement();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TerminatedStatementContext* terminatedStatement();

  class  MultipleStatementsContext : public antlr4::ParserRuleContext {
  public:
    MultipleStatementsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EOF();
    std::vector<StatementContext *> statement();
    StatementContext* statement(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  MultipleStatementsContext* multipleStatements();

  class  StatementContext : public antlr4::ParserRuleContext {
  public:
    StatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryWithOptionsContext *queryWithOptions();
    CreateStatementContext *createStatement();
    DropStatementContext *dropStatement();
    ShowStatementContext *showStatement();
    ExplainStatementContext *explainStatement();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  StatementContext* statement();

  class  ExplainStatementContext : public antlr4::ParserRuleContext {
  public:
    ExplainStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EXPLAIN();
    QueryContext *query();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ExplainStatementContext* explainStatement();

  class  CreateStatementContext : public antlr4::ParserRuleContext {
  public:
    CreateStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    CreateDefinitionContext *createDefinition();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateStatementContext* createStatement();

  class  CreateDefinitionContext : public antlr4::ParserRuleContext {
  public:
    CreateDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CreateLogicalSourceDefinitionContext *createLogicalSourceDefinition();
    CreatePhysicalSourceDefinitionContext *createPhysicalSourceDefinition();
    CreateSinkDefinitionContext *createSinkDefinition();
    CreateWorkerDefinitionContext *createWorkerDefinition();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateDefinitionContext* createDefinition();

  class  CreateLogicalSourceDefinitionContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::IdentifierContext *sourceName = nullptr;
    CreateLogicalSourceDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOGICAL();
    antlr4::tree::TerminalNode *SOURCE();
    SchemaDefinitionContext *schemaDefinition();
    IdentifierContext *identifier();
    FromQueryContext *fromQuery();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateLogicalSourceDefinitionContext* createLogicalSourceDefinition();

  class  CreatePhysicalSourceDefinitionContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::IdentifierContext *logicalSource = nullptr;
    AntlrSQLParser::IdentifierContext *type = nullptr;
    CreatePhysicalSourceDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PHYSICAL();
    antlr4::tree::TerminalNode *SOURCE();
    antlr4::tree::TerminalNode *FOR();
    antlr4::tree::TerminalNode *TYPE();
    std::vector<IdentifierContext *> identifier();
    IdentifierContext* identifier(size_t i);
    OptionsClauseContext *optionsClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreatePhysicalSourceDefinitionContext* createPhysicalSourceDefinition();

  class  OptionsClauseContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::NamedConfigExpressionSeqContext *options = nullptr;
    OptionsClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET();
    NamedConfigExpressionSeqContext *namedConfigExpressionSeq();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  OptionsClauseContext* optionsClause();

  class  CreateSinkDefinitionContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::IdentifierContext *sinkName = nullptr;
    AntlrSQLParser::IdentifierContext *type = nullptr;
    CreateSinkDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SINK();
    SchemaDefinitionContext *schemaDefinition();
    antlr4::tree::TerminalNode *TYPE();
    std::vector<IdentifierContext *> identifier();
    IdentifierContext* identifier(size_t i);
    OptionsClauseContext *optionsClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateSinkDefinitionContext* createSinkDefinition();

  class  CreateWorkerDefinitionContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *hostaddr = nullptr;
    CreateWorkerDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WORKER();
    antlr4::tree::TerminalNode *STRING();
    OptionsClauseContext *optionsClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  CreateWorkerDefinitionContext* createWorkerDefinition();

  class  SchemaDefinitionContext : public antlr4::ParserRuleContext {
  public:
    SchemaDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ColumnDefinitionContext *> columnDefinition();
    ColumnDefinitionContext* columnDefinition(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SchemaDefinitionContext* schemaDefinition();

  class  ColumnDefinitionContext : public antlr4::ParserRuleContext {
  public:
    ColumnDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierChainContext *identifierChain();
    TypeDefinitionContext *typeDefinition();
    NullableDefinitionContext *nullableDefinition();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ColumnDefinitionContext* columnDefinition();

  class  TypeDefinitionContext : public antlr4::ParserRuleContext {
  public:
    TypeDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DATA_TYPE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TypeDefinitionContext* typeDefinition();

  class  NullableDefinitionContext : public antlr4::ParserRuleContext {
  public:
    NullableDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *NULLTOKEN();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NullableDefinitionContext* nullableDefinition();

  class  FromQueryContext : public antlr4::ParserRuleContext {
  public:
    FromQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AS();
    QueryContext *query();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FromQueryContext* fromQuery();

  class  DropStatementContext : public antlr4::ParserRuleContext {
  public:
    DropStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP();
    DropSubjectContext *dropSubject();
    antlr4::tree::TerminalNode *WHERE();
    DropFilterContext *dropFilter();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropStatementContext* dropStatement();

  class  DropSubjectContext : public antlr4::ParserRuleContext {
  public:
    DropSubjectContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DropQueryContext *dropQuery();
    DropSourceContext *dropSource();
    DropSinkContext *dropSink();
    DropWorkerContext *dropWorker();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropSubjectContext* dropSubject();

  class  DropQueryContext : public antlr4::ParserRuleContext {
  public:
    DropQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *QUERY();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropQueryContext* dropQuery();

  class  DropSourceContext : public antlr4::ParserRuleContext {
  public:
    DropSourceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DropLogicalSourceSubjectContext *dropLogicalSourceSubject();
    DropPhysicalSourceSubjectContext *dropPhysicalSourceSubject();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropSourceContext* dropSource();

  class  DropLogicalSourceSubjectContext : public antlr4::ParserRuleContext {
  public:
    DropLogicalSourceSubjectContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOGICAL();
    antlr4::tree::TerminalNode *SOURCE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropLogicalSourceSubjectContext* dropLogicalSourceSubject();

  class  DropPhysicalSourceSubjectContext : public antlr4::ParserRuleContext {
  public:
    DropPhysicalSourceSubjectContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PHYSICAL();
    antlr4::tree::TerminalNode *SOURCE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropPhysicalSourceSubjectContext* dropPhysicalSourceSubject();

  class  DropWorkerContext : public antlr4::ParserRuleContext {
  public:
    DropWorkerContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WORKER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropWorkerContext* dropWorker();

  class  DropSinkContext : public antlr4::ParserRuleContext {
  public:
    DropSinkContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SINK();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropSinkContext* dropSink();

  class  DropFilterContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::StrictIdentifierContext *attr = nullptr;
    AntlrSQLParser::ConstantContext *value = nullptr;
    DropFilterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EQ();
    StrictIdentifierContext *strictIdentifier();
    ConstantContext *constant();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  DropFilterContext* dropFilter();

  class  ShowStatementContext : public antlr4::ParserRuleContext {
  public:
    ShowStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    ShowSubjectContext *showSubject();
    antlr4::tree::TerminalNode *WHERE();
    ShowFilterContext *showFilter();
    antlr4::tree::TerminalNode *FORMAT();
    ShowFormatContext *showFormat();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ShowStatementContext* showStatement();

  class  ShowFormatContext : public antlr4::ParserRuleContext {
  public:
    ShowFormatContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TEXT();
    antlr4::tree::TerminalNode *JSON();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ShowFormatContext* showFormat();

  class  ShowSubjectContext : public antlr4::ParserRuleContext {
  public:
    ShowSubjectContext(antlr4::ParserRuleContext *parent, size_t invokingState);
   
    ShowSubjectContext() = default;
    void copyFrom(ShowSubjectContext *context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;

   
  };

  class  ShowPhysicalSourcesSubjectContext : public ShowSubjectContext {
  public:
    ShowPhysicalSourcesSubjectContext(ShowSubjectContext *ctx);

    AntlrSQLParser::StrictIdentifierContext *logicalSourceName = nullptr;
    antlr4::tree::TerminalNode *PHYSICAL();
    antlr4::tree::TerminalNode *SOURCES();
    antlr4::tree::TerminalNode *FOR();
    StrictIdentifierContext *strictIdentifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ShowSinksSubjectContext : public ShowSubjectContext {
  public:
    ShowSinksSubjectContext(ShowSubjectContext *ctx);

    antlr4::tree::TerminalNode *SINKS();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ShowQueriesSubjectContext : public ShowSubjectContext {
  public:
    ShowQueriesSubjectContext(ShowSubjectContext *ctx);

    antlr4::tree::TerminalNode *QUERIES();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ShowLogicalSourcesSubjectContext : public ShowSubjectContext {
  public:
    ShowLogicalSourcesSubjectContext(ShowSubjectContext *ctx);

    antlr4::tree::TerminalNode *LOGICAL();
    antlr4::tree::TerminalNode *SOURCES();
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  ShowSubjectContext* showSubject();

  class  ShowFilterContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::StrictIdentifierContext *attr = nullptr;
    AntlrSQLParser::ConstantContext *value = nullptr;
    ShowFilterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EQ();
    StrictIdentifierContext *strictIdentifier();
    ConstantContext *constant();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  ShowFilterContext* showFilter();

  class  QueryWithOptionsContext : public antlr4::ParserRuleContext {
  public:
    QueryWithOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryContext *query();
    OptionsClauseContext *optionsClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  QueryWithOptionsContext* queryWithOptions();

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
    AntlrSQLParser::SortItemContext *sortItemContext = nullptr;
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

    AntlrSQLParser::QueryTermContext *left = nullptr;
    antlr4::Token *setoperator = nullptr;
    AntlrSQLParser::QueryTermContext *right = nullptr;
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
    AntlrSQLParser::RelationPrimaryContext *right = nullptr;
    JoinRelationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *JOIN();
    WindowClauseContext *windowClause();
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

  class  InlineDefinedSourceContext : public RelationPrimaryContext {
  public:
    InlineDefinedSourceContext(RelationPrimaryContext *ctx);

    InlineSourceContext *inlineSource();
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

  class  InlineSourceContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::IdentifierContext *type = nullptr;
    AntlrSQLParser::NamedConfigExpressionSeqContext *parameters = nullptr;
    InlineSourceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    NamedConfigExpressionSeqContext *namedConfigExpressionSeq();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InlineSourceContext* inlineSource();

  class  SchemaContext : public antlr4::ParserRuleContext {
  public:
    SchemaContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SCHEMA();
    SchemaDefinitionContext *schemaDefinition();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SchemaContext* schema();

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
    GroupByClauseContext *groupByClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FromStatementBodyContext* fromStatementBody();

  class  SelectClauseContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::HintContext *hintContext = nullptr;
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
    IdentifierContext *identifier();
    antlr4::tree::TerminalNode *AS();
    IdentifierListContext *identifierList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TableAliasContext* tableAlias();

  class  MultipartIdentifierContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::ErrorCapturingIdentifierContext *errorCapturingIdentifierContext = nullptr;
    std::vector<ErrorCapturingIdentifierContext *> parts;
    MultipartIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ErrorCapturingIdentifierContext *> errorCapturingIdentifier();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  MultipartIdentifierContext* multipartIdentifier();

  class  NamedConfigExpressionContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::IdentifierChainContext *name = nullptr;
    NamedConfigExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AS();
    IdentifierChainContext *identifierChain();
    ConstantContext *constant();
    SchemaContext *schema();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NamedConfigExpressionContext* namedConfigExpression();

  class  NamedExpressionContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::IdentifierContext *name = nullptr;
    NamedExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext *expression();
    antlr4::tree::TerminalNode *AS();
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NamedExpressionContext* namedExpression();

  class  IdentifierContext : public antlr4::ParserRuleContext {
  public:
    IdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StrictIdentifierContext *strictIdentifier();

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

  class  IdentifierChainContext : public antlr4::ParserRuleContext {
  public:
    IdentifierChainContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<StrictIdentifierContext *> strictIdentifier();
    StrictIdentifierContext* strictIdentifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  IdentifierChainContext* identifierChain();

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
    AntlrSQLParser::ErrorCapturingIdentifierContext *errorCapturingIdentifierContext = nullptr;
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

  class  NamedConfigExpressionSeqContext : public antlr4::ParserRuleContext {
  public:
    NamedConfigExpressionSeqContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NamedConfigExpressionContext *> namedConfigExpression();
    NamedConfigExpressionContext* namedConfigExpression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  NamedConfigExpressionSeqContext* namedConfigExpressionSeq();

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
    ValueExpressionContext *valueExpression();
    BooleanExpressionContext *booleanExpression();
    IdentifierContext *identifier();
    SchemaContext *schema();

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

    AntlrSQLParser::BooleanExpressionContext *left = nullptr;
    antlr4::Token *op = nullptr;
    AntlrSQLParser::BooleanExpressionContext *right = nullptr;
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
    GroupByClauseContext *groupByClause();
    WatermarkClauseContext *watermarkClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  WindowedAggregationClauseContext* windowedAggregationClause();

  class  GroupByClauseContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::ExpressionContext *expressionContext = nullptr;
    std::vector<ExpressionContext *> groupingExpressions;
    antlr4::Token *kind = nullptr;
    GroupByClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
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

  GroupByClauseContext* groupByClause();

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
    AntlrSQLParser::IdentifierContext *watermarkIdentifier = nullptr;
    antlr4::Token *watermark = nullptr;
    AntlrSQLParser::TimeUnitContext *watermarkTimeUnit = nullptr;
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
    antlr4::tree::TerminalNode *MINUTE();
    antlr4::tree::TerminalNode *HOUR();
    antlr4::tree::TerminalNode *DAY();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TimeUnitContext* timeUnit();

  class  TimestampParameterContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::IdentifierContext *name = nullptr;
    TimestampParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  TimestampParameterContext* timestampParameter();

  class  FunctionNameContext : public antlr4::ParserRuleContext {
  public:
    FunctionNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IDENTIFIER();
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
    std::vector<SinkContext *> sink();
    SinkContext* sink(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SinkClauseContext* sinkClause();

  class  SinkContext : public antlr4::ParserRuleContext {
  public:
    SinkContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    InlineSinkContext *inlineSink();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SinkContext* sink();

  class  InlineSinkContext : public antlr4::ParserRuleContext {
  public:
    AntlrSQLParser::IdentifierContext *type = nullptr;
    AntlrSQLParser::NamedConfigExpressionSeqContext *parameters = nullptr;
    InlineSinkContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext *identifier();
    NamedConfigExpressionSeqContext *namedConfigExpressionSeq();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  InlineSinkContext* inlineSink();

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

  class  StreamNameContext : public antlr4::ParserRuleContext {
  public:
    StreamNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IDENTIFIER();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  StreamNameContext* streamName();

  class  FileFormatContext : public antlr4::ParserRuleContext {
  public:
    FileFormatContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CSV_FORMAT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  FileFormatContext* fileFormat();

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
    AntlrSQLParser::ValueExpressionContext *lower = nullptr;
    AntlrSQLParser::ValueExpressionContext *upper = nullptr;
    AntlrSQLParser::ValueExpressionContext *pattern = nullptr;
    antlr4::Token *quantifier = nullptr;
    antlr4::Token *escapeChar = nullptr;
    AntlrSQLParser::ValueExpressionContext *right = nullptr;
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

    AntlrSQLParser::ValueExpressionContext *left = nullptr;
    AntlrSQLParser::ValueExpressionContext *right = nullptr;
    ComparisonOperatorContext *comparisonOperator();
    std::vector<ValueExpressionContext *> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  FunctionCallContext : public ValueExpressionContext {
  public:
    FunctionCallContext(ValueExpressionContext *ctx);

    AntlrSQLParser::ExpressionContext *expressionContext = nullptr;
    std::vector<ExpressionContext *> argument;
    FunctionNameContext *functionName();
    TypeDefinitionContext *typeDefinition();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  class  ArithmeticBinaryContext : public ValueExpressionContext {
  public:
    ArithmeticBinaryContext(ValueExpressionContext *ctx);

    AntlrSQLParser::ValueExpressionContext *left = nullptr;
    antlr4::Token *op = nullptr;
    AntlrSQLParser::ValueExpressionContext *right = nullptr;
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
    AntlrSQLParser::HintStatementContext *hintStatementContext = nullptr;
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
    AntlrSQLParser::IdentifierContext *hintName = nullptr;
    AntlrSQLParser::PrimaryExpressionContext *primaryExpressionContext = nullptr;
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

    AntlrSQLParser::PrimaryExpressionContext *base = nullptr;
    AntlrSQLParser::IdentifierContext *fieldName = nullptr;
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

  NumberContext* number();

  class  UnsignedIntegerLiteralContext : public antlr4::ParserRuleContext {
  public:
    UnsignedIntegerLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INTEGER_VALUE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  UnsignedIntegerLiteralContext* unsignedIntegerLiteral();

  class  SignedIntegerLiteralContext : public antlr4::ParserRuleContext {
  public:
    SignedIntegerLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MINUS();
    antlr4::tree::TerminalNode *INTEGER_VALUE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
   
  };

  SignedIntegerLiteralContext* signedIntegerLiteral();

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

    antlr4::tree::TerminalNode *STRING();
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


  bool sempred(antlr4::RuleContext *_localctx, size_t ruleIndex, size_t predicateIndex) override;

  bool queryTermSempred(QueryTermContext *_localctx, size_t predicateIndex);
  bool booleanExpressionSempred(BooleanExpressionContext *_localctx, size_t predicateIndex);
  bool valueExpressionSempred(ValueExpressionContext *_localctx, size_t predicateIndex);
  bool primaryExpressionSempred(PrimaryExpressionContext *_localctx, size_t predicateIndex);

  // By default the static state used to implement the parser is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:
};

