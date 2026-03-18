
// Generated from /IdeaProjects/nebulastream2/nes-sql-parser/AntlrSQL.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "AntlrSQLParser.h"


/**
 * This interface defines an abstract listener for a parse tree produced by AntlrSQLParser.
 */
class  AntlrSQLListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterSingleStatement(AntlrSQLParser::SingleStatementContext *ctx) = 0;
  virtual void exitSingleStatement(AntlrSQLParser::SingleStatementContext *ctx) = 0;

  virtual void enterTerminatedStatement(AntlrSQLParser::TerminatedStatementContext *ctx) = 0;
  virtual void exitTerminatedStatement(AntlrSQLParser::TerminatedStatementContext *ctx) = 0;

  virtual void enterMultipleStatements(AntlrSQLParser::MultipleStatementsContext *ctx) = 0;
  virtual void exitMultipleStatements(AntlrSQLParser::MultipleStatementsContext *ctx) = 0;

  virtual void enterStatement(AntlrSQLParser::StatementContext *ctx) = 0;
  virtual void exitStatement(AntlrSQLParser::StatementContext *ctx) = 0;

  virtual void enterExplainStatement(AntlrSQLParser::ExplainStatementContext *ctx) = 0;
  virtual void exitExplainStatement(AntlrSQLParser::ExplainStatementContext *ctx) = 0;

  virtual void enterCreateStatement(AntlrSQLParser::CreateStatementContext *ctx) = 0;
  virtual void exitCreateStatement(AntlrSQLParser::CreateStatementContext *ctx) = 0;

  virtual void enterCreateDefinition(AntlrSQLParser::CreateDefinitionContext *ctx) = 0;
  virtual void exitCreateDefinition(AntlrSQLParser::CreateDefinitionContext *ctx) = 0;

  virtual void enterCreateLogicalSourceDefinition(AntlrSQLParser::CreateLogicalSourceDefinitionContext *ctx) = 0;
  virtual void exitCreateLogicalSourceDefinition(AntlrSQLParser::CreateLogicalSourceDefinitionContext *ctx) = 0;

  virtual void enterCreatePhysicalSourceDefinition(AntlrSQLParser::CreatePhysicalSourceDefinitionContext *ctx) = 0;
  virtual void exitCreatePhysicalSourceDefinition(AntlrSQLParser::CreatePhysicalSourceDefinitionContext *ctx) = 0;

  virtual void enterOptionsClause(AntlrSQLParser::OptionsClauseContext *ctx) = 0;
  virtual void exitOptionsClause(AntlrSQLParser::OptionsClauseContext *ctx) = 0;

  virtual void enterCreateSinkDefinition(AntlrSQLParser::CreateSinkDefinitionContext *ctx) = 0;
  virtual void exitCreateSinkDefinition(AntlrSQLParser::CreateSinkDefinitionContext *ctx) = 0;

  virtual void enterCreateWorkerDefinition(AntlrSQLParser::CreateWorkerDefinitionContext *ctx) = 0;
  virtual void exitCreateWorkerDefinition(AntlrSQLParser::CreateWorkerDefinitionContext *ctx) = 0;

  virtual void enterSchemaDefinition(AntlrSQLParser::SchemaDefinitionContext *ctx) = 0;
  virtual void exitSchemaDefinition(AntlrSQLParser::SchemaDefinitionContext *ctx) = 0;

  virtual void enterColumnDefinition(AntlrSQLParser::ColumnDefinitionContext *ctx) = 0;
  virtual void exitColumnDefinition(AntlrSQLParser::ColumnDefinitionContext *ctx) = 0;

  virtual void enterTypeDefinition(AntlrSQLParser::TypeDefinitionContext *ctx) = 0;
  virtual void exitTypeDefinition(AntlrSQLParser::TypeDefinitionContext *ctx) = 0;

  virtual void enterNullableDefinition(AntlrSQLParser::NullableDefinitionContext *ctx) = 0;
  virtual void exitNullableDefinition(AntlrSQLParser::NullableDefinitionContext *ctx) = 0;

  virtual void enterFromQuery(AntlrSQLParser::FromQueryContext *ctx) = 0;
  virtual void exitFromQuery(AntlrSQLParser::FromQueryContext *ctx) = 0;

  virtual void enterDropStatement(AntlrSQLParser::DropStatementContext *ctx) = 0;
  virtual void exitDropStatement(AntlrSQLParser::DropStatementContext *ctx) = 0;

  virtual void enterDropSubject(AntlrSQLParser::DropSubjectContext *ctx) = 0;
  virtual void exitDropSubject(AntlrSQLParser::DropSubjectContext *ctx) = 0;

  virtual void enterDropQuery(AntlrSQLParser::DropQueryContext *ctx) = 0;
  virtual void exitDropQuery(AntlrSQLParser::DropQueryContext *ctx) = 0;

  virtual void enterDropSource(AntlrSQLParser::DropSourceContext *ctx) = 0;
  virtual void exitDropSource(AntlrSQLParser::DropSourceContext *ctx) = 0;

  virtual void enterDropLogicalSourceSubject(AntlrSQLParser::DropLogicalSourceSubjectContext *ctx) = 0;
  virtual void exitDropLogicalSourceSubject(AntlrSQLParser::DropLogicalSourceSubjectContext *ctx) = 0;

  virtual void enterDropPhysicalSourceSubject(AntlrSQLParser::DropPhysicalSourceSubjectContext *ctx) = 0;
  virtual void exitDropPhysicalSourceSubject(AntlrSQLParser::DropPhysicalSourceSubjectContext *ctx) = 0;

  virtual void enterDropWorker(AntlrSQLParser::DropWorkerContext *ctx) = 0;
  virtual void exitDropWorker(AntlrSQLParser::DropWorkerContext *ctx) = 0;

  virtual void enterDropSink(AntlrSQLParser::DropSinkContext *ctx) = 0;
  virtual void exitDropSink(AntlrSQLParser::DropSinkContext *ctx) = 0;

  virtual void enterDropFilter(AntlrSQLParser::DropFilterContext *ctx) = 0;
  virtual void exitDropFilter(AntlrSQLParser::DropFilterContext *ctx) = 0;

  virtual void enterShowStatement(AntlrSQLParser::ShowStatementContext *ctx) = 0;
  virtual void exitShowStatement(AntlrSQLParser::ShowStatementContext *ctx) = 0;

  virtual void enterShowFormat(AntlrSQLParser::ShowFormatContext *ctx) = 0;
  virtual void exitShowFormat(AntlrSQLParser::ShowFormatContext *ctx) = 0;

  virtual void enterShowQueriesSubject(AntlrSQLParser::ShowQueriesSubjectContext *ctx) = 0;
  virtual void exitShowQueriesSubject(AntlrSQLParser::ShowQueriesSubjectContext *ctx) = 0;

  virtual void enterShowLogicalSourcesSubject(AntlrSQLParser::ShowLogicalSourcesSubjectContext *ctx) = 0;
  virtual void exitShowLogicalSourcesSubject(AntlrSQLParser::ShowLogicalSourcesSubjectContext *ctx) = 0;

  virtual void enterShowPhysicalSourcesSubject(AntlrSQLParser::ShowPhysicalSourcesSubjectContext *ctx) = 0;
  virtual void exitShowPhysicalSourcesSubject(AntlrSQLParser::ShowPhysicalSourcesSubjectContext *ctx) = 0;

  virtual void enterShowSinksSubject(AntlrSQLParser::ShowSinksSubjectContext *ctx) = 0;
  virtual void exitShowSinksSubject(AntlrSQLParser::ShowSinksSubjectContext *ctx) = 0;

  virtual void enterShowFilter(AntlrSQLParser::ShowFilterContext *ctx) = 0;
  virtual void exitShowFilter(AntlrSQLParser::ShowFilterContext *ctx) = 0;

  virtual void enterQueryWithOptions(AntlrSQLParser::QueryWithOptionsContext *ctx) = 0;
  virtual void exitQueryWithOptions(AntlrSQLParser::QueryWithOptionsContext *ctx) = 0;

  virtual void enterQuery(AntlrSQLParser::QueryContext *ctx) = 0;
  virtual void exitQuery(AntlrSQLParser::QueryContext *ctx) = 0;

  virtual void enterQueryOrganization(AntlrSQLParser::QueryOrganizationContext *ctx) = 0;
  virtual void exitQueryOrganization(AntlrSQLParser::QueryOrganizationContext *ctx) = 0;

  virtual void enterPrimaryQuery(AntlrSQLParser::PrimaryQueryContext *ctx) = 0;
  virtual void exitPrimaryQuery(AntlrSQLParser::PrimaryQueryContext *ctx) = 0;

  virtual void enterSetOperation(AntlrSQLParser::SetOperationContext *ctx) = 0;
  virtual void exitSetOperation(AntlrSQLParser::SetOperationContext *ctx) = 0;

  virtual void enterQueryPrimaryDefault(AntlrSQLParser::QueryPrimaryDefaultContext *ctx) = 0;
  virtual void exitQueryPrimaryDefault(AntlrSQLParser::QueryPrimaryDefaultContext *ctx) = 0;

  virtual void enterFromStmt(AntlrSQLParser::FromStmtContext *ctx) = 0;
  virtual void exitFromStmt(AntlrSQLParser::FromStmtContext *ctx) = 0;

  virtual void enterTable(AntlrSQLParser::TableContext *ctx) = 0;
  virtual void exitTable(AntlrSQLParser::TableContext *ctx) = 0;

  virtual void enterInlineTableDefault1(AntlrSQLParser::InlineTableDefault1Context *ctx) = 0;
  virtual void exitInlineTableDefault1(AntlrSQLParser::InlineTableDefault1Context *ctx) = 0;

  virtual void enterSubquery(AntlrSQLParser::SubqueryContext *ctx) = 0;
  virtual void exitSubquery(AntlrSQLParser::SubqueryContext *ctx) = 0;

  virtual void enterQuerySpecification(AntlrSQLParser::QuerySpecificationContext *ctx) = 0;
  virtual void exitQuerySpecification(AntlrSQLParser::QuerySpecificationContext *ctx) = 0;

  virtual void enterFromClause(AntlrSQLParser::FromClauseContext *ctx) = 0;
  virtual void exitFromClause(AntlrSQLParser::FromClauseContext *ctx) = 0;

  virtual void enterRelation(AntlrSQLParser::RelationContext *ctx) = 0;
  virtual void exitRelation(AntlrSQLParser::RelationContext *ctx) = 0;

  virtual void enterJoinRelation(AntlrSQLParser::JoinRelationContext *ctx) = 0;
  virtual void exitJoinRelation(AntlrSQLParser::JoinRelationContext *ctx) = 0;

  virtual void enterJoinType(AntlrSQLParser::JoinTypeContext *ctx) = 0;
  virtual void exitJoinType(AntlrSQLParser::JoinTypeContext *ctx) = 0;

  virtual void enterJoinCriteria(AntlrSQLParser::JoinCriteriaContext *ctx) = 0;
  virtual void exitJoinCriteria(AntlrSQLParser::JoinCriteriaContext *ctx) = 0;

  virtual void enterTableName(AntlrSQLParser::TableNameContext *ctx) = 0;
  virtual void exitTableName(AntlrSQLParser::TableNameContext *ctx) = 0;

  virtual void enterAliasedQuery(AntlrSQLParser::AliasedQueryContext *ctx) = 0;
  virtual void exitAliasedQuery(AntlrSQLParser::AliasedQueryContext *ctx) = 0;

  virtual void enterAliasedRelation(AntlrSQLParser::AliasedRelationContext *ctx) = 0;
  virtual void exitAliasedRelation(AntlrSQLParser::AliasedRelationContext *ctx) = 0;

  virtual void enterInlineTableDefault2(AntlrSQLParser::InlineTableDefault2Context *ctx) = 0;
  virtual void exitInlineTableDefault2(AntlrSQLParser::InlineTableDefault2Context *ctx) = 0;

  virtual void enterInlineDefinedSource(AntlrSQLParser::InlineDefinedSourceContext *ctx) = 0;
  virtual void exitInlineDefinedSource(AntlrSQLParser::InlineDefinedSourceContext *ctx) = 0;

  virtual void enterInlineSource(AntlrSQLParser::InlineSourceContext *ctx) = 0;
  virtual void exitInlineSource(AntlrSQLParser::InlineSourceContext *ctx) = 0;

  virtual void enterSchema(AntlrSQLParser::SchemaContext *ctx) = 0;
  virtual void exitSchema(AntlrSQLParser::SchemaContext *ctx) = 0;

  virtual void enterFromStatement(AntlrSQLParser::FromStatementContext *ctx) = 0;
  virtual void exitFromStatement(AntlrSQLParser::FromStatementContext *ctx) = 0;

  virtual void enterFromStatementBody(AntlrSQLParser::FromStatementBodyContext *ctx) = 0;
  virtual void exitFromStatementBody(AntlrSQLParser::FromStatementBodyContext *ctx) = 0;

  virtual void enterSelectClause(AntlrSQLParser::SelectClauseContext *ctx) = 0;
  virtual void exitSelectClause(AntlrSQLParser::SelectClauseContext *ctx) = 0;

  virtual void enterWhereClause(AntlrSQLParser::WhereClauseContext *ctx) = 0;
  virtual void exitWhereClause(AntlrSQLParser::WhereClauseContext *ctx) = 0;

  virtual void enterHavingClause(AntlrSQLParser::HavingClauseContext *ctx) = 0;
  virtual void exitHavingClause(AntlrSQLParser::HavingClauseContext *ctx) = 0;

  virtual void enterInlineTable(AntlrSQLParser::InlineTableContext *ctx) = 0;
  virtual void exitInlineTable(AntlrSQLParser::InlineTableContext *ctx) = 0;

  virtual void enterTableAlias(AntlrSQLParser::TableAliasContext *ctx) = 0;
  virtual void exitTableAlias(AntlrSQLParser::TableAliasContext *ctx) = 0;

  virtual void enterMultipartIdentifier(AntlrSQLParser::MultipartIdentifierContext *ctx) = 0;
  virtual void exitMultipartIdentifier(AntlrSQLParser::MultipartIdentifierContext *ctx) = 0;

  virtual void enterNamedConfigExpression(AntlrSQLParser::NamedConfigExpressionContext *ctx) = 0;
  virtual void exitNamedConfigExpression(AntlrSQLParser::NamedConfigExpressionContext *ctx) = 0;

  virtual void enterNamedExpression(AntlrSQLParser::NamedExpressionContext *ctx) = 0;
  virtual void exitNamedExpression(AntlrSQLParser::NamedExpressionContext *ctx) = 0;

  virtual void enterIdentifier(AntlrSQLParser::IdentifierContext *ctx) = 0;
  virtual void exitIdentifier(AntlrSQLParser::IdentifierContext *ctx) = 0;

  virtual void enterUnquotedIdentifier(AntlrSQLParser::UnquotedIdentifierContext *ctx) = 0;
  virtual void exitUnquotedIdentifier(AntlrSQLParser::UnquotedIdentifierContext *ctx) = 0;

  virtual void enterQuotedIdentifierAlternative(AntlrSQLParser::QuotedIdentifierAlternativeContext *ctx) = 0;
  virtual void exitQuotedIdentifierAlternative(AntlrSQLParser::QuotedIdentifierAlternativeContext *ctx) = 0;

  virtual void enterQuotedIdentifier(AntlrSQLParser::QuotedIdentifierContext *ctx) = 0;
  virtual void exitQuotedIdentifier(AntlrSQLParser::QuotedIdentifierContext *ctx) = 0;

  virtual void enterIdentifierChain(AntlrSQLParser::IdentifierChainContext *ctx) = 0;
  virtual void exitIdentifierChain(AntlrSQLParser::IdentifierChainContext *ctx) = 0;

  virtual void enterIdentifierList(AntlrSQLParser::IdentifierListContext *ctx) = 0;
  virtual void exitIdentifierList(AntlrSQLParser::IdentifierListContext *ctx) = 0;

  virtual void enterIdentifierSeq(AntlrSQLParser::IdentifierSeqContext *ctx) = 0;
  virtual void exitIdentifierSeq(AntlrSQLParser::IdentifierSeqContext *ctx) = 0;

  virtual void enterErrorCapturingIdentifier(AntlrSQLParser::ErrorCapturingIdentifierContext *ctx) = 0;
  virtual void exitErrorCapturingIdentifier(AntlrSQLParser::ErrorCapturingIdentifierContext *ctx) = 0;

  virtual void enterErrorIdent(AntlrSQLParser::ErrorIdentContext *ctx) = 0;
  virtual void exitErrorIdent(AntlrSQLParser::ErrorIdentContext *ctx) = 0;

  virtual void enterRealIdent(AntlrSQLParser::RealIdentContext *ctx) = 0;
  virtual void exitRealIdent(AntlrSQLParser::RealIdentContext *ctx) = 0;

  virtual void enterNamedConfigExpressionSeq(AntlrSQLParser::NamedConfigExpressionSeqContext *ctx) = 0;
  virtual void exitNamedConfigExpressionSeq(AntlrSQLParser::NamedConfigExpressionSeqContext *ctx) = 0;

  virtual void enterNamedExpressionSeq(AntlrSQLParser::NamedExpressionSeqContext *ctx) = 0;
  virtual void exitNamedExpressionSeq(AntlrSQLParser::NamedExpressionSeqContext *ctx) = 0;

  virtual void enterExpression(AntlrSQLParser::ExpressionContext *ctx) = 0;
  virtual void exitExpression(AntlrSQLParser::ExpressionContext *ctx) = 0;

  virtual void enterLogicalNot(AntlrSQLParser::LogicalNotContext *ctx) = 0;
  virtual void exitLogicalNot(AntlrSQLParser::LogicalNotContext *ctx) = 0;

  virtual void enterPredicated(AntlrSQLParser::PredicatedContext *ctx) = 0;
  virtual void exitPredicated(AntlrSQLParser::PredicatedContext *ctx) = 0;

  virtual void enterExists(AntlrSQLParser::ExistsContext *ctx) = 0;
  virtual void exitExists(AntlrSQLParser::ExistsContext *ctx) = 0;

  virtual void enterLogicalBinary(AntlrSQLParser::LogicalBinaryContext *ctx) = 0;
  virtual void exitLogicalBinary(AntlrSQLParser::LogicalBinaryContext *ctx) = 0;

  virtual void enterWindowedAggregationClause(AntlrSQLParser::WindowedAggregationClauseContext *ctx) = 0;
  virtual void exitWindowedAggregationClause(AntlrSQLParser::WindowedAggregationClauseContext *ctx) = 0;

  virtual void enterGroupByClause(AntlrSQLParser::GroupByClauseContext *ctx) = 0;
  virtual void exitGroupByClause(AntlrSQLParser::GroupByClauseContext *ctx) = 0;

  virtual void enterGroupingSet(AntlrSQLParser::GroupingSetContext *ctx) = 0;
  virtual void exitGroupingSet(AntlrSQLParser::GroupingSetContext *ctx) = 0;

  virtual void enterWindowClause(AntlrSQLParser::WindowClauseContext *ctx) = 0;
  virtual void exitWindowClause(AntlrSQLParser::WindowClauseContext *ctx) = 0;

  virtual void enterWatermarkClause(AntlrSQLParser::WatermarkClauseContext *ctx) = 0;
  virtual void exitWatermarkClause(AntlrSQLParser::WatermarkClauseContext *ctx) = 0;

  virtual void enterWatermarkParameters(AntlrSQLParser::WatermarkParametersContext *ctx) = 0;
  virtual void exitWatermarkParameters(AntlrSQLParser::WatermarkParametersContext *ctx) = 0;

  virtual void enterTimeBasedWindow(AntlrSQLParser::TimeBasedWindowContext *ctx) = 0;
  virtual void exitTimeBasedWindow(AntlrSQLParser::TimeBasedWindowContext *ctx) = 0;

  virtual void enterCountBasedWindow(AntlrSQLParser::CountBasedWindowContext *ctx) = 0;
  virtual void exitCountBasedWindow(AntlrSQLParser::CountBasedWindowContext *ctx) = 0;

  virtual void enterThresholdBasedWindow(AntlrSQLParser::ThresholdBasedWindowContext *ctx) = 0;
  virtual void exitThresholdBasedWindow(AntlrSQLParser::ThresholdBasedWindowContext *ctx) = 0;

  virtual void enterTumblingWindow(AntlrSQLParser::TumblingWindowContext *ctx) = 0;
  virtual void exitTumblingWindow(AntlrSQLParser::TumblingWindowContext *ctx) = 0;

  virtual void enterSlidingWindow(AntlrSQLParser::SlidingWindowContext *ctx) = 0;
  virtual void exitSlidingWindow(AntlrSQLParser::SlidingWindowContext *ctx) = 0;

  virtual void enterCountBasedTumbling(AntlrSQLParser::CountBasedTumblingContext *ctx) = 0;
  virtual void exitCountBasedTumbling(AntlrSQLParser::CountBasedTumblingContext *ctx) = 0;

  virtual void enterThresholdWindow(AntlrSQLParser::ThresholdWindowContext *ctx) = 0;
  virtual void exitThresholdWindow(AntlrSQLParser::ThresholdWindowContext *ctx) = 0;

  virtual void enterConditionParameter(AntlrSQLParser::ConditionParameterContext *ctx) = 0;
  virtual void exitConditionParameter(AntlrSQLParser::ConditionParameterContext *ctx) = 0;

  virtual void enterThresholdMinSizeParameter(AntlrSQLParser::ThresholdMinSizeParameterContext *ctx) = 0;
  virtual void exitThresholdMinSizeParameter(AntlrSQLParser::ThresholdMinSizeParameterContext *ctx) = 0;

  virtual void enterSizeParameter(AntlrSQLParser::SizeParameterContext *ctx) = 0;
  virtual void exitSizeParameter(AntlrSQLParser::SizeParameterContext *ctx) = 0;

  virtual void enterAdvancebyParameter(AntlrSQLParser::AdvancebyParameterContext *ctx) = 0;
  virtual void exitAdvancebyParameter(AntlrSQLParser::AdvancebyParameterContext *ctx) = 0;

  virtual void enterTimeUnit(AntlrSQLParser::TimeUnitContext *ctx) = 0;
  virtual void exitTimeUnit(AntlrSQLParser::TimeUnitContext *ctx) = 0;

  virtual void enterTimestampParameter(AntlrSQLParser::TimestampParameterContext *ctx) = 0;
  virtual void exitTimestampParameter(AntlrSQLParser::TimestampParameterContext *ctx) = 0;

  virtual void enterFunctionName(AntlrSQLParser::FunctionNameContext *ctx) = 0;
  virtual void exitFunctionName(AntlrSQLParser::FunctionNameContext *ctx) = 0;

  virtual void enterSinkClause(AntlrSQLParser::SinkClauseContext *ctx) = 0;
  virtual void exitSinkClause(AntlrSQLParser::SinkClauseContext *ctx) = 0;

  virtual void enterSink(AntlrSQLParser::SinkContext *ctx) = 0;
  virtual void exitSink(AntlrSQLParser::SinkContext *ctx) = 0;

  virtual void enterInlineSink(AntlrSQLParser::InlineSinkContext *ctx) = 0;
  virtual void exitInlineSink(AntlrSQLParser::InlineSinkContext *ctx) = 0;

  virtual void enterNullNotnull(AntlrSQLParser::NullNotnullContext *ctx) = 0;
  virtual void exitNullNotnull(AntlrSQLParser::NullNotnullContext *ctx) = 0;

  virtual void enterStreamName(AntlrSQLParser::StreamNameContext *ctx) = 0;
  virtual void exitStreamName(AntlrSQLParser::StreamNameContext *ctx) = 0;

  virtual void enterFileFormat(AntlrSQLParser::FileFormatContext *ctx) = 0;
  virtual void exitFileFormat(AntlrSQLParser::FileFormatContext *ctx) = 0;

  virtual void enterSortItem(AntlrSQLParser::SortItemContext *ctx) = 0;
  virtual void exitSortItem(AntlrSQLParser::SortItemContext *ctx) = 0;

  virtual void enterPredicate(AntlrSQLParser::PredicateContext *ctx) = 0;
  virtual void exitPredicate(AntlrSQLParser::PredicateContext *ctx) = 0;

  virtual void enterValueExpressionDefault(AntlrSQLParser::ValueExpressionDefaultContext *ctx) = 0;
  virtual void exitValueExpressionDefault(AntlrSQLParser::ValueExpressionDefaultContext *ctx) = 0;

  virtual void enterComparison(AntlrSQLParser::ComparisonContext *ctx) = 0;
  virtual void exitComparison(AntlrSQLParser::ComparisonContext *ctx) = 0;

  virtual void enterFunctionCall(AntlrSQLParser::FunctionCallContext *ctx) = 0;
  virtual void exitFunctionCall(AntlrSQLParser::FunctionCallContext *ctx) = 0;

  virtual void enterArithmeticBinary(AntlrSQLParser::ArithmeticBinaryContext *ctx) = 0;
  virtual void exitArithmeticBinary(AntlrSQLParser::ArithmeticBinaryContext *ctx) = 0;

  virtual void enterArithmeticUnary(AntlrSQLParser::ArithmeticUnaryContext *ctx) = 0;
  virtual void exitArithmeticUnary(AntlrSQLParser::ArithmeticUnaryContext *ctx) = 0;

  virtual void enterComparisonOperator(AntlrSQLParser::ComparisonOperatorContext *ctx) = 0;
  virtual void exitComparisonOperator(AntlrSQLParser::ComparisonOperatorContext *ctx) = 0;

  virtual void enterHint(AntlrSQLParser::HintContext *ctx) = 0;
  virtual void exitHint(AntlrSQLParser::HintContext *ctx) = 0;

  virtual void enterHintStatement(AntlrSQLParser::HintStatementContext *ctx) = 0;
  virtual void exitHintStatement(AntlrSQLParser::HintStatementContext *ctx) = 0;

  virtual void enterDereference(AntlrSQLParser::DereferenceContext *ctx) = 0;
  virtual void exitDereference(AntlrSQLParser::DereferenceContext *ctx) = 0;

  virtual void enterConstantDefault(AntlrSQLParser::ConstantDefaultContext *ctx) = 0;
  virtual void exitConstantDefault(AntlrSQLParser::ConstantDefaultContext *ctx) = 0;

  virtual void enterColumnReference(AntlrSQLParser::ColumnReferenceContext *ctx) = 0;
  virtual void exitColumnReference(AntlrSQLParser::ColumnReferenceContext *ctx) = 0;

  virtual void enterRowConstructor(AntlrSQLParser::RowConstructorContext *ctx) = 0;
  virtual void exitRowConstructor(AntlrSQLParser::RowConstructorContext *ctx) = 0;

  virtual void enterParenthesizedExpression(AntlrSQLParser::ParenthesizedExpressionContext *ctx) = 0;
  virtual void exitParenthesizedExpression(AntlrSQLParser::ParenthesizedExpressionContext *ctx) = 0;

  virtual void enterStar(AntlrSQLParser::StarContext *ctx) = 0;
  virtual void exitStar(AntlrSQLParser::StarContext *ctx) = 0;

  virtual void enterSubqueryExpression(AntlrSQLParser::SubqueryExpressionContext *ctx) = 0;
  virtual void exitSubqueryExpression(AntlrSQLParser::SubqueryExpressionContext *ctx) = 0;

  virtual void enterQualifiedName(AntlrSQLParser::QualifiedNameContext *ctx) = 0;
  virtual void exitQualifiedName(AntlrSQLParser::QualifiedNameContext *ctx) = 0;

  virtual void enterIntegerLiteral(AntlrSQLParser::IntegerLiteralContext *ctx) = 0;
  virtual void exitIntegerLiteral(AntlrSQLParser::IntegerLiteralContext *ctx) = 0;

  virtual void enterFloatLiteral(AntlrSQLParser::FloatLiteralContext *ctx) = 0;
  virtual void exitFloatLiteral(AntlrSQLParser::FloatLiteralContext *ctx) = 0;

  virtual void enterUnsignedIntegerLiteral(AntlrSQLParser::UnsignedIntegerLiteralContext *ctx) = 0;
  virtual void exitUnsignedIntegerLiteral(AntlrSQLParser::UnsignedIntegerLiteralContext *ctx) = 0;

  virtual void enterSignedIntegerLiteral(AntlrSQLParser::SignedIntegerLiteralContext *ctx) = 0;
  virtual void exitSignedIntegerLiteral(AntlrSQLParser::SignedIntegerLiteralContext *ctx) = 0;

  virtual void enterNullLiteral(AntlrSQLParser::NullLiteralContext *ctx) = 0;
  virtual void exitNullLiteral(AntlrSQLParser::NullLiteralContext *ctx) = 0;

  virtual void enterTypeConstructor(AntlrSQLParser::TypeConstructorContext *ctx) = 0;
  virtual void exitTypeConstructor(AntlrSQLParser::TypeConstructorContext *ctx) = 0;

  virtual void enterNumericLiteral(AntlrSQLParser::NumericLiteralContext *ctx) = 0;
  virtual void exitNumericLiteral(AntlrSQLParser::NumericLiteralContext *ctx) = 0;

  virtual void enterBooleanLiteral(AntlrSQLParser::BooleanLiteralContext *ctx) = 0;
  virtual void exitBooleanLiteral(AntlrSQLParser::BooleanLiteralContext *ctx) = 0;

  virtual void enterStringLiteral(AntlrSQLParser::StringLiteralContext *ctx) = 0;
  virtual void exitStringLiteral(AntlrSQLParser::StringLiteralContext *ctx) = 0;

  virtual void enterBooleanValue(AntlrSQLParser::BooleanValueContext *ctx) = 0;
  virtual void exitBooleanValue(AntlrSQLParser::BooleanValueContext *ctx) = 0;


};

