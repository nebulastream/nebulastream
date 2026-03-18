
// Generated from /IdeaProjects/nebulastream2/nes-sql-parser/AntlrSQL.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "AntlrSQLListener.h"


/**
 * This class provides an empty implementation of AntlrSQLListener,
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
class  AntlrSQLBaseListener : public AntlrSQLListener {
public:

  virtual void enterSingleStatement(AntlrSQLParser::SingleStatementContext * /*ctx*/) override { }
  virtual void exitSingleStatement(AntlrSQLParser::SingleStatementContext * /*ctx*/) override { }

  virtual void enterTerminatedStatement(AntlrSQLParser::TerminatedStatementContext * /*ctx*/) override { }
  virtual void exitTerminatedStatement(AntlrSQLParser::TerminatedStatementContext * /*ctx*/) override { }

  virtual void enterMultipleStatements(AntlrSQLParser::MultipleStatementsContext * /*ctx*/) override { }
  virtual void exitMultipleStatements(AntlrSQLParser::MultipleStatementsContext * /*ctx*/) override { }

  virtual void enterStatement(AntlrSQLParser::StatementContext * /*ctx*/) override { }
  virtual void exitStatement(AntlrSQLParser::StatementContext * /*ctx*/) override { }

  virtual void enterExplainStatement(AntlrSQLParser::ExplainStatementContext * /*ctx*/) override { }
  virtual void exitExplainStatement(AntlrSQLParser::ExplainStatementContext * /*ctx*/) override { }

  virtual void enterCreateStatement(AntlrSQLParser::CreateStatementContext * /*ctx*/) override { }
  virtual void exitCreateStatement(AntlrSQLParser::CreateStatementContext * /*ctx*/) override { }

  virtual void enterCreateDefinition(AntlrSQLParser::CreateDefinitionContext * /*ctx*/) override { }
  virtual void exitCreateDefinition(AntlrSQLParser::CreateDefinitionContext * /*ctx*/) override { }

  virtual void enterCreateLogicalSourceDefinition(AntlrSQLParser::CreateLogicalSourceDefinitionContext * /*ctx*/) override { }
  virtual void exitCreateLogicalSourceDefinition(AntlrSQLParser::CreateLogicalSourceDefinitionContext * /*ctx*/) override { }

  virtual void enterCreatePhysicalSourceDefinition(AntlrSQLParser::CreatePhysicalSourceDefinitionContext * /*ctx*/) override { }
  virtual void exitCreatePhysicalSourceDefinition(AntlrSQLParser::CreatePhysicalSourceDefinitionContext * /*ctx*/) override { }

  virtual void enterOptionsClause(AntlrSQLParser::OptionsClauseContext * /*ctx*/) override { }
  virtual void exitOptionsClause(AntlrSQLParser::OptionsClauseContext * /*ctx*/) override { }

  virtual void enterCreateSinkDefinition(AntlrSQLParser::CreateSinkDefinitionContext * /*ctx*/) override { }
  virtual void exitCreateSinkDefinition(AntlrSQLParser::CreateSinkDefinitionContext * /*ctx*/) override { }

  virtual void enterCreateWorkerDefinition(AntlrSQLParser::CreateWorkerDefinitionContext * /*ctx*/) override { }
  virtual void exitCreateWorkerDefinition(AntlrSQLParser::CreateWorkerDefinitionContext * /*ctx*/) override { }

  virtual void enterSchemaDefinition(AntlrSQLParser::SchemaDefinitionContext * /*ctx*/) override { }
  virtual void exitSchemaDefinition(AntlrSQLParser::SchemaDefinitionContext * /*ctx*/) override { }

  virtual void enterColumnDefinition(AntlrSQLParser::ColumnDefinitionContext * /*ctx*/) override { }
  virtual void exitColumnDefinition(AntlrSQLParser::ColumnDefinitionContext * /*ctx*/) override { }

  virtual void enterTypeDefinition(AntlrSQLParser::TypeDefinitionContext * /*ctx*/) override { }
  virtual void exitTypeDefinition(AntlrSQLParser::TypeDefinitionContext * /*ctx*/) override { }

  virtual void enterNullableDefinition(AntlrSQLParser::NullableDefinitionContext * /*ctx*/) override { }
  virtual void exitNullableDefinition(AntlrSQLParser::NullableDefinitionContext * /*ctx*/) override { }

  virtual void enterFromQuery(AntlrSQLParser::FromQueryContext * /*ctx*/) override { }
  virtual void exitFromQuery(AntlrSQLParser::FromQueryContext * /*ctx*/) override { }

  virtual void enterDropStatement(AntlrSQLParser::DropStatementContext * /*ctx*/) override { }
  virtual void exitDropStatement(AntlrSQLParser::DropStatementContext * /*ctx*/) override { }

  virtual void enterDropSubject(AntlrSQLParser::DropSubjectContext * /*ctx*/) override { }
  virtual void exitDropSubject(AntlrSQLParser::DropSubjectContext * /*ctx*/) override { }

  virtual void enterDropQuery(AntlrSQLParser::DropQueryContext * /*ctx*/) override { }
  virtual void exitDropQuery(AntlrSQLParser::DropQueryContext * /*ctx*/) override { }

  virtual void enterDropSource(AntlrSQLParser::DropSourceContext * /*ctx*/) override { }
  virtual void exitDropSource(AntlrSQLParser::DropSourceContext * /*ctx*/) override { }

  virtual void enterDropLogicalSourceSubject(AntlrSQLParser::DropLogicalSourceSubjectContext * /*ctx*/) override { }
  virtual void exitDropLogicalSourceSubject(AntlrSQLParser::DropLogicalSourceSubjectContext * /*ctx*/) override { }

  virtual void enterDropPhysicalSourceSubject(AntlrSQLParser::DropPhysicalSourceSubjectContext * /*ctx*/) override { }
  virtual void exitDropPhysicalSourceSubject(AntlrSQLParser::DropPhysicalSourceSubjectContext * /*ctx*/) override { }

  virtual void enterDropWorker(AntlrSQLParser::DropWorkerContext * /*ctx*/) override { }
  virtual void exitDropWorker(AntlrSQLParser::DropWorkerContext * /*ctx*/) override { }

  virtual void enterDropSink(AntlrSQLParser::DropSinkContext * /*ctx*/) override { }
  virtual void exitDropSink(AntlrSQLParser::DropSinkContext * /*ctx*/) override { }

  virtual void enterDropFilter(AntlrSQLParser::DropFilterContext * /*ctx*/) override { }
  virtual void exitDropFilter(AntlrSQLParser::DropFilterContext * /*ctx*/) override { }

  virtual void enterShowStatement(AntlrSQLParser::ShowStatementContext * /*ctx*/) override { }
  virtual void exitShowStatement(AntlrSQLParser::ShowStatementContext * /*ctx*/) override { }

  virtual void enterShowFormat(AntlrSQLParser::ShowFormatContext * /*ctx*/) override { }
  virtual void exitShowFormat(AntlrSQLParser::ShowFormatContext * /*ctx*/) override { }

  virtual void enterShowQueriesSubject(AntlrSQLParser::ShowQueriesSubjectContext * /*ctx*/) override { }
  virtual void exitShowQueriesSubject(AntlrSQLParser::ShowQueriesSubjectContext * /*ctx*/) override { }

  virtual void enterShowLogicalSourcesSubject(AntlrSQLParser::ShowLogicalSourcesSubjectContext * /*ctx*/) override { }
  virtual void exitShowLogicalSourcesSubject(AntlrSQLParser::ShowLogicalSourcesSubjectContext * /*ctx*/) override { }

  virtual void enterShowPhysicalSourcesSubject(AntlrSQLParser::ShowPhysicalSourcesSubjectContext * /*ctx*/) override { }
  virtual void exitShowPhysicalSourcesSubject(AntlrSQLParser::ShowPhysicalSourcesSubjectContext * /*ctx*/) override { }

  virtual void enterShowSinksSubject(AntlrSQLParser::ShowSinksSubjectContext * /*ctx*/) override { }
  virtual void exitShowSinksSubject(AntlrSQLParser::ShowSinksSubjectContext * /*ctx*/) override { }

  virtual void enterShowFilter(AntlrSQLParser::ShowFilterContext * /*ctx*/) override { }
  virtual void exitShowFilter(AntlrSQLParser::ShowFilterContext * /*ctx*/) override { }

  virtual void enterQueryWithOptions(AntlrSQLParser::QueryWithOptionsContext * /*ctx*/) override { }
  virtual void exitQueryWithOptions(AntlrSQLParser::QueryWithOptionsContext * /*ctx*/) override { }

  virtual void enterQuery(AntlrSQLParser::QueryContext * /*ctx*/) override { }
  virtual void exitQuery(AntlrSQLParser::QueryContext * /*ctx*/) override { }

  virtual void enterQueryOrganization(AntlrSQLParser::QueryOrganizationContext * /*ctx*/) override { }
  virtual void exitQueryOrganization(AntlrSQLParser::QueryOrganizationContext * /*ctx*/) override { }

  virtual void enterPrimaryQuery(AntlrSQLParser::PrimaryQueryContext * /*ctx*/) override { }
  virtual void exitPrimaryQuery(AntlrSQLParser::PrimaryQueryContext * /*ctx*/) override { }

  virtual void enterSetOperation(AntlrSQLParser::SetOperationContext * /*ctx*/) override { }
  virtual void exitSetOperation(AntlrSQLParser::SetOperationContext * /*ctx*/) override { }

  virtual void enterQueryPrimaryDefault(AntlrSQLParser::QueryPrimaryDefaultContext * /*ctx*/) override { }
  virtual void exitQueryPrimaryDefault(AntlrSQLParser::QueryPrimaryDefaultContext * /*ctx*/) override { }

  virtual void enterFromStmt(AntlrSQLParser::FromStmtContext * /*ctx*/) override { }
  virtual void exitFromStmt(AntlrSQLParser::FromStmtContext * /*ctx*/) override { }

  virtual void enterTable(AntlrSQLParser::TableContext * /*ctx*/) override { }
  virtual void exitTable(AntlrSQLParser::TableContext * /*ctx*/) override { }

  virtual void enterInlineTableDefault1(AntlrSQLParser::InlineTableDefault1Context * /*ctx*/) override { }
  virtual void exitInlineTableDefault1(AntlrSQLParser::InlineTableDefault1Context * /*ctx*/) override { }

  virtual void enterSubquery(AntlrSQLParser::SubqueryContext * /*ctx*/) override { }
  virtual void exitSubquery(AntlrSQLParser::SubqueryContext * /*ctx*/) override { }

  virtual void enterQuerySpecification(AntlrSQLParser::QuerySpecificationContext * /*ctx*/) override { }
  virtual void exitQuerySpecification(AntlrSQLParser::QuerySpecificationContext * /*ctx*/) override { }

  virtual void enterFromClause(AntlrSQLParser::FromClauseContext * /*ctx*/) override { }
  virtual void exitFromClause(AntlrSQLParser::FromClauseContext * /*ctx*/) override { }

  virtual void enterRelation(AntlrSQLParser::RelationContext * /*ctx*/) override { }
  virtual void exitRelation(AntlrSQLParser::RelationContext * /*ctx*/) override { }

  virtual void enterJoinRelation(AntlrSQLParser::JoinRelationContext * /*ctx*/) override { }
  virtual void exitJoinRelation(AntlrSQLParser::JoinRelationContext * /*ctx*/) override { }

  virtual void enterJoinType(AntlrSQLParser::JoinTypeContext * /*ctx*/) override { }
  virtual void exitJoinType(AntlrSQLParser::JoinTypeContext * /*ctx*/) override { }

  virtual void enterJoinCriteria(AntlrSQLParser::JoinCriteriaContext * /*ctx*/) override { }
  virtual void exitJoinCriteria(AntlrSQLParser::JoinCriteriaContext * /*ctx*/) override { }

  virtual void enterTableName(AntlrSQLParser::TableNameContext * /*ctx*/) override { }
  virtual void exitTableName(AntlrSQLParser::TableNameContext * /*ctx*/) override { }

  virtual void enterAliasedQuery(AntlrSQLParser::AliasedQueryContext * /*ctx*/) override { }
  virtual void exitAliasedQuery(AntlrSQLParser::AliasedQueryContext * /*ctx*/) override { }

  virtual void enterAliasedRelation(AntlrSQLParser::AliasedRelationContext * /*ctx*/) override { }
  virtual void exitAliasedRelation(AntlrSQLParser::AliasedRelationContext * /*ctx*/) override { }

  virtual void enterInlineTableDefault2(AntlrSQLParser::InlineTableDefault2Context * /*ctx*/) override { }
  virtual void exitInlineTableDefault2(AntlrSQLParser::InlineTableDefault2Context * /*ctx*/) override { }

  virtual void enterInlineDefinedSource(AntlrSQLParser::InlineDefinedSourceContext * /*ctx*/) override { }
  virtual void exitInlineDefinedSource(AntlrSQLParser::InlineDefinedSourceContext * /*ctx*/) override { }

  virtual void enterInlineSource(AntlrSQLParser::InlineSourceContext * /*ctx*/) override { }
  virtual void exitInlineSource(AntlrSQLParser::InlineSourceContext * /*ctx*/) override { }

  virtual void enterSchema(AntlrSQLParser::SchemaContext * /*ctx*/) override { }
  virtual void exitSchema(AntlrSQLParser::SchemaContext * /*ctx*/) override { }

  virtual void enterFromStatement(AntlrSQLParser::FromStatementContext * /*ctx*/) override { }
  virtual void exitFromStatement(AntlrSQLParser::FromStatementContext * /*ctx*/) override { }

  virtual void enterFromStatementBody(AntlrSQLParser::FromStatementBodyContext * /*ctx*/) override { }
  virtual void exitFromStatementBody(AntlrSQLParser::FromStatementBodyContext * /*ctx*/) override { }

  virtual void enterSelectClause(AntlrSQLParser::SelectClauseContext * /*ctx*/) override { }
  virtual void exitSelectClause(AntlrSQLParser::SelectClauseContext * /*ctx*/) override { }

  virtual void enterWhereClause(AntlrSQLParser::WhereClauseContext * /*ctx*/) override { }
  virtual void exitWhereClause(AntlrSQLParser::WhereClauseContext * /*ctx*/) override { }

  virtual void enterHavingClause(AntlrSQLParser::HavingClauseContext * /*ctx*/) override { }
  virtual void exitHavingClause(AntlrSQLParser::HavingClauseContext * /*ctx*/) override { }

  virtual void enterInlineTable(AntlrSQLParser::InlineTableContext * /*ctx*/) override { }
  virtual void exitInlineTable(AntlrSQLParser::InlineTableContext * /*ctx*/) override { }

  virtual void enterTableAlias(AntlrSQLParser::TableAliasContext * /*ctx*/) override { }
  virtual void exitTableAlias(AntlrSQLParser::TableAliasContext * /*ctx*/) override { }

  virtual void enterMultipartIdentifier(AntlrSQLParser::MultipartIdentifierContext * /*ctx*/) override { }
  virtual void exitMultipartIdentifier(AntlrSQLParser::MultipartIdentifierContext * /*ctx*/) override { }

  virtual void enterNamedConfigExpression(AntlrSQLParser::NamedConfigExpressionContext * /*ctx*/) override { }
  virtual void exitNamedConfigExpression(AntlrSQLParser::NamedConfigExpressionContext * /*ctx*/) override { }

  virtual void enterNamedExpression(AntlrSQLParser::NamedExpressionContext * /*ctx*/) override { }
  virtual void exitNamedExpression(AntlrSQLParser::NamedExpressionContext * /*ctx*/) override { }

  virtual void enterIdentifier(AntlrSQLParser::IdentifierContext * /*ctx*/) override { }
  virtual void exitIdentifier(AntlrSQLParser::IdentifierContext * /*ctx*/) override { }

  virtual void enterUnquotedIdentifier(AntlrSQLParser::UnquotedIdentifierContext * /*ctx*/) override { }
  virtual void exitUnquotedIdentifier(AntlrSQLParser::UnquotedIdentifierContext * /*ctx*/) override { }

  virtual void enterQuotedIdentifierAlternative(AntlrSQLParser::QuotedIdentifierAlternativeContext * /*ctx*/) override { }
  virtual void exitQuotedIdentifierAlternative(AntlrSQLParser::QuotedIdentifierAlternativeContext * /*ctx*/) override { }

  virtual void enterQuotedIdentifier(AntlrSQLParser::QuotedIdentifierContext * /*ctx*/) override { }
  virtual void exitQuotedIdentifier(AntlrSQLParser::QuotedIdentifierContext * /*ctx*/) override { }

  virtual void enterIdentifierChain(AntlrSQLParser::IdentifierChainContext * /*ctx*/) override { }
  virtual void exitIdentifierChain(AntlrSQLParser::IdentifierChainContext * /*ctx*/) override { }

  virtual void enterIdentifierList(AntlrSQLParser::IdentifierListContext * /*ctx*/) override { }
  virtual void exitIdentifierList(AntlrSQLParser::IdentifierListContext * /*ctx*/) override { }

  virtual void enterIdentifierSeq(AntlrSQLParser::IdentifierSeqContext * /*ctx*/) override { }
  virtual void exitIdentifierSeq(AntlrSQLParser::IdentifierSeqContext * /*ctx*/) override { }

  virtual void enterErrorCapturingIdentifier(AntlrSQLParser::ErrorCapturingIdentifierContext * /*ctx*/) override { }
  virtual void exitErrorCapturingIdentifier(AntlrSQLParser::ErrorCapturingIdentifierContext * /*ctx*/) override { }

  virtual void enterErrorIdent(AntlrSQLParser::ErrorIdentContext * /*ctx*/) override { }
  virtual void exitErrorIdent(AntlrSQLParser::ErrorIdentContext * /*ctx*/) override { }

  virtual void enterRealIdent(AntlrSQLParser::RealIdentContext * /*ctx*/) override { }
  virtual void exitRealIdent(AntlrSQLParser::RealIdentContext * /*ctx*/) override { }

  virtual void enterNamedConfigExpressionSeq(AntlrSQLParser::NamedConfigExpressionSeqContext * /*ctx*/) override { }
  virtual void exitNamedConfigExpressionSeq(AntlrSQLParser::NamedConfigExpressionSeqContext * /*ctx*/) override { }

  virtual void enterNamedExpressionSeq(AntlrSQLParser::NamedExpressionSeqContext * /*ctx*/) override { }
  virtual void exitNamedExpressionSeq(AntlrSQLParser::NamedExpressionSeqContext * /*ctx*/) override { }

  virtual void enterExpression(AntlrSQLParser::ExpressionContext * /*ctx*/) override { }
  virtual void exitExpression(AntlrSQLParser::ExpressionContext * /*ctx*/) override { }

  virtual void enterLogicalNot(AntlrSQLParser::LogicalNotContext * /*ctx*/) override { }
  virtual void exitLogicalNot(AntlrSQLParser::LogicalNotContext * /*ctx*/) override { }

  virtual void enterPredicated(AntlrSQLParser::PredicatedContext * /*ctx*/) override { }
  virtual void exitPredicated(AntlrSQLParser::PredicatedContext * /*ctx*/) override { }

  virtual void enterExists(AntlrSQLParser::ExistsContext * /*ctx*/) override { }
  virtual void exitExists(AntlrSQLParser::ExistsContext * /*ctx*/) override { }

  virtual void enterLogicalBinary(AntlrSQLParser::LogicalBinaryContext * /*ctx*/) override { }
  virtual void exitLogicalBinary(AntlrSQLParser::LogicalBinaryContext * /*ctx*/) override { }

  virtual void enterWindowedAggregationClause(AntlrSQLParser::WindowedAggregationClauseContext * /*ctx*/) override { }
  virtual void exitWindowedAggregationClause(AntlrSQLParser::WindowedAggregationClauseContext * /*ctx*/) override { }

  virtual void enterGroupByClause(AntlrSQLParser::GroupByClauseContext * /*ctx*/) override { }
  virtual void exitGroupByClause(AntlrSQLParser::GroupByClauseContext * /*ctx*/) override { }

  virtual void enterGroupingSet(AntlrSQLParser::GroupingSetContext * /*ctx*/) override { }
  virtual void exitGroupingSet(AntlrSQLParser::GroupingSetContext * /*ctx*/) override { }

  virtual void enterWindowClause(AntlrSQLParser::WindowClauseContext * /*ctx*/) override { }
  virtual void exitWindowClause(AntlrSQLParser::WindowClauseContext * /*ctx*/) override { }

  virtual void enterWatermarkClause(AntlrSQLParser::WatermarkClauseContext * /*ctx*/) override { }
  virtual void exitWatermarkClause(AntlrSQLParser::WatermarkClauseContext * /*ctx*/) override { }

  virtual void enterWatermarkParameters(AntlrSQLParser::WatermarkParametersContext * /*ctx*/) override { }
  virtual void exitWatermarkParameters(AntlrSQLParser::WatermarkParametersContext * /*ctx*/) override { }

  virtual void enterTimeBasedWindow(AntlrSQLParser::TimeBasedWindowContext * /*ctx*/) override { }
  virtual void exitTimeBasedWindow(AntlrSQLParser::TimeBasedWindowContext * /*ctx*/) override { }

  virtual void enterCountBasedWindow(AntlrSQLParser::CountBasedWindowContext * /*ctx*/) override { }
  virtual void exitCountBasedWindow(AntlrSQLParser::CountBasedWindowContext * /*ctx*/) override { }

  virtual void enterThresholdBasedWindow(AntlrSQLParser::ThresholdBasedWindowContext * /*ctx*/) override { }
  virtual void exitThresholdBasedWindow(AntlrSQLParser::ThresholdBasedWindowContext * /*ctx*/) override { }

  virtual void enterTumblingWindow(AntlrSQLParser::TumblingWindowContext * /*ctx*/) override { }
  virtual void exitTumblingWindow(AntlrSQLParser::TumblingWindowContext * /*ctx*/) override { }

  virtual void enterSlidingWindow(AntlrSQLParser::SlidingWindowContext * /*ctx*/) override { }
  virtual void exitSlidingWindow(AntlrSQLParser::SlidingWindowContext * /*ctx*/) override { }

  virtual void enterCountBasedTumbling(AntlrSQLParser::CountBasedTumblingContext * /*ctx*/) override { }
  virtual void exitCountBasedTumbling(AntlrSQLParser::CountBasedTumblingContext * /*ctx*/) override { }

  virtual void enterThresholdWindow(AntlrSQLParser::ThresholdWindowContext * /*ctx*/) override { }
  virtual void exitThresholdWindow(AntlrSQLParser::ThresholdWindowContext * /*ctx*/) override { }

  virtual void enterConditionParameter(AntlrSQLParser::ConditionParameterContext * /*ctx*/) override { }
  virtual void exitConditionParameter(AntlrSQLParser::ConditionParameterContext * /*ctx*/) override { }

  virtual void enterThresholdMinSizeParameter(AntlrSQLParser::ThresholdMinSizeParameterContext * /*ctx*/) override { }
  virtual void exitThresholdMinSizeParameter(AntlrSQLParser::ThresholdMinSizeParameterContext * /*ctx*/) override { }

  virtual void enterSizeParameter(AntlrSQLParser::SizeParameterContext * /*ctx*/) override { }
  virtual void exitSizeParameter(AntlrSQLParser::SizeParameterContext * /*ctx*/) override { }

  virtual void enterAdvancebyParameter(AntlrSQLParser::AdvancebyParameterContext * /*ctx*/) override { }
  virtual void exitAdvancebyParameter(AntlrSQLParser::AdvancebyParameterContext * /*ctx*/) override { }

  virtual void enterTimeUnit(AntlrSQLParser::TimeUnitContext * /*ctx*/) override { }
  virtual void exitTimeUnit(AntlrSQLParser::TimeUnitContext * /*ctx*/) override { }

  virtual void enterTimestampParameter(AntlrSQLParser::TimestampParameterContext * /*ctx*/) override { }
  virtual void exitTimestampParameter(AntlrSQLParser::TimestampParameterContext * /*ctx*/) override { }

  virtual void enterFunctionName(AntlrSQLParser::FunctionNameContext * /*ctx*/) override { }
  virtual void exitFunctionName(AntlrSQLParser::FunctionNameContext * /*ctx*/) override { }

  virtual void enterSinkClause(AntlrSQLParser::SinkClauseContext * /*ctx*/) override { }
  virtual void exitSinkClause(AntlrSQLParser::SinkClauseContext * /*ctx*/) override { }

  virtual void enterSink(AntlrSQLParser::SinkContext * /*ctx*/) override { }
  virtual void exitSink(AntlrSQLParser::SinkContext * /*ctx*/) override { }

  virtual void enterInlineSink(AntlrSQLParser::InlineSinkContext * /*ctx*/) override { }
  virtual void exitInlineSink(AntlrSQLParser::InlineSinkContext * /*ctx*/) override { }

  virtual void enterNullNotnull(AntlrSQLParser::NullNotnullContext * /*ctx*/) override { }
  virtual void exitNullNotnull(AntlrSQLParser::NullNotnullContext * /*ctx*/) override { }

  virtual void enterStreamName(AntlrSQLParser::StreamNameContext * /*ctx*/) override { }
  virtual void exitStreamName(AntlrSQLParser::StreamNameContext * /*ctx*/) override { }

  virtual void enterFileFormat(AntlrSQLParser::FileFormatContext * /*ctx*/) override { }
  virtual void exitFileFormat(AntlrSQLParser::FileFormatContext * /*ctx*/) override { }

  virtual void enterSortItem(AntlrSQLParser::SortItemContext * /*ctx*/) override { }
  virtual void exitSortItem(AntlrSQLParser::SortItemContext * /*ctx*/) override { }

  virtual void enterPredicate(AntlrSQLParser::PredicateContext * /*ctx*/) override { }
  virtual void exitPredicate(AntlrSQLParser::PredicateContext * /*ctx*/) override { }

  virtual void enterValueExpressionDefault(AntlrSQLParser::ValueExpressionDefaultContext * /*ctx*/) override { }
  virtual void exitValueExpressionDefault(AntlrSQLParser::ValueExpressionDefaultContext * /*ctx*/) override { }

  virtual void enterComparison(AntlrSQLParser::ComparisonContext * /*ctx*/) override { }
  virtual void exitComparison(AntlrSQLParser::ComparisonContext * /*ctx*/) override { }

  virtual void enterFunctionCall(AntlrSQLParser::FunctionCallContext * /*ctx*/) override { }
  virtual void exitFunctionCall(AntlrSQLParser::FunctionCallContext * /*ctx*/) override { }

  virtual void enterArithmeticBinary(AntlrSQLParser::ArithmeticBinaryContext * /*ctx*/) override { }
  virtual void exitArithmeticBinary(AntlrSQLParser::ArithmeticBinaryContext * /*ctx*/) override { }

  virtual void enterArithmeticUnary(AntlrSQLParser::ArithmeticUnaryContext * /*ctx*/) override { }
  virtual void exitArithmeticUnary(AntlrSQLParser::ArithmeticUnaryContext * /*ctx*/) override { }

  virtual void enterComparisonOperator(AntlrSQLParser::ComparisonOperatorContext * /*ctx*/) override { }
  virtual void exitComparisonOperator(AntlrSQLParser::ComparisonOperatorContext * /*ctx*/) override { }

  virtual void enterHint(AntlrSQLParser::HintContext * /*ctx*/) override { }
  virtual void exitHint(AntlrSQLParser::HintContext * /*ctx*/) override { }

  virtual void enterHintStatement(AntlrSQLParser::HintStatementContext * /*ctx*/) override { }
  virtual void exitHintStatement(AntlrSQLParser::HintStatementContext * /*ctx*/) override { }

  virtual void enterDereference(AntlrSQLParser::DereferenceContext * /*ctx*/) override { }
  virtual void exitDereference(AntlrSQLParser::DereferenceContext * /*ctx*/) override { }

  virtual void enterConstantDefault(AntlrSQLParser::ConstantDefaultContext * /*ctx*/) override { }
  virtual void exitConstantDefault(AntlrSQLParser::ConstantDefaultContext * /*ctx*/) override { }

  virtual void enterColumnReference(AntlrSQLParser::ColumnReferenceContext * /*ctx*/) override { }
  virtual void exitColumnReference(AntlrSQLParser::ColumnReferenceContext * /*ctx*/) override { }

  virtual void enterRowConstructor(AntlrSQLParser::RowConstructorContext * /*ctx*/) override { }
  virtual void exitRowConstructor(AntlrSQLParser::RowConstructorContext * /*ctx*/) override { }

  virtual void enterParenthesizedExpression(AntlrSQLParser::ParenthesizedExpressionContext * /*ctx*/) override { }
  virtual void exitParenthesizedExpression(AntlrSQLParser::ParenthesizedExpressionContext * /*ctx*/) override { }

  virtual void enterStar(AntlrSQLParser::StarContext * /*ctx*/) override { }
  virtual void exitStar(AntlrSQLParser::StarContext * /*ctx*/) override { }

  virtual void enterSubqueryExpression(AntlrSQLParser::SubqueryExpressionContext * /*ctx*/) override { }
  virtual void exitSubqueryExpression(AntlrSQLParser::SubqueryExpressionContext * /*ctx*/) override { }

  virtual void enterQualifiedName(AntlrSQLParser::QualifiedNameContext * /*ctx*/) override { }
  virtual void exitQualifiedName(AntlrSQLParser::QualifiedNameContext * /*ctx*/) override { }

  virtual void enterIntegerLiteral(AntlrSQLParser::IntegerLiteralContext * /*ctx*/) override { }
  virtual void exitIntegerLiteral(AntlrSQLParser::IntegerLiteralContext * /*ctx*/) override { }

  virtual void enterFloatLiteral(AntlrSQLParser::FloatLiteralContext * /*ctx*/) override { }
  virtual void exitFloatLiteral(AntlrSQLParser::FloatLiteralContext * /*ctx*/) override { }

  virtual void enterUnsignedIntegerLiteral(AntlrSQLParser::UnsignedIntegerLiteralContext * /*ctx*/) override { }
  virtual void exitUnsignedIntegerLiteral(AntlrSQLParser::UnsignedIntegerLiteralContext * /*ctx*/) override { }

  virtual void enterSignedIntegerLiteral(AntlrSQLParser::SignedIntegerLiteralContext * /*ctx*/) override { }
  virtual void exitSignedIntegerLiteral(AntlrSQLParser::SignedIntegerLiteralContext * /*ctx*/) override { }

  virtual void enterNullLiteral(AntlrSQLParser::NullLiteralContext * /*ctx*/) override { }
  virtual void exitNullLiteral(AntlrSQLParser::NullLiteralContext * /*ctx*/) override { }

  virtual void enterTypeConstructor(AntlrSQLParser::TypeConstructorContext * /*ctx*/) override { }
  virtual void exitTypeConstructor(AntlrSQLParser::TypeConstructorContext * /*ctx*/) override { }

  virtual void enterNumericLiteral(AntlrSQLParser::NumericLiteralContext * /*ctx*/) override { }
  virtual void exitNumericLiteral(AntlrSQLParser::NumericLiteralContext * /*ctx*/) override { }

  virtual void enterBooleanLiteral(AntlrSQLParser::BooleanLiteralContext * /*ctx*/) override { }
  virtual void exitBooleanLiteral(AntlrSQLParser::BooleanLiteralContext * /*ctx*/) override { }

  virtual void enterStringLiteral(AntlrSQLParser::StringLiteralContext * /*ctx*/) override { }
  virtual void exitStringLiteral(AntlrSQLParser::StringLiteralContext * /*ctx*/) override { }

  virtual void enterBooleanValue(AntlrSQLParser::BooleanValueContext * /*ctx*/) override { }
  virtual void exitBooleanValue(AntlrSQLParser::BooleanValueContext * /*ctx*/) override { }


  virtual void enterEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void exitEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void visitTerminal(antlr4::tree::TerminalNode * /*node*/) override { }
  virtual void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override { }

};

