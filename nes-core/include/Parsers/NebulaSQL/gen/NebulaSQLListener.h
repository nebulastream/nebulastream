
// Generated from /Users/sumalvico/CLionProjects/nebulastream/nes-core/src/Parsers/NebulaSQL/gen/NebulaSQL.g4 by ANTLR 4.12.0

#pragma once


#include "antlr4-runtime.h"
#include "NebulaSQLParser.h"


/**
 * This interface defines an abstract listener for a parse tree produced by NebulaSQLParser.
 */
class  NebulaSQLListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterSingleStatement(NebulaSQLParser::SingleStatementContext *ctx) = 0;
  virtual void exitSingleStatement(NebulaSQLParser::SingleStatementContext *ctx) = 0;

  virtual void enterStatement(NebulaSQLParser::StatementContext *ctx) = 0;
  virtual void exitStatement(NebulaSQLParser::StatementContext *ctx) = 0;

  virtual void enterQuery(NebulaSQLParser::QueryContext *ctx) = 0;
  virtual void exitQuery(NebulaSQLParser::QueryContext *ctx) = 0;

  virtual void enterQueryOrganization(NebulaSQLParser::QueryOrganizationContext *ctx) = 0;
  virtual void exitQueryOrganization(NebulaSQLParser::QueryOrganizationContext *ctx) = 0;

  virtual void enterPrimaryQuery(NebulaSQLParser::PrimaryQueryContext *ctx) = 0;
  virtual void exitPrimaryQuery(NebulaSQLParser::PrimaryQueryContext *ctx) = 0;

  virtual void enterSetOperation(NebulaSQLParser::SetOperationContext *ctx) = 0;
  virtual void exitSetOperation(NebulaSQLParser::SetOperationContext *ctx) = 0;

  virtual void enterQueryPrimaryDefault(NebulaSQLParser::QueryPrimaryDefaultContext *ctx) = 0;
  virtual void exitQueryPrimaryDefault(NebulaSQLParser::QueryPrimaryDefaultContext *ctx) = 0;

  virtual void enterFromStmt(NebulaSQLParser::FromStmtContext *ctx) = 0;
  virtual void exitFromStmt(NebulaSQLParser::FromStmtContext *ctx) = 0;

  virtual void enterTable(NebulaSQLParser::TableContext *ctx) = 0;
  virtual void exitTable(NebulaSQLParser::TableContext *ctx) = 0;

  virtual void enterInlineTableDefault1(NebulaSQLParser::InlineTableDefault1Context *ctx) = 0;
  virtual void exitInlineTableDefault1(NebulaSQLParser::InlineTableDefault1Context *ctx) = 0;

  virtual void enterSubquery(NebulaSQLParser::SubqueryContext *ctx) = 0;
  virtual void exitSubquery(NebulaSQLParser::SubqueryContext *ctx) = 0;

  virtual void enterQuerySpecification(NebulaSQLParser::QuerySpecificationContext *ctx) = 0;
  virtual void exitQuerySpecification(NebulaSQLParser::QuerySpecificationContext *ctx) = 0;

  virtual void enterFromClause(NebulaSQLParser::FromClauseContext *ctx) = 0;
  virtual void exitFromClause(NebulaSQLParser::FromClauseContext *ctx) = 0;

  virtual void enterRelation(NebulaSQLParser::RelationContext *ctx) = 0;
  virtual void exitRelation(NebulaSQLParser::RelationContext *ctx) = 0;

  virtual void enterJoinRelation(NebulaSQLParser::JoinRelationContext *ctx) = 0;
  virtual void exitJoinRelation(NebulaSQLParser::JoinRelationContext *ctx) = 0;

  virtual void enterJoinType(NebulaSQLParser::JoinTypeContext *ctx) = 0;
  virtual void exitJoinType(NebulaSQLParser::JoinTypeContext *ctx) = 0;

  virtual void enterJoinCriteria(NebulaSQLParser::JoinCriteriaContext *ctx) = 0;
  virtual void exitJoinCriteria(NebulaSQLParser::JoinCriteriaContext *ctx) = 0;

  virtual void enterTableName(NebulaSQLParser::TableNameContext *ctx) = 0;
  virtual void exitTableName(NebulaSQLParser::TableNameContext *ctx) = 0;

  virtual void enterAliasedQuery(NebulaSQLParser::AliasedQueryContext *ctx) = 0;
  virtual void exitAliasedQuery(NebulaSQLParser::AliasedQueryContext *ctx) = 0;

  virtual void enterAliasedRelation(NebulaSQLParser::AliasedRelationContext *ctx) = 0;
  virtual void exitAliasedRelation(NebulaSQLParser::AliasedRelationContext *ctx) = 0;

  virtual void enterInlineTableDefault2(NebulaSQLParser::InlineTableDefault2Context *ctx) = 0;
  virtual void exitInlineTableDefault2(NebulaSQLParser::InlineTableDefault2Context *ctx) = 0;

  virtual void enterTableValuedFunction(NebulaSQLParser::TableValuedFunctionContext *ctx) = 0;
  virtual void exitTableValuedFunction(NebulaSQLParser::TableValuedFunctionContext *ctx) = 0;

  virtual void enterFunctionTable(NebulaSQLParser::FunctionTableContext *ctx) = 0;
  virtual void exitFunctionTable(NebulaSQLParser::FunctionTableContext *ctx) = 0;

  virtual void enterFromStatement(NebulaSQLParser::FromStatementContext *ctx) = 0;
  virtual void exitFromStatement(NebulaSQLParser::FromStatementContext *ctx) = 0;

  virtual void enterFromStatementBody(NebulaSQLParser::FromStatementBodyContext *ctx) = 0;
  virtual void exitFromStatementBody(NebulaSQLParser::FromStatementBodyContext *ctx) = 0;

  virtual void enterSelectClause(NebulaSQLParser::SelectClauseContext *ctx) = 0;
  virtual void exitSelectClause(NebulaSQLParser::SelectClauseContext *ctx) = 0;

  virtual void enterWhereClause(NebulaSQLParser::WhereClauseContext *ctx) = 0;
  virtual void exitWhereClause(NebulaSQLParser::WhereClauseContext *ctx) = 0;

  virtual void enterHavingClause(NebulaSQLParser::HavingClauseContext *ctx) = 0;
  virtual void exitHavingClause(NebulaSQLParser::HavingClauseContext *ctx) = 0;

  virtual void enterInlineTable(NebulaSQLParser::InlineTableContext *ctx) = 0;
  virtual void exitInlineTable(NebulaSQLParser::InlineTableContext *ctx) = 0;

  virtual void enterTableAlias(NebulaSQLParser::TableAliasContext *ctx) = 0;
  virtual void exitTableAlias(NebulaSQLParser::TableAliasContext *ctx) = 0;

  virtual void enterMultipartIdentifierList(NebulaSQLParser::MultipartIdentifierListContext *ctx) = 0;
  virtual void exitMultipartIdentifierList(NebulaSQLParser::MultipartIdentifierListContext *ctx) = 0;

  virtual void enterMultipartIdentifier(NebulaSQLParser::MultipartIdentifierContext *ctx) = 0;
  virtual void exitMultipartIdentifier(NebulaSQLParser::MultipartIdentifierContext *ctx) = 0;

  virtual void enterNamedExpression(NebulaSQLParser::NamedExpressionContext *ctx) = 0;
  virtual void exitNamedExpression(NebulaSQLParser::NamedExpressionContext *ctx) = 0;

  virtual void enterIdentifier(NebulaSQLParser::IdentifierContext *ctx) = 0;
  virtual void exitIdentifier(NebulaSQLParser::IdentifierContext *ctx) = 0;

  virtual void enterUnquotedIdentifier(NebulaSQLParser::UnquotedIdentifierContext *ctx) = 0;
  virtual void exitUnquotedIdentifier(NebulaSQLParser::UnquotedIdentifierContext *ctx) = 0;

  virtual void enterQuotedIdentifierAlternative(NebulaSQLParser::QuotedIdentifierAlternativeContext *ctx) = 0;
  virtual void exitQuotedIdentifierAlternative(NebulaSQLParser::QuotedIdentifierAlternativeContext *ctx) = 0;

  virtual void enterQuotedIdentifier(NebulaSQLParser::QuotedIdentifierContext *ctx) = 0;
  virtual void exitQuotedIdentifier(NebulaSQLParser::QuotedIdentifierContext *ctx) = 0;

  virtual void enterIdentifierList(NebulaSQLParser::IdentifierListContext *ctx) = 0;
  virtual void exitIdentifierList(NebulaSQLParser::IdentifierListContext *ctx) = 0;

  virtual void enterIdentifierSeq(NebulaSQLParser::IdentifierSeqContext *ctx) = 0;
  virtual void exitIdentifierSeq(NebulaSQLParser::IdentifierSeqContext *ctx) = 0;

  virtual void enterErrorCapturingIdentifier(NebulaSQLParser::ErrorCapturingIdentifierContext *ctx) = 0;
  virtual void exitErrorCapturingIdentifier(NebulaSQLParser::ErrorCapturingIdentifierContext *ctx) = 0;

  virtual void enterErrorIdent(NebulaSQLParser::ErrorIdentContext *ctx) = 0;
  virtual void exitErrorIdent(NebulaSQLParser::ErrorIdentContext *ctx) = 0;

  virtual void enterRealIdent(NebulaSQLParser::RealIdentContext *ctx) = 0;
  virtual void exitRealIdent(NebulaSQLParser::RealIdentContext *ctx) = 0;

  virtual void enterNamedExpressionSeq(NebulaSQLParser::NamedExpressionSeqContext *ctx) = 0;
  virtual void exitNamedExpressionSeq(NebulaSQLParser::NamedExpressionSeqContext *ctx) = 0;

  virtual void enterExpression(NebulaSQLParser::ExpressionContext *ctx) = 0;
  virtual void exitExpression(NebulaSQLParser::ExpressionContext *ctx) = 0;

  virtual void enterLogicalNot(NebulaSQLParser::LogicalNotContext *ctx) = 0;
  virtual void exitLogicalNot(NebulaSQLParser::LogicalNotContext *ctx) = 0;

  virtual void enterPredicated(NebulaSQLParser::PredicatedContext *ctx) = 0;
  virtual void exitPredicated(NebulaSQLParser::PredicatedContext *ctx) = 0;

  virtual void enterExists(NebulaSQLParser::ExistsContext *ctx) = 0;
  virtual void exitExists(NebulaSQLParser::ExistsContext *ctx) = 0;

  virtual void enterLogicalBinary(NebulaSQLParser::LogicalBinaryContext *ctx) = 0;
  virtual void exitLogicalBinary(NebulaSQLParser::LogicalBinaryContext *ctx) = 0;

  virtual void enterWindowedAggregationClause(NebulaSQLParser::WindowedAggregationClauseContext *ctx) = 0;
  virtual void exitWindowedAggregationClause(NebulaSQLParser::WindowedAggregationClauseContext *ctx) = 0;

  virtual void enterAggregationClause(NebulaSQLParser::AggregationClauseContext *ctx) = 0;
  virtual void exitAggregationClause(NebulaSQLParser::AggregationClauseContext *ctx) = 0;

  virtual void enterGroupingSet(NebulaSQLParser::GroupingSetContext *ctx) = 0;
  virtual void exitGroupingSet(NebulaSQLParser::GroupingSetContext *ctx) = 0;

  virtual void enterWindowClause(NebulaSQLParser::WindowClauseContext *ctx) = 0;
  virtual void exitWindowClause(NebulaSQLParser::WindowClauseContext *ctx) = 0;

  virtual void enterWatermarkClause(NebulaSQLParser::WatermarkClauseContext *ctx) = 0;
  virtual void exitWatermarkClause(NebulaSQLParser::WatermarkClauseContext *ctx) = 0;

  virtual void enterWatermarkParameters(NebulaSQLParser::WatermarkParametersContext *ctx) = 0;
  virtual void exitWatermarkParameters(NebulaSQLParser::WatermarkParametersContext *ctx) = 0;

  virtual void enterTimeBasedWindow(NebulaSQLParser::TimeBasedWindowContext *ctx) = 0;
  virtual void exitTimeBasedWindow(NebulaSQLParser::TimeBasedWindowContext *ctx) = 0;

  virtual void enterCountBasedWindow(NebulaSQLParser::CountBasedWindowContext *ctx) = 0;
  virtual void exitCountBasedWindow(NebulaSQLParser::CountBasedWindowContext *ctx) = 0;

  virtual void enterTumblingWindow(NebulaSQLParser::TumblingWindowContext *ctx) = 0;
  virtual void exitTumblingWindow(NebulaSQLParser::TumblingWindowContext *ctx) = 0;

  virtual void enterSlidingWindow(NebulaSQLParser::SlidingWindowContext *ctx) = 0;
  virtual void exitSlidingWindow(NebulaSQLParser::SlidingWindowContext *ctx) = 0;

  virtual void enterCountBasedTumbling(NebulaSQLParser::CountBasedTumblingContext *ctx) = 0;
  virtual void exitCountBasedTumbling(NebulaSQLParser::CountBasedTumblingContext *ctx) = 0;

  virtual void enterSizeParameter(NebulaSQLParser::SizeParameterContext *ctx) = 0;
  virtual void exitSizeParameter(NebulaSQLParser::SizeParameterContext *ctx) = 0;

  virtual void enterAdvancebyParameter(NebulaSQLParser::AdvancebyParameterContext *ctx) = 0;
  virtual void exitAdvancebyParameter(NebulaSQLParser::AdvancebyParameterContext *ctx) = 0;

  virtual void enterTimeUnit(NebulaSQLParser::TimeUnitContext *ctx) = 0;
  virtual void exitTimeUnit(NebulaSQLParser::TimeUnitContext *ctx) = 0;

  virtual void enterTimestampParameter(NebulaSQLParser::TimestampParameterContext *ctx) = 0;
  virtual void exitTimestampParameter(NebulaSQLParser::TimestampParameterContext *ctx) = 0;

  virtual void enterFunctionName(NebulaSQLParser::FunctionNameContext *ctx) = 0;
  virtual void exitFunctionName(NebulaSQLParser::FunctionNameContext *ctx) = 0;

  virtual void enterSinkClause(NebulaSQLParser::SinkClauseContext *ctx) = 0;
  virtual void exitSinkClause(NebulaSQLParser::SinkClauseContext *ctx) = 0;

  virtual void enterSinkType(NebulaSQLParser::SinkTypeContext *ctx) = 0;
  virtual void exitSinkType(NebulaSQLParser::SinkTypeContext *ctx) = 0;

  virtual void enterSinkTypeZMQ(NebulaSQLParser::SinkTypeZMQContext *ctx) = 0;
  virtual void exitSinkTypeZMQ(NebulaSQLParser::SinkTypeZMQContext *ctx) = 0;

  virtual void enterNullNotnull(NebulaSQLParser::NullNotnullContext *ctx) = 0;
  virtual void exitNullNotnull(NebulaSQLParser::NullNotnullContext *ctx) = 0;

  virtual void enterZmqKeyword(NebulaSQLParser::ZmqKeywordContext *ctx) = 0;
  virtual void exitZmqKeyword(NebulaSQLParser::ZmqKeywordContext *ctx) = 0;

  virtual void enterStreamName(NebulaSQLParser::StreamNameContext *ctx) = 0;
  virtual void exitStreamName(NebulaSQLParser::StreamNameContext *ctx) = 0;

  virtual void enterHost(NebulaSQLParser::HostContext *ctx) = 0;
  virtual void exitHost(NebulaSQLParser::HostContext *ctx) = 0;

  virtual void enterPort(NebulaSQLParser::PortContext *ctx) = 0;
  virtual void exitPort(NebulaSQLParser::PortContext *ctx) = 0;

  virtual void enterSinkTypeKafka(NebulaSQLParser::SinkTypeKafkaContext *ctx) = 0;
  virtual void exitSinkTypeKafka(NebulaSQLParser::SinkTypeKafkaContext *ctx) = 0;

  virtual void enterKafkaKeyword(NebulaSQLParser::KafkaKeywordContext *ctx) = 0;
  virtual void exitKafkaKeyword(NebulaSQLParser::KafkaKeywordContext *ctx) = 0;

  virtual void enterKafkaBroker(NebulaSQLParser::KafkaBrokerContext *ctx) = 0;
  virtual void exitKafkaBroker(NebulaSQLParser::KafkaBrokerContext *ctx) = 0;

  virtual void enterKafkaTopic(NebulaSQLParser::KafkaTopicContext *ctx) = 0;
  virtual void exitKafkaTopic(NebulaSQLParser::KafkaTopicContext *ctx) = 0;

  virtual void enterKafkaProducerTimout(NebulaSQLParser::KafkaProducerTimoutContext *ctx) = 0;
  virtual void exitKafkaProducerTimout(NebulaSQLParser::KafkaProducerTimoutContext *ctx) = 0;

  virtual void enterSinkTypeFile(NebulaSQLParser::SinkTypeFileContext *ctx) = 0;
  virtual void exitSinkTypeFile(NebulaSQLParser::SinkTypeFileContext *ctx) = 0;

  virtual void enterFileFormat(NebulaSQLParser::FileFormatContext *ctx) = 0;
  virtual void exitFileFormat(NebulaSQLParser::FileFormatContext *ctx) = 0;

  virtual void enterSinkTypeMQTT(NebulaSQLParser::SinkTypeMQTTContext *ctx) = 0;
  virtual void exitSinkTypeMQTT(NebulaSQLParser::SinkTypeMQTTContext *ctx) = 0;

  virtual void enterQos(NebulaSQLParser::QosContext *ctx) = 0;
  virtual void exitQos(NebulaSQLParser::QosContext *ctx) = 0;

  virtual void enterSinkTypeOPC(NebulaSQLParser::SinkTypeOPCContext *ctx) = 0;
  virtual void exitSinkTypeOPC(NebulaSQLParser::SinkTypeOPCContext *ctx) = 0;

  virtual void enterSinkTypePrint(NebulaSQLParser::SinkTypePrintContext *ctx) = 0;
  virtual void exitSinkTypePrint(NebulaSQLParser::SinkTypePrintContext *ctx) = 0;

  virtual void enterSortItem(NebulaSQLParser::SortItemContext *ctx) = 0;
  virtual void exitSortItem(NebulaSQLParser::SortItemContext *ctx) = 0;

  virtual void enterPredicate(NebulaSQLParser::PredicateContext *ctx) = 0;
  virtual void exitPredicate(NebulaSQLParser::PredicateContext *ctx) = 0;

  virtual void enterValueExpressionDefault(NebulaSQLParser::ValueExpressionDefaultContext *ctx) = 0;
  virtual void exitValueExpressionDefault(NebulaSQLParser::ValueExpressionDefaultContext *ctx) = 0;

  virtual void enterComparison(NebulaSQLParser::ComparisonContext *ctx) = 0;
  virtual void exitComparison(NebulaSQLParser::ComparisonContext *ctx) = 0;

  virtual void enterArithmeticBinary(NebulaSQLParser::ArithmeticBinaryContext *ctx) = 0;
  virtual void exitArithmeticBinary(NebulaSQLParser::ArithmeticBinaryContext *ctx) = 0;

  virtual void enterArithmeticUnary(NebulaSQLParser::ArithmeticUnaryContext *ctx) = 0;
  virtual void exitArithmeticUnary(NebulaSQLParser::ArithmeticUnaryContext *ctx) = 0;

  virtual void enterComparisonOperator(NebulaSQLParser::ComparisonOperatorContext *ctx) = 0;
  virtual void exitComparisonOperator(NebulaSQLParser::ComparisonOperatorContext *ctx) = 0;

  virtual void enterHint(NebulaSQLParser::HintContext *ctx) = 0;
  virtual void exitHint(NebulaSQLParser::HintContext *ctx) = 0;

  virtual void enterHintStatement(NebulaSQLParser::HintStatementContext *ctx) = 0;
  virtual void exitHintStatement(NebulaSQLParser::HintStatementContext *ctx) = 0;

  virtual void enterDereference(NebulaSQLParser::DereferenceContext *ctx) = 0;
  virtual void exitDereference(NebulaSQLParser::DereferenceContext *ctx) = 0;

  virtual void enterConstantDefault(NebulaSQLParser::ConstantDefaultContext *ctx) = 0;
  virtual void exitConstantDefault(NebulaSQLParser::ConstantDefaultContext *ctx) = 0;

  virtual void enterColumnReference(NebulaSQLParser::ColumnReferenceContext *ctx) = 0;
  virtual void exitColumnReference(NebulaSQLParser::ColumnReferenceContext *ctx) = 0;

  virtual void enterRowConstructor(NebulaSQLParser::RowConstructorContext *ctx) = 0;
  virtual void exitRowConstructor(NebulaSQLParser::RowConstructorContext *ctx) = 0;

  virtual void enterParenthesizedExpression(NebulaSQLParser::ParenthesizedExpressionContext *ctx) = 0;
  virtual void exitParenthesizedExpression(NebulaSQLParser::ParenthesizedExpressionContext *ctx) = 0;

  virtual void enterStar(NebulaSQLParser::StarContext *ctx) = 0;
  virtual void exitStar(NebulaSQLParser::StarContext *ctx) = 0;

  virtual void enterFunctionCall(NebulaSQLParser::FunctionCallContext *ctx) = 0;
  virtual void exitFunctionCall(NebulaSQLParser::FunctionCallContext *ctx) = 0;

  virtual void enterSubqueryExpression(NebulaSQLParser::SubqueryExpressionContext *ctx) = 0;
  virtual void exitSubqueryExpression(NebulaSQLParser::SubqueryExpressionContext *ctx) = 0;

  virtual void enterQualifiedName(NebulaSQLParser::QualifiedNameContext *ctx) = 0;
  virtual void exitQualifiedName(NebulaSQLParser::QualifiedNameContext *ctx) = 0;

  virtual void enterExponentLiteral(NebulaSQLParser::ExponentLiteralContext *ctx) = 0;
  virtual void exitExponentLiteral(NebulaSQLParser::ExponentLiteralContext *ctx) = 0;

  virtual void enterDecimalLiteral(NebulaSQLParser::DecimalLiteralContext *ctx) = 0;
  virtual void exitDecimalLiteral(NebulaSQLParser::DecimalLiteralContext *ctx) = 0;

  virtual void enterLegacyDecimalLiteral(NebulaSQLParser::LegacyDecimalLiteralContext *ctx) = 0;
  virtual void exitLegacyDecimalLiteral(NebulaSQLParser::LegacyDecimalLiteralContext *ctx) = 0;

  virtual void enterIntegerLiteral(NebulaSQLParser::IntegerLiteralContext *ctx) = 0;
  virtual void exitIntegerLiteral(NebulaSQLParser::IntegerLiteralContext *ctx) = 0;

  virtual void enterBigIntLiteral(NebulaSQLParser::BigIntLiteralContext *ctx) = 0;
  virtual void exitBigIntLiteral(NebulaSQLParser::BigIntLiteralContext *ctx) = 0;

  virtual void enterSmallIntLiteral(NebulaSQLParser::SmallIntLiteralContext *ctx) = 0;
  virtual void exitSmallIntLiteral(NebulaSQLParser::SmallIntLiteralContext *ctx) = 0;

  virtual void enterTinyIntLiteral(NebulaSQLParser::TinyIntLiteralContext *ctx) = 0;
  virtual void exitTinyIntLiteral(NebulaSQLParser::TinyIntLiteralContext *ctx) = 0;

  virtual void enterDoubleLiteral(NebulaSQLParser::DoubleLiteralContext *ctx) = 0;
  virtual void exitDoubleLiteral(NebulaSQLParser::DoubleLiteralContext *ctx) = 0;

  virtual void enterFloatLiteral(NebulaSQLParser::FloatLiteralContext *ctx) = 0;
  virtual void exitFloatLiteral(NebulaSQLParser::FloatLiteralContext *ctx) = 0;

  virtual void enterBigDecimalLiteral(NebulaSQLParser::BigDecimalLiteralContext *ctx) = 0;
  virtual void exitBigDecimalLiteral(NebulaSQLParser::BigDecimalLiteralContext *ctx) = 0;

  virtual void enterNullLiteral(NebulaSQLParser::NullLiteralContext *ctx) = 0;
  virtual void exitNullLiteral(NebulaSQLParser::NullLiteralContext *ctx) = 0;

  virtual void enterTypeConstructor(NebulaSQLParser::TypeConstructorContext *ctx) = 0;
  virtual void exitTypeConstructor(NebulaSQLParser::TypeConstructorContext *ctx) = 0;

  virtual void enterNumericLiteral(NebulaSQLParser::NumericLiteralContext *ctx) = 0;
  virtual void exitNumericLiteral(NebulaSQLParser::NumericLiteralContext *ctx) = 0;

  virtual void enterBooleanLiteral(NebulaSQLParser::BooleanLiteralContext *ctx) = 0;
  virtual void exitBooleanLiteral(NebulaSQLParser::BooleanLiteralContext *ctx) = 0;

  virtual void enterStringLiteral(NebulaSQLParser::StringLiteralContext *ctx) = 0;
  virtual void exitStringLiteral(NebulaSQLParser::StringLiteralContext *ctx) = 0;

  virtual void enterBooleanValue(NebulaSQLParser::BooleanValueContext *ctx) = 0;
  virtual void exitBooleanValue(NebulaSQLParser::BooleanValueContext *ctx) = 0;

  virtual void enterStrictNonReserved(NebulaSQLParser::StrictNonReservedContext *ctx) = 0;
  virtual void exitStrictNonReserved(NebulaSQLParser::StrictNonReservedContext *ctx) = 0;

  virtual void enterAnsiNonReserved(NebulaSQLParser::AnsiNonReservedContext *ctx) = 0;
  virtual void exitAnsiNonReserved(NebulaSQLParser::AnsiNonReservedContext *ctx) = 0;

  virtual void enterNonReserved(NebulaSQLParser::NonReservedContext *ctx) = 0;
  virtual void exitNonReserved(NebulaSQLParser::NonReservedContext *ctx) = 0;


};

