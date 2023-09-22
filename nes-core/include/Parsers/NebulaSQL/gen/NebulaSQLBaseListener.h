
// Generated from CLionProjects/nebulastream/nes-core/src/Parsers/NebulaSQL/gen/NebulaSQL.g4 by ANTLR 4.9.2

#pragma once

#include <Parsers/NebulaSQL/gen/NebulaSQLListener.h>
#include <antlr4-runtime.h>

namespace NES::Parsers {

/**
 * This class provides an empty implementation of NebulaSQLListener,
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
class  NebulaSQLBaseListener : public NebulaSQLListener {
public:

  virtual void enterSingleStatement(NebulaSQLParser::SingleStatementContext * /*ctx*/) override { }
  virtual void exitSingleStatement(NebulaSQLParser::SingleStatementContext * /*ctx*/) override { }

  virtual void enterStatement(NebulaSQLParser::StatementContext * /*ctx*/) override { }
  virtual void exitStatement(NebulaSQLParser::StatementContext * /*ctx*/) override { }

  virtual void enterQuery(NebulaSQLParser::QueryContext * /*ctx*/) override { }
  virtual void exitQuery(NebulaSQLParser::QueryContext * /*ctx*/) override { }

  virtual void enterQueryOrganization(NebulaSQLParser::QueryOrganizationContext * /*ctx*/) override { }
  virtual void exitQueryOrganization(NebulaSQLParser::QueryOrganizationContext * /*ctx*/) override { }

  virtual void enterPrimaryQuery(NebulaSQLParser::PrimaryQueryContext * /*ctx*/) override { }
  virtual void exitPrimaryQuery(NebulaSQLParser::PrimaryQueryContext * /*ctx*/) override { }

  virtual void enterSetOperation(NebulaSQLParser::SetOperationContext * /*ctx*/) override { }
  virtual void exitSetOperation(NebulaSQLParser::SetOperationContext * /*ctx*/) override { }

  virtual void enterQueryPrimaryDefault(NebulaSQLParser::QueryPrimaryDefaultContext * /*ctx*/) override { }
  virtual void exitQueryPrimaryDefault(NebulaSQLParser::QueryPrimaryDefaultContext * /*ctx*/) override { }

  virtual void enterFromStmt(NebulaSQLParser::FromStmtContext * /*ctx*/) override { }
  virtual void exitFromStmt(NebulaSQLParser::FromStmtContext * /*ctx*/) override { }

  virtual void enterTable(NebulaSQLParser::TableContext * /*ctx*/) override { }
  virtual void exitTable(NebulaSQLParser::TableContext * /*ctx*/) override { }

  virtual void enterInlineTableDefault1(NebulaSQLParser::InlineTableDefault1Context * /*ctx*/) override { }
  virtual void exitInlineTableDefault1(NebulaSQLParser::InlineTableDefault1Context * /*ctx*/) override { }

  virtual void enterSubquery(NebulaSQLParser::SubqueryContext * /*ctx*/) override { }
  virtual void exitSubquery(NebulaSQLParser::SubqueryContext * /*ctx*/) override { }

  virtual void enterQuerySpecification(NebulaSQLParser::QuerySpecificationContext * /*ctx*/) override { }
  virtual void exitQuerySpecification(NebulaSQLParser::QuerySpecificationContext * /*ctx*/) override { }

  virtual void enterFromClause(NebulaSQLParser::FromClauseContext * /*ctx*/) override { }
  virtual void exitFromClause(NebulaSQLParser::FromClauseContext * /*ctx*/) override { }

  virtual void enterRelation(NebulaSQLParser::RelationContext * /*ctx*/) override { }
  virtual void exitRelation(NebulaSQLParser::RelationContext * /*ctx*/) override { }

  virtual void enterJoinRelation(NebulaSQLParser::JoinRelationContext * /*ctx*/) override { }
  virtual void exitJoinRelation(NebulaSQLParser::JoinRelationContext * /*ctx*/) override { }

  virtual void enterJoinType(NebulaSQLParser::JoinTypeContext * /*ctx*/) override { }
  virtual void exitJoinType(NebulaSQLParser::JoinTypeContext * /*ctx*/) override { }

  virtual void enterJoinCriteria(NebulaSQLParser::JoinCriteriaContext * /*ctx*/) override { }
  virtual void exitJoinCriteria(NebulaSQLParser::JoinCriteriaContext * /*ctx*/) override { }

  virtual void enterTableName(NebulaSQLParser::TableNameContext * /*ctx*/) override { }
  virtual void exitTableName(NebulaSQLParser::TableNameContext * /*ctx*/) override { }

  virtual void enterAliasedQuery(NebulaSQLParser::AliasedQueryContext * /*ctx*/) override { }
  virtual void exitAliasedQuery(NebulaSQLParser::AliasedQueryContext * /*ctx*/) override { }

  virtual void enterAliasedRelation(NebulaSQLParser::AliasedRelationContext * /*ctx*/) override { }
  virtual void exitAliasedRelation(NebulaSQLParser::AliasedRelationContext * /*ctx*/) override { }

  virtual void enterInlineTableDefault2(NebulaSQLParser::InlineTableDefault2Context * /*ctx*/) override { }
  virtual void exitInlineTableDefault2(NebulaSQLParser::InlineTableDefault2Context * /*ctx*/) override { }

  virtual void enterTableValuedFunction(NebulaSQLParser::TableValuedFunctionContext * /*ctx*/) override { }
  virtual void exitTableValuedFunction(NebulaSQLParser::TableValuedFunctionContext * /*ctx*/) override { }

  virtual void enterFunctionTable(NebulaSQLParser::FunctionTableContext * /*ctx*/) override { }
  virtual void exitFunctionTable(NebulaSQLParser::FunctionTableContext * /*ctx*/) override { }

  virtual void enterFromStatement(NebulaSQLParser::FromStatementContext * /*ctx*/) override { }
  virtual void exitFromStatement(NebulaSQLParser::FromStatementContext * /*ctx*/) override { }

  virtual void enterFromStatementBody(NebulaSQLParser::FromStatementBodyContext * /*ctx*/) override { }
  virtual void exitFromStatementBody(NebulaSQLParser::FromStatementBodyContext * /*ctx*/) override { }

  virtual void enterSelectClause(NebulaSQLParser::SelectClauseContext * /*ctx*/) override { }
  virtual void exitSelectClause(NebulaSQLParser::SelectClauseContext * /*ctx*/) override { }

  virtual void enterWhereClause(NebulaSQLParser::WhereClauseContext * /*ctx*/) override { }
  virtual void exitWhereClause(NebulaSQLParser::WhereClauseContext * /*ctx*/) override { }

  virtual void enterHavingClause(NebulaSQLParser::HavingClauseContext * /*ctx*/) override { }
  virtual void exitHavingClause(NebulaSQLParser::HavingClauseContext * /*ctx*/) override { }

  virtual void enterInlineTable(NebulaSQLParser::InlineTableContext * /*ctx*/) override { }
  virtual void exitInlineTable(NebulaSQLParser::InlineTableContext * /*ctx*/) override { }

  virtual void enterTableAlias(NebulaSQLParser::TableAliasContext * /*ctx*/) override { }
  virtual void exitTableAlias(NebulaSQLParser::TableAliasContext * /*ctx*/) override { }

  virtual void enterMultipartIdentifierList(NebulaSQLParser::MultipartIdentifierListContext * /*ctx*/) override { }
  virtual void exitMultipartIdentifierList(NebulaSQLParser::MultipartIdentifierListContext * /*ctx*/) override { }

  virtual void enterMultipartIdentifier(NebulaSQLParser::MultipartIdentifierContext * /*ctx*/) override { }
  virtual void exitMultipartIdentifier(NebulaSQLParser::MultipartIdentifierContext * /*ctx*/) override { }

  virtual void enterNamedExpression(NebulaSQLParser::NamedExpressionContext * /*ctx*/) override { }
  virtual void exitNamedExpression(NebulaSQLParser::NamedExpressionContext * /*ctx*/) override { }

  virtual void enterIdentifier(NebulaSQLParser::IdentifierContext * /*ctx*/) override { }
  virtual void exitIdentifier(NebulaSQLParser::IdentifierContext * /*ctx*/) override { }

  virtual void enterUnquotedIdentifier(NebulaSQLParser::UnquotedIdentifierContext * /*ctx*/) override { }
  virtual void exitUnquotedIdentifier(NebulaSQLParser::UnquotedIdentifierContext * /*ctx*/) override { }

  virtual void enterQuotedIdentifierAlternative(NebulaSQLParser::QuotedIdentifierAlternativeContext * /*ctx*/) override { }
  virtual void exitQuotedIdentifierAlternative(NebulaSQLParser::QuotedIdentifierAlternativeContext * /*ctx*/) override { }

  virtual void enterQuotedIdentifier(NebulaSQLParser::QuotedIdentifierContext * /*ctx*/) override { }
  virtual void exitQuotedIdentifier(NebulaSQLParser::QuotedIdentifierContext * /*ctx*/) override { }

  virtual void enterIdentifierList(NebulaSQLParser::IdentifierListContext * /*ctx*/) override { }
  virtual void exitIdentifierList(NebulaSQLParser::IdentifierListContext * /*ctx*/) override { }

  virtual void enterIdentifierSeq(NebulaSQLParser::IdentifierSeqContext * /*ctx*/) override { }
  virtual void exitIdentifierSeq(NebulaSQLParser::IdentifierSeqContext * /*ctx*/) override { }

  virtual void enterErrorCapturingIdentifier(NebulaSQLParser::ErrorCapturingIdentifierContext * /*ctx*/) override { }
  virtual void exitErrorCapturingIdentifier(NebulaSQLParser::ErrorCapturingIdentifierContext * /*ctx*/) override { }

  virtual void enterErrorIdent(NebulaSQLParser::ErrorIdentContext * /*ctx*/) override { }
  virtual void exitErrorIdent(NebulaSQLParser::ErrorIdentContext * /*ctx*/) override { }

  virtual void enterRealIdent(NebulaSQLParser::RealIdentContext * /*ctx*/) override { }
  virtual void exitRealIdent(NebulaSQLParser::RealIdentContext * /*ctx*/) override { }

  virtual void enterNamedExpressionSeq(NebulaSQLParser::NamedExpressionSeqContext * /*ctx*/) override { }
  virtual void exitNamedExpressionSeq(NebulaSQLParser::NamedExpressionSeqContext * /*ctx*/) override { }

  virtual void enterExpression(NebulaSQLParser::ExpressionContext * /*ctx*/) override { }
  virtual void exitExpression(NebulaSQLParser::ExpressionContext * /*ctx*/) override { }

  virtual void enterLogicalNot(NebulaSQLParser::LogicalNotContext * /*ctx*/) override { }
  virtual void exitLogicalNot(NebulaSQLParser::LogicalNotContext * /*ctx*/) override { }

  virtual void enterPredicated(NebulaSQLParser::PredicatedContext * /*ctx*/) override { }
  virtual void exitPredicated(NebulaSQLParser::PredicatedContext * /*ctx*/) override { }

  virtual void enterExists(NebulaSQLParser::ExistsContext * /*ctx*/) override { }
  virtual void exitExists(NebulaSQLParser::ExistsContext * /*ctx*/) override { }

  virtual void enterLogicalBinary(NebulaSQLParser::LogicalBinaryContext * /*ctx*/) override { }
  virtual void exitLogicalBinary(NebulaSQLParser::LogicalBinaryContext * /*ctx*/) override { }

  virtual void enterWindowedAggregationClause(NebulaSQLParser::WindowedAggregationClauseContext * /*ctx*/) override { }
  virtual void exitWindowedAggregationClause(NebulaSQLParser::WindowedAggregationClauseContext * /*ctx*/) override { }

  virtual void enterAggregationClause(NebulaSQLParser::AggregationClauseContext * /*ctx*/) override { }
  virtual void exitAggregationClause(NebulaSQLParser::AggregationClauseContext * /*ctx*/) override { }

  virtual void enterGroupingSet(NebulaSQLParser::GroupingSetContext * /*ctx*/) override { }
  virtual void exitGroupingSet(NebulaSQLParser::GroupingSetContext * /*ctx*/) override { }

  virtual void enterWindowClause(NebulaSQLParser::WindowClauseContext * /*ctx*/) override { }
  virtual void exitWindowClause(NebulaSQLParser::WindowClauseContext * /*ctx*/) override { }

  virtual void enterWatermarkClause(NebulaSQLParser::WatermarkClauseContext * /*ctx*/) override { }
  virtual void exitWatermarkClause(NebulaSQLParser::WatermarkClauseContext * /*ctx*/) override { }

  virtual void enterWatermarkParameters(NebulaSQLParser::WatermarkParametersContext * /*ctx*/) override { }
  virtual void exitWatermarkParameters(NebulaSQLParser::WatermarkParametersContext * /*ctx*/) override { }

  virtual void enterTimeBasedWindow(NebulaSQLParser::TimeBasedWindowContext * /*ctx*/) override { }
  virtual void exitTimeBasedWindow(NebulaSQLParser::TimeBasedWindowContext * /*ctx*/) override { }

  virtual void enterCountBasedWindow(NebulaSQLParser::CountBasedWindowContext * /*ctx*/) override { }
  virtual void exitCountBasedWindow(NebulaSQLParser::CountBasedWindowContext * /*ctx*/) override { }

  virtual void enterTumblingWindow(NebulaSQLParser::TumblingWindowContext * /*ctx*/) override { }
  virtual void exitTumblingWindow(NebulaSQLParser::TumblingWindowContext * /*ctx*/) override { }

  virtual void enterSlidingWindow(NebulaSQLParser::SlidingWindowContext * /*ctx*/) override { }
  virtual void exitSlidingWindow(NebulaSQLParser::SlidingWindowContext * /*ctx*/) override { }

  virtual void enterCountBasedTumbling(NebulaSQLParser::CountBasedTumblingContext * /*ctx*/) override { }
  virtual void exitCountBasedTumbling(NebulaSQLParser::CountBasedTumblingContext * /*ctx*/) override { }

  virtual void enterSizeParameter(NebulaSQLParser::SizeParameterContext * /*ctx*/) override { }
  virtual void exitSizeParameter(NebulaSQLParser::SizeParameterContext * /*ctx*/) override { }

  virtual void enterAdvancebyParameter(NebulaSQLParser::AdvancebyParameterContext * /*ctx*/) override { }
  virtual void exitAdvancebyParameter(NebulaSQLParser::AdvancebyParameterContext * /*ctx*/) override { }

  virtual void enterTimeUnit(NebulaSQLParser::TimeUnitContext * /*ctx*/) override { }
  virtual void exitTimeUnit(NebulaSQLParser::TimeUnitContext * /*ctx*/) override { }

  virtual void enterTimestampParameter(NebulaSQLParser::TimestampParameterContext * /*ctx*/) override { }
  virtual void exitTimestampParameter(NebulaSQLParser::TimestampParameterContext * /*ctx*/) override { }

  virtual void enterFunctionName(NebulaSQLParser::FunctionNameContext * /*ctx*/) override { }
  virtual void exitFunctionName(NebulaSQLParser::FunctionNameContext * /*ctx*/) override { }

  virtual void enterSinkClause(NebulaSQLParser::SinkClauseContext * /*ctx*/) override { }
  virtual void exitSinkClause(NebulaSQLParser::SinkClauseContext * /*ctx*/) override { }

  virtual void enterSinkType(NebulaSQLParser::SinkTypeContext * /*ctx*/) override { }
  virtual void exitSinkType(NebulaSQLParser::SinkTypeContext * /*ctx*/) override { }

  virtual void enterSinkTypeZMQ(NebulaSQLParser::SinkTypeZMQContext * /*ctx*/) override { }
  virtual void exitSinkTypeZMQ(NebulaSQLParser::SinkTypeZMQContext * /*ctx*/) override { }

  virtual void enterNullNotnull(NebulaSQLParser::NullNotnullContext * /*ctx*/) override { }
  virtual void exitNullNotnull(NebulaSQLParser::NullNotnullContext * /*ctx*/) override { }

  virtual void enterZmqKeyword(NebulaSQLParser::ZmqKeywordContext * /*ctx*/) override { }
  virtual void exitZmqKeyword(NebulaSQLParser::ZmqKeywordContext * /*ctx*/) override { }

  virtual void enterStreamName(NebulaSQLParser::StreamNameContext * /*ctx*/) override { }
  virtual void exitStreamName(NebulaSQLParser::StreamNameContext * /*ctx*/) override { }

  virtual void enterHost(NebulaSQLParser::HostContext * /*ctx*/) override { }
  virtual void exitHost(NebulaSQLParser::HostContext * /*ctx*/) override { }

  virtual void enterPort(NebulaSQLParser::PortContext * /*ctx*/) override { }
  virtual void exitPort(NebulaSQLParser::PortContext * /*ctx*/) override { }

  virtual void enterSinkTypeKafka(NebulaSQLParser::SinkTypeKafkaContext * /*ctx*/) override { }
  virtual void exitSinkTypeKafka(NebulaSQLParser::SinkTypeKafkaContext * /*ctx*/) override { }

  virtual void enterKafkaKeyword(NebulaSQLParser::KafkaKeywordContext * /*ctx*/) override { }
  virtual void exitKafkaKeyword(NebulaSQLParser::KafkaKeywordContext * /*ctx*/) override { }

  virtual void enterKafkaBroker(NebulaSQLParser::KafkaBrokerContext * /*ctx*/) override { }
  virtual void exitKafkaBroker(NebulaSQLParser::KafkaBrokerContext * /*ctx*/) override { }

  virtual void enterKafkaTopic(NebulaSQLParser::KafkaTopicContext * /*ctx*/) override { }
  virtual void exitKafkaTopic(NebulaSQLParser::KafkaTopicContext * /*ctx*/) override { }

  virtual void enterKafkaProducerTimout(NebulaSQLParser::KafkaProducerTimoutContext * /*ctx*/) override { }
  virtual void exitKafkaProducerTimout(NebulaSQLParser::KafkaProducerTimoutContext * /*ctx*/) override { }

  virtual void enterSinkTypeFile(NebulaSQLParser::SinkTypeFileContext * /*ctx*/) override { }
  virtual void exitSinkTypeFile(NebulaSQLParser::SinkTypeFileContext * /*ctx*/) override { }

  virtual void enterFileFormat(NebulaSQLParser::FileFormatContext * /*ctx*/) override { }
  virtual void exitFileFormat(NebulaSQLParser::FileFormatContext * /*ctx*/) override { }

  virtual void enterSinkTypeMQTT(NebulaSQLParser::SinkTypeMQTTContext * /*ctx*/) override { }
  virtual void exitSinkTypeMQTT(NebulaSQLParser::SinkTypeMQTTContext * /*ctx*/) override { }

  virtual void enterQos(NebulaSQLParser::QosContext * /*ctx*/) override { }
  virtual void exitQos(NebulaSQLParser::QosContext * /*ctx*/) override { }

  virtual void enterSinkTypeOPC(NebulaSQLParser::SinkTypeOPCContext * /*ctx*/) override { }
  virtual void exitSinkTypeOPC(NebulaSQLParser::SinkTypeOPCContext * /*ctx*/) override { }

  virtual void enterSinkTypePrint(NebulaSQLParser::SinkTypePrintContext * /*ctx*/) override { }
  virtual void exitSinkTypePrint(NebulaSQLParser::SinkTypePrintContext * /*ctx*/) override { }

  virtual void enterSortItem(NebulaSQLParser::SortItemContext * /*ctx*/) override { }
  virtual void exitSortItem(NebulaSQLParser::SortItemContext * /*ctx*/) override { }

  virtual void enterPredicate(NebulaSQLParser::PredicateContext * /*ctx*/) override { }
  virtual void exitPredicate(NebulaSQLParser::PredicateContext * /*ctx*/) override { }

  virtual void enterValueExpressionDefault(NebulaSQLParser::ValueExpressionDefaultContext * /*ctx*/) override { }
  virtual void exitValueExpressionDefault(NebulaSQLParser::ValueExpressionDefaultContext * /*ctx*/) override { }

  virtual void enterComparison(NebulaSQLParser::ComparisonContext * /*ctx*/) override { }
  virtual void exitComparison(NebulaSQLParser::ComparisonContext * /*ctx*/) override { }

  virtual void enterArithmeticBinary(NebulaSQLParser::ArithmeticBinaryContext * /*ctx*/) override { }
  virtual void exitArithmeticBinary(NebulaSQLParser::ArithmeticBinaryContext * /*ctx*/) override { }

  virtual void enterArithmeticUnary(NebulaSQLParser::ArithmeticUnaryContext * /*ctx*/) override { }
  virtual void exitArithmeticUnary(NebulaSQLParser::ArithmeticUnaryContext * /*ctx*/) override { }

  virtual void enterComparisonOperator(NebulaSQLParser::ComparisonOperatorContext * /*ctx*/) override { }
  virtual void exitComparisonOperator(NebulaSQLParser::ComparisonOperatorContext * /*ctx*/) override { }

  virtual void enterHint(NebulaSQLParser::HintContext * /*ctx*/) override { }
  virtual void exitHint(NebulaSQLParser::HintContext * /*ctx*/) override { }

  virtual void enterHintStatement(NebulaSQLParser::HintStatementContext * /*ctx*/) override { }
  virtual void exitHintStatement(NebulaSQLParser::HintStatementContext * /*ctx*/) override { }

  virtual void enterDereference(NebulaSQLParser::DereferenceContext * /*ctx*/) override { }
  virtual void exitDereference(NebulaSQLParser::DereferenceContext * /*ctx*/) override { }

  virtual void enterConstantDefault(NebulaSQLParser::ConstantDefaultContext * /*ctx*/) override { }
  virtual void exitConstantDefault(NebulaSQLParser::ConstantDefaultContext * /*ctx*/) override { }

  virtual void enterColumnReference(NebulaSQLParser::ColumnReferenceContext * /*ctx*/) override { }
  virtual void exitColumnReference(NebulaSQLParser::ColumnReferenceContext * /*ctx*/) override { }

  virtual void enterRowConstructor(NebulaSQLParser::RowConstructorContext * /*ctx*/) override { }
  virtual void exitRowConstructor(NebulaSQLParser::RowConstructorContext * /*ctx*/) override { }

  virtual void enterParenthesizedExpression(NebulaSQLParser::ParenthesizedExpressionContext * /*ctx*/) override { }
  virtual void exitParenthesizedExpression(NebulaSQLParser::ParenthesizedExpressionContext * /*ctx*/) override { }

  virtual void enterStar(NebulaSQLParser::StarContext * /*ctx*/) override { }
  virtual void exitStar(NebulaSQLParser::StarContext * /*ctx*/) override { }

  virtual void enterFunctionCall(NebulaSQLParser::FunctionCallContext * /*ctx*/) override { }
  virtual void exitFunctionCall(NebulaSQLParser::FunctionCallContext * /*ctx*/) override { }

  virtual void enterSubqueryExpression(NebulaSQLParser::SubqueryExpressionContext * /*ctx*/) override { }
  virtual void exitSubqueryExpression(NebulaSQLParser::SubqueryExpressionContext * /*ctx*/) override { }

  virtual void enterQualifiedName(NebulaSQLParser::QualifiedNameContext * /*ctx*/) override { }
  virtual void exitQualifiedName(NebulaSQLParser::QualifiedNameContext * /*ctx*/) override { }

  virtual void enterExponentLiteral(NebulaSQLParser::ExponentLiteralContext * /*ctx*/) override { }
  virtual void exitExponentLiteral(NebulaSQLParser::ExponentLiteralContext * /*ctx*/) override { }

  virtual void enterDecimalLiteral(NebulaSQLParser::DecimalLiteralContext * /*ctx*/) override { }
  virtual void exitDecimalLiteral(NebulaSQLParser::DecimalLiteralContext * /*ctx*/) override { }

  virtual void enterLegacyDecimalLiteral(NebulaSQLParser::LegacyDecimalLiteralContext * /*ctx*/) override { }
  virtual void exitLegacyDecimalLiteral(NebulaSQLParser::LegacyDecimalLiteralContext * /*ctx*/) override { }

  virtual void enterIntegerLiteral(NebulaSQLParser::IntegerLiteralContext * /*ctx*/) override { }
  virtual void exitIntegerLiteral(NebulaSQLParser::IntegerLiteralContext * /*ctx*/) override { }

  virtual void enterBigIntLiteral(NebulaSQLParser::BigIntLiteralContext * /*ctx*/) override { }
  virtual void exitBigIntLiteral(NebulaSQLParser::BigIntLiteralContext * /*ctx*/) override { }

  virtual void enterSmallIntLiteral(NebulaSQLParser::SmallIntLiteralContext * /*ctx*/) override { }
  virtual void exitSmallIntLiteral(NebulaSQLParser::SmallIntLiteralContext * /*ctx*/) override { }

  virtual void enterTinyIntLiteral(NebulaSQLParser::TinyIntLiteralContext * /*ctx*/) override { }
  virtual void exitTinyIntLiteral(NebulaSQLParser::TinyIntLiteralContext * /*ctx*/) override { }

  virtual void enterDoubleLiteral(NebulaSQLParser::DoubleLiteralContext * /*ctx*/) override { }
  virtual void exitDoubleLiteral(NebulaSQLParser::DoubleLiteralContext * /*ctx*/) override { }

  virtual void enterFloatLiteral(NebulaSQLParser::FloatLiteralContext * /*ctx*/) override { }
  virtual void exitFloatLiteral(NebulaSQLParser::FloatLiteralContext * /*ctx*/) override { }

  virtual void enterBigDecimalLiteral(NebulaSQLParser::BigDecimalLiteralContext * /*ctx*/) override { }
  virtual void exitBigDecimalLiteral(NebulaSQLParser::BigDecimalLiteralContext * /*ctx*/) override { }

  virtual void enterNullLiteral(NebulaSQLParser::NullLiteralContext * /*ctx*/) override { }
  virtual void exitNullLiteral(NebulaSQLParser::NullLiteralContext * /*ctx*/) override { }

  virtual void enterTypeConstructor(NebulaSQLParser::TypeConstructorContext * /*ctx*/) override { }
  virtual void exitTypeConstructor(NebulaSQLParser::TypeConstructorContext * /*ctx*/) override { }

  virtual void enterNumericLiteral(NebulaSQLParser::NumericLiteralContext * /*ctx*/) override { }
  virtual void exitNumericLiteral(NebulaSQLParser::NumericLiteralContext * /*ctx*/) override { }

  virtual void enterBooleanLiteral(NebulaSQLParser::BooleanLiteralContext * /*ctx*/) override { }
  virtual void exitBooleanLiteral(NebulaSQLParser::BooleanLiteralContext * /*ctx*/) override { }

  virtual void enterStringLiteral(NebulaSQLParser::StringLiteralContext * /*ctx*/) override { }
  virtual void exitStringLiteral(NebulaSQLParser::StringLiteralContext * /*ctx*/) override { }

  virtual void enterBooleanValue(NebulaSQLParser::BooleanValueContext * /*ctx*/) override { }
  virtual void exitBooleanValue(NebulaSQLParser::BooleanValueContext * /*ctx*/) override { }

  virtual void enterStrictNonReserved(NebulaSQLParser::StrictNonReservedContext * /*ctx*/) override { }
  virtual void exitStrictNonReserved(NebulaSQLParser::StrictNonReservedContext * /*ctx*/) override { }

  virtual void enterAnsiNonReserved(NebulaSQLParser::AnsiNonReservedContext * /*ctx*/) override { }
  virtual void exitAnsiNonReserved(NebulaSQLParser::AnsiNonReservedContext * /*ctx*/) override { }

  virtual void enterNonReserved(NebulaSQLParser::NonReservedContext * /*ctx*/) override { }
  virtual void exitNonReserved(NebulaSQLParser::NonReservedContext * /*ctx*/) override { }


  virtual void enterEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void exitEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void visitTerminal(antlr4::tree::TerminalNode * /*node*/) override { }
  virtual void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override { }

};

}  // namespace NES::Parsers
