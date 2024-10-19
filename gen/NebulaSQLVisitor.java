// Generated from /home/rudi/dima/nebulastream-public/nes-nebuli/parser/NebulaSQL.g4 by ANTLR 4.13.2
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link NebulaSQLParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface NebulaSQLVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#singleStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleStatement(NebulaSQLParser.SingleStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatement(NebulaSQLParser.StatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery(NebulaSQLParser.QueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#queryOrganization}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryOrganization(NebulaSQLParser.QueryOrganizationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code primaryQuery}
	 * labeled alternative in {@link NebulaSQLParser#queryTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrimaryQuery(NebulaSQLParser.PrimaryQueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link NebulaSQLParser#queryTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetOperation(NebulaSQLParser.SetOperationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryPrimaryDefault(NebulaSQLParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code fromStmt}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFromStmt(NebulaSQLParser.FromStmtContext ctx);
	/**
	 * Visit a parse tree produced by the {@code table}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable(NebulaSQLParser.TableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInlineTableDefault1(NebulaSQLParser.InlineTableDefault1Context ctx);
	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubquery(NebulaSQLParser.SubqueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#querySpecification}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuerySpecification(NebulaSQLParser.QuerySpecificationContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#fromClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFromClause(NebulaSQLParser.FromClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#relation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelation(NebulaSQLParser.RelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#joinRelation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinRelation(NebulaSQLParser.JoinRelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#joinType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinType(NebulaSQLParser.JoinTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#joinCriteria}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinCriteria(NebulaSQLParser.JoinCriteriaContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableName(NebulaSQLParser.TableNameContext ctx);
	/**
	 * Visit a parse tree produced by the {@code aliasedQuery}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAliasedQuery(NebulaSQLParser.AliasedQueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code aliasedRelation}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAliasedRelation(NebulaSQLParser.AliasedRelationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault2}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInlineTableDefault2(NebulaSQLParser.InlineTableDefault2Context ctx);
	/**
	 * Visit a parse tree produced by the {@code tableValuedFunction}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableValuedFunction(NebulaSQLParser.TableValuedFunctionContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#functionTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionTable(NebulaSQLParser.FunctionTableContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#fromStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFromStatement(NebulaSQLParser.FromStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#fromStatementBody}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFromStatementBody(NebulaSQLParser.FromStatementBodyContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#selectClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectClause(NebulaSQLParser.SelectClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#whereClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhereClause(NebulaSQLParser.WhereClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#havingClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHavingClause(NebulaSQLParser.HavingClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#inlineTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInlineTable(NebulaSQLParser.InlineTableContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#tableAlias}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableAlias(NebulaSQLParser.TableAliasContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#multipartIdentifierList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultipartIdentifierList(NebulaSQLParser.MultipartIdentifierListContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultipartIdentifier(NebulaSQLParser.MultipartIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#namedExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedExpression(NebulaSQLParser.NamedExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(NebulaSQLParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link NebulaSQLParser#strictIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnquotedIdentifier(NebulaSQLParser.UnquotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link NebulaSQLParser#strictIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifierAlternative(NebulaSQLParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifier(NebulaSQLParser.QuotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#identifierList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifierList(NebulaSQLParser.IdentifierListContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#identifierSeq}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifierSeq(NebulaSQLParser.IdentifierSeqContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#errorCapturingIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitErrorCapturingIdentifier(NebulaSQLParser.ErrorCapturingIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code errorIdent}
	 * labeled alternative in {@link NebulaSQLParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitErrorIdent(NebulaSQLParser.ErrorIdentContext ctx);
	/**
	 * Visit a parse tree produced by the {@code realIdent}
	 * labeled alternative in {@link NebulaSQLParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRealIdent(NebulaSQLParser.RealIdentContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedExpressionSeq(NebulaSQLParser.NamedExpressionSeqContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(NebulaSQLParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link NebulaSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalNot(NebulaSQLParser.LogicalNotContext ctx);
	/**
	 * Visit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link NebulaSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicated(NebulaSQLParser.PredicatedContext ctx);
	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link NebulaSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExists(NebulaSQLParser.ExistsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link NebulaSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalBinary(NebulaSQLParser.LogicalBinaryContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#windowedAggregationClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowedAggregationClause(NebulaSQLParser.WindowedAggregationClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#aggregationClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAggregationClause(NebulaSQLParser.AggregationClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#groupingSet}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupingSet(NebulaSQLParser.GroupingSetContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#windowClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowClause(NebulaSQLParser.WindowClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#watermarkClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWatermarkClause(NebulaSQLParser.WatermarkClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#watermarkParameters}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWatermarkParameters(NebulaSQLParser.WatermarkParametersContext ctx);
	/**
	 * Visit a parse tree produced by the {@code timeBasedWindow}
	 * labeled alternative in {@link NebulaSQLParser#windowSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimeBasedWindow(NebulaSQLParser.TimeBasedWindowContext ctx);
	/**
	 * Visit a parse tree produced by the {@code countBasedWindow}
	 * labeled alternative in {@link NebulaSQLParser#windowSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCountBasedWindow(NebulaSQLParser.CountBasedWindowContext ctx);
	/**
	 * Visit a parse tree produced by the {@code thresholdBasedWindow}
	 * labeled alternative in {@link NebulaSQLParser#windowSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitThresholdBasedWindow(NebulaSQLParser.ThresholdBasedWindowContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tumblingWindow}
	 * labeled alternative in {@link NebulaSQLParser#timeWindow}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTumblingWindow(NebulaSQLParser.TumblingWindowContext ctx);
	/**
	 * Visit a parse tree produced by the {@code slidingWindow}
	 * labeled alternative in {@link NebulaSQLParser#timeWindow}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSlidingWindow(NebulaSQLParser.SlidingWindowContext ctx);
	/**
	 * Visit a parse tree produced by the {@code countBasedTumbling}
	 * labeled alternative in {@link NebulaSQLParser#countWindow}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCountBasedTumbling(NebulaSQLParser.CountBasedTumblingContext ctx);
	/**
	 * Visit a parse tree produced by the {@code thresholdWindow}
	 * labeled alternative in {@link NebulaSQLParser#conditionWindow}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitThresholdWindow(NebulaSQLParser.ThresholdWindowContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#conditionParameter}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConditionParameter(NebulaSQLParser.ConditionParameterContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#thresholdMinSizeParameter}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitThresholdMinSizeParameter(NebulaSQLParser.ThresholdMinSizeParameterContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#sizeParameter}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSizeParameter(NebulaSQLParser.SizeParameterContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#advancebyParameter}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAdvancebyParameter(NebulaSQLParser.AdvancebyParameterContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#timeUnit}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimeUnit(NebulaSQLParser.TimeUnitContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#timestampParameter}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimestampParameter(NebulaSQLParser.TimestampParameterContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#functionName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionName(NebulaSQLParser.FunctionNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#sinkClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSinkClause(NebulaSQLParser.SinkClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#sinkType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSinkType(NebulaSQLParser.SinkTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#nullNotnull}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullNotnull(NebulaSQLParser.NullNotnullContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#streamName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStreamName(NebulaSQLParser.StreamNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#sinkTypeFile}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSinkTypeFile(NebulaSQLParser.SinkTypeFileContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#sinkTypePrint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSinkTypePrint(NebulaSQLParser.SinkTypePrintContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#fileFormat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFileFormat(NebulaSQLParser.FileFormatContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#sortItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSortItem(NebulaSQLParser.SortItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicate(NebulaSQLParser.PredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link NebulaSQLParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValueExpressionDefault(NebulaSQLParser.ValueExpressionDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link NebulaSQLParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparison(NebulaSQLParser.ComparisonContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link NebulaSQLParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticBinary(NebulaSQLParser.ArithmeticBinaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link NebulaSQLParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticUnary(NebulaSQLParser.ArithmeticUnaryContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#comparisonOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparisonOperator(NebulaSQLParser.ComparisonOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#hint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHint(NebulaSQLParser.HintContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#hintStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHintStatement(NebulaSQLParser.HintStatementContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDereference(NebulaSQLParser.DereferenceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstantDefault(NebulaSQLParser.ConstantDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnReference(NebulaSQLParser.ColumnReferenceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRowConstructor(NebulaSQLParser.RowConstructorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenthesizedExpression(NebulaSQLParser.ParenthesizedExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code star}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStar(NebulaSQLParser.StarContext ctx);
	/**
	 * Visit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionCall(NebulaSQLParser.FunctionCallContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubqueryExpression(NebulaSQLParser.SubqueryExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#qualifiedName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifiedName(NebulaSQLParser.QualifiedNameContext ctx);
	/**
	 * Visit a parse tree produced by the {@code exponentLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExponentLiteral(NebulaSQLParser.ExponentLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDecimalLiteral(NebulaSQLParser.DecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code legacyDecimalLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLegacyDecimalLiteral(NebulaSQLParser.LegacyDecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntegerLiteral(NebulaSQLParser.IntegerLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBigIntLiteral(NebulaSQLParser.BigIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSmallIntLiteral(NebulaSQLParser.SmallIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTinyIntLiteral(NebulaSQLParser.TinyIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDoubleLiteral(NebulaSQLParser.DoubleLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code floatLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFloatLiteral(NebulaSQLParser.FloatLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBigDecimalLiteral(NebulaSQLParser.BigDecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullLiteral(NebulaSQLParser.NullLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTypeConstructor(NebulaSQLParser.TypeConstructorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumericLiteral(NebulaSQLParser.NumericLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanLiteral(NebulaSQLParser.BooleanLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringLiteral(NebulaSQLParser.StringLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#booleanValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanValue(NebulaSQLParser.BooleanValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#strictNonReserved}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStrictNonReserved(NebulaSQLParser.StrictNonReservedContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#ansiNonReserved}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnsiNonReserved(NebulaSQLParser.AnsiNonReservedContext ctx);
	/**
	 * Visit a parse tree produced by {@link NebulaSQLParser#nonReserved}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonReserved(NebulaSQLParser.NonReservedContext ctx);
}
