// Generated from /home/rudi/dima/nebulastream-public/nes-nebuli/parser/NebulaSQL.g4 by ANTLR 4.13.2
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link NebulaSQLParser}.
 */
public interface NebulaSQLListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void enterSingleStatement(NebulaSQLParser.SingleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void exitSingleStatement(NebulaSQLParser.SingleStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatement(NebulaSQLParser.StatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatement(NebulaSQLParser.StatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#query}.
	 * @param ctx the parse tree
	 */
	void enterQuery(NebulaSQLParser.QueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#query}.
	 * @param ctx the parse tree
	 */
	void exitQuery(NebulaSQLParser.QueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#queryOrganization}.
	 * @param ctx the parse tree
	 */
	void enterQueryOrganization(NebulaSQLParser.QueryOrganizationContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#queryOrganization}.
	 * @param ctx the parse tree
	 */
	void exitQueryOrganization(NebulaSQLParser.QueryOrganizationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code primaryQuery}
	 * labeled alternative in {@link NebulaSQLParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterPrimaryQuery(NebulaSQLParser.PrimaryQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code primaryQuery}
	 * labeled alternative in {@link NebulaSQLParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitPrimaryQuery(NebulaSQLParser.PrimaryQueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link NebulaSQLParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterSetOperation(NebulaSQLParser.SetOperationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link NebulaSQLParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitSetOperation(NebulaSQLParser.SetOperationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterQueryPrimaryDefault(NebulaSQLParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitQueryPrimaryDefault(NebulaSQLParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code fromStmt}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterFromStmt(NebulaSQLParser.FromStmtContext ctx);
	/**
	 * Exit a parse tree produced by the {@code fromStmt}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitFromStmt(NebulaSQLParser.FromStmtContext ctx);
	/**
	 * Enter a parse tree produced by the {@code table}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTable(NebulaSQLParser.TableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code table}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTable(NebulaSQLParser.TableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterInlineTableDefault1(NebulaSQLParser.InlineTableDefault1Context ctx);
	/**
	 * Exit a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitInlineTableDefault1(NebulaSQLParser.InlineTableDefault1Context ctx);
	/**
	 * Enter a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterSubquery(NebulaSQLParser.SubqueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link NebulaSQLParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitSubquery(NebulaSQLParser.SubqueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void enterQuerySpecification(NebulaSQLParser.QuerySpecificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void exitQuerySpecification(NebulaSQLParser.QuerySpecificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void enterFromClause(NebulaSQLParser.FromClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void exitFromClause(NebulaSQLParser.FromClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#relation}.
	 * @param ctx the parse tree
	 */
	void enterRelation(NebulaSQLParser.RelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#relation}.
	 * @param ctx the parse tree
	 */
	void exitRelation(NebulaSQLParser.RelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#joinRelation}.
	 * @param ctx the parse tree
	 */
	void enterJoinRelation(NebulaSQLParser.JoinRelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#joinRelation}.
	 * @param ctx the parse tree
	 */
	void exitJoinRelation(NebulaSQLParser.JoinRelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#joinType}.
	 * @param ctx the parse tree
	 */
	void enterJoinType(NebulaSQLParser.JoinTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#joinType}.
	 * @param ctx the parse tree
	 */
	void exitJoinType(NebulaSQLParser.JoinTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	void enterJoinCriteria(NebulaSQLParser.JoinCriteriaContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	void exitJoinCriteria(NebulaSQLParser.JoinCriteriaContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTableName(NebulaSQLParser.TableNameContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTableName(NebulaSQLParser.TableNameContext ctx);
	/**
	 * Enter a parse tree produced by the {@code aliasedQuery}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterAliasedQuery(NebulaSQLParser.AliasedQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code aliasedQuery}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitAliasedQuery(NebulaSQLParser.AliasedQueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code aliasedRelation}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterAliasedRelation(NebulaSQLParser.AliasedRelationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code aliasedRelation}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitAliasedRelation(NebulaSQLParser.AliasedRelationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code inlineTableDefault2}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterInlineTableDefault2(NebulaSQLParser.InlineTableDefault2Context ctx);
	/**
	 * Exit a parse tree produced by the {@code inlineTableDefault2}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitInlineTableDefault2(NebulaSQLParser.InlineTableDefault2Context ctx);
	/**
	 * Enter a parse tree produced by the {@code tableValuedFunction}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTableValuedFunction(NebulaSQLParser.TableValuedFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableValuedFunction}
	 * labeled alternative in {@link NebulaSQLParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTableValuedFunction(NebulaSQLParser.TableValuedFunctionContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#functionTable}.
	 * @param ctx the parse tree
	 */
	void enterFunctionTable(NebulaSQLParser.FunctionTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#functionTable}.
	 * @param ctx the parse tree
	 */
	void exitFunctionTable(NebulaSQLParser.FunctionTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#fromStatement}.
	 * @param ctx the parse tree
	 */
	void enterFromStatement(NebulaSQLParser.FromStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#fromStatement}.
	 * @param ctx the parse tree
	 */
	void exitFromStatement(NebulaSQLParser.FromStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#fromStatementBody}.
	 * @param ctx the parse tree
	 */
	void enterFromStatementBody(NebulaSQLParser.FromStatementBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#fromStatementBody}.
	 * @param ctx the parse tree
	 */
	void exitFromStatementBody(NebulaSQLParser.FromStatementBodyContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void enterSelectClause(NebulaSQLParser.SelectClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void exitSelectClause(NebulaSQLParser.SelectClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void enterWhereClause(NebulaSQLParser.WhereClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void exitWhereClause(NebulaSQLParser.WhereClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void enterHavingClause(NebulaSQLParser.HavingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void exitHavingClause(NebulaSQLParser.HavingClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#inlineTable}.
	 * @param ctx the parse tree
	 */
	void enterInlineTable(NebulaSQLParser.InlineTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#inlineTable}.
	 * @param ctx the parse tree
	 */
	void exitInlineTable(NebulaSQLParser.InlineTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	void enterTableAlias(NebulaSQLParser.TableAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	void exitTableAlias(NebulaSQLParser.TableAliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#multipartIdentifierList}.
	 * @param ctx the parse tree
	 */
	void enterMultipartIdentifierList(NebulaSQLParser.MultipartIdentifierListContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#multipartIdentifierList}.
	 * @param ctx the parse tree
	 */
	void exitMultipartIdentifierList(NebulaSQLParser.MultipartIdentifierListContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterMultipartIdentifier(NebulaSQLParser.MultipartIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitMultipartIdentifier(NebulaSQLParser.MultipartIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#namedExpression}.
	 * @param ctx the parse tree
	 */
	void enterNamedExpression(NebulaSQLParser.NamedExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#namedExpression}.
	 * @param ctx the parse tree
	 */
	void exitNamedExpression(NebulaSQLParser.NamedExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(NebulaSQLParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(NebulaSQLParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link NebulaSQLParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterUnquotedIdentifier(NebulaSQLParser.UnquotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link NebulaSQLParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitUnquotedIdentifier(NebulaSQLParser.UnquotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link NebulaSQLParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifierAlternative(NebulaSQLParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link NebulaSQLParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifierAlternative(NebulaSQLParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifier(NebulaSQLParser.QuotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifier(NebulaSQLParser.QuotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#identifierList}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierList(NebulaSQLParser.IdentifierListContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#identifierList}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierList(NebulaSQLParser.IdentifierListContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierSeq(NebulaSQLParser.IdentifierSeqContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierSeq(NebulaSQLParser.IdentifierSeqContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#errorCapturingIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterErrorCapturingIdentifier(NebulaSQLParser.ErrorCapturingIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#errorCapturingIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitErrorCapturingIdentifier(NebulaSQLParser.ErrorCapturingIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code errorIdent}
	 * labeled alternative in {@link NebulaSQLParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 */
	void enterErrorIdent(NebulaSQLParser.ErrorIdentContext ctx);
	/**
	 * Exit a parse tree produced by the {@code errorIdent}
	 * labeled alternative in {@link NebulaSQLParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 */
	void exitErrorIdent(NebulaSQLParser.ErrorIdentContext ctx);
	/**
	 * Enter a parse tree produced by the {@code realIdent}
	 * labeled alternative in {@link NebulaSQLParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 */
	void enterRealIdent(NebulaSQLParser.RealIdentContext ctx);
	/**
	 * Exit a parse tree produced by the {@code realIdent}
	 * labeled alternative in {@link NebulaSQLParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 */
	void exitRealIdent(NebulaSQLParser.RealIdentContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 */
	void enterNamedExpressionSeq(NebulaSQLParser.NamedExpressionSeqContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 */
	void exitNamedExpressionSeq(NebulaSQLParser.NamedExpressionSeqContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(NebulaSQLParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(NebulaSQLParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link NebulaSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalNot(NebulaSQLParser.LogicalNotContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link NebulaSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalNot(NebulaSQLParser.LogicalNotContext ctx);
	/**
	 * Enter a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link NebulaSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterPredicated(NebulaSQLParser.PredicatedContext ctx);
	/**
	 * Exit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link NebulaSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitPredicated(NebulaSQLParser.PredicatedContext ctx);
	/**
	 * Enter a parse tree produced by the {@code exists}
	 * labeled alternative in {@link NebulaSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterExists(NebulaSQLParser.ExistsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link NebulaSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitExists(NebulaSQLParser.ExistsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link NebulaSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalBinary(NebulaSQLParser.LogicalBinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link NebulaSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalBinary(NebulaSQLParser.LogicalBinaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#windowedAggregationClause}.
	 * @param ctx the parse tree
	 */
	void enterWindowedAggregationClause(NebulaSQLParser.WindowedAggregationClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#windowedAggregationClause}.
	 * @param ctx the parse tree
	 */
	void exitWindowedAggregationClause(NebulaSQLParser.WindowedAggregationClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#aggregationClause}.
	 * @param ctx the parse tree
	 */
	void enterAggregationClause(NebulaSQLParser.AggregationClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#aggregationClause}.
	 * @param ctx the parse tree
	 */
	void exitAggregationClause(NebulaSQLParser.AggregationClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSet(NebulaSQLParser.GroupingSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSet(NebulaSQLParser.GroupingSetContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#windowClause}.
	 * @param ctx the parse tree
	 */
	void enterWindowClause(NebulaSQLParser.WindowClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#windowClause}.
	 * @param ctx the parse tree
	 */
	void exitWindowClause(NebulaSQLParser.WindowClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#watermarkClause}.
	 * @param ctx the parse tree
	 */
	void enterWatermarkClause(NebulaSQLParser.WatermarkClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#watermarkClause}.
	 * @param ctx the parse tree
	 */
	void exitWatermarkClause(NebulaSQLParser.WatermarkClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#watermarkParameters}.
	 * @param ctx the parse tree
	 */
	void enterWatermarkParameters(NebulaSQLParser.WatermarkParametersContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#watermarkParameters}.
	 * @param ctx the parse tree
	 */
	void exitWatermarkParameters(NebulaSQLParser.WatermarkParametersContext ctx);
	/**
	 * Enter a parse tree produced by the {@code timeBasedWindow}
	 * labeled alternative in {@link NebulaSQLParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void enterTimeBasedWindow(NebulaSQLParser.TimeBasedWindowContext ctx);
	/**
	 * Exit a parse tree produced by the {@code timeBasedWindow}
	 * labeled alternative in {@link NebulaSQLParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void exitTimeBasedWindow(NebulaSQLParser.TimeBasedWindowContext ctx);
	/**
	 * Enter a parse tree produced by the {@code countBasedWindow}
	 * labeled alternative in {@link NebulaSQLParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void enterCountBasedWindow(NebulaSQLParser.CountBasedWindowContext ctx);
	/**
	 * Exit a parse tree produced by the {@code countBasedWindow}
	 * labeled alternative in {@link NebulaSQLParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void exitCountBasedWindow(NebulaSQLParser.CountBasedWindowContext ctx);
	/**
	 * Enter a parse tree produced by the {@code thresholdBasedWindow}
	 * labeled alternative in {@link NebulaSQLParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void enterThresholdBasedWindow(NebulaSQLParser.ThresholdBasedWindowContext ctx);
	/**
	 * Exit a parse tree produced by the {@code thresholdBasedWindow}
	 * labeled alternative in {@link NebulaSQLParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void exitThresholdBasedWindow(NebulaSQLParser.ThresholdBasedWindowContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tumblingWindow}
	 * labeled alternative in {@link NebulaSQLParser#timeWindow}.
	 * @param ctx the parse tree
	 */
	void enterTumblingWindow(NebulaSQLParser.TumblingWindowContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tumblingWindow}
	 * labeled alternative in {@link NebulaSQLParser#timeWindow}.
	 * @param ctx the parse tree
	 */
	void exitTumblingWindow(NebulaSQLParser.TumblingWindowContext ctx);
	/**
	 * Enter a parse tree produced by the {@code slidingWindow}
	 * labeled alternative in {@link NebulaSQLParser#timeWindow}.
	 * @param ctx the parse tree
	 */
	void enterSlidingWindow(NebulaSQLParser.SlidingWindowContext ctx);
	/**
	 * Exit a parse tree produced by the {@code slidingWindow}
	 * labeled alternative in {@link NebulaSQLParser#timeWindow}.
	 * @param ctx the parse tree
	 */
	void exitSlidingWindow(NebulaSQLParser.SlidingWindowContext ctx);
	/**
	 * Enter a parse tree produced by the {@code countBasedTumbling}
	 * labeled alternative in {@link NebulaSQLParser#countWindow}.
	 * @param ctx the parse tree
	 */
	void enterCountBasedTumbling(NebulaSQLParser.CountBasedTumblingContext ctx);
	/**
	 * Exit a parse tree produced by the {@code countBasedTumbling}
	 * labeled alternative in {@link NebulaSQLParser#countWindow}.
	 * @param ctx the parse tree
	 */
	void exitCountBasedTumbling(NebulaSQLParser.CountBasedTumblingContext ctx);
	/**
	 * Enter a parse tree produced by the {@code thresholdWindow}
	 * labeled alternative in {@link NebulaSQLParser#conditionWindow}.
	 * @param ctx the parse tree
	 */
	void enterThresholdWindow(NebulaSQLParser.ThresholdWindowContext ctx);
	/**
	 * Exit a parse tree produced by the {@code thresholdWindow}
	 * labeled alternative in {@link NebulaSQLParser#conditionWindow}.
	 * @param ctx the parse tree
	 */
	void exitThresholdWindow(NebulaSQLParser.ThresholdWindowContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#conditionParameter}.
	 * @param ctx the parse tree
	 */
	void enterConditionParameter(NebulaSQLParser.ConditionParameterContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#conditionParameter}.
	 * @param ctx the parse tree
	 */
	void exitConditionParameter(NebulaSQLParser.ConditionParameterContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#thresholdMinSizeParameter}.
	 * @param ctx the parse tree
	 */
	void enterThresholdMinSizeParameter(NebulaSQLParser.ThresholdMinSizeParameterContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#thresholdMinSizeParameter}.
	 * @param ctx the parse tree
	 */
	void exitThresholdMinSizeParameter(NebulaSQLParser.ThresholdMinSizeParameterContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#sizeParameter}.
	 * @param ctx the parse tree
	 */
	void enterSizeParameter(NebulaSQLParser.SizeParameterContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#sizeParameter}.
	 * @param ctx the parse tree
	 */
	void exitSizeParameter(NebulaSQLParser.SizeParameterContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#advancebyParameter}.
	 * @param ctx the parse tree
	 */
	void enterAdvancebyParameter(NebulaSQLParser.AdvancebyParameterContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#advancebyParameter}.
	 * @param ctx the parse tree
	 */
	void exitAdvancebyParameter(NebulaSQLParser.AdvancebyParameterContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#timeUnit}.
	 * @param ctx the parse tree
	 */
	void enterTimeUnit(NebulaSQLParser.TimeUnitContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#timeUnit}.
	 * @param ctx the parse tree
	 */
	void exitTimeUnit(NebulaSQLParser.TimeUnitContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#timestampParameter}.
	 * @param ctx the parse tree
	 */
	void enterTimestampParameter(NebulaSQLParser.TimestampParameterContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#timestampParameter}.
	 * @param ctx the parse tree
	 */
	void exitTimestampParameter(NebulaSQLParser.TimestampParameterContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#functionName}.
	 * @param ctx the parse tree
	 */
	void enterFunctionName(NebulaSQLParser.FunctionNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#functionName}.
	 * @param ctx the parse tree
	 */
	void exitFunctionName(NebulaSQLParser.FunctionNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#sinkClause}.
	 * @param ctx the parse tree
	 */
	void enterSinkClause(NebulaSQLParser.SinkClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#sinkClause}.
	 * @param ctx the parse tree
	 */
	void exitSinkClause(NebulaSQLParser.SinkClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#sinkType}.
	 * @param ctx the parse tree
	 */
	void enterSinkType(NebulaSQLParser.SinkTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#sinkType}.
	 * @param ctx the parse tree
	 */
	void exitSinkType(NebulaSQLParser.SinkTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#nullNotnull}.
	 * @param ctx the parse tree
	 */
	void enterNullNotnull(NebulaSQLParser.NullNotnullContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#nullNotnull}.
	 * @param ctx the parse tree
	 */
	void exitNullNotnull(NebulaSQLParser.NullNotnullContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#streamName}.
	 * @param ctx the parse tree
	 */
	void enterStreamName(NebulaSQLParser.StreamNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#streamName}.
	 * @param ctx the parse tree
	 */
	void exitStreamName(NebulaSQLParser.StreamNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#sinkTypeFile}.
	 * @param ctx the parse tree
	 */
	void enterSinkTypeFile(NebulaSQLParser.SinkTypeFileContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#sinkTypeFile}.
	 * @param ctx the parse tree
	 */
	void exitSinkTypeFile(NebulaSQLParser.SinkTypeFileContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#sinkTypePrint}.
	 * @param ctx the parse tree
	 */
	void enterSinkTypePrint(NebulaSQLParser.SinkTypePrintContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#sinkTypePrint}.
	 * @param ctx the parse tree
	 */
	void exitSinkTypePrint(NebulaSQLParser.SinkTypePrintContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	void enterFileFormat(NebulaSQLParser.FileFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	void exitFileFormat(NebulaSQLParser.FileFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#sortItem}.
	 * @param ctx the parse tree
	 */
	void enterSortItem(NebulaSQLParser.SortItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#sortItem}.
	 * @param ctx the parse tree
	 */
	void exitSortItem(NebulaSQLParser.SortItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterPredicate(NebulaSQLParser.PredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitPredicate(NebulaSQLParser.PredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link NebulaSQLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterValueExpressionDefault(NebulaSQLParser.ValueExpressionDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link NebulaSQLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitValueExpressionDefault(NebulaSQLParser.ValueExpressionDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link NebulaSQLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterComparison(NebulaSQLParser.ComparisonContext ctx);
	/**
	 * Exit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link NebulaSQLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitComparison(NebulaSQLParser.ComparisonContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link NebulaSQLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticBinary(NebulaSQLParser.ArithmeticBinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link NebulaSQLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticBinary(NebulaSQLParser.ArithmeticBinaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link NebulaSQLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticUnary(NebulaSQLParser.ArithmeticUnaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link NebulaSQLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticUnary(NebulaSQLParser.ArithmeticUnaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void enterComparisonOperator(NebulaSQLParser.ComparisonOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void exitComparisonOperator(NebulaSQLParser.ComparisonOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#hint}.
	 * @param ctx the parse tree
	 */
	void enterHint(NebulaSQLParser.HintContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#hint}.
	 * @param ctx the parse tree
	 */
	void exitHint(NebulaSQLParser.HintContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#hintStatement}.
	 * @param ctx the parse tree
	 */
	void enterHintStatement(NebulaSQLParser.HintStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#hintStatement}.
	 * @param ctx the parse tree
	 */
	void exitHintStatement(NebulaSQLParser.HintStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterDereference(NebulaSQLParser.DereferenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitDereference(NebulaSQLParser.DereferenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterConstantDefault(NebulaSQLParser.ConstantDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitConstantDefault(NebulaSQLParser.ConstantDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterColumnReference(NebulaSQLParser.ColumnReferenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitColumnReference(NebulaSQLParser.ColumnReferenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterRowConstructor(NebulaSQLParser.RowConstructorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitRowConstructor(NebulaSQLParser.RowConstructorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterParenthesizedExpression(NebulaSQLParser.ParenthesizedExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitParenthesizedExpression(NebulaSQLParser.ParenthesizedExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code star}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterStar(NebulaSQLParser.StarContext ctx);
	/**
	 * Exit a parse tree produced by the {@code star}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitStar(NebulaSQLParser.StarContext ctx);
	/**
	 * Enter a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterFunctionCall(NebulaSQLParser.FunctionCallContext ctx);
	/**
	 * Exit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitFunctionCall(NebulaSQLParser.FunctionCallContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubqueryExpression(NebulaSQLParser.SubqueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link NebulaSQLParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubqueryExpression(NebulaSQLParser.SubqueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedName(NebulaSQLParser.QualifiedNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedName(NebulaSQLParser.QualifiedNameContext ctx);
	/**
	 * Enter a parse tree produced by the {@code exponentLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void enterExponentLiteral(NebulaSQLParser.ExponentLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code exponentLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void exitExponentLiteral(NebulaSQLParser.ExponentLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDecimalLiteral(NebulaSQLParser.DecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDecimalLiteral(NebulaSQLParser.DecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code legacyDecimalLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void enterLegacyDecimalLiteral(NebulaSQLParser.LegacyDecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code legacyDecimalLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void exitLegacyDecimalLiteral(NebulaSQLParser.LegacyDecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void enterIntegerLiteral(NebulaSQLParser.IntegerLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void exitIntegerLiteral(NebulaSQLParser.IntegerLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void enterBigIntLiteral(NebulaSQLParser.BigIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void exitBigIntLiteral(NebulaSQLParser.BigIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void enterSmallIntLiteral(NebulaSQLParser.SmallIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void exitSmallIntLiteral(NebulaSQLParser.SmallIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void enterTinyIntLiteral(NebulaSQLParser.TinyIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void exitTinyIntLiteral(NebulaSQLParser.TinyIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDoubleLiteral(NebulaSQLParser.DoubleLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDoubleLiteral(NebulaSQLParser.DoubleLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code floatLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void enterFloatLiteral(NebulaSQLParser.FloatLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code floatLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void exitFloatLiteral(NebulaSQLParser.FloatLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void enterBigDecimalLiteral(NebulaSQLParser.BigDecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link NebulaSQLParser#number}.
	 * @param ctx the parse tree
	 */
	void exitBigDecimalLiteral(NebulaSQLParser.BigDecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterNullLiteral(NebulaSQLParser.NullLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitNullLiteral(NebulaSQLParser.NullLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterTypeConstructor(NebulaSQLParser.TypeConstructorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitTypeConstructor(NebulaSQLParser.TypeConstructorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterNumericLiteral(NebulaSQLParser.NumericLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitNumericLiteral(NebulaSQLParser.NumericLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterBooleanLiteral(NebulaSQLParser.BooleanLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitBooleanLiteral(NebulaSQLParser.BooleanLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(NebulaSQLParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link NebulaSQLParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(NebulaSQLParser.StringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void enterBooleanValue(NebulaSQLParser.BooleanValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void exitBooleanValue(NebulaSQLParser.BooleanValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#strictNonReserved}.
	 * @param ctx the parse tree
	 */
	void enterStrictNonReserved(NebulaSQLParser.StrictNonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#strictNonReserved}.
	 * @param ctx the parse tree
	 */
	void exitStrictNonReserved(NebulaSQLParser.StrictNonReservedContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#ansiNonReserved}.
	 * @param ctx the parse tree
	 */
	void enterAnsiNonReserved(NebulaSQLParser.AnsiNonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#ansiNonReserved}.
	 * @param ctx the parse tree
	 */
	void exitAnsiNonReserved(NebulaSQLParser.AnsiNonReservedContext ctx);
	/**
	 * Enter a parse tree produced by {@link NebulaSQLParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void enterNonReserved(NebulaSQLParser.NonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link NebulaSQLParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void exitNonReserved(NebulaSQLParser.NonReservedContext ctx);
}
