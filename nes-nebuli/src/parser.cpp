/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <iostream>

#include <ostream>


#include <nodes/parsenodes.hpp>
#include <nodes/pg_list.hpp>

/// The Postgres Enum is to big for magic enum to handle by default...
#define MAGIC_ENUM_RANGE_MIN 0
#define MAGIC_ENUM_RANGE_MAX 512
#include <concepts>
#include <memory>
#include <ranges>
#include <sstream>
#include <utility>
#include <parser.hpp>

#include <Expressions/ArithmeticalExpressions/ModExpressionNode.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Expressions/ExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <fmt/format.h>
#include <nodes/parsenodes.hpp>
#include <magic_enum.hpp>
#include <postgres_parser.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/ValueTypes/BasicValue.hpp>

#include <ErrorHandling.hpp>


class Visitor
{
public:
    struct Context
    {
        using Value = std::variant<std::string, ssize_t, NES::ExpressionNodePtr, NES::QueryPlanPtr>;
        bool in_source;

        std::stringstream log;
        std::function<void(Value)> consume = [](auto) {};
        std::function<void(std::string_view)> fail = [](auto) {};
    };

    struct ContextHandle
    {
        std::shared_ptr<Context> context;
        size_t indent = 0;
        ContextHandle(std::shared_ptr<Context> context)
            : context(context), indent(0), consume_old(context->consume), fail_old(context->fail)
        {
        }
        ContextHandle(const ContextHandle& other)
            : context(other.context), indent(other.indent + 1), consume_old(context->consume), fail_old(context->fail)
        {
        }

        ContextHandle(ContextHandle&& other) noexcept = delete;
        ContextHandle& operator=(const ContextHandle& other) = delete;
        ContextHandle& operator=(ContextHandle&& other) noexcept = delete;

        ~ContextHandle()
        {
            context->consume = consume_old;
            context->fail = fail_old;
        }

        void log(std::string_view msg)
        {
            for (auto it : std::ranges::views::split(msg, "\n"))
            {
                std::string_view sv(it.begin(), it.end());
                context->log << std::string(indent, ' ') << sv << '\n';
            }
        }

        void registerConsume(decltype(Context::consume) fn) { context->consume = std::move(fn); }

        void registerFail(decltype(Context::fail) fn) { context->fail = std::move(fn); }

        void consume(Context::Value value) { context->consume(value); }
        void fail(std::string_view error) { context->fail(error); }

        std::function<void(Context::Value)> consume_old;
        std::function<void(std::string_view)> fail_old;
    };

    void dispatch_and_visit(ContextHandle context, duckdb_libpgquery::PGValue* value)
    {
        dispatch_and_visit(context, reinterpret_cast<duckdb_libpgquery::PGNode*>(value));
    }
    void dispatch_and_visit(ContextHandle context, duckdb_libpgquery::PGList* list)
    {
        duckdb_libpgquery::PGListCell* cell;
        for_each_cell(cell, list->head)
        {
            dispatch_and_visit(context, static_cast<duckdb_libpgquery::PGNode*>(cell->data.ptr_value));
        }
    }
    void dispatch_and_visit(ContextHandle context, duckdb_libpgquery::PGNode* node)
    {
        using namespace duckdb_libpgquery;
#define PREPEND(prefix, name) prefix##name
#define VISIT(NodeType) \
    case PREPEND(T_, NodeType): \
        visit(context, reinterpret_cast<NodeType*>(node)); \
        break;

        switch (node->type)
        {
            VISIT(PGRawStmt)
            VISIT(PGAlias)
            VISIT(PGRangeVar)
            VISIT(PGTableFunc)
            VISIT(PGExpr)
            VISIT(PGVar)
            VISIT(PGConst)
            VISIT(PGParam)
            VISIT(PGAggref)
            VISIT(PGGroupingFunc)
            VISIT(PGWindowFunc)
            VISIT(PGArrayRef)
            VISIT(PGFuncExpr)
            VISIT(PGNamedArgExpr)
            VISIT(PGOpExpr)
            VISIT(PGScalarArrayOpExpr)
            VISIT(PGBoolExpr)
            VISIT(PGSubLink)
            VISIT(PGSubPlan)
            VISIT(PGAlternativeSubPlan)
            VISIT(PGFieldSelect)
            VISIT(PGFieldStore)
            VISIT(PGRelabelType)
            VISIT(PGCoerceViaIO)
            VISIT(PGArrayCoerceExpr)
            VISIT(PGConvertRowtypeExpr)
            VISIT(PGCollateExpr)
            VISIT(PGCaseExpr)
            VISIT(PGCaseWhen)
            VISIT(PGCaseTestExpr)
            VISIT(PGArrayExpr)
            VISIT(PGRowExpr)
            VISIT(PGRowCompareExpr)
            VISIT(PGCoalesceExpr)
            VISIT(PGMinMaxExpr)
            VISIT(PGSQLValueFunction)
            VISIT(PGNullTest)
            VISIT(PGBooleanTest)
            VISIT(PGCoerceToDomain)
            VISIT(PGCoerceToDomainValue)
            VISIT(PGSetToDefault)
            VISIT(PGCurrentOfExpr)
            VISIT(PGNextValueExpr)
            VISIT(PGInferenceElem)
            VISIT(PGTargetEntry)
            VISIT(PGRangeTblRef)
            VISIT(PGJoinExpr)
            VISIT(PGFromExpr)
            VISIT(PGOnConflictExpr)
            VISIT(PGIntoClause)
            VISIT(PGLambdaFunction)
            VISIT(PGPivotExpr)
            VISIT(PGPivot)
            VISIT(PGPivotStmt)
            VISIT(PGMemoryContext)
            VISIT(PGValue)
            VISIT(PGList)
            VISIT(PGQuery)
            VISIT(PGInsertStmt)
            VISIT(PGDeleteStmt)
            VISIT(PGUpdateStmt)
            VISIT(PGUpdateExtensionsStmt)
            VISIT(PGAlterTableStmt)
            VISIT(PGAlterTableCmd)
            VISIT(PGCopyStmt)
            VISIT(PGCopyDatabaseStmt)
            VISIT(PGCreateStmt)
            VISIT(PGDropStmt)
            VISIT(PGCommentOnStmt)
            VISIT(PGIndexStmt)
            VISIT(PGFunctionDefinition)
            VISIT(PGCreateFunctionStmt)
            VISIT(PGRenameStmt)
            VISIT(PGTransactionStmt)
            VISIT(PGViewStmt)
            VISIT(PGLoadStmt)
            VISIT(PGVacuumStmt)
            VISIT(PGExplainStmt)
            VISIT(PGCreateTableAsStmt)
            VISIT(PGCreateSeqStmt)
            VISIT(PGAlterSeqStmt)
            VISIT(PGVariableSetStmt)
            VISIT(PGVariableShowStmt)
            VISIT(PGVariableShowSelectStmt)
            VISIT(PGCheckPointStmt)
            VISIT(PGCreateSchemaStmt)
            VISIT(PGCreateSecretStmt)
            VISIT(PGPrepareStmt)
            VISIT(PGExecuteStmt)
            VISIT(PGCallStmt)
            VISIT(PGDeallocateStmt)
            VISIT(PGDropSecretStmt)
            VISIT(PGAlterObjectSchemaStmt)
            VISIT(PGCreateTypeStmt)
            VISIT(PGPragmaStmt)
            VISIT(PGExportStmt)
            VISIT(PGImportStmt)
            VISIT(PGAttachStmt)
            VISIT(PGDetachStmt)
            VISIT(PGUseStmt)
            VISIT(PGAExpr)
            VISIT(PGColumnRef)
            VISIT(PGParamRef)
            VISIT(PGAConst)
            VISIT(PGFuncCall)
            VISIT(PGAStar)
            VISIT(PGAIndices)
            VISIT(PGAIndirection)
            VISIT(PGAArrayExpr)
            VISIT(PGResTarget)
            VISIT(PGMultiAssignRef)
            VISIT(PGTypeCast)
            VISIT(PGCollateClause)
            VISIT(PGSortBy)
            VISIT(PGWindowDef)
            VISIT(PGRangeSubselect)
            VISIT(PGRangeFunction)
            VISIT(PGTypeName)
            VISIT(PGColumnDef)
            VISIT(PGIndexElem)
            VISIT(PGConstraint)
            VISIT(PGDefElem)
            VISIT(PGRangeTblEntry)
            VISIT(PGRangeTblFunction)
            VISIT(PGGroupingSet)
            VISIT(PGWindowClause)
            VISIT(PGObjectWithArgs)
            VISIT(PGTableLikeClause)
            VISIT(PGLockingClause)
            VISIT(PGWithClause)
            VISIT(PGInferClause)
            VISIT(PGOnConflictClause)
            VISIT(PGCommonTableExpr)
            VISIT(PGIntervalConstant)
            VISIT(PGSampleSize)
            VISIT(PGSampleOptions)
            VISIT(PGLimitPercent)
            VISIT(PGPositionalReference)
            VISIT(PGSelectStmt)
            case T_PGString:
                context.consume(reinterpret_cast<PGValue*>(node)->val.str);
                break;
            case T_PGInteger:
                context.consume(reinterpret_cast<PGValue*>(node)->val.ival);
                break;
            default:
                context.fail((std::stringstream() << "Disptach of type " << magic_enum::enum_name<duckdb_libpgquery::PGNodeTag>(node->type)
                                                  << " is not implemented \n")
                                 .str());
        }
    }

    template <typename T>
    void visit(ContextHandle context, T*)
    {
        context.fail((std::stringstream() << typeid(T).name() << "' is not implemented" << '\n').str());
    }

    static_assert(std::convertible_to<std::string, std::variant<std::string, int>>);

    template <>
    void visit(ContextHandle context, duckdb_libpgquery::PGRawStmt* raw)
    {
        context.log("Stmt:");
        dispatch_and_visit(context, raw->stmt);
    }

    struct Error
    {
        std::string message;
    };
    template <typename T>
    using result = std::variant<T, Error>;
    result<std::string> single_source_name(duckdb_libpgquery::PGList* list)
    {
        if (list->length != 1)
        {
            return Error{"Expected single source"};
        }

        auto* source = static_cast<duckdb_libpgquery::PGNode*>(list->head->data.ptr_value);
        if (source->type != duckdb_libpgquery::T_PGRangeVar)
        {
            return Error{"Expected single source to be a range var"};
        }

        auto* source_range_var = reinterpret_cast<duckdb_libpgquery::PGRangeVar*>(source);
        return std::string(source_range_var->relname);
    }

    template <typename T>
    static std::function<void(Context::Value)> extract(ContextHandle& context, std::optional<T>& value)
    {
        return [called_once = false, fail = context.context->fail, &value](Context::Value val) mutable
        {
            std::visit(
                [&]<typename Res>(Res val)
                {
                    if (called_once)
                    {
                        fail("Called more than once");
                        return;
                    }
                    if constexpr (std::convertible_to<Res, T>)
                    {
                        value.emplace(std::move(static_cast<T>(val)));
                    }
                    else
                    {
                        fail(fmt::format("Consumed unexpected type, {} vs. {}", typeid(T).name(), typeid(Res).name()));
                    }
                },
                val);
        };
    }
    template <typename T>
    static std::function<void(Context::Value)> expect_consume(ContextHandle& context, std::function<void(T)> fn)
    {
        return [&, fail = context.context->fail, called_once = false](Context::Value v) mutable
        {
            if (called_once)
            {
                fail("Called more than once");
                return;
            }
            else
            {
                called_once = true;
            }

            if (auto* t_ptr = std::get_if<T>(&v))
            {
                fn(*t_ptr);
            }
        };
    }

    template <>
    void visit(ContextHandle context, duckdb_libpgquery::PGSelectStmt* select)
    {
        std::optional<std::string> source_name;
        std::optional<NES::ExpressionNodePtr> filter_expression;
        std::optional<std::string> sink_name;

        if (select->fromClause)
        {
            ContextHandle fromContext = context;
            fromContext.log("From:");
            context.registerConsume(extract(fromContext, source_name));
            dispatch_and_visit(fromContext, select->fromClause);
            fromContext.log(fmt::format("Source name: {}", source_name.value()));
        }

        if (select->whereClause)
        {
            ContextHandle whereContext = context;
            whereContext.registerConsume(extract(whereContext, filter_expression));
            dispatch_and_visit(whereContext, select->whereClause);
            whereContext.log(fmt::format("Where: {}", filter_expression.value()->toString()));
        }

        if (select->intoClause)
        {
            ContextHandle intoContext = context;
            context.registerConsume(extract(intoContext, sink_name));
            visit(intoContext, select->intoClause);
        }

        auto source = std::make_shared<NES::SourceLogicalOperator>(
            NES::LogicalSourceDescriptor::create(*source_name), NES::OperatorId(3), NES::OriginId(1));
        auto filter = std::make_shared<NES::LogicalFilterOperator>(*filter_expression, NES::OperatorId(2));
        auto sink = std::make_shared<NES::SinkLogicalOperator>(NES::PrintSinkDescriptor::create(), NES::OperatorId(1));

        sink->addChild(filter);
        filter->addChild(source);
        auto qp = NES::QueryPlan::create();
        qp->addRootOperator(sink);
        context.consume(qp);
    }

    template <>
    void visit(ContextHandle context, duckdb_libpgquery::PGRangeVar* range)
    {
        context.log("Range:");
        context.consume(range->relname);
    }

    template <>
    void visit(ContextHandle context, duckdb_libpgquery::PGBoolExpr* bool_expr)
    {
        if (bool_expr->boolop == duckdb_libpgquery::PG_AND_EXPR)
        {
            std::optional<NES::ExpressionNodePtr> left;
            std::optional<NES::ExpressionNodePtr> right;

            {
                ContextHandle leftContext(context);
                leftContext.log("Left:");
                leftContext.registerConsume(extract(leftContext, left));
                dispatch_and_visit(leftContext, static_cast<duckdb_libpgquery::PGNode*>(bool_expr->args->head->data.ptr_value));
            }
            {
                ContextHandle rightContext(context);
                rightContext.log("Right:");
                rightContext.registerConsume(extract(rightContext, right));
                dispatch_and_visit(rightContext, static_cast<duckdb_libpgquery::PGNode*>(bool_expr->args->head->next->data.ptr_value));
            }

            auto expression = NES::AndExpressionNode::create(*left, *right);

            context.log(fmt::format("AND Expr: {}", expression->toString()));
            context.consume(expression);
        }
    }

    template <>
    void visit(ContextHandle context, duckdb_libpgquery::PGIntoClause* into)
    {
        context.log("Into:");
        std::optional<std::string> sink_name;
        context.registerConsume(extract(context, sink_name));
        visit(context, into->rel);
        context.log(fmt::format("Sink name: {}", *sink_name));
    }

    template <>
    void visit(ContextHandle context, duckdb_libpgquery::PGExpr*)
    {
        context.log("Expr:");
        context.fail("Expression not handled");
    }

    template <>
    void visit(ContextHandle context, duckdb_libpgquery::PGColumnRef* expr)
    {
        context.log("ColumnRef:");
        std::optional<std::string> ref;
        if (expr->fields)
        {
            ContextHandle fieldContext(context);
            fieldContext.registerConsume(extract(context, ref));
            dispatch_and_visit(fieldContext, expr->fields);
        }
        context.consume(NES::FieldAccessExpressionNode::create(*ref));
    }

    template <>
    void visit(ContextHandle context, duckdb_libpgquery::PGAConst* expr)
    {
        std::optional<std::variant<int64_t, std::string>> value;
        {
            ContextHandle exprContext(context);
            exprContext.registerConsume(extract(exprContext, value));
            dispatch_and_visit(exprContext, &expr->val);
        }

        std::visit(
            [&]<typename T>(T value)
            {
                std::string string_val;
                if constexpr (std::is_convertible_v<T, std::string>)
                {
                    string_val = static_cast<std::string>(value);
                }
                else if constexpr (requires { std::to_string(value); })
                {
                    string_val = std::to_string(value);
                }

                context.consume(NES::ConstantValueExpressionNode::create(
                    std::make_shared<NES::BasicValue>(std::make_shared<NES::Integer>(0, INT64_MIN, INT64_MAX), std::move(string_val))));
            },
            *value);

        context.log("AConst:");
    }

    template <>
    void visit(ContextHandle context, duckdb_libpgquery::PGAExpr* expr)
    {
        std::string op;
        NES::ExpressionNodePtr left;
        NES::ExpressionNodePtr right;

        if (expr->name)
        {
            ContextHandle operationContext(context);
            operationContext.log("Operation:");
            operationContext.registerConsume(expect_consume(operationContext, std::function([&](std::string s) { op = std::move(s); })));
            dispatch_and_visit(operationContext, expr->name);
            operationContext.log(fmt::format("op: {}", op));
        }

        if (expr->lexpr)
        {
            ContextHandle leftContext(context);
            leftContext.log("Left:");
            leftContext.registerConsume(expect_consume(leftContext, std::function([&](NES::ExpressionNodePtr s) { left = std::move(s); })));
            dispatch_and_visit(leftContext, expr->lexpr);
        }

        if (expr->rexpr)
        {
            ContextHandle rightContext(context);
            rightContext.log("Right:");
            rightContext.registerConsume(
                expect_consume(rightContext, std::function([&](NES::ExpressionNodePtr s) { right = std::move(s); })));
            dispatch_and_visit(rightContext, expr->rexpr);
        }

        context.log(fmt::format("AExpr({})", op));

        if (op == "==")
        {
            context.consume(NES::EqualsExpressionNode::create(left, right));
        }
        else if (op == "%")
        {
            context.consume(NES::ModExpressionNode::create(left, right));
        }
        else if (op == "<")
        {
            context.consume(NES::LessExpressionNode::create(left, right));
        }
        else if (op == "<=")
        {
            context.consume(NES::LessEqualsExpressionNode::create(left, right));
        }
        else if (op == ">")
        {
            context.consume(NES::GreaterExpressionNode::create(left, right));
        }
        else if (op == ">=")
        {
            context.consume(NES::GreaterEqualsExpressionNode::create(left, right));
        }
        else
        {
            context.fail(fmt::format("Operator not Implemented '{}'", op));
        }
    }
};

ParserResult parse(const std::string& input)
{
    duckdb::PostgresParser parser;
    parser.Parse(input.c_str());

    if (!parser.success)
    {
        return ParserError{input, parser.error_location, parser.error_message};
    }
    else
    {
        Visitor visitor;
        auto context = std::make_shared<Visitor::Context>();
        Visitor::ContextHandle handle(context);
        std::optional<NES::QueryPlanPtr> qp;
        std::stringstream error_stream;
        bool failed = false;
        context->fail = [&error_stream, &failed](std::string_view msg)
        {
            error_stream << msg << std::endl;
            failed = true;
        };
        handle.registerConsume(Visitor::extract(handle, qp));
        try
        {
            visitor.dispatch_and_visit(handle, static_cast<duckdb_libpgquery::PGNode*>(parser.parse_tree->head->data.ptr_value));
        }
        catch (...)
        {
            NES::tryLogCurrentException();
        }

        if (failed)
        {
            return TranslationError{error_stream.str(), input};
        }

        return *qp;
    }
}
NES::QueryPlanPtr query_plan_or_fail(ParserResult result)
{
    return std::visit(
        [&](auto result) -> NES::QueryPlanPtr
        {
            if constexpr (std::is_same_v<decltype(result), NES::QueryPlanPtr>)
            {
                return result;
            }
            else
            {
                std::cerr << result.query << std::endl;
                std::cerr << result.error_message << std::endl;
                throw NES::QueryInvalid();
            }
        },
        result);
}