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
#include <HttpCallLogicalOperator.hpp>

#include <cstddef>
#include <functional>
#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/CheckedInteger.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/CheckedDeadline.hpp>
#include <Util/PlanRenderer.hpp>
#include <boost/url/parse.hpp>
#include <folly/hash/Hash.h>
#include <AsyncOperatorLoweringRegistry.hpp>
#include <ErrorHandling.hpp>
#include <HttpCallAsyncOperator.hpp>
#include <HttpCallDefaults.hpp>
#include <OperatorFunctionRegistry.hpp>

namespace NES
{
namespace
{
std::optional<RuntimeValue> constantValue(const LogicalFunction& function)
{
    const auto literal = function.tryGetAs<ConstantValueLogicalFunction>();
    if (!literal)
    {
        return std::nullopt;
    }
    auto value = RuntimeValue::fromLiteral(function.getDataType(), literal.value()->getConstantValue());
    if (!value)
    {
        throw CannotInferSchema("Invalid HTTP_CALL constant argument: {}", value.error().what());
    }
    return std::move(*value);
}

void requireText(const LogicalFunction& f, const char* name)
{
    if (!f.getDataType().isType(DataType::Type::VARSIZED) || f.getDataType().nullable)
    {
        throw DifferentFieldTypeExpected("HTTP_CALL {} must be non-nullable VARSIZED", name);
    }
}

void requireInteger(const LogicalFunction& f, const char* name)
{
    if (!f.getDataType().isInteger() || f.getDataType().nullable)
    {
        throw DifferentFieldTypeExpected("HTTP_CALL {} must be a non-nullable integer", name);
    }
}

void validate(
    const LogicalFunction& body,
    const LogicalFunction& url,
    const std::optional<LogicalFunction>& timeout,
    const std::optional<LogicalFunction>& retries)
{
    requireText(body, "body");
    requireText(url, "URL");
    if (const auto value = constantValue(url))
    {
        const auto parsed = boost::urls::parse_uri(value->view().get<std::string_view>());
        if (!parsed || parsed->scheme() != "http" || parsed->host().empty())
        {
            throw CannotInferSchema("HTTP_CALL constant URL must be an http:// URL with a host");
        }
    }
    if (timeout)
    {
        requireInteger(*timeout, "timeout");
        if (const auto value = constantValue(*timeout))
        {
            const auto millis = checkedUnsignedIntegerOption(value->view(), "HTTP_CALL timeout");
            if (!millis || *millis == 0 || !checkedSteadyDeadline(*millis, "HTTP_CALL"))
            {
                throw CannotInferSchema("HTTP_CALL constant timeout is outside its representable positive range");
            }
        }
    }
    if (retries)
    {
        requireInteger(*retries, "retries");
        if (const auto value = constantValue(*retries))
        {
            const auto count = checkedUnsignedIntegerOption(value->view(), "HTTP_CALL retries");
            if (!count || !std::in_range<size_t>(*count) || *count == std::numeric_limits<size_t>::max())
            {
                throw CannotInferSchema("HTTP_CALL constant retry count is outside its representable range");
            }
        }
    }
}

const std::vector<LogicalFunction>& unpack(const std::vector<LogicalFunction>& args)
{
    if (args.size() < 2 || args.size() > 4)
    {
        throw InvalidQuerySyntax("HTTP_CALL takes (body, url[, timeout_ms[, retries]])");
    }
    return args;
}
}

HttpCallLogicalOperator::HttpCallLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    LogicalFunction body,
    LogicalFunction url,
    std::optional<LogicalFunction> timeoutMillis,
    std::optional<LogicalFunction> retries)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , body(std::move(body))
    , url(std::move(url))
    , timeoutMillis(std::move(timeoutMillis))
    , retries(std::move(retries))
{
    inferLocalSchema();
}

TypedLogicalOperator<HttpCallLogicalOperator>
HttpCallLogicalOperator::create(LogicalOperator child, LogicalFunction body, LogicalFunction url)
{
    return TypedLogicalOperator<HttpCallLogicalOperator>{std::move(child), std::move(body), std::move(url), std::nullopt, std::nullopt};
}

TypedLogicalOperator<HttpCallLogicalOperator> HttpCallLogicalOperator::create(
    LogicalOperator child,
    LogicalFunction body,
    LogicalFunction url,
    std::optional<LogicalFunction> timeout,
    std::optional<LogicalFunction> retries)
{
    return TypedLogicalOperator<HttpCallLogicalOperator>{
        std::move(child), std::move(body), std::move(url), std::move(timeout), std::move(retries)};
}

AsyncOperatorLowering HttpCallLogicalOperator::lowerToAsync(LogicalOperator op)
{
    const auto& v = *op.getAs<HttpCallLogicalOperator>();
    auto args = v.functionArguments();
    std::vector<DataType> types;
    for (const auto& a : args)
    {
        types.push_back(a.getDataType());
    }
    auto runtime = HttpCallAsyncOperator::create({.inputs = AsyncInputAccessors{std::move(types)}});
    return {.implementation = std::move(runtime), .arguments = std::move(args)};
}

OperatorFunctionRegistryEntry HttpCallLogicalOperator::functionSyntax()
{
    return {
        .resultName = Identifier::parse("response"),
        .inferResultType =
            [](const std::vector<LogicalFunction>& input)
        {
            const auto& a = unpack(input);
            validate(a[0], a[1], a.size() > 2 ? std::optional{a[2]} : std::nullopt, a.size() > 3 ? std::optional{a[3]} : std::nullopt);
            return DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE};
        },
        .createOperator =
            [](LogicalOperator child, const std::vector<LogicalFunction>& input)
        {
            const auto& a = unpack(input);
            return LogicalOperator{create(
                std::move(child),
                a[0],
                a[1],
                a.size() > 2 ? std::optional{a[2]} : std::nullopt,
                a.size() > 3 ? std::optional{a[3]} : std::nullopt)};
        },
        .getArgumentsFromOperator = [](const LogicalOperator& op) -> std::optional<std::vector<LogicalFunction>>
        {
            if (auto v = op.tryGetAs<HttpCallLogicalOperator>())
            {
                return v.value()->functionArguments();
            }
            return std::nullopt;
        }};
}

bool HttpCallLogicalOperator::operator==(const HttpCallLogicalOperator& o) const
{
    return *body == *o.body && *url == *o.url && timeoutMillis.has_value() == o.timeoutMillis.has_value()
        && (!timeoutMillis || **timeoutMillis == **o.timeoutMillis) && retries.has_value() == o.retries.has_value()
        && (!retries || **retries == **o.retries) && outputSchema == o.outputSchema && traitSet == o.traitSet;
}

void HttpCallLogicalOperator::inferLocalSchema()
{
    const auto input = child.getOutputSchema();
    TypeInferenceContext context{input};
    body = context.inferType(body);
    url = context.inferType(url);
    if (timeoutMillis)
    {
        *timeoutMillis = context.inferType(*timeoutMillis);
    }
    if (retries)
    {
        *retries = context.inferType(*retries);
    }
    validate(body, url, timeoutMillis, retries);
    auto fields = input | RangeUnbinder{} | std::ranges::to<std::vector>();
    fields.emplace_back(Identifier::parse("response"), DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE});
    auto schema = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(fields);
    if (!schema)
    {
        throw CannotInferSchema(
            "HttpCall result fields collide; use a projection to rename fields: {}",
            Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(schema.error()));
    }
    outputSchema = std::move(schema).value();
}

HttpCallLogicalOperator HttpCallLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> c) const
{
    PRECONDITION(c.size() == 1, "HttpCall requires exactly one child");
    auto x = *this;
    x.child = std::move(c.front());
    return x;
}

HttpCallLogicalOperator HttpCallLogicalOperator::withChildren(std::vector<LogicalOperator> c) const
{
    auto x = withChildrenUnsafe(std::move(c));
    x.inferLocalSchema();
    return x;
}

HttpCallLogicalOperator HttpCallLogicalOperator::withInferredSchema() const
{
    return withChildren({child.withInferredSchema()});
}

HttpCallLogicalOperator HttpCallLogicalOperator::withTraitSet(TraitSet t) const
{
    auto x = *this;
    x.traitSet = std::move(t);
    return x;
}

Schema<Field, Unordered> HttpCallLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "HttpCall schema has not been inferred");
    return bindToOperator(self.lock(), *outputSchema);
}

Schema<Field, Ordered> HttpCallLogicalOperator::getOrderedOutputSchema(ChildOutputOrderProvider p) const
{
    auto f = p(child) | RangeUnbinder{} | std::ranges::to<std::vector>();
    f.emplace_back(Identifier::parse("response"), DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE});
    return bindToOperator(self.lock(), Schema<UnqualifiedUnboundField, Ordered>::tryCreateCollisionFree(std::move(f)).value());
}

std::vector<LogicalFunction> HttpCallLogicalOperator::functionArguments() const
{
    std::vector<LogicalFunction> result{body, url};
    if (timeoutMillis)
    {
        result.push_back(*timeoutMillis);
    }
    else if (retries)
    {
        result.emplace_back(
            ConstantValueLogicalFunction{
                DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, std::to_string(HTTP_CALL_DEFAULT_TIMEOUT_MILLIS)});
    }
    if (retries)
    {
        result.push_back(*retries);
    }
    return result;
}

std::string HttpCallLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId) const
{
    std::string result = "ASYNC HTTP_CALL(args: [";
    bool first = true;
    for (const auto& argument : functionArguments())
    {
        if (!first)
        {
            result += ", ";
        }
        first = false;
        result += argument.explain(verbosity);
    }
    return result + "], outputs: [RESPONSE])";
}

Reflected Reflector<TypedLogicalOperator<HttpCallLogicalOperator>>::operator()(
    const TypedLogicalOperator<HttpCallLogicalOperator>& op, const ReflectionContext& c) const
{
    return c.reflect(
        detail::ReflectedHttpCallLogicalOperator{op.getId(), op->getBody(), op->getUrl(), op->getTimeoutMillis(), op->getRetries()});
}

Unreflector<TypedLogicalOperator<HttpCallLogicalOperator>>::Unreflector(ContextType p) : plan(std::move(p))
{
}

TypedLogicalOperator<HttpCallLogicalOperator>
Unreflector<TypedLogicalOperator<HttpCallLogicalOperator>>::operator()(const Reflected& r, const ReflectionContext& c) const
{
    auto v = c.unreflect<detail::ReflectedHttpCallLogicalOperator>(r);
    auto ch = plan->getChildrenFor(v.operatorId, c);
    if (ch.size() != 1)
    {
        throw CannotDeserialize("HttpCall requires exactly one child");
    }
    return HttpCallLogicalOperator::create(
        std::move(ch.front()), std::move(v.body), std::move(v.url), std::move(v.timeoutMillis), std::move(v.retries));
}
}

size_t std::hash<NES::HttpCallLogicalOperator>::operator()(const NES::HttpCallLogicalOperator& op) const noexcept
{
    size_t h = NES::StructuralFunctionHash{}(op.getBody());
    h = folly::hash::hash_combine(h, NES::StructuralFunctionHash{}(op.getUrl()));
    if (op.getTimeoutMillis())
    {
        h = folly::hash::hash_combine(h, NES::StructuralFunctionHash{}(*op.getTimeoutMillis()));
    }
    if (op.getRetries())
    {
        h = folly::hash::hash_combine(h, NES::StructuralFunctionHash{}(*op.getRetries()));
    }
    return h;
}
