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
#include <Sources/AsyncCallGraph.hpp>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncOperatorReflection.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace detail
{
struct AsyncGraphScope
{
    Schema<QualifiedUnboundField, Ordered> inputSchema;
};

struct WireCall
{
    std::string id;
    Reflected asyncOperator;
    std::vector<std::string> dependencies;
};

struct WireOutput
{
    ReflectedAsyncResultReference result;
    QualifiedIdentifier field;
};

struct WireGraph
{
    Schema<QualifiedUnboundField, Ordered> inputSchema;
    std::vector<WireCall> calls;
    std::vector<std::string> required;
    std::vector<WireOutput> outputs;
};

bool validRuntimeType(DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
        case DataType::Type::VARSIZED:
            return true;
        case DataType::Type::UNDEFINED:
            return false;
    }
    return false;
}
}

InputFieldReference::InputFieldReference(std::shared_ptr<const detail::AsyncGraphScope> s, QualifiedIdentifier f)
    : scope(std::move(s)), field(std::move(f))
{
}

DataType InputFieldReference::getDataType() const
{
    const auto fieldValue = scope->inputSchema[field];
    INVARIANT(fieldValue.has_value(), "Async input field {} must belong to its graph", field);
    return fieldValue->getDataType();
}

AsyncCall::AsyncCall(std::shared_ptr<const detail::AsyncGraphScope> s, AsyncOperator op, std::vector<Ptr> deps)
    : scope(std::move(s)), asyncOperator(std::move(op)), dependencies(std::move(deps))
{
    for (const auto& field : asyncOperator.getWrittenFields())
    {
        resultTypes.push_back(field.getDataType());
    }
}

std::string AsyncCall::getFunctionType() const
{
    return asyncOperator.getType();
}

const AsyncOperator& AsyncCall::getAsyncOperator() const
{
    return asyncOperator;
}

const std::vector<DataType>& AsyncCall::getResultTypes() const
{
    return resultTypes;
}

const std::vector<AsyncCall::Ptr>& AsyncCall::getDependencies() const
{
    return dependencies;
}

AsyncResultReference AsyncCall::output(size_t index) const
{
    PRECONDITION(index < resultTypes.size(), "Async call has no output {}", index);
    return AsyncResultReference{shared_from_this(), index};
}

AsyncCallGraph::AsyncCallGraph(std::shared_ptr<const detail::AsyncGraphScope> s,
                               std::vector<AsyncCall::Ptr> c,
                               std::vector<AsyncOutputMapping> o,
                               std::vector<AsyncCall::Ptr> r)
    : scope(std::move(s)), calls(std::move(c)), outputs(std::move(o)), requiredCalls(std::move(r))
{
}

const Schema<QualifiedUnboundField, Ordered>& AsyncCallGraph::getInputSchema() const
{
    return scope->inputSchema;
}

const std::vector<AsyncCall::Ptr>& AsyncCallGraph::getCalls() const
{
    return calls;
}

const std::vector<AsyncOutputMapping>& AsyncCallGraph::getOutputs() const
{
    return outputs;
}

const std::vector<AsyncCall::Ptr>& AsyncCallGraph::getRequiredCalls() const
{
    return requiredCalls;
}

AsyncRowHandle AsyncCallGraph::createRowHandle(std::function<RuntimeValue(const InputFieldReference&)> reader,
                                               std::unordered_map<AsyncCall::Ptr, SharedRuntimeValues> results) const
{
    return AsyncRowHandle{scope, std::move(reader), std::move(results)};
}

AsyncCallGraphBuilder::AsyncCallGraphBuilder(Schema<QualifiedUnboundField, Ordered> schema)
    : scope(std::make_shared<const detail::AsyncGraphScope>(detail::AsyncGraphScope{std::move(schema)}))
{
    IF_PRECONDITION(std::unordered_set<QualifiedIdentifier> names;)
    for (const auto& field : scope->inputSchema)
    {
        visitRuntimeType(field.getDataType().type, []<typename>() { });
        PRECONDITION(names.insert(field.getFullyQualifiedName()).second, "Duplicate async input field {}", field.getFullyQualifiedName());
    }
}

void AsyncCallGraphBuilder::requireOpen() const
{
    PRECONDITION(!built, "Async graph builder already finalized");
}

InputFieldReference AsyncCallGraphBuilder::input(QualifiedIdentifier field) const
{
    requireOpen();
    PRECONDITION(std::ranges::any_of(scope->inputSchema, [&](const auto& f) { return f.getFullyQualifiedName() == field; }),
                 "Unknown input field {}",
                 field);
    return InputFieldReference{scope, std::move(field)};
}

AsyncCall::Ptr AsyncCallGraphBuilder::call(AsyncOperator op, std::vector<AsyncCall::Ptr> deps)
{
    requireOpen();
    PRECONDITION(!op.getType().empty() && op.getWrittenFields().size() != 0, "An async call needs a function and at least one output type");
    IF_PRECONDITION(std::unordered_set<AsyncCall::Ptr> unique;)
    for (const auto& dep : deps)
    {
        PRECONDITION(dep && dep->scope == scope, "Dependency belongs to another graph");
        PRECONDITION(unique.insert(dep).second, "Duplicate async dependency");
    }
    for (const auto& field : op.getWrittenFields())
    {
        visitRuntimeType(field.getDataType().type, []<typename>() { });
    }
    auto node = std::shared_ptr<AsyncCall>{new AsyncCall{scope, std::move(op), std::move(deps)}};
    calls.push_back(node);
    return node;
}

void AsyncCallGraphBuilder::require(AsyncCall::Ptr node)
{
    requireOpen();
    PRECONDITION(node && node->scope == scope, "Required call belongs to another graph");
    if (std::ranges::find(requiredCalls, node) == requiredCalls.end())
    {
        requiredCalls.push_back(std::move(node));
    }
}

void AsyncCallGraphBuilder::exportOutput(AsyncResultReference result, QualifiedIdentifier field)
{
    requireOpen();
    PRECONDITION(result.getProducer()->scope == scope, "Cannot export foreign result");
    PRECONDITION(
        std::ranges::none_of(outputs, [&](const auto& output) { return output.field == field; }), "Duplicate async output field {}", field);
    outputs.push_back({std::move(result), std::move(field)});
}

AsyncCallGraph AsyncCallGraphBuilder::build()
{
    requireOpen();
    std::unordered_set<AsyncCall::Ptr> reachable;
    auto visit = [&](auto&& self, const AsyncCall::Ptr& n) -> void
    {
        if (!reachable.insert(n).second)
        {
            return;
        }
        for (const auto& d : n->getDependencies())
        {
            self(self, d);
        }
    };
    for (const auto& r : requiredCalls)
    {
        visit(visit, r);
    }
    for (const auto& o : outputs)
    {
        visit(visit, o.result.getProducer());
    }
    std::erase_if(calls, [&](const auto& c) { return !reachable.contains(c); });
    built = true;
    return AsyncCallGraph{scope, std::move(calls), std::move(outputs), std::move(requiredCalls)};
}

Reflected Reflector<AsyncCallGraph>::operator()(const AsyncCallGraph& graph, const ReflectionContext& context) const
{
    std::unordered_map<AsyncCall::Ptr, std::string> ids;
    for (size_t i = 0; i < graph.getCalls().size(); ++i)
    {
        ids.emplace(graph.getCalls()[i], "node-" + std::to_string(i));
    }
    const auto encodeResult
        = [&](const AsyncResultReference& r) { return detail::ReflectedAsyncResultReference{ids.at(r.getProducer()), r.getOutputIndex()}; };
    detail::WireGraph wire{.inputSchema = graph.getInputSchema(), .calls = {}, .required = {}, .outputs = {}};
    for (const auto& call : graph.getCalls())
    {
        std::vector<std::string> deps;
        for (const auto& d : call->getDependencies())
        {
            deps.push_back(ids.at(d));
        }
        const AsyncReferenceSerializationContext references{graph.scope, ids, call->getDependencies()};
        const auto scoped = context.with(references);
        wire.calls.push_back({ids.at(call), scoped.reflect(call->getAsyncOperator()), std::move(deps)});
    }
    for (const auto& root : graph.getRequiredCalls())
    {
        wire.required.push_back(ids.at(root));
    }
    for (const auto& output : graph.getOutputs())
    {
        wire.outputs.push_back({encodeResult(output.result), output.field});
    }
    return context.reflect(wire);
}

AsyncCallGraph Unreflector<AsyncCallGraph>::operator()(const Reflected& data, const ReflectionContext& context) const
{
    const auto wire = context.unreflect<detail::WireGraph>(data);
    std::unordered_map<std::string, const detail::WireCall*> defs;
    for (const auto& call : wire.calls)
    {
        if (call.id.empty() || !defs.emplace(call.id, &call).second)
        {
            throw CannotDeserialize("Duplicate or empty async call ID {}", call.id);
        }
    }
    std::unordered_set<QualifiedIdentifier> inputNames;
    for (const auto& field : wire.inputSchema)
    {
        if (!inputNames.insert(field.getFullyQualifiedName()).second || !detail::validRuntimeType(field.getDataType().type))
        {
            throw CannotDeserialize("Invalid or duplicate async input field");
        }
    }
    for (const auto& call : wire.calls)
    {
        std::unordered_set<std::string> unique;
        for (const auto& id : call.dependencies)
        {
            if (!defs.contains(id) || !unique.insert(id).second)
            {
                throw CannotDeserialize("Dangling or duplicate async dependency {}", id);
            }
        }
    }
    std::unordered_set<std::string> done, active;
    std::vector<std::string> order;
    auto visit = [&](auto&& self, const std::string& id) -> void
    {
        if (done.contains(id))
        {
            return;
        }
        if (!active.insert(id).second)
        {
            throw CannotDeserialize("Cycle in async dependency graph at {}", id);
        }
        for (const auto& dep : defs.at(id)->dependencies)
        {
            self(self, dep);
        }
        active.erase(id);
        done.insert(id);
        order.push_back(id);
    };
    for (const auto& call : wire.calls)
    {
        visit(visit, call.id);
    }
    std::unordered_set<std::string> requiredIds;
    for (const auto& id : wire.required)
    {
        if (!defs.contains(id) || !requiredIds.insert(id).second)
        {
            throw CannotDeserialize("Dangling or duplicate required async root {}", id);
        }
    }
    for (const auto& output : wire.outputs)
    {
        if (!defs.contains(output.result.producer))
        {
            throw CannotDeserialize("Dangling output producer {}", output.result.producer);
        }
    }

    AsyncCallGraphBuilder builder{wire.inputSchema};
    std::unordered_map<std::string, AsyncCall::Ptr> nodes;
    for (const auto& id : order)
    {
        const auto& call = *defs.at(id);
        const std::unordered_set<std::string> declared(call.dependencies.begin(), call.dependencies.end());
        const AsyncReferenceDeserializationContext references{builder, inputNames, declared, nodes};
        const auto scoped = context.with(references);
        AsyncOperator op = scoped.unreflect<AsyncOperator>(call.asyncOperator);
        const auto writtenFields = op.getWrittenFields();
        if (op.getType().empty() || writtenFields.size() == 0)
        {
            throw CannotDeserialize("Async call needs a function and at least one output");
        }
        for (const auto& field : writtenFields)
        {
            if (!detail::validRuntimeType(field.getDataType().type))
            {
                throw CannotDeserialize("Invalid async output type");
            }
        }
        std::vector<AsyncCall::Ptr> dependencies;
        for (const auto& dep : call.dependencies)
        {
            dependencies.push_back(nodes.at(dep));
        }
        auto node = builder.call(std::move(op), std::move(dependencies));
        nodes.emplace(id, std::move(node));
    }
    for (const auto& id : wire.required)
    {
        builder.require(nodes.at(id));
    }
    std::unordered_set<QualifiedIdentifier> outputNames;
    for (const auto& output : wire.outputs)
    {
        if (!outputNames.insert(output.field).second)
        {
            throw CannotDeserialize("Duplicate async output field {}", output.field);
        }
        const auto& producer = nodes.at(output.result.producer);
        if (output.result.output >= producer->getResultTypes().size())
        {
            throw CannotDeserialize("Async producer {} has no output {}", output.result.producer, output.result.output);
        }
        builder.exportOutput(producer->output(output.result.output), output.field);
    }
    return builder.build();
}
}
