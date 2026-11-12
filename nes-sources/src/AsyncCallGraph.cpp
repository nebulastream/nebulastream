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
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace detail
{
struct AsyncCallScope
{
    Schema<QualifiedUnboundField, Ordered> inputSchema;
};

/// IDs are strictly a wire-format concern. They are resolved/validated before a graph can be used.
struct WireAsyncResult
{
    std::string producer;
    size_t output;
};

using WireAsyncArgument = std::variant<RuntimeValue, QualifiedIdentifier, WireAsyncResult>;

struct WireAsyncCall
{
    std::string id;
    std::string functionType;
    std::vector<DataType> resultTypes;
    std::vector<WireAsyncArgument> arguments;
};

struct WireAsyncOutput
{
    WireAsyncResult result;
    QualifiedIdentifier field;
};

struct WireAsyncGraph
{
    Schema<QualifiedUnboundField, Ordered> inputSchema;
    std::vector<WireAsyncCall> calls;
    std::vector<WireAsyncOutput> outputs;
};
}

InputFieldReference::InputFieldReference(std::shared_ptr<const detail::AsyncCallScope> scope, QualifiedIdentifier field)
    : scope(std::move(scope)), field(std::move(field))
{
}

QualifiedIdentifier InputFieldReference::getField() const
{
    return field;
}

DataType InputFieldReference::getDataType() const
{
    return scope->inputSchema[field].value().getDataType();
}

AsyncResultReference::AsyncResultReference(std::shared_ptr<const AsyncCall> producer, const size_t index)
    : producer(std::move(producer)), index(index)
{
}

AsyncCall::Ptr AsyncResultReference::getProducer() const
{
    return producer;
}

size_t AsyncResultReference::getOutputIndex() const
{
    return index;
}

DataType AsyncResultReference::getDataType() const
{
    return producer->getResultTypes().at(index);
}

AsyncCall::AsyncCall(
    std::shared_ptr<const detail::AsyncCallScope> scope,
    std::string functionType,
    std::vector<AsyncCallArgument> arguments,
    std::vector<DataType> resultTypes)
    : scope(std::move(scope)), functionType(std::move(functionType)), arguments(std::move(arguments)), resultTypes(std::move(resultTypes))
{
}

const std::string& AsyncCall::getFunctionType() const
{
    return functionType;
}

const std::vector<DataType>& AsyncCall::getResultTypes() const
{
    return resultTypes;
}

const std::vector<AsyncCallArgument>& AsyncCall::getArguments() const
{
    return arguments;
}

AsyncResultReference AsyncCall::output(const size_t index) const
{
    if (index >= resultTypes.size())
    {
        throw InvalidQuerySyntax("Async call {} has no output {}", functionType, index);
    }
    return AsyncResultReference{shared_from_this(), index};
}

AsyncCallGraph::AsyncCallGraph(
    std::shared_ptr<const detail::AsyncCallScope> scope, std::vector<AsyncCall::Ptr> calls, std::vector<AsyncCallOutput> outputs)
    : scope(std::move(scope)), calls(std::move(calls)), outputs(std::move(outputs))
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

const std::vector<AsyncCallOutput>& AsyncCallGraph::getOutputs() const
{
    return outputs;
}

AsyncCallGraphBuilder::AsyncCallGraphBuilder(Schema<QualifiedUnboundField, Ordered> inputSchema)
    : scope(std::make_shared<const detail::AsyncCallScope>(detail::AsyncCallScope{std::move(inputSchema)}))
{
    std::unordered_set<QualifiedIdentifier> names;
    for (const auto& field : scope->inputSchema)
    {
        visitRuntimeType(field.getDataType().type, []<typename>() { });
        if (!names.insert(field.getFullyQualifiedName()).second)
        {
            throw InvalidQuerySyntax("Duplicate async input field {}", field.getFullyQualifiedName());
        }
    }
}

void AsyncCallGraphBuilder::requireOpen() const
{
    if (built)
    {
        throw InvalidQuerySyntax("Async graph builder has already been finalized");
    }
}

InputFieldReference AsyncCallGraphBuilder::input(QualifiedIdentifier field) const
{
    requireOpen();
    if (std::ranges::none_of(scope->inputSchema, [&](const auto& input) { return input.getFullyQualifiedName() == field; }))
    {
        throw InvalidQuerySyntax("Async input field {} is not present in this graph's input schema", field);
    }
    return InputFieldReference{scope, std::move(field)};
}

AsyncCall::Ptr
AsyncCallGraphBuilder::call(std::string functionType, std::vector<AsyncCallArgument> arguments, std::vector<DataType> resultTypes)
{
    requireOpen();
    if (functionType.empty() || resultTypes.empty())
    {
        throw InvalidQuerySyntax("An async call needs a function and at least one output type");
    }
    for (const auto type : resultTypes)
    {
        visitRuntimeType(type.type, []<typename>() { });
    }
    for (const auto& argument : arguments)
    {
        std::visit(
            [&]<typename T>(const T& value)
            {
                if constexpr (std::is_same_v<T, InputFieldReference>)
                {
                    if (value.scope != scope)
                    {
                        throw InvalidQuerySyntax("Async input reference belongs to another graph's input scope");
                    }
                }
                else if constexpr (std::is_same_v<T, AsyncResultReference>)
                {
                    if (value.getProducer()->scope != scope)
                    {
                        throw InvalidQuerySyntax("Async result reference belongs to another graph");
                    }
                }
            },
            argument);
    }
    auto node = std::shared_ptr<AsyncCall>{new AsyncCall{scope, std::move(functionType), std::move(arguments), std::move(resultTypes)}};
    calls.push_back(node);
    return node;
}

void AsyncCallGraphBuilder::exportOutput(AsyncResultReference result, QualifiedIdentifier field)
{
    requireOpen();
    if (result.getProducer()->scope != scope)
    {
        throw InvalidQuerySyntax("Cannot export an async result from another graph");
    }
    if (std::ranges::any_of(outputs, [&](const auto& output) { return output.field == field; }))
    {
        throw InvalidQuerySyntax("Duplicate async output field {}", field);
    }
    outputs.push_back({std::move(result), std::move(field)});
}

AsyncCallGraph AsyncCallGraphBuilder::build()
{
    requireOpen();
    std::unordered_set<AsyncCall::Ptr> reachable;
    const auto visit = [&](this const auto& self, const AsyncCall::Ptr& node) -> void
    {
        if (!reachable.insert(node).second)
        {
            return;
        }
        for (const auto& argument : node->getArguments())
        {
            if (const auto* reference = std::get_if<AsyncResultReference>(&argument))
            {
                self(reference->getProducer());
            }
        }
    };
    for (const auto& output : outputs)
    {
        visit(output.result.getProducer());
    }
    std::erase_if(calls, [&](const auto& node) { return !reachable.contains(node); });
    built = true;
    return AsyncCallGraph{scope, std::move(calls), std::move(outputs)};
}

Reflected Reflector<AsyncCallGraph>::operator()(const AsyncCallGraph& graph, const ReflectionContext& context) const
{
    std::unordered_map<AsyncCall::Ptr, std::string> ids;
    for (const auto& node : graph.getCalls())
    {
        ids.emplace(node, "node-" + std::to_string(ids.size()));
    }
    detail::WireAsyncGraph wire{.inputSchema = graph.getInputSchema(), .calls = {}, .outputs = {}};
    const auto result
        = [&](const AsyncResultReference& value) { return detail::WireAsyncResult{ids.at(value.getProducer()), value.getOutputIndex()}; };
    for (const auto& node : graph.getCalls())
    {
        detail::WireAsyncCall call{
            .id = ids.at(node), .functionType = node->getFunctionType(), .resultTypes = node->getResultTypes(), .arguments = {}};
        for (const auto& argument : node->getArguments())
        {
            call.arguments.push_back(
                std::visit(
                    [&]<typename T>(const T& value) -> detail::WireAsyncArgument
                    {
                        if constexpr (std::is_same_v<T, RuntimeValue>)
                        {
                            return value;
                        }
                        else if constexpr (std::is_same_v<T, InputFieldReference>)
                        {
                            return value.getField();
                        }
                        else
                        {
                            return result(value);
                        }
                    },
                    argument));
        }
        wire.calls.push_back(std::move(call));
    }
    for (const auto& output : graph.getOutputs())
    {
        wire.outputs.push_back({result(output.result), output.field});
    }
    return context.reflect(wire);
}

AsyncCallGraph Unreflector<AsyncCallGraph>::operator()(const Reflected& data, const ReflectionContext& context) const
{
    const auto wire = context.unreflect<detail::WireAsyncGraph>(data);
    AsyncCallGraphBuilder builder{wire.inputSchema};
    std::unordered_map<std::string, const detail::WireAsyncCall*> definitions;
    for (const auto& call : wire.calls)
    {
        if (call.id.empty() || !definitions.emplace(call.id, &call).second)
        {
            throw CannotDeserialize("Duplicate or empty async call ID {}", call.id);
        }
    }
    std::unordered_map<std::string, AsyncCall::Ptr> resolved;
    std::unordered_set<std::string> resolving;
    const auto resolve = [&](this const auto& self, const std::string& id) -> AsyncCall::Ptr
    {
        if (const auto found = resolved.find(id); found != resolved.end())
        {
            return found->second;
        }
        const auto definition = definitions.find(id);
        if (definition == definitions.end())
        {
            throw CannotDeserialize("Dangling async producer reference {}", id);
        }
        if (!resolving.insert(id).second)
        {
            throw CannotDeserialize("Cycle in async dependency graph at {}", id);
        }
        const auto& call = *definition->second;
        std::vector<AsyncCallArgument> arguments;
        for (const auto& argument : call.arguments)
        {
            arguments.push_back(
                std::visit(
                    [&]<typename T>(const T& value) -> AsyncCallArgument
                    {
                        if constexpr (std::is_same_v<T, RuntimeValue>)
                        {
                            return value;
                        }
                        else if constexpr (std::is_same_v<T, QualifiedIdentifier>)
                        {
                            return builder.input(value);
                        }
                        else
                        {
                            return self(value.producer)->output(value.output);
                        }
                    },
                    argument));
        }
        auto node = builder.call(call.functionType, std::move(arguments), call.resultTypes);
        resolving.erase(id);
        resolved.emplace(id, node);
        return node;
    };
    /// Validate even unreachable definitions; malformed cycles must not hide in an unused branch.
    for (const auto& call : wire.calls)
    {
        resolve(call.id);
    }
    for (const auto& output : wire.outputs)
    {
        builder.exportOutput(resolve(output.result.producer)->output(output.result.output), output.field);
    }
    return builder.build();
}

}
