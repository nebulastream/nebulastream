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
#include <Sources/AsyncOperatorReflection.hpp>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace detail
{
struct AsyncGraphScope
{
    Schema<QualifiedUnboundField, Ordered> inputSchema;
};

namespace
{
/// IDs are strictly a wire-format concern. They are resolved/validated before a graph can be used.
struct ReflectedAsyncResultReference
{
    std::string producer;
    size_t output;
};

using ReflectedAsyncArgument = std::variant<RuntimeValue, QualifiedIdentifier, ReflectedAsyncResultReference>;

bool hasRuntimeType(const DataType::Type type)
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

struct ReflectedAsyncCall
{
    std::string id;
    AsyncOperator asyncOperator;
    std::vector<ReflectedAsyncArgument> arguments;
};

struct ReflectedAsyncOutput
{
    ReflectedAsyncResultReference result;
    QualifiedIdentifier field;
};

struct ReflectedAsyncGraph
{
    Schema<QualifiedUnboundField, Ordered> inputSchema;
    std::vector<ReflectedAsyncCall> calls;
    std::vector<ReflectedAsyncOutput> outputs;
};
}
}

InputFieldReference::InputFieldReference(std::shared_ptr<const detail::AsyncGraphScope> scope, QualifiedIdentifier field)
    : scope(std::move(scope)), field(std::move(field))
{
}

InputFieldReference::InputFieldReference(InputFieldReference&& other) noexcept(std::is_nothrow_copy_constructible_v<QualifiedIdentifier>)
    : InputFieldReference(static_cast<const InputFieldReference&>(other))
{
}

InputFieldReference&
InputFieldReference::operator=(InputFieldReference&& other) noexcept(std::is_nothrow_copy_assignable_v<QualifiedIdentifier>)
{
    return *this = static_cast<const InputFieldReference&>(other);
}

QualifiedIdentifier InputFieldReference::getField() const
{
    return field;
}

DataType InputFieldReference::getDataType() const
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) schema lookup returns an optional
    const auto input = scope->inputSchema[field];
    INVARIANT(input.has_value(), "Async input field {} must belong to its graph's input schema", field);
    return input->getDataType();
}

AsyncResultReference::AsyncResultReference(std::shared_ptr<const AsyncCall> producer, const size_t index)
    : producer(std::move(producer)), index(index)
{
}

AsyncResultReference::AsyncResultReference(AsyncResultReference&& other) noexcept
    : AsyncResultReference(static_cast<const AsyncResultReference&>(other))
{
}

AsyncResultReference& AsyncResultReference::operator=(AsyncResultReference&& other) noexcept
{
    return *this = static_cast<const AsyncResultReference&>(other);
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
    std::shared_ptr<const detail::AsyncGraphScope> scope, AsyncOperator asyncOperator, std::vector<AsyncCallArgument> arguments)
    : scope(std::move(scope)), asyncOperator(std::move(asyncOperator)), arguments(std::move(arguments))
{
    std::vector<DataType> inputTypes;
    inputTypes.reserve(this->arguments.size());
    for (const auto& argument : this->arguments)
    {
        inputTypes.push_back(std::visit([](const auto& value) { return value.getDataType(); }, argument));
    }
    this->asyncOperator = this->asyncOperator.withInputAccessors(AsyncInputAccessors{std::move(inputTypes)});
    for (const auto& field : this->asyncOperator.getOutputSchema())
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

const std::vector<AsyncCallArgument>& AsyncCall::getArguments() const
{
    return arguments;
}

AsyncResultReference AsyncCall::output(const size_t index) const
{
    PRECONDITION(index < resultTypes.size(), "Async call {} has no output {}", getFunctionType(), index);
    return AsyncResultReference{shared_from_this(), index};
}

AsyncCallGraph::AsyncCallGraph(
    std::shared_ptr<const detail::AsyncGraphScope> scope, std::vector<AsyncCall::Ptr> calls, std::vector<AsyncOutputMapping> outputs)
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

const std::vector<AsyncOutputMapping>& AsyncCallGraph::getOutputs() const
{
    return outputs;
}

AsyncCallGraphBuilder::AsyncCallGraphBuilder(Schema<QualifiedUnboundField, Ordered> inputSchema)
    : scope(std::make_shared<const detail::AsyncGraphScope>(detail::AsyncGraphScope{std::move(inputSchema)}))
{
    std::unordered_set<QualifiedIdentifier> names;
    for (const auto& field : scope->inputSchema)
    {
        visitRuntimeType(field.getDataType().type, []<typename>() { });
        PRECONDITION(names.insert(field.getFullyQualifiedName()).second, "Duplicate async input field {}", field.getFullyQualifiedName());
    }
}

void AsyncCallGraphBuilder::requireOpen() const
{
    PRECONDITION(!built, "Async graph builder has already been finalized");
}

InputFieldReference AsyncCallGraphBuilder::input(QualifiedIdentifier field) const
{
    requireOpen();
    PRECONDITION(
        std::ranges::any_of(scope->inputSchema, [&](const auto& input) { return input.getFullyQualifiedName() == field; }),
        "Async input field {} is not present in this graph's input schema",
        field);
    return InputFieldReference{scope, std::move(field)};
}

AsyncCall::Ptr AsyncCallGraphBuilder::call(AsyncOperator asyncOperator, std::vector<AsyncCallArgument> arguments)
{
    requireOpen();
    /// NOLINTNEXTLINE(readability-simplify-boolean-expr) state the nonempty function and output requirements directly
    PRECONDITION(
        !asyncOperator.getType().empty() && asyncOperator.getOutputSchema().size() != 0,
        "An async call needs a function and at least one output type");
    for (const auto& field : asyncOperator.getOutputSchema())
    {
        visitRuntimeType(field.getDataType().type, []<typename>() { });
    }
    for (const auto& argument : arguments)
    {
        std::visit(
            [&]<typename T>(const T& IF_PRECONDITION(value))
            {
                if constexpr (std::is_same_v<T, InputFieldReference>)
                {
                    PRECONDITION(value.scope == scope, "Async input reference belongs to another graph's input scope");
                }
                else if constexpr (std::is_same_v<T, AsyncResultReference>)
                {
                    PRECONDITION(value.getProducer()->scope == scope, "Async result reference belongs to another graph");
                }
            },
            argument);
    }
    auto node = std::shared_ptr<AsyncCall>{new AsyncCall{scope, std::move(asyncOperator), std::move(arguments)}};
    calls.push_back(node);
    return node;
}

void AsyncCallGraphBuilder::exportOutput(AsyncResultReference result, QualifiedIdentifier field)
{
    requireOpen();
    PRECONDITION(result.getProducer()->scope == scope, "Cannot export an async result from another graph");
    PRECONDITION(
        std::ranges::none_of(outputs, [&](const auto& output) { return output.field == field; }), "Duplicate async output field {}", field);
    outputs.push_back({.result = result, .field = std::move(field)});
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
    detail::ReflectedAsyncGraph wire{.inputSchema = graph.getInputSchema(), .calls = {}, .outputs = {}};
    const auto result = [&](const AsyncResultReference& value)
    { return detail::ReflectedAsyncResultReference{.producer = ids.at(value.getProducer()), .output = value.getOutputIndex()}; };
    for (const auto& node : graph.getCalls())
    {
        detail::ReflectedAsyncCall call{.id = ids.at(node), .asyncOperator = node->getAsyncOperator(), .arguments = {}};
        for (const auto& argument : node->getArguments())
        {
            call.arguments.push_back(
                std::visit(
                    [&]<typename T>(const T& value) -> detail::ReflectedAsyncArgument
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
        wire.outputs.push_back({.result = result(output.result), .field = output.field});
    }
    return context.reflect(wire);
}

AsyncCallGraph Unreflector<AsyncCallGraph>::operator()(const Reflected& data, const ReflectionContext& context) const
{
    const auto wire = context.unreflect<detail::ReflectedAsyncGraph>(data);
    /// Wire data is untrusted: validate all builder preconditions before using its contract-only API.
    std::unordered_set<QualifiedIdentifier> inputNames;
    for (const auto& field : wire.inputSchema)
    {
        if (!inputNames.insert(field.getFullyQualifiedName()).second)
        {
            throw CannotDeserialize("Duplicate async input field {}", field.getFullyQualifiedName());
        }
        if (!detail::hasRuntimeType(field.getDataType().type))
        {
            throw CannotDeserialize("Invalid async input field type");
        }
    }
    std::unordered_set<QualifiedIdentifier> outputNames;
    for (const auto& output : wire.outputs)
    {
        if (!outputNames.insert(output.field).second)
        {
            throw CannotDeserialize("Duplicate async output field {}", output.field);
        }
    }
    for (const auto& call : wire.calls)
    {
        if (call.asyncOperator.getType().empty() || call.asyncOperator.getOutputSchema().size() == 0)
        {
            throw CannotDeserialize("Async call needs a function and at least one output type");
        }
        for (const auto& field : call.asyncOperator.getOutputSchema())
        {
            if (!detail::hasRuntimeType(field.getDataType().type))
            {
                throw CannotDeserialize("Invalid async call output type");
            }
        }
        for (const auto& argument : call.arguments)
        {
            if (const auto* input = std::get_if<QualifiedIdentifier>(&argument); (input != nullptr) && !inputNames.contains(*input))
            {
                throw CannotDeserialize("Async input field {} is absent", *input);
            }
        }
    }
    AsyncCallGraphBuilder builder{wire.inputSchema};
    std::unordered_map<std::string, const detail::ReflectedAsyncCall*> definitions;
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
        arguments.reserve(call.arguments.size());
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
                            const auto producer = self(value.producer);
                            if (value.output >= producer->getResultTypes().size())
                            {
                                throw CannotDeserialize("Async producer {} has no output {}", value.producer, value.output);
                            }
                            return producer->output(value.output);
                        }
                    },
                    argument));
        }
        auto node = builder.call(call.asyncOperator, std::move(arguments));
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
        const auto producer = resolve(output.result.producer);
        if (output.result.output >= producer->getResultTypes().size())
        {
            throw CannotDeserialize("Async producer {} has no output {}", output.result.producer, output.result.output);
        }
        builder.exportOutput(producer->output(output.result.output), output.field);
    }
    return builder.build();
}

}
