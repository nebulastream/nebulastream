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
#include <Sources/AsyncInputAccessor.hpp>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
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

AsyncResultReference::AsyncResultReference(std::shared_ptr<const AsyncCall> p, size_t i) : producer(std::move(p)), index(i)
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

AsyncRowHandle::AsyncRowHandle(std::shared_ptr<const detail::AsyncGraphScope> s,
                               std::function<RuntimeValue(const InputFieldReference&)> reader,
                               std::unordered_map<std::shared_ptr<const AsyncCall>, SharedRuntimeValues> results)
    : scope(std::move(s)), readInput(std::move(reader)), readyResults(std::move(results))
{
}

AsyncInputAccessor::AsyncInputAccessor(RuntimeValue value) : source(std::move(value))
{
}

AsyncInputAccessor::AsyncInputAccessor(InputFieldReference field) : source(std::move(field))
{
}

AsyncInputAccessor::AsyncInputAccessor(AsyncResultReference result) : source(std::move(result))
{
}

DataType AsyncInputAccessor::getDataType() const
{
    return std::visit([](const auto& value) { return value.getDataType(); }, source);
}

RuntimeValue AsyncInputAccessor::operator()(const AsyncRowHandle& row) const
{
    auto result = std::visit(
        [&](const auto& value) -> RuntimeValue
        {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, RuntimeValue>)
            {
                return value;
            }
            else if constexpr (std::is_same_v<T, InputFieldReference>)
            {
                PRECONDITION(row.scope == value.scope, "Input field belongs to another graph");
                PRECONDITION(row.readInput, "No reader for graph input");
                return row.readInput(value);
            }
            else
            {
                const auto producer = value.getProducer();
                PRECONDITION(producer->scope == row.scope, "Result belongs to another graph");
                const auto found = row.readyResults.find(producer);
                PRECONDITION(found != row.readyResults.end(), "Async result is not ready");
                return found->second[value.getOutputIndex()].toOwned();
            }
        },
        source);
    INVARIANT(result.getDataType() == getDataType() && (!result.isNull() || getDataType().nullable),
              "Async accessor value has type {}, expected {}",
              result.getDataType(),
              getDataType());
    return result;
}

std::shared_ptr<const AsyncCall> AsyncInputAccessor::getProducer() const
{
    if (const auto* result = std::get_if<AsyncResultReference>(&source))
    {
        return result->getProducer();
    }
    return {};
}

const AsyncInputAccessorSource& AsyncInputAccessor::getSource() const
{
    return source;
}

Reflected Reflector<InputFieldReference>::operator()(const InputFieldReference& field, const ReflectionContext& context) const
{
    const auto& state = this->state;
    if (field.scope != state.scope)
    {
        throw CannotSerialize("Async accessor input belongs to another graph");
    }
    return context.reflect(field.getField());
}

InputFieldReference Unreflector<InputFieldReference>::operator()(const Reflected& data, const ReflectionContext& context) const
{
    const auto& state = this->state;
    const auto field = context.unreflect<QualifiedIdentifier>(data);
    if (!state.inputNames.contains(field))
    {
        throw CannotDeserialize("Async input field is absent");
    }
    return state.builder.input(field);
}

Reflected Reflector<AsyncResultReference>::operator()(const AsyncResultReference& result, const ReflectionContext& context) const
{
    const auto& state = this->state;
    const auto producer = result.getProducer();
    if (!state.ids.contains(producer) || std::ranges::find(state.dependencies, producer) == state.dependencies.end())
    {
        throw CannotSerialize("Async accessor producer is not a declared dependency");
    }
    return context.reflect(detail::ReflectedAsyncResultReference{state.ids.at(producer), result.getOutputIndex()});
}

AsyncResultReference Unreflector<AsyncResultReference>::operator()(const Reflected& data, const ReflectionContext& context) const
{
    const auto& state = this->state;
    const auto wire = context.unreflect<detail::ReflectedAsyncResultReference>(data);
    const auto producer = state.nodes.find(wire.producer);
    if (!state.declaredDependencies.contains(wire.producer) || producer == state.nodes.end())
    {
        throw CannotDeserialize("Async producer {} is not a declared dependency", wire.producer);
    }
    if (wire.output >= producer->second->getResultTypes().size())
    {
        throw CannotDeserialize("Async producer {} has no output {}", wire.producer, wire.output);
    }
    return producer->second->output(wire.output);
}

Reflected Reflector<AsyncInputAccessor>::operator()(const AsyncInputAccessor& accessor, const ReflectionContext& context) const
{
    return context.reflect(accessor.getSource());
}

AsyncInputAccessor Unreflector<AsyncInputAccessor>::operator()(const Reflected& data, const ReflectionContext& context) const
{
    return std::visit([](auto&& source) { return AsyncInputAccessor{std::forward<decltype(source)>(source)}; },
                      context.unreflect<AsyncInputAccessorSource>(data));
}
}
