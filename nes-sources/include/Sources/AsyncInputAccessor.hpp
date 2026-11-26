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
#pragma once
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Util/Reflection.hpp>

namespace NES
{
namespace detail
{
struct AsyncGraphScope;

struct ReflectedAsyncResultReference
{
    std::string producer;
    size_t output;
};
}
class AsyncCall;
class AsyncCallGraphBuilder;
class AsyncCallGraph;
class AsyncRowHandle;

class InputFieldReference
{
public:
    InputFieldReference(const InputFieldReference&) = default;
    InputFieldReference& operator=(const InputFieldReference&) = default;
    InputFieldReference(InputFieldReference&&) noexcept(std::is_nothrow_copy_constructible_v<QualifiedIdentifier>);
    InputFieldReference& operator=(InputFieldReference&&) noexcept(std::is_nothrow_copy_assignable_v<QualifiedIdentifier>);
    ~InputFieldReference() = default;
    [[nodiscard]] QualifiedIdentifier getField() const;
    [[nodiscard]] DataType getDataType() const;

private:
    friend class AsyncCallGraphBuilder;
    friend class AsyncInputAccessor;
    friend struct Reflector<InputFieldReference>;
    InputFieldReference(std::shared_ptr<const detail::AsyncGraphScope>, QualifiedIdentifier);
    std::shared_ptr<const detail::AsyncGraphScope> scope;
    QualifiedIdentifier field;
};

class AsyncResultReference
{
public:
    AsyncResultReference(const AsyncResultReference&) = default;
    AsyncResultReference& operator=(const AsyncResultReference&) = default;
    AsyncResultReference(AsyncResultReference&&) noexcept;
    AsyncResultReference& operator=(AsyncResultReference&&) noexcept;
    ~AsyncResultReference() = default;
    [[nodiscard]] std::shared_ptr<const AsyncCall> getProducer() const;
    [[nodiscard]] size_t getOutputIndex() const;
    [[nodiscard]] DataType getDataType() const;

private:
    friend class AsyncCall;
    friend class AsyncInputAccessor;
    AsyncResultReference(std::shared_ptr<const AsyncCall>, size_t);
    std::shared_ptr<const AsyncCall> producer;
    size_t index;
};

using AsyncInputAccessorSource = std::variant<RuntimeValue, InputFieldReference, AsyncResultReference>;

class AsyncRowHandle
{
public:
    AsyncRowHandle() = default;

private:
    friend class AsyncInputAccessor;
    friend class AsyncCallGraph;
    AsyncRowHandle(std::shared_ptr<const detail::AsyncGraphScope>,
                   std::function<RuntimeValue(const InputFieldReference&)>,
                   std::unordered_map<std::shared_ptr<const AsyncCall>, SharedRuntimeValues>);
    std::shared_ptr<const detail::AsyncGraphScope> scope;
    std::function<RuntimeValue(const InputFieldReference&)> readInput;
    std::unordered_map<std::shared_ptr<const AsyncCall>, SharedRuntimeValues> readyResults;
};

class AsyncInputAccessor
{
public:
    explicit AsyncInputAccessor(RuntimeValue value);
    explicit AsyncInputAccessor(InputFieldReference field);
    explicit AsyncInputAccessor(AsyncResultReference result);
    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] RuntimeValue operator()(const AsyncRowHandle&) const;
    [[nodiscard]] std::shared_ptr<const AsyncCall> getProducer() const;
    [[nodiscard]] const AsyncInputAccessorSource& getSource() const;

private:
    AsyncInputAccessorSource source;
};

/// Scoped data for encoding references in one concrete call's configuration.
struct AsyncReferenceSerializationContext
{
    std::shared_ptr<const detail::AsyncGraphScope> scope;
    const std::unordered_map<std::shared_ptr<const AsyncCall>, std::string>& ids;
    const std::vector<std::shared_ptr<const AsyncCall>>& dependencies;
};

/// All members are required; references are resolved synchronously while the graph is decoded.
struct AsyncReferenceDeserializationContext
{
    AsyncCallGraphBuilder& builder;
    const std::unordered_set<QualifiedIdentifier>& inputNames;
    const std::unordered_set<std::string>& declaredDependencies;
    const std::unordered_map<std::string, std::shared_ptr<const AsyncCall>>& nodes;
};

template <>
struct Reflector<InputFieldReference>
{
    using ContextType = AsyncReferenceSerializationContext;

    explicit Reflector(ContextType state) : state(std::move(state)) { }

    Reflected operator()(const InputFieldReference&, const ReflectionContext&) const;

private:
    ContextType state;
};

template <>
struct Unreflector<InputFieldReference>
{
    using ContextType = AsyncReferenceDeserializationContext;

    explicit Unreflector(ContextType state) : state(std::move(state)) { }

    InputFieldReference operator()(const Reflected&, const ReflectionContext&) const;

private:
    ContextType state;
};

template <>
struct Reflector<AsyncResultReference>
{
    using ContextType = AsyncReferenceSerializationContext;

    explicit Reflector(ContextType state) : state(std::move(state)) { }

    Reflected operator()(const AsyncResultReference&, const ReflectionContext&) const;

private:
    ContextType state;
};

template <>
struct Unreflector<AsyncResultReference>
{
    using ContextType = AsyncReferenceDeserializationContext;

    explicit Unreflector(ContextType state) : state(std::move(state)) { }

    AsyncResultReference operator()(const Reflected&, const ReflectionContext&) const;

private:
    ContextType state;
};

template <>
struct Reflector<AsyncInputAccessor>
{
    Reflected operator()(const AsyncInputAccessor&, const ReflectionContext&) const;
};

template <>
struct Unreflector<AsyncInputAccessor>
{
    AsyncInputAccessor operator()(const Reflected&, const ReflectionContext&) const;
};
}
