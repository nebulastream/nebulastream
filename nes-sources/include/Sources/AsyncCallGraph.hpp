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
#include <memory>
#include <string>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{
namespace detail
{
struct AsyncCallScope;
}

class AsyncCall;
class AsyncCallGraph;
class AsyncCallGraphBuilder;

/// An input bound to one graph's immutable input schema. A name alone cannot be reused in another input scope.
class InputFieldReference
{
public:
    /// Value handles remain bound when passed as rvalues; moving cannot leave an empty reference behind.
    InputFieldReference(const InputFieldReference&) = default;
    InputFieldReference& operator=(const InputFieldReference&) = default;
    [[nodiscard]] QualifiedIdentifier getField() const;
    [[nodiscard]] DataType getDataType() const;

private:
    friend class AsyncCallGraphBuilder;
    InputFieldReference(std::shared_ptr<const detail::AsyncCallScope> scope, QualifiedIdentifier field);
    std::shared_ptr<const detail::AsyncCallScope> scope;
    QualifiedIdentifier field;
};

/// A validated output of an owning producer, never a position in some unrelated call array.
/// Only the producer can construct this handle, and its immutable signature remains alive with it.
class AsyncResultReference
{
public:
    AsyncResultReference(const AsyncResultReference&) = default;
    AsyncResultReference& operator=(const AsyncResultReference&) = default;
    [[nodiscard]] std::shared_ptr<const AsyncCall> getProducer() const;
    [[nodiscard]] size_t getOutputIndex() const;
    [[nodiscard]] DataType getDataType() const;

private:
    friend class AsyncCall;
    AsyncResultReference(std::shared_ptr<const AsyncCall> producer, size_t index);
    std::shared_ptr<const AsyncCall> producer;
    size_t index;
};

using AsyncCallArgument = std::variant<RuntimeValue, InputFieldReference, AsyncResultReference>;

/// Immutable computation description, separate from any row's live coroutine/result state.
class AsyncCall final : public std::enable_shared_from_this<AsyncCall>
{
public:
    using Ptr = std::shared_ptr<const AsyncCall>;
    AsyncCall(const AsyncCall&) = delete;
    AsyncCall& operator=(const AsyncCall&) = delete;

    [[nodiscard]] const std::string& getFunctionType() const;
    [[nodiscard]] const std::vector<DataType>& getResultTypes() const;
    [[nodiscard]] const std::vector<AsyncCallArgument>& getArguments() const;
    [[nodiscard]] AsyncResultReference output(size_t index) const;

private:
    friend class AsyncCallGraphBuilder;
    AsyncCall(
        std::shared_ptr<const detail::AsyncCallScope> scope,
        std::string functionType,
        std::vector<AsyncCallArgument> arguments,
        std::vector<DataType> resultTypes);
    std::shared_ptr<const detail::AsyncCallScope> scope;
    std::string functionType;
    std::vector<AsyncCallArgument> arguments;
    std::vector<DataType> resultTypes;
};

struct AsyncCallOutput
{
    AsyncResultReference result;
    QualifiedIdentifier field;
};

/// A reusable DAG: producer handles preserve identity/sharing; no live awaitables are stored in the plan.
/// Reflection resolves local wire IDs into checked owning references, rejecting dangling edges/cycles.
class AsyncCallGraph
{
public:
    [[nodiscard]] const Schema<QualifiedUnboundField, Ordered>& getInputSchema() const;
    [[nodiscard]] const std::vector<AsyncCall::Ptr>& getCalls() const;
    [[nodiscard]] const std::vector<AsyncCallOutput>& getOutputs() const;

private:
    friend class AsyncCallGraphBuilder;
    AsyncCallGraph(
        std::shared_ptr<const detail::AsyncCallScope> scope, std::vector<AsyncCall::Ptr> calls, std::vector<AsyncCallOutput> outputs);
    std::shared_ptr<const detail::AsyncCallScope> scope;
    std::vector<AsyncCall::Ptr> calls;
    std::vector<AsyncCallOutput> outputs;
};

/// A single-use builder defines the binding scope. Foreign field/result handles are rejected, including after a rewrite.
/// Calls can only depend on already constructed immutable producers, making cycles impossible through this API.
class AsyncCallGraphBuilder
{
public:
    explicit AsyncCallGraphBuilder(Schema<QualifiedUnboundField, Ordered> inputSchema);
    AsyncCallGraphBuilder(const AsyncCallGraphBuilder&) = delete;
    AsyncCallGraphBuilder& operator=(const AsyncCallGraphBuilder&) = delete;
    AsyncCallGraphBuilder(AsyncCallGraphBuilder&&) = delete;
    AsyncCallGraphBuilder& operator=(AsyncCallGraphBuilder&&) = delete;

    [[nodiscard]] InputFieldReference input(QualifiedIdentifier field) const;
    [[nodiscard]] AsyncCall::Ptr
    call(std::string functionType, std::vector<AsyncCallArgument> arguments, std::vector<DataType> resultTypes);
    void exportOutput(AsyncResultReference result, QualifiedIdentifier field);
    [[nodiscard]] AsyncCallGraph build();

private:
    void requireOpen() const;
    std::shared_ptr<const detail::AsyncCallScope> scope;
    std::vector<AsyncCall::Ptr> calls;
    std::vector<AsyncCallOutput> outputs;
    bool built = false;
};

template <>
struct Reflector<AsyncCallGraph>
{
    Reflected operator()(const AsyncCallGraph& graph, const ReflectionContext& context) const;
};

template <>
struct Unreflector<AsyncCallGraph>
{
    AsyncCallGraph operator()(const Reflected& data, const ReflectionContext& context) const;
};

}
