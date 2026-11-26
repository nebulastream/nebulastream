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
#include <unordered_map>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Sources/AsyncOperator.hpp>

namespace NES
{
namespace detail
{
struct AsyncGraphScope;
}

class AsyncCall final : public std::enable_shared_from_this<AsyncCall>
{
public:
    using Ptr = std::shared_ptr<const AsyncCall>;
    AsyncCall(const AsyncCall&) = delete;
    AsyncCall& operator=(const AsyncCall&) = delete;
    AsyncCall(AsyncCall&&) = delete;
    AsyncCall& operator=(AsyncCall&&) = delete;
    ~AsyncCall() = default;
    [[nodiscard]] std::string getFunctionType() const;
    [[nodiscard]] const AsyncOperator& getAsyncOperator() const;
    [[nodiscard]] const std::vector<DataType>& getResultTypes() const;
    [[nodiscard]] const std::vector<Ptr>& getDependencies() const;
    [[nodiscard]] AsyncResultReference output(size_t index) const;

private:
    friend class AsyncCallGraphBuilder;
    friend class AsyncInputAccessor;
    AsyncCall(std::shared_ptr<const detail::AsyncGraphScope>, AsyncOperator, std::vector<Ptr>);
    std::shared_ptr<const detail::AsyncGraphScope> scope;
    AsyncOperator asyncOperator;
    std::vector<Ptr> dependencies;
    std::vector<DataType> resultTypes;
};

struct AsyncOutputMapping
{
    AsyncResultReference result;
    QualifiedIdentifier field;
};

class AsyncCallGraph
{
public:
    [[nodiscard]] const Schema<QualifiedUnboundField, Ordered>& getInputSchema() const;
    [[nodiscard]] const std::vector<AsyncCall::Ptr>& getCalls() const;
    [[nodiscard]] const std::vector<AsyncOutputMapping>& getOutputs() const;
    [[nodiscard]] const std::vector<AsyncCall::Ptr>& getRequiredCalls() const;
    [[nodiscard]] AsyncRowHandle createRowHandle(std::function<RuntimeValue(const InputFieldReference&)>,
                                                 std::unordered_map<AsyncCall::Ptr, SharedRuntimeValues>) const;

private:
    friend class AsyncCallGraphBuilder;
    friend struct Reflector<AsyncCallGraph>;
    AsyncCallGraph(std::shared_ptr<const detail::AsyncGraphScope>,
                   std::vector<AsyncCall::Ptr>,
                   std::vector<AsyncOutputMapping>,
                   std::vector<AsyncCall::Ptr>);
    std::shared_ptr<const detail::AsyncGraphScope> scope;
    std::vector<AsyncCall::Ptr> calls;
    std::vector<AsyncOutputMapping> outputs;
    std::vector<AsyncCall::Ptr> requiredCalls;
};

class AsyncCallGraphBuilder
{
public:
    explicit AsyncCallGraphBuilder(Schema<QualifiedUnboundField, Ordered>);
    AsyncCallGraphBuilder(const AsyncCallGraphBuilder&) = delete;
    AsyncCallGraphBuilder& operator=(const AsyncCallGraphBuilder&) = delete;
    AsyncCallGraphBuilder(AsyncCallGraphBuilder&&) = delete;
    AsyncCallGraphBuilder& operator=(AsyncCallGraphBuilder&&) = delete;
    ~AsyncCallGraphBuilder() = default;
    [[nodiscard]] InputFieldReference input(QualifiedIdentifier) const;
    [[nodiscard]] AsyncCall::Ptr call(AsyncOperator, std::vector<AsyncCall::Ptr>);
    void require(AsyncCall::Ptr);
    void exportOutput(AsyncResultReference, QualifiedIdentifier);
    [[nodiscard]] AsyncCallGraph build();

private:
    void requireOpen() const;
    std::shared_ptr<const detail::AsyncGraphScope> scope;
    std::vector<AsyncCall::Ptr> calls;
    std::vector<AsyncOutputMapping> outputs;
    std::vector<AsyncCall::Ptr> requiredCalls;
    bool built = false;
};

template <>
struct Reflector<AsyncCallGraph>
{
    Reflected operator()(const AsyncCallGraph&, const ReflectionContext&) const;
};

template <>
struct Unreflector<AsyncCallGraph>
{
    AsyncCallGraph operator()(const Reflected&, const ReflectionContext&) const;
};
}
