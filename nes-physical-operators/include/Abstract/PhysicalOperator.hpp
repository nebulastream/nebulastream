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

#include <memory>
#include <optional>
#include <string>
#include <sstream>
#include <typeinfo>
#include <atomic>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>

namespace NES
{
using namespace Nautilus;
using namespace Nautilus::Interface::MemoryProvider;

struct ExecutionContext;

/// Unique ID generation for physical operators.
namespace
{
inline OperatorId getNextPhysicalOperatorId()
{
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}
}

/// Each operator can implement setup, open, close, execute, and terminate.
/// The concept carries an OperatorId that is stable across copies.
struct PhysicalOperatorConcept
{
    explicit PhysicalOperatorConcept();
    explicit PhysicalOperatorConcept(OperatorId existingId);
    virtual ~PhysicalOperatorConcept() = default;

    virtual std::optional<struct PhysicalOperator> getChild() const = 0;
    virtual void setChild(struct PhysicalOperator child) = 0;

    /// @brief Setup initializes this operator for execution.
    virtual void setup(ExecutionContext& executionCtx) const;

    /// @brief Open is called for each record buffer and is used to initialize execution local state.
    virtual void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /// @brief Close is called for each record buffer and clears execution local state.
    virtual void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /// @brief Terminates the operator and clears all operator state.
    virtual void terminate(ExecutionContext& executionCtx) const;

    /// @brief Called by the upstream operator (parent) to process one record.
    virtual void execute(ExecutionContext&, Record&) const;

    virtual std::string toString() const;

    OperatorId id = INVALID_OPERATOR_ID;
};

struct PhysicalOperator {
    template<typename T>
    PhysicalOperator(const T& op) : self(std::make_unique<Model<T>>(op)) {}
    PhysicalOperator(const PhysicalOperator& other);

    template<typename T>
    [[nodiscard]] std::optional<T> tryGet() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get())) {
            return p->data;
        }
        return std::nullopt;
    }

    template<typename T>
    [[nodiscard]] const T get() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
    }

    PhysicalOperator(PhysicalOperator&&) noexcept;
    PhysicalOperator& operator=(const PhysicalOperator& other);

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const;
    void setChild(PhysicalOperator child);

    void setup(ExecutionContext& executionCtx) const;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void terminate(ExecutionContext& executionCtx) const;
    void execute(ExecutionContext& executionCtx, Record& record) const;

    [[nodiscard]] std::string toString() const;
    [[nodiscard]] OperatorId getId() const;

    struct Concept : PhysicalOperatorConcept {
        explicit Concept(OperatorId existingId) : PhysicalOperatorConcept(existingId) {}
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
    };

    template<typename T>
    struct Model : Concept {
        T data;
        explicit Model(T d)
            : Concept(getNextPhysicalOperatorId()), data(std::move(d)) {}

        Model(T d, OperatorId existingId)
            : Concept(existingId), data(std::move(d)) {}

        [[nodiscard]] std::unique_ptr<Concept> clone() const override {
            return std::make_unique<Model<T>>(data, this->id);
        }

        [[nodiscard]] std::optional<PhysicalOperator> getChild() const override {
            return data.getChild();
        }

        void setChild(PhysicalOperator child) override {
            data.setChild(child);
        }

        void setup(ExecutionContext& executionCtx) const override {
            data.setup(executionCtx);
        }

        void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override {
            data.open(executionCtx, recordBuffer);
        }

        void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override {
            data.close(executionCtx, recordBuffer);
        }

        void terminate(ExecutionContext& executionCtx) const override {
            data.terminate(executionCtx);
        }

        void execute(ExecutionContext& executionCtx, Record& record) const override {
            data.execute(executionCtx, record);
        }

        std::string toString() const override {
            const auto name = typeid(T).name();
            int status = 0;
            std::unique_ptr<char, void(*)(void*)> demangled(
                abi::__cxa_demangle(name, nullptr, nullptr, &status), std::free
            );
            const auto nameDemangled = status == 0 ? demangled.get() : name;
            return "PhysicalOperator(" + std::string(nameDemangled) + ")";
        }
    };

    std::unique_ptr<Concept> self;
};

/// Wrapper for the physical operator to store input and output schema after query optimization.
struct PhysicalOperatorWrapper
{
    PhysicalOperatorWrapper(PhysicalOperator physicalOperator, Schema inputSchema, Schema outputSchema);

    PhysicalOperator physicalOperator;
    std::optional<Schema> inputSchema, outputSchema;
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> children {};

    std::optional<std::shared_ptr<OperatorHandler>> handler;
    std::optional<OperatorHandlerId> handlerId;

    bool isScan = false;
    bool isEmit = false;

    /// Returns a string representation of the wrapper
    std::string toString() const;
};
}
