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

#include <atomic>
#include <concepts>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <nameof.hpp>

namespace NES
{
struct ExecutionContext;

/// Unique ID generation for physical operators.
inline OperatorId getNextPhysicalOperatorId()
{
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}

namespace detail
{
struct ErasedPhysicalOperator;
}

template <typename Checked = NES::detail::ErasedPhysicalOperator>
struct TypedPhysicalOperator;
using PhysicalOperator = TypedPhysicalOperator<>;

/// Concept defining the interface for all logical functions in the query plan.
/// This concept defines the common interface that all logical functions must implement.
/// Logical functions represent functions in the query plan and are used during query
/// planning and optimization.
template <typename T>
concept PhysicalOperatorConceptBase
    = requires(T& op, ExecutionContext& execCtx, RecordBuffer& recordBuffer, CompilationContext& compCtx, Record& record) {
          /// This is called once before the operator starts processing records.
          { op.setup(execCtx, compCtx) } -> std::same_as<void>;

          /// Opens the operator for processing records.
          /// This is called before each batch of records is processed.
          { op.open(execCtx, recordBuffer) } -> std::same_as<void>;

          /// Closes the operator after processing records.
          /// This is called after each batch of records is processed.
          { op.close(execCtx, recordBuffer) } -> std::same_as<void>;

          /// Terminates the operator.
          /// This is called once after all records have been processed.
          { op.terminate(execCtx) } -> std::same_as<void>;

          /// Executes the operator on the given record.
          { op.execute(execCtx, record) } -> std::same_as<void>;

          { op.getId() } -> std::convertible_to<OperatorId>;
      };

template <typename T>
concept PhysicalOperatorConcept = PhysicalOperatorConceptBase<T> && requires(T& op, PhysicalOperator& child) {
    { op.getChild() } -> std::convertible_to<std::optional<PhysicalOperator>>;
    { op.withChild(child) } -> std::convertible_to<T>;
};

namespace detail
{
/// @brief A type erased wrapper for physical operators
struct ErasedPhysicalOperator
{
    virtual ~ErasedPhysicalOperator() = default;

    [[nodiscard]] virtual std::optional<PhysicalOperator> getChild() const = 0;
    [[nodiscard]] virtual PhysicalOperator withChild(const PhysicalOperator& child) const = 0;

    virtual void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const = 0;
    virtual void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const = 0;
    virtual void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const = 0;
    virtual void terminate(ExecutionContext& executionCtx) const = 0;
    virtual void execute(ExecutionContext& executionCtx, Record& record) const = 0;

    [[nodiscard]] virtual OperatorId getId() const = 0;
    [[nodiscard]] virtual std::string toString() const = 0;

private:
    template <typename T>
    friend struct NES::TypedPhysicalOperator;
    ///If the function inherits from DynamicBase (over Castable), then returns a pointer to the wrapped operator as DynamicBase,
    ///so that we can then safely try to dyncast the DynamicBase* to Castable<T>*
    [[nodiscard]] virtual std::optional<const DynamicBase*> getImpl() const = 0;
};

template <PhysicalOperatorConcept PhysicalOperatorType>
struct PhysicalOperatorModel;
}

template <typename Checked>
struct TypedPhysicalOperator
{
    template <PhysicalOperatorConcept T>
    requires std::same_as<Checked, NES::detail::ErasedPhysicalOperator>
    TypedPhysicalOperator(TypedPhysicalOperator<T> other) : self(other.self) /// NOLINT(google-explicit-constructor)
    {
    }

    /// Constructs a LogicalOperator from a concrete operator type.
    /// @tparam T The type of the operator. Must satisfy IsLogicalOperator concept.
    /// @param op The operator to wrap.
    template <PhysicalOperatorConceptBase T>
    TypedPhysicalOperator(const T& op)
        : self(std::make_shared<NES::detail::PhysicalOperatorModel<T>>(op)) /// NOLINT(google-explicit-constructor)
    {
    }

    template <PhysicalOperatorConcept T>
    TypedPhysicalOperator(const NES::detail::PhysicalOperatorModel<T>& op) /// NOLINT(google-explicit-constructor)
        : self(std::make_shared<NES::detail::PhysicalOperatorModel<T>>(op.impl))
    {
    }

    explicit TypedPhysicalOperator(std::shared_ptr<const NES::detail::ErasedPhysicalOperator> op) : self(std::move(op)) { }

    TypedPhysicalOperator() = default;

    /// @brief Access to the underlying erased interface
    [[nodiscard]] const Checked& get() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedPhysicalOperator, Checked>)
        {
            return *std::dynamic_pointer_cast<const NES::detail::ErasedPhysicalOperator>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<const NES::detail::PhysicalOperatorModel<Checked>>(self)->impl;
        }
    }

    const Checked& operator*() const { return get(); }

    const Checked* operator->() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedPhysicalOperator, Checked>)
        {
            return std::dynamic_pointer_cast<const NES::detail::ErasedPhysicalOperator>(self).get();
        }
        else
        {
            auto casted = std::dynamic_pointer_cast<const NES::detail::PhysicalOperatorModel<Checked>>(self);
            return &casted->impl;
        }
    }

    /// Attempts to get the underlying operator as type T (Concept-based).
    template <PhysicalOperatorConcept T>
    std::optional<TypedPhysicalOperator<T>> tryGetAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const NES::detail::PhysicalOperatorModel<T>>(self))
        {
            return TypedPhysicalOperator<T>{std::static_pointer_cast<const NES::detail::ErasedPhysicalOperator>(model)};
        }
        return std::nullopt;
    }

    /// Attempts to get the underlying operator as type T (DynamicBase/Castable based).
    template <typename T>
    requires(!PhysicalOperatorConcept<T>)
    std::optional<std::shared_ptr<const Castable<T>>> tryGetAs() const
    {
        if (auto castable = self->getImpl(); castable.has_value())
        {
            if (auto ptr = dynamic_cast<const Castable<T>*>(castable.value()))
            {
                return std::shared_ptr<const Castable<T>>{self, ptr};
            }
        }
        return std::nullopt;
    }

    /// Gets the underlying operator as type T (Concept-based). throws if failed.
    template <PhysicalOperatorConcept T>
    TypedPhysicalOperator<T> getAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const NES::detail::PhysicalOperatorModel<T>>(self))
        {
            return TypedPhysicalOperator<T>{std::static_pointer_cast<const NES::detail::ErasedPhysicalOperator>(model)};
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", NAMEOF_TYPE(T), NAMEOF_TYPE_EXPR(self));
    }

    /// Gets the underlying operator as type T (Castable based). throws if failed.
    template <typename T>
    requires(!PhysicalOperatorConcept<T>)
    std::shared_ptr<const Castable<T>> getAs() const
    {
        if (auto castable = self->getImpl(); castable.has_value())
        {
            if (auto ptr = dynamic_cast<const Castable<T>*>(castable.value()))
            {
                return std::shared_ptr<const Castable<T>>{self, ptr};
            }
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", NAMEOF_TYPE(T), NAMEOF_TYPE_EXPR(self));
    }

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const { return self->getChild(); }

    [[nodiscard]] TypedPhysicalOperator withChild(const PhysicalOperator& child) const { return self->withChild(child); }

    void setup(ExecutionContext& ctx, CompilationContext& compCtx) const { self->setup(ctx, compCtx); }

    void open(ExecutionContext& ctx, RecordBuffer& buffer) const { self->open(ctx, buffer); }

    void close(ExecutionContext& ctx, RecordBuffer& buffer) const { self->close(ctx, buffer); }

    void terminate(ExecutionContext& ctx) const { self->terminate(ctx); }

    void execute(ExecutionContext& ctx, Record& record) const { self->execute(ctx, record); }

    [[nodiscard]] OperatorId getId() const { return self->getId(); }

    [[nodiscard]] std::string toString() const { return self->toString(); }

private:
    template <typename FriendChecked>
    friend struct TypedPhysicalOperator;

    std::shared_ptr<const NES::detail::ErasedPhysicalOperator> self;
};

namespace detail
{
/// @brief Wrapper type that acts as a bridge between a type satisfying PhysicalOperatorConcept and TypedPhysicalOperator
template <PhysicalOperatorConcept PhysicalOperatorType>
struct PhysicalOperatorModel : ErasedPhysicalOperator
{
    PhysicalOperatorType impl;
    OperatorId id;

    explicit PhysicalOperatorModel(PhysicalOperatorType impl) : impl(std::move(impl)), id(getNextPhysicalOperatorId())
    {
        impl.id = this->id;
    }

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override { return impl.getChild(); }

    [[nodiscard]] PhysicalOperator withChild(const PhysicalOperator& child) const override { return impl.withChild(child); }

    void setup(ExecutionContext& ctx, CompilationContext& compCtx) const override { impl.setup(ctx, compCtx); }

    void open(ExecutionContext& ctx, RecordBuffer& buffer) const override { impl.open(ctx, buffer); }

    void close(ExecutionContext& ctx, RecordBuffer& buffer) const override { impl.close(ctx, buffer); }

    void terminate(ExecutionContext& ctx) const override { impl.terminate(ctx); }

    void execute(ExecutionContext& ctx, Record& record) const override { impl.execute(ctx, record); }

    [[nodiscard]] PhysicalOperatorType get() const { return impl; }

    [[nodiscard]] OperatorId getId() const override { return id; }

    [[nodiscard]] std::string toString() const override { return fmt::format("PhysicalOperator({})", NAMEOF_TYPE(PhysicalOperatorType)); }

    [[nodiscard]] bool operator==(const PhysicalOperator& other) const
    {
        if (auto ptr = dynamic_cast<const PhysicalOperatorModel*>(&other))
        {
            return impl.operator==(ptr->impl);
        }
        return false;
    }

private:
    template <typename T>
    friend struct NES::TypedPhysicalOperator;

    [[nodiscard]] std::optional<const DynamicBase*> getImpl() const override
    {
        if constexpr (std::is_base_of_v<DynamicBase, PhysicalOperatorType>)
        {
            return static_cast<const DynamicBase*>(&impl);
        }
        else
        {
            return std::nullopt;
        }
    }
};
}

inline std::ostream& operator<<(std::ostream& os, const PhysicalOperator& op)
{
    return os << op.toString();
}

/// The wrapper provides all information of a physical operator needed for correct pipeline construction during query compilation.
/// The wrapper is removed after pipeline creation. Thus, our physical operators only contain information needed for the actual execution.
class PhysicalOperatorWrapper
{
public:
    enum class PipelineLocation : uint8_t
    {
        SCAN, /// pipeline scan
        EMIT, /// pipeline emit
        INTERMEDIATE, /// neither of them, intermediate operator
    };

    PhysicalOperatorWrapper(
        PhysicalOperator physicalOperator,
        Schema inputSchema,
        Schema outputSchema,
        MemoryLayoutType inputMemoryLayoutType,
        MemoryLayoutType outputMemoryLayoutType);
    PhysicalOperatorWrapper(
        PhysicalOperator physicalOperator,
        Schema inputSchema,
        Schema outputSchema,
        MemoryLayoutType inputMemoryLayoutType,
        MemoryLayoutType outputMemoryLayoutType,
        PipelineLocation pipelineLocation);
    PhysicalOperatorWrapper(
        PhysicalOperator physicalOperator,
        Schema inputSchema,
        Schema outputSchema,
        MemoryLayoutType inputMemoryLayoutType,
        MemoryLayoutType outputMemoryLayoutType,
        std::optional<OperatorHandlerId> handlerId,
        std::optional<std::shared_ptr<OperatorHandler>> handler,
        PipelineLocation pipelineLocation);
    PhysicalOperatorWrapper(
        PhysicalOperator physicalOperator,
        Schema inputSchema,
        Schema outputSchema,
        MemoryLayoutType inputMemoryLayoutType,
        MemoryLayoutType outputMemoryLayoutType,
        std::optional<OperatorHandlerId> handlerId,
        std::optional<std::shared_ptr<OperatorHandler>> handler,
        PipelineLocation pipelineLocation,
        std::vector<std::shared_ptr<PhysicalOperatorWrapper>> children);

    /// for compatibility with free functions requiring getChildren()
    [[nodiscard]] std::vector<std::shared_ptr<PhysicalOperatorWrapper>> getChildren() const;

    /// Returns a string representation of the wrapper
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] const PhysicalOperator& getPhysicalOperator() const;
    [[nodiscard]] const std::optional<Schema>& getInputSchema() const;
    [[nodiscard]] const std::optional<Schema>& getOutputSchema() const;
    [[nodiscard]] const std::optional<MemoryLayoutType>& getInputMemoryLayoutType() const;
    [[nodiscard]] const std::optional<MemoryLayoutType>& getOutputMemoryLayoutType() const;


    void addChild(const std::shared_ptr<PhysicalOperatorWrapper>& child);
    void setChildren(const std::vector<std::shared_ptr<PhysicalOperatorWrapper>>& newChildren);

    [[nodiscard]] const std::optional<std::shared_ptr<OperatorHandler>>& getHandler() const;
    [[nodiscard]] const std::optional<OperatorHandlerId>& getHandlerId() const;
    [[nodiscard]] PipelineLocation getPipelineLocation() const;

private:
    PhysicalOperator physicalOperator;
    std::optional<MemoryLayoutType> inputMemoryLayoutType;
    std::optional<MemoryLayoutType> outputMemoryLayoutType;
    std::optional<Schema> inputSchema;
    std::optional<Schema> outputSchema;
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> children;

    std::optional<std::shared_ptr<OperatorHandler>> handler;
    std::optional<OperatorHandlerId> handlerId;
    PipelineLocation pipelineLocation;
};
}

FMT_OSTREAM(NES::PhysicalOperator);
