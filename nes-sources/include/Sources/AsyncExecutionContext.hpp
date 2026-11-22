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
#include <mutex>
#include <typeindex>
#include <utility>
#include <ErrorHandling.hpp>

namespace NES
{
/// Runtime state for one operator slot. Neither state scope is reflected into the plan.
/// Sibling contexts explicitly share operator state while retaining independent slot state.
class AsyncExecutionContext
{
    struct OperatorState;

    struct StateBase
    {
        explicit StateBase(std::shared_ptr<OperatorState> operatorOwner) : operatorOwner(std::move(operatorOwner)) { }

        StateBase(const StateBase&) = delete;
        StateBase& operator=(const StateBase&) = delete;
        StateBase(StateBase&&) = delete;
        StateBase& operator=(StateBase&&) = delete;
        virtual ~StateBase() = default;

        /// A retained slot keeps its operator state alive until after the slot payload is destroyed.
        std::shared_ptr<OperatorState> operatorOwner;
    };

    template <typename T>
    struct State final : StateBase
    {
        template <typename... Args>
        explicit State(std::shared_ptr<OperatorState> operatorOwner, Args&&... args)
            : StateBase(std::move(operatorOwner)), value(std::forward<Args>(args)...)
        {
        }

        T value;
    };

    struct StateStorage
    {
        template <typename T, typename... Args>
        T& get(const std::shared_ptr<OperatorState>& operatorOwner, Args&&... args)
        {
            return ensure<T>(operatorOwner, std::forward<Args>(args)...).value;
        }

        template <typename T, typename... Args>
        std::shared_ptr<T> share(const std::shared_ptr<OperatorState>& operatorOwner, Args&&... args)
        {
            auto& stored = ensure<T>(operatorOwner, std::forward<Args>(args)...);
            return std::shared_ptr<T>{state, &stored.value};
        }

    private:
        template <typename T, typename... Args>
        State<T>& ensure(const std::shared_ptr<OperatorState>& operatorOwner, Args&&... args)
        {
            if (!state)
            {
                state = std::make_shared<State<T>>(operatorOwner, std::forward<Args>(args)...);
                stateType = std::type_index(typeid(T));
            }
            PRECONDITION(stateType == std::type_index(typeid(T)), "Async execution context state type mismatch");
            return static_cast<State<T>&>(*state);
        }

        std::shared_ptr<StateBase> state;
        std::type_index stateType{typeid(void)};
    };

    struct OperatorState
    {
        std::mutex initializationMutex;
        StateStorage storage;
    };

public:
    /// Starts a fresh operator instance with its first slot.
    AsyncExecutionContext() = default;
    ~AsyncExecutionContext() = default;
    AsyncExecutionContext(const AsyncExecutionContext&) = delete;
    AsyncExecutionContext& operator=(const AsyncExecutionContext&) = delete;
    AsyncExecutionContext(AsyncExecutionContext&&) noexcept = default;

    AsyncExecutionContext& operator=(AsyncExecutionContext&& other) noexcept
    {
        if (this != &other)
        {
            slotState = StateStorage{};
            operatorState = std::move(other.operatorState);
            slotState = std::move(other.slotState);
        }
        return *this;
    }

    /// Creates another slot for this operator, not another operator instance.
    [[nodiscard]] AsyncExecutionContext createSlotContext() const
    {
        PRECONDITION(operatorState != nullptr, "Cannot access a moved-from async execution context");
        return AsyncExecutionContext{operatorState};
    }

    /// One state object shared by every slot of this operator instance. Construction is synchronized;
    /// the plugin coordinates subsequent access, including interleaving across co_await.
    /// Constructor arguments are used only on first access and must be operator-stable configuration.
    template <typename T, typename... Args>
    T& getOperatorState(Args&&... args)
    {
        PRECONDITION(operatorState != nullptr, "Cannot access a moved-from async execution context");
        const std::scoped_lock lock(operatorState->initializationMutex);
        return operatorState->storage.get<T>(nullptr, std::forward<Args>(args)...);
    }

    /// Retains the actual operator state for prepared calls that may outlive this context.
    template <typename T, typename... Args>
    [[nodiscard]] std::shared_ptr<T> getSharedOperatorState(Args&&... args)
    {
        PRECONDITION(operatorState != nullptr, "Cannot access a moved-from async execution context");
        const std::scoped_lock lock(operatorState->initializationMutex);
        return operatorState->storage.share<T>(nullptr, std::forward<Args>(args)...);
    }

    /// One state object exclusive to this slot, reused across its sequential invocations.
    /// Constructor arguments are used only on first access and must be slot-stable configuration.
    template <typename T, typename... Args>
    T& getSlotState(Args&&... args)
    {
        PRECONDITION(operatorState != nullptr, "Cannot access a moved-from async execution context");
        return slotState.get<T>(operatorState, std::forward<Args>(args)...);
    }

    /// Retains this slot and its operator-state dependency; ownership does not permit concurrent slot use.
    template <typename T, typename... Args>
    [[nodiscard]] std::shared_ptr<T> getSharedSlotState(Args&&... args)
    {
        PRECONDITION(operatorState != nullptr, "Cannot access a moved-from async execution context");
        return slotState.share<T>(operatorState, std::forward<Args>(args)...);
    }

private:
    explicit AsyncExecutionContext(std::shared_ptr<OperatorState> operatorState) : operatorState(std::move(operatorState)) { }

    /// Declared first so slot-state destructors can still access shared operator state.
    std::shared_ptr<OperatorState> operatorState = std::make_shared<OperatorState>();
    StateStorage slotState;
};
}
