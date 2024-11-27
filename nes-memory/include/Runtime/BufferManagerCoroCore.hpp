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
#include <any>
#include <concepts>
#include <coroutine>
#include <memory>

#include <ErrorHandling.hpp>


namespace NES::Memory::detail
{

template <typename T>
concept SuspendReturnType = std::same_as<T, bool> || std::same_as<T, std::coroutine_handle<>>;

template <typename T, typename Promise, typename ReturnType = decltype(std::declval<T>().await_resume())>
concept Awaiter = requires(T awaiter, std::coroutine_handle<Promise> handle) {
    { awaiter.await_ready() } -> std::same_as<bool>;
    { awaiter.await_suspend(handle) } -> SuspendReturnType;
    { awaiter.await_resume() } -> std::same_as<ReturnType>;
    { awaiter.isDone() } -> std::same_as<bool>;
};
//Using a concept allows us to "forward declare" behaviour of types that at the end depend on the future type again
template <typename T, typename Promise, typename ReturnType = decltype(std::declval<T>().await_resume()), typename WrappedAwaiter>
concept UnderlyingAwaiter = requires(T underlyingAwaiter, std::coroutine_handle<Promise> handle, WrappedAwaiter wrappedAwaiter) {
    { handle.promise().exchangeAwaiter(&wrappedAwaiter) };
} && Awaiter<T, Promise, ReturnType>;

///Concept that guarantees that an instance of this awaiter is always providing a valid pointer to its state as long as an instance exists.
///Mostly used for the futures, as they are just thin wrappers around std::shared_ptr<Promise>
template <typename T>
concept SafeAwaiter = std::is_same_v<typename T::safeAwaiter, std::true_type> && std::is_copy_assignable_v<T>;

class VirtualAwaiter
{
public:
    virtual ~VirtualAwaiter() = default;
    virtual bool await_ready() const noexcept = 0;
    virtual std::coroutine_handle<> await_suspend(std::coroutine_handle<>) = 0;
    virtual std::any await_resume() const noexcept = 0;
    virtual bool isDone() = 0;
};

class ErasedAwaiter
{
    std::unique_ptr<VirtualAwaiter> awaiter;

public:
    explicit ErasedAwaiter(std::unique_ptr<VirtualAwaiter>&& awaiter) : awaiter(std::move(awaiter)) { }
    ErasedAwaiter(const ErasedAwaiter& other) = delete;
    ErasedAwaiter(ErasedAwaiter&& other) noexcept = default;
    ErasedAwaiter& operator=(const ErasedAwaiter& other) = delete;
    ErasedAwaiter& operator=(ErasedAwaiter&& other) noexcept = default;

    bool await_ready() const noexcept { return awaiter->await_ready(); }

    std::coroutine_handle<> await_suspend(const std::coroutine_handle<> handle) const noexcept { return awaiter->await_suspend(handle); }

    std::any await_resume() const noexcept { return awaiter->await_resume(); }

    bool isDone() const { return awaiter->isDone(); }
};

using CoroutineError = uint32_t;
class VirtualPromise
{
public:
    //Delete destructor, needs to be handled over the destroy function of the coroutine handle
    virtual ~VirtualPromise() = default;
    virtual std::weak_ptr<VirtualPromise> setWaitingCoroutine(std::weak_ptr<VirtualPromise>) noexcept = 0;
    virtual std::coroutine_handle<> getHandle() noexcept = 0;
};

template <typename Promise>
concept SharedPromise = requires(Promise promise) {
    { promise.getSharedPromise() } -> std::same_as<std::shared_ptr<Promise>>;
    { promise.updateIsDone() };
    { promise.getHandle() } -> std::same_as<std::coroutine_handle<>>;
};


///Do not set suspendIfNotContinue to false if used as the final_suspend awaiter,
///the coroutine might never end properly while at the same time memory might be double freed.
///It's mostly intended for yield_value
template <SharedPromise InnerPromise, bool suspendIfNotContinue = true>
class ContinueOuterCoroutineAwaiter : public VirtualAwaiter
{
    //We use a weak pointer, because the outer coroutine will very likely already own the inner coroutine promise,
    //so everything that we do from the innerPromise towards the outer promise should use weak references to avoid cyclic references.
    std::weak_ptr<VirtualPromise> toContinue;
    //Technically not needed, but its good to have another invariant to make this awaiter strictly oneshot to prohibit multiple resumptions
    //of the same coroutine
    std::atomic_flag hasSuspended{false};

public:
    explicit ContinueOuterCoroutineAwaiter(const std::weak_ptr<VirtualPromise>& toContinue) : toContinue(toContinue) { }
    bool await_ready() const noexcept override
    {
        //We have to return always false so that await_suspend is actually called
        return false;
    }

    std::coroutine_handle<> await_suspend(const std::coroutine_handle<> handle) noexcept override
    {
        auto castHandle = std::coroutine_handle<InnerPromise>::from_address(handle.address());
        // castHandle.promise().updateIsDone();
        const auto wasFinalSuspendedBefore = hasSuspended.test_and_set();
        INVARIANT(!wasFinalSuspendedBefore, "Final suspension awaiter of promise was called more then once");
        if (auto locked = toContinue.lock())
        {
            return std::coroutine_handle<VirtualPromise>::from_promise(*locked);
        }
        if constexpr (suspendIfNotContinue)
        {
            return std::noop_coroutine();
        } else
        {
            return handle;
        }
    }

    std::any await_resume() const noexcept override { return std::any(); }

    bool isDone() override { return hasSuspended.test(); }
};
}