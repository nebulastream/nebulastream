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

#include <Callback.hpp>

#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <absl/functional/any_invocable.h>

namespace NES
{

void Callback::tryExecute()
{
    std::unique_lock lock(mutex);
    if (cancelled)
    {
        return;
    }
    if (!callback)
    {
        return;
    }
    executing = true;
    executingThread = std::this_thread::get_id();
    auto movedCallback = std::move(callback);
    lock.unlock();

    movedCallback();

    lock.lock();
    executing = false;
    executingThread = std::thread::id{};
    executingCondition.notify_all();
}

void Callback::cancelAndWait()
{
    std::unique_lock lock(mutex);
    cancelled = true;
    callback = nullptr;

    /// If called from within the callback itself, don't wait (would deadlock).
    /// The callback will finish when we return up the stack.
    if (executingThread == std::this_thread::get_id())
    {
        return;
    }

    executingCondition.wait(lock, [this] { return !executing; });
}

std::pair<CallbackOwner, CallbackRef> Callback::create()
{
    auto callback = std::make_shared<Callback>();
    CallbackOwner owner{callback};
    CallbackRef ref{std::move(callback)};
    return {std::move(owner), std::move(ref)};
}

CallbackRef::CallbackRef(std::shared_ptr<Callback> ref) : ref(std::move(ref))
{
    if (this->ref)
    {
        ++this->ref->guards;
    }
}

CallbackRef::~CallbackRef()
{
    decrementAndMaybeTrigger();
}

CallbackRef::CallbackRef(const CallbackRef& other) : ref(other.ref)
{
    if (ref)
    {
        ++ref->guards;
    }
}

CallbackRef::CallbackRef(CallbackRef&& other) noexcept : ref(std::move(other.ref))
{
}

CallbackRef& CallbackRef::operator=(const CallbackRef& other)
{
    *this = CallbackRef(other);
    return *this;
}

CallbackRef& CallbackRef::operator=(CallbackRef&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    decrementAndMaybeTrigger();
    ref = std::move(other.ref);
    return *this;
}

void CallbackRef::decrementAndMaybeTrigger()
{
    if (!ref)
    {
        return;
    }
    if (--ref->guards == 0)
    {
        ref->tryExecute();
    }
}

CallbackOwner::CallbackOwner(std::shared_ptr<Callback> ref) : ref(std::move(ref))
{
}

CallbackOwner::~CallbackOwner()
{
    if (ref)
    {
        ref->cancelAndWait();
    }
}

CallbackOwner::CallbackOwner(CallbackOwner&& other) noexcept : ref(std::move(other.ref))
{
}

CallbackOwner& CallbackOwner::operator=(CallbackOwner&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    if (ref)
    {
        ref->cancelAndWait();
    }
    ref = std::move(other.ref);
    return *this;
}

void CallbackOwner::setCallback(absl::AnyInvocable<void()> callback)
{
    if (ref)
    {
        const std::lock_guard lock(ref->mutex);
        ref->callback = std::move(callback);
    }
}

}
