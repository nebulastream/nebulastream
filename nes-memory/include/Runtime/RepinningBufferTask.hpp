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

#include <Runtime/FloatingBuffer.hpp>

namespace NES::Memory
{
class RepinningBufferTask
{
public:
    struct promise_type;
private:
    std::coroutine_handle<promise_type> handle;
    FloatingBuffer floatingBuffer;

public:
    struct promise_type
    {
        std::optional<PinnedBuffer> result = std::nullopt;
        RepinningBufferTask get_return_object() { return RepinningBufferTask(std::coroutine_handle<promise_type>::from_promise(*this)); }
        void unhandled_exception() noexcept { };
        void return_value(PinnedBuffer pinnedBuffer) noexcept
        {
            result = pinnedBuffer;
        }
        std::suspend_never initial_suspend() noexcept { return {}; };
        std::suspend_never final_suspend() noexcept { return {}; };
    };


    explicit RepinningBufferTask(const std::coroutine_handle<promise_type> handle) : handle(handle) { }
    RepinningBufferTask(const RepinningBufferTask& other) = delete;
    RepinningBufferTask(RepinningBufferTask&& other) noexcept : handle(std::exchange(other.handle, nullptr)) { }
    RepinningBufferTask& operator=(const RepinningBufferTask& other) = delete;
    RepinningBufferTask& operator=(RepinningBufferTask&& other) noexcept
    {
        if (this == &other)
            return *this;
        if (handle)
        {
            handle.destroy();
        }
        handle = std::exchange(other.handle, nullptr);
        return *this;
    }


    //Ensures that if IO is necessary, it was submitted. May block
    void waitForSubmission();
    std::optional<PinnedBuffer> poll();
    PinnedBuffer waitFor();

    ~RepinningBufferTask()
    {
        if (handle)
        {
            handle.destroy();
        }
    }
};

class NewBufferTask
{
public:
    struct promise_type;
private:
    std::coroutine_handle<promise_type> handle;

public:
    struct promise_type
    {
        std::optional<PinnedBuffer> result = std::nullopt;
        NewBufferTask get_return_object() { return NewBufferTask(std::coroutine_handle<promise_type>::from_promise(*this)); }
        void unhandled_exception() noexcept { };
        void return_value(PinnedBuffer pinnedBuffer) noexcept
        {
            result = pinnedBuffer;
        }
        std::suspend_never initial_suspend() noexcept { return {}; };
        std::suspend_never final_suspend() noexcept { return {}; };
    };


    explicit NewBufferTask(const std::coroutine_handle<promise_type> handle) : handle(handle) { }
    NewBufferTask(const NewBufferTask& other) = delete;
    NewBufferTask(NewBufferTask&& other) noexcept : handle(std::exchange(other.handle, nullptr)) { }
    NewBufferTask& operator=(const NewBufferTask& other) = delete;
    NewBufferTask& operator=(NewBufferTask&& other) noexcept
    {
        if (this == &other)
            return *this;
        if (handle)
        {
            handle.destroy();
        }
        handle = std::exchange(other.handle, nullptr);
        return *this;
    }


    //Ensures that if IO is necessary, it was submitted. May block
    void waitForSubmission();
    std::optional<PinnedBuffer> poll();
    PinnedBuffer waitFor();

    ~NewBufferTask()()
    {
        if (handle)
        {
            handle.destroy();
        }
    }
};
}
