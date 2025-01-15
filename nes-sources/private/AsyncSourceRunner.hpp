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
#include <iostream>
#include <memory>
#include <optional>
#include <variant>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Logger.hpp>
#include <SourceCoroutine.hpp>
#include <Sources/SourceExecutionContext.hpp>

namespace NES::Sources
{


class AsyncSourceRunner
{
public:
    /* -------------------------------- Events -------------------------------- */
    struct EventStart
    {
        SourceReturnType::EmitFunction emitFn;
    };
    struct EventStop
    {
    };

    using Event = std::variant<EventStart, EventStop>;
    /* ------------------------------------------------------------------------ */


    explicit AsyncSourceRunner(AsyncSourceExecutionContext&& ctx) : currentState(Idle{(std::move(ctx))})
    {
    }
    ~AsyncSourceRunner() = default;

    AsyncSourceRunner(const AsyncSourceRunner&) = delete;
    AsyncSourceRunner& operator=(const AsyncSourceRunner&) = delete;
    AsyncSourceRunner(AsyncSourceRunner&&) = delete;
    AsyncSourceRunner& operator=(AsyncSourceRunner&&) = delete;

    void dispatch(const Event& event);

private:
    /* -------------------------------- States -------------------------------- */
    struct Idle
    {
        AsyncSourceExecutionContext sourceExecutionContext;
    };
    struct Running
    {
        std::shared_ptr<SourceCoroutineFunctor> coro;
    };
    struct Stopped
    {
    };

    using State = std::variant<Idle, Running, Stopped>;

    State currentState;
    /* ------------------------------------------------------------------------ */


    /* ------------------------------ Transitions ----------------------------- */
    struct Transitions
    {
        /// Transition from Idle to Running state.
        /// This does two things:
        /// 1. Create a shared pointer to the functor that maintains the root coroutine of the sources
        /// 2. Give one shared pointer object to the executor to spawn the root coroutine
        /// 3. Keep one shared pointer object in the Running state
        /// With this, we ensure that the lifetime of the functor will be longer than the Running state if necessary
        std::optional<State> operator()(Idle& state, EventStart& event) const
        {
            NES_DEBUG("Idle -> Running");
            auto& ioc = state.sourceExecutionContext.executor->ioContext();
            const auto executor = state.sourceExecutionContext.executor;
            state.sourceExecutionContext.setEmitFunction(event.emitFn);

            auto coro = std::make_shared<SourceCoroutineFunctor>(state.sourceExecutionContext);
            executor->execute([&ioc, coro] { asio::co_spawn(ioc, (*coro)(), asio::detached); });
            return Running{.coro = coro};
        }

        /// Transition from Running to Stopped state
        /// Call stop() on the coroutine functor, which will call cancel on the source's I/O object
        /// It is safe to destroy this, as we still have a shared pointer living in the executor.
        std::optional<State> operator()(const Running& runningState, const EventStop&) const
        {
            NES_DEBUG("Running -> Stopped");
            runningState.coro->stop();
            return Stopped{};
        }

        template <typename StateT, typename EventT>
        std::optional<State> operator()(StateT&, const EventT&) const
        {
            NES_DEBUG("Undefined state transition.");
            return std::nullopt;
        }
    };
    /* ------------------------------------------------------------------------ */


    friend std::ostream& operator<<(std::ostream& out, const AsyncSourceRunner& /*sourceRunner*/)
    {
        out << "\nAsyncSourceRunner()";
        return out;
    }
};

}
