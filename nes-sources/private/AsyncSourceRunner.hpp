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

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <CoroutineExecutionContext.hpp>
#include <SourceCoroutine.hpp>

namespace NES::Sources
{


class AsyncSourceRunner
{
public:
    /* -------------------------------- Events -------------------------------- */
    struct EventStart
    {
        OriginId originId;
        std::unique_ptr<Source> sourceImpl;
        SourceReturnType::EmitFunction emitFn;
        std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    };
    struct EventStop
    {
    };

    using Event = std::variant<EventStart, EventStop>;
    /* ------------------------------------------------------------------------ */


    explicit AsyncSourceRunner(OriginId originId);
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
    };
    struct Running
    {
        std::shared_ptr<SourceCoroutine> coro;
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
        std::optional<State> operator()(Idle&, EventStart& event) const
        {
            NES_DEBUG("Idle -> Running");
            CoroutineExecutionContext cec = {
                .originId = event.originId,
                .sourceImpl = std::move(event.sourceImpl),
                .emitFn = event.emitFn,
                .bufferProvider = event.bufferProvider
            };
            auto coro = std::make_shared<SourceCoroutine>(cec);
            auto& ioc = cec.executor->ioContext();
            cec.executor->execute([&ioc, coro]
            {
                asio::co_spawn(ioc, (*coro)(), asio::detached);
            });
            return Running{.coro = std::make_shared<SourceCoroutine>(cec)};
        }

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
