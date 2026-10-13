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
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/awaitable.hpp>

namespace NES
{

/// The implementation of an `AsyncLogicalFunction`, run by the `Async` source on its event loop.
/// An instance serves one call at a time, so it may keep per-connection state between calls.
class AsyncFunction
{
public:
    virtual ~AsyncFunction() = default;
    virtual boost::asio::awaitable<std::string> call(std::vector<std::string> arguments) = 0;
};

/// Where one argument of an async call comes from: a handed-off field, a constant, or the result of an
/// earlier call in the same list.
struct AsyncCallArgument
{
    std::optional<std::string> field;
    std::optional<std::string> constant;
    std::optional<size_t> call;
};

/// One async call as the `Async` source receives it in its `CALLS` config. Calls are listed after every call
/// they read. Only calls whose result is a field of the source's output have one; the others feed calls only.
struct AsyncCall
{
    std::optional<std::string> output;
    std::string function;
    std::vector<AsyncCallArgument> arguments;
};

}
