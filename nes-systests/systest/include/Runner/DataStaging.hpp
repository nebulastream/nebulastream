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

#include <string>
#include <thread>
#include <vector>

#include <Model/RunnableTest.hpp>

namespace NES
{

/// Writes the rewriter's planned rows to the file that the physical source reads, creating the parent directory first.
void writeInlineData(const InlineData& data);

/// A data server for one source, and the source options for the endpoint that it bound.
/// The server sends its data once and then closes the connection, which ends the stream.
/// Stopping the thread earlier leaves the source without its data, so the thread has to outlive the queries reading from it.
struct RunningServer
{
    std::jthread thread;
    std::vector<std::string> options;
};

/// Starts a server on an ephemeral port that sends the given data.
/// The port is known only once the server binds, so the options come from here rather than from the rewriter.
[[nodiscard]] RunningServer serve(const ServedData& data);

}
