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

#include <chrono>
#include <string>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// Transfers the serialized state of a stateful operator over a dedicated network channel of the data plane (ISQD state migration).
///
/// Deviation: the original deploys an ad-hoc state migration query (a source that reads the state file of the old operator, network
/// operators and a sink that writes the state file for the new operator). Here the old operator sends its state over a dedicated channel
/// to the new operator directly, which avoids the intermediate files and the extra query, but keeps the transfer worker-to-worker over
/// the data plane. Every state buffer is a flat buffer whose payload size is its number of tuples.
namespace StateMigrationTransfer
{
/// Sends the state and waits until the receiver took all of it. Throws if this does not happen within the timeout.
void send(
    const std::string& channel,
    const std::string& senderDataEndpoint,
    const std::string& receiverDataEndpoint,
    const std::vector<TupleBuffer>& state,
    std::chrono::milliseconds timeout);

/// Receives the state until the sender closes the channel. Throws if this does not happen within the timeout.
std::vector<TupleBuffer> receive(
    const std::string& channel,
    const std::string& receiverDataEndpoint,
    AbstractBufferProvider& bufferProvider,
    std::chrono::milliseconds timeout);
}

}
