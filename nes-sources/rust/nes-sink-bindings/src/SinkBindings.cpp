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

#include <SinkBindings.hpp>

#include <cstdint>
#include <Util/Logger/Logger.hpp>

void on_sink_error_callback(
    void* context,
    uint64_t sink_id,
    const char* message)
{
    auto* ctx = static_cast<NES::ErrorContext*>(context);
    NES_ERROR("TokioSink {} (ctx sinkId={}): sink error: {}",
              sink_id, ctx->sourceId, message);
}
