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

#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/RuntimeEventListener.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
namespace NES
{
namespace Runtime
{
class TupleBuffer;
}

/**
 * @brief Interface that classes have to adhere to emit data as tasks
 */
class DataEmitter : public Runtime::RuntimeEventListener
{
public:
    /**
     * @brief create a task using the provided buffer and submit it to a task consumer, e.g., query manager
     * @param buffer
     * @param addBufferMetaData: If true, buffer meta data (e.g., sequence number, origin id, ...) is added to the buffer
     */
    virtual void emitWork(Memory::TupleBuffer& buffer, bool addBufferMetaData = true) = 0;

    virtual ~DataEmitter() NES_NOEXCEPT(false) = default;

    virtual void onEndOfStream(Runtime::QueryTerminationType) { }

    virtual DecomposedQueryPlanVersion getVersion() const
    {
        NES_WARNING("Trying to get version of a data emitter that does not carry version information, returning 0");
        return 0;
    };

    /**
     * @brief start a previously scheduled new version for this data emitter
     */
    virtual bool startNewVersion() { return false; };
};
} /// namespace NES
