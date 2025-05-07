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
#include <Sources/SourceHandle.hpp>
#include <folly/Synchronized.h>

#include "SourceRegistry.hpp"
#include "VideoSourceValidation.hpp"

namespace NES
{
class VideoSourceService
{
public:
    using Handle = size_t;

private:
    struct State
    {
        Handle nextHandle;
        std::unordered_map<Handle, AsyncSource::AsyncSourceEmit> consumers;
    };

    std::shared_ptr<folly::Synchronized<State>> stateMutex;
    std::jthread thread;
    friend void sourceThreadInner(
        const std::stop_token&,
        SourceDescriptor descriptor,
        std::shared_ptr<AbstractBufferProvider> bufferProvider,
        std::shared_ptr<folly::Synchronized<State>> stateMutex);
    friend void sourceThread(
        const std::stop_token&,
        SourceDescriptor descriptor,
        std::shared_ptr<AbstractBufferProvider> bufferProvider,
        std::shared_ptr<folly::Synchronized<State>> stateMutex);

    struct Private
    {
    };

public:
    VideoSourceService(Private, SourceDescriptor config, std::shared_ptr<AbstractBufferProvider> bufferProvider);
    Handle addConsumer(AsyncSource::AsyncSourceEmit&& consumer);
    void removeConsumer(Handle handle);

    static std::shared_ptr<VideoSourceService>
    instance(SourceDescriptor descriptor, std::shared_ptr<AbstractBufferProvider> bufferProvider);
};
}
