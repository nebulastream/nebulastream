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

#include <Sources/SourceHandle.hpp>

#include <memory>

#include <Identifiers/Identifiers.hpp>
#include <AsyncSourceRunner.hpp>
#include <Sources/SourceExecutionContext.hpp>

namespace NES::Sources
{

SourceHandle::SourceHandle(SourceExecutionContext context) : originId(context.originId), sourceRunner(std::make_unique<AsyncSourceRunner>()), sourceExecutionContext(std::move(context))
{
}

SourceHandle::~SourceHandle() = default;

void SourceHandle::start()
{

    sourceRunner->dispatch(AsyncSourceRunner::EventStart{.sourceExecutionContext = std::move(sourceExecutionContext)});
}

void SourceHandle::stop() const
{
    sourceRunner->dispatch(AsyncSourceRunner::EventStop{});
}

OriginId SourceHandle::getSourceId() const
{
    return this->sourceExecutionContext.originId;
}

std::ostream& operator<<(std::ostream& out, const SourceHandle& sourceHandle)
{
    return out << *sourceHandle.sourceRunner;
}

}
