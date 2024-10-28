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

#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Sinks/SinkDescriptor.hpp>

namespace NES::Sinks::SinkProvider
{

/// Takes a SinkDescriptor and in exchange returns a SinkPipeline, which Tasks can process (together with a TupleBuffer).
std::shared_ptr<Sink> lower(QueryId queryId, const SinkDescriptor& sinkDescriptor);

}
