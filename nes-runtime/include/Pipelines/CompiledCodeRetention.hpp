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
#include <QueryId.hpp>

namespace NES
{

/// Keeps the compiled code of a query's pipelines alive after the pipelines are gone, until release() is called for the query.
///
/// nautilus drops a module's symbol names and IR line table from its JIT symbol registry when the compiled code is freed, so an
/// in-process profiler can only name the samples of a query, and attribute them to IR lines, while the code still exists. A profiled
/// query's pipeline stages hand their code over here when they are destroyed, and the profiler releases it once the query's profile
/// is written.
class CompiledCodeRetention
{
public:
    static void retain(const QueryId& queryId, std::shared_ptr<void> compiledCode);
    static void release(const QueryId& queryId);
};

}
