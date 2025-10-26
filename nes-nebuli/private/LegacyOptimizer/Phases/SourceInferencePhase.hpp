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

#include <Plans/LogicalPlan.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>

namespace NES
{

class SourceInferencePhase
{
public:
    SourceInferencePhase() = delete;
    ~SourceInferencePhase() = delete;
    SourceInferencePhase(const SourceInferencePhase&) = delete;
    SourceInferencePhase(SourceInferencePhase&&) = delete;
    SourceInferencePhase& operator=(const SourceInferencePhase&) = delete;
    SourceInferencePhase& operator=(SourceInferencePhase&&) = delete;

    /// For each source, sets the schema by receiving it from the source catalog and formatting the field names (adding a prefix qualifier name).
    static LogicalPlan apply(const LogicalPlan& inputPlan, SharedPtr<const SourceCatalog> sourceCatalog);
};
}
