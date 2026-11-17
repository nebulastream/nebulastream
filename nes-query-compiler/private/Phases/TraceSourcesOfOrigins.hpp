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
#include <BackpressureManager.hpp>

namespace NES
{

/// Maps every origin of the plan to the sources below the operator that assigns it. A source is identified by its first output origin,
/// as `LowerToPhysicalSource` lowers a source with it. A network source carries all origins of its upstream plan, which all map to it.
/// Visits every operator of the plan once.
SourcesOfOrigins traceSourcesOfOrigins(const LogicalPlan& plan);

}
