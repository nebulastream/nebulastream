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
#include <PhysicalPlan.hpp>
#include <PipelinedQueryPlan.hpp>

namespace NES::QueryCompilation::PipeliningPhase
{
/// During this step we create a PipelinedQueryPlan out of the QueryPlan obj.
/// With `tappablePipelines` every operator pipeline is closed with a native emit and non-native sinks get
/// their own formatting pipeline (as fan-out points do), keeping every pipeline boundary observable for
/// branches attached to the running query.
std::shared_ptr<PipelinedQueryPlan> apply(const PhysicalPlan& queryPlan, bool tappablePipelines = false);
}
