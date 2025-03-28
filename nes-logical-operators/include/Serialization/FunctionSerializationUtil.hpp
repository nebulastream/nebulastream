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
< < < < < < < < HEAD : nes - execution / include / QueryCompiler / Phases / Translations / LowerToExecutableQueryPlanPhase.hpp
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <CompiledQueryPlan.hpp>
    == == == ==
#include <Abstract/LogicalFunction.hpp>
#include <SerializableFunction.pb.h>
    >>>>>>>> 6bb4678727(refactor(LogicalOperators))
    : nes - logical
    - operators / include / Serialization
        / FunctionSerializationUtil.hpp

          namespace NES::FunctionSerializationUtil
{
    < < < < < < < < HEAD : nes
                           - execution / include / QueryCompiler / Phases / Translations
                               / LowerToExecutableQueryPlanPhase.hpp std::unique_ptr<CompiledQueryPlan> apply(
                                   const std::shared_ptr<PipelineQueryPlan>& pipelineQueryPlan);
}
== == == ==
    /// Note: corresponding serialization is implemented as member function of each function
    static std::shared_ptr<LogicalFunction> deserializeFunction(const SerializableFunction& serializedFunction);
}
>>>>>>>> 6bb4678727(refactor(LogicalOperators)) : nes - logical - operators / include / Serialization / FunctionSerializationUtil.hpp
