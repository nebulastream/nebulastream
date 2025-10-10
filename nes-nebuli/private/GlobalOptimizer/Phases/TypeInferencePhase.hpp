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

namespace NES
{

/// The type inference phase receives and query plan and infers all input and output schemata for all operators.
class TypeInferencePhase
{
public:
    TypeInferencePhase() = delete;
    ~TypeInferencePhase() = delete;
    TypeInferencePhase(const TypeInferencePhase&) = delete;
    TypeInferencePhase(TypeInferencePhase&&) = delete;
    TypeInferencePhase& operator=(const TypeInferencePhase&) = delete;
    TypeInferencePhase& operator=(TypeInferencePhase&&) = delete;

    static LogicalPlan apply(const LogicalPlan& inputPlan);
};

}
