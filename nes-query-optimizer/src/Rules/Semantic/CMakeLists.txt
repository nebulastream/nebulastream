# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-query-optimizer
        AnonymousSinkBindingRule.cpp
        AnonymousSourceBindingRule.cpp
        LogicalSourceExpansionRule.cpp
        TypeInferenceRule.cpp
        SinkBindingRule.cpp
        InferModelResolutionRule.cpp
        CalcTargetOrderRule.cpp
)

add_plugin(AnonymousSinkBinding PlanRule AnonymousSinkBindingRule.cpp)
add_plugin(AnonymousSourceBinding PlanRule AnonymousSourceBindingRule.cpp)
add_plugin(LogicalSourceExpansion PlanRule LogicalSourceExpansionRule.cpp)
add_plugin(TypeInference PlanRule TypeInferenceRule.cpp)
add_plugin(SinkBinding PlanRule SinkBindingRule.cpp)
add_plugin(InferModelResolution PlanRule InferModelResolutionRule.cpp)
add_plugin(CalcTargetOrder PlanRule CalcTargetOrderRule.cpp)

