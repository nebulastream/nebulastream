# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_optimizer_test(DecideJoinTypesTest DecideJoinTypesTest.cpp)
add_nes_optimizer_test(DecideMemoryLayoutTest DecideMemoryLayoutTest.cpp)
add_nes_optimizer_test(DecideFieldOrderTest DecideFieldOrderTest.cpp)
add_nes_optimizer_test(CalcTargetOrderTest CalcTargetOrderTest.cpp)
add_nes_optimizer_test(DecideFieldMappingsTest DecideFieldMappingsTest.cpp)
add_nes_optimizer_test(OriginIdInferenceRuleTest OriginIdInferenceRuleTest.cpp)
add_nes_optimizer_test(PredicatePushdownTest PredicatePushdownTest.cpp)
add_nes_optimizer_test(ProjectionPushdownRuleTest ProjectionPushdownRuleTest.cpp)
if (ENABLE_IREE_TESTS)
    ### InferModel is the only operator that reaches projectionPushdown's default branch
    target_link_libraries(ProjectionPushdownRuleTest nes-inference nes-grpc)
    target_compile_definitions(ProjectionPushdownRuleTest PRIVATE INFERENCE_TEST_DATA="${CMAKE_SOURCE_DIR}/nes-inference/tests/testdata")
endif ()
add_nes_optimizer_test(RedundantProjectionRemovalRuleTest RedundantProjectionRemovalRuleTest.cpp)
add_nes_optimizer_test(WatermarkAssignerPushdownRuleTest WatermarkAssignerPushdownRuleTest.cpp)
