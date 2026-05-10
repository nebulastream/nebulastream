# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_unit_test(thermal-frame-data-unit-tests "ThermalFrameDataTest.cpp")
target_link_libraries(thermal-frame-data-unit-tests image_thermal_frame_lib nes-nautilus)

# Round-trip test exercises the full Schema → TupleBufferRef → readRecord path,
# so it needs the same support libs the in-tree TupleBuffer tests pull in.
add_nes_unit_test(thermal-frame-tuple-buffer-roundtrip-tests "ThermalFrameTupleBufferRoundtripTest.cpp")
target_link_libraries(thermal-frame-tuple-buffer-roundtrip-tests image_thermal_frame_lib nes-nautilus-test-util)
