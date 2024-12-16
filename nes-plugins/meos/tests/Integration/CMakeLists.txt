# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_integration_test(meos-speedAgregation "MeosSpeedAgregation.cpp")
add_nes_integration_test(meos-intersection "MeosIntersectionThresold.cpp")
add_nes_integration_test(meos-trajectoryInputation "MeosTrajectoryInputation.cpp")
add_nes_integration_test(meos-SNCBBattery "MeosSNCBBattery.cpp")
add_nes_integration_test(meos-SNCBGPS "MeosSNCBGPS.cpp")


target_link_libraries(meos-speedAgregation nes-meos)
target_link_libraries(meos-intersection nes-meos)
target_link_libraries(meos-trajectoryInputation nes-meos)
target_link_libraries(meos-SNCBBattery nes-meos)
target_link_libraries(meos-SNCBGPS nes-meos)