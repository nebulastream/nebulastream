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
add_nes_integration_test(meos-SNCBPress "MeosSNCBPress.cpp")
add_nes_integration_test(meos-SNCBSCEP "MeosSNCBSCEP.cpp")
add_nes_integration_test(meos-MeosSNCBQuery1 "MeosSNCBQuery1.cpp")
add_nes_integration_test(meos-MeosSNCBQuery2 "MeosSNCBQuery2.cpp")
add_nes_integration_test(meos-MeosSNCBQuery3 "MeosSNCBQuery3.cpp")
add_nes_integration_test(meos-MeosSNCBQuery4 "MeosSNCBQuery4.cpp")
add_nes_integration_test(meos-MeosSNCBQuery5 "MeosSNCBQuery5.cpp")
add_nes_integration_test(meos-MeosSNCBQuery6 "MeosSNCBQuery6.cpp")
add_nes_integration_test(meos-MeosSNCBQuery7 "MeosSNCBQuery7.cpp")
add_nes_integration_test(meos-MeosSNCBQuery8 "MeosSNCBQuery8.cpp")
add_nes_integration_test(SNCBBreakF1 "SNCBBreakF1.cpp")
add_nes_integration_test(SNCBBreakF1-2 "SNCBBreakF1-2.cpp")
add_nes_integration_test(SNCBBreakF1-1 "SNCBBreakF1-1.cpp")
add_nes_integration_test(SNCBBreakF1-3 "SNCBBreakF1-3.cpp")
add_nes_integration_test(SNCBBreakF1-4 "SNCBBreakF1-4.cpp")
add_nes_integration_test(SNCBBreakF2 "SNCBBreakF2.cpp")
add_nes_integration_test(SNCBBreakF3 "SNCBBreakF3.cpp")

target_link_libraries(meos-speedAgregation nes-meos)
target_link_libraries(meos-intersection nes-meos)
target_link_libraries(meos-trajectoryInputation nes-meos)
target_link_libraries(meos-SNCBBattery nes-meos)
target_link_libraries(meos-SNCBGPS nes-meos)
target_link_libraries(meos-SNCBPress nes-meos)
target_link_libraries(meos-SNCBSCEP nes-meos)
target_link_libraries(meos-MeosSNCBQuery1 nes-meos)
target_link_libraries(meos-MeosSNCBQuery2 nes-meos)
target_link_libraries(meos-MeosSNCBQuery3 nes-meos)
target_link_libraries(meos-MeosSNCBQuery4 nes-meos)
target_link_libraries(meos-MeosSNCBQuery5 nes-meos)
target_link_libraries(meos-MeosSNCBQuery6 nes-meos)
target_link_libraries(meos-MeosSNCBQuery7 nes-meos)
target_link_libraries(meos-MeosSNCBQuery8 nes-meos)
target_link_libraries(SNCBBreakF1 nes-meos)
target_link_libraries(SNCBBreakF1-2 nes-meos)
target_link_libraries(SNCBBreakF1-1 nes-meos)
target_link_libraries(SNCBBreakF1-3 nes-meos)
target_link_libraries(SNCBBreakF1-4 nes-meos)
target_link_libraries(SNCBBreakF2 nes-meos)
target_link_libraries(SNCBBreakF3 nes-meos)