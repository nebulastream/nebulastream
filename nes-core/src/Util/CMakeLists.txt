# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_nes(
        UtilityFunctions.cpp
        SharedLibrary.cpp
        TimeMeasurement.cpp
        Logger.cpp
        StacktraceLoader.cpp
        ThreadNaming.cpp
        BufferSequenceNumber.cpp
        TestUtils.cpp
        FaultToleranceType.cpp
        LineageType.cpp
        PlacementType.cpp
        GatheringMode.cpp
        SourceMode.cpp
        KalmanFilter.cpp
)
if (NES_USE_MQTT)
    add_source_nes(
        MQTTClientWrapper.cpp
    )
endif ()
add_subdirectory(Subprocess)
add_subdirectory(yaml)