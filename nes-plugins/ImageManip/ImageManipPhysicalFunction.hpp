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

#include <PhysicalFunctionRegistry.hpp>

namespace NES
{
struct ImageManipPhysicalFunction
{
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MONO8_TO_JPG(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_TO_MONO8(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_YUYV_TO_JPG(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_TO_PNG16(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_TO_JPG(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_TO_YUYV(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MONO8_TO_YUYV(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_FACE_DETECTION(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_SERIALIZE(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_DESERIALIZE(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_DRAW_RECTANGLE(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_ROI(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_AVG(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_TO_CELSIUS(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_MAX(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MONO16_MIN(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_RECTANGLE(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_AUDIO_TO_MFCC(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_ARGMAX_F32(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MAX_F32(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createIMAGE_MANIP_MAX_ABS_F32(PhysicalFunctionRegistryArguments arguments);
};
}
