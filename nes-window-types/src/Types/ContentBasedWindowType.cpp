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

#include <memory>
#include <Types/ContentBasedWindowType.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Windowing
{

ContentBasedWindowType::ContentBasedWindowType() = default;

std::shared_ptr<ThresholdWindow>
ContentBasedWindowType::asThresholdWindow(const std::shared_ptr<ContentBasedWindowType>& contentBasedWindowType)
{
    if (auto thresholdWindow = std::dynamic_pointer_cast<ThresholdWindow>(contentBasedWindowType))
    {
        return thresholdWindow;
    }
    INVARIANT(false, "Can not cast the content based window type to a threshold window");
}
}
