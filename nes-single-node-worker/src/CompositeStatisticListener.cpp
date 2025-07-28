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

#include <CompositeStatisticListener.hpp>
#include <ReplayPrinter.hpp>

namespace NES
{

void CompositeStatisticListener::onEvent(Event event)
{
    for (auto& listener : statisticListeners)
    {
        listener->onEvent(event);
    }
}

void CompositeStatisticListener::onEvent(SystemEvent event)
{
    for (auto& listener : systemListeners)
    {
        listener->onEvent(event);
    }
}

void CompositeStatisticListener::addListener(std::shared_ptr<QueryEngineStatisticListener> listener)
{
    statisticListeners.push_back(std::move(listener));
}

void CompositeStatisticListener::addSystemListener(std::shared_ptr<SystemEventListener> listener)
{
    systemListeners.push_back(std::move(listener));
}

std::shared_ptr<ReplayPrinter> CompositeStatisticListener::getReplayPrinter() const
{
    /// Look for a ReplayPrinter in the query engine listeners
    for (const auto& listener : statisticListeners)
    {
        if (auto replayPrinter = std::dynamic_pointer_cast<ReplayPrinter>(listener))
        {
            return replayPrinter;
        }
    }
    
    /// Look for a ReplayPrinter in the system listeners
    for (const auto& listener : systemListeners)
    {
        if (auto replayPrinter = std::dynamic_pointer_cast<ReplayPrinter>(listener))
        {
            return replayPrinter;
        }
    }
    
    return nullptr;
}

bool CompositeStatisticListener::hasListeners() const
{
    return !statisticListeners.empty() || !systemListeners.empty();
}

} 