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

namespace NES
{

void CompositeStatisticListener::onEvent(Event event)
{
    for (auto& listener : queryEngineListeners)
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

void CompositeStatisticListener::addQueryEngineListener(std::shared_ptr<QueryEngineStatisticListener> listener)
{
    queryEngineListeners.push_back(std::move(listener));
}

void CompositeStatisticListener::addSystemListener(std::shared_ptr<SystemEventListener> listener)
{
    systemListeners.push_back(std::move(listener));
}

void CompositeStatisticListener::addListener(std::shared_ptr<StatisticListener> listener)
{
    queryEngineListeners.push_back(listener);
    systemListeners.push_back(listener);
}

bool CompositeStatisticListener::hasListeners() const
{
    return not queryEngineListeners.empty() or not systemListeners.empty();
}

}
