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

#include <SystemStatsBroadcaster.hpp>

#include <algorithm>

namespace NES
{

void SystemStatsBroadcaster::broadcast(const SystemStatEvent& event)
{
    std::lock_guard lock(mutex);
    for (const auto& subscriber : subscribers)
    {
        subscriber->tryPush(event);
    }
}

std::shared_ptr<SystemStatsSubscriber> SystemStatsBroadcaster::subscribe()
{
    auto subscriber = std::make_shared<SystemStatsSubscriber>();
    std::lock_guard lock(mutex);
    subscribers.push_back(subscriber);
    return subscriber;
}

void SystemStatsBroadcaster::unsubscribe(const std::shared_ptr<SystemStatsSubscriber>& subscriber)
{
    subscriber->close();
    std::lock_guard lock(mutex);
    subscribers.erase(std::remove(subscribers.begin(), subscribers.end(), subscriber), subscribers.end());
}

}
