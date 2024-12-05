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

#include <optional>
#include <tuple>
#include <folly/Synchronized.h>

namespace folly
{
/// Tries to acquire two locks in a deadlock-free manner. If it fails, it returns an empty optional.
template <class Sync1, class Sync2>
auto tryAcquireLocked(Synchronized<Sync1>& lock1, Synchronized<Sync2>& lock2)
{
    if (static_cast<const void*>(&lock1) < static_cast<const void*>(&lock2))
    {
        if (auto locked1 = lock1.tryWLock())
        {
            if (auto locked2 = lock2.tryWLock())
            {
                return std::optional(std::make_tuple(std::move(locked1), std::move(locked2)));
            }
        }
    }
    else
    {
        if (auto locked2 = lock2.tryWLock())
        {
            if (auto locked1 = lock1.tryWLock())
            {
                return std::optional(std::make_tuple(std::move(locked1), std::move(locked2)));
            }
        }
    }
    return std::optional<std::tuple<
        LockedPtr<Synchronized<Sync1>, detail::SynchronizedLockPolicyTryExclusive>,
        LockedPtr<Synchronized<Sync2>, detail::SynchronizedLockPolicyTryExclusive>>>{};
}
}
