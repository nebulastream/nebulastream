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

#include <semaphore>

namespace NES
{

/// Simple wrapper (syntactic sugar) around 'std::binary_semaphore'.
/// Allows exactly two threads, a WaiterThread and a NotifierThread to synchronize.
/// 'waitForNotifierThread()' waits until the counter of the semaphore is 1 and then sets it to 0.
/// 'notifyWaiterThread()' sets the counter of the semaphore to 1.
/// Thus, it is not a problem if the Notifier thread calls 'notifyWaiterThread()' before the WaiterThread calls 'waitForNotifierThread()'.
/// The counter will be at 1 when the WaiterThread calls 'waitForNotifierThread()', it decrements the counter and continues.
class Notifier
{
public:
    Notifier() = default;

    /// Notify other thread, e.g., so that it can continue (setting the semaphore to 1).
    void notifyWaiterThread() { m_semaphore.release(); }

    /// Wait for other thread to call 'notifyWaiterThread()' (setting the semaphore to 0, continues directly if the semaphore is 1 already).
    void waitForNotifierThread() { m_semaphore.acquire(); }

private:
    std::binary_semaphore m_semaphore{0};
};
}
