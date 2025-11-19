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
#include <functional>
#include <nautilus/val.hpp>

namespace NES::Nautilus::Interface
{

/// Index with rollover behavior and a callback on rollover.
/// When the index reaches or exceeds the rollover value, the counter is reset to zero and the callback is called.
template <typename T>
class RolloverIndex
{
public:
    /// The return value will be used as its new rolloverValue. The other param is the old roll over value
    using Callback = std::function<nautilus::val<T>(const nautilus::val<T>&, const nautilus::val<uint64_t>&)>;
    explicit RolloverIndex(const nautilus::val<T>& startIndex, const nautilus::val<T>& rolloverValue, Callback onRollover);

    /// Returns the index of this current loop before a rollover happened
    nautilus::val<T> getCurrentIndex() const;

    /// Returns the index that stores the overall number of increment calls without ever being resetted
    nautilus::val<T> getOverallIndex() const;

    /// Increment the underlying indices and potentially calling
    void increment();

private:
    nautilus::val<T> index;
    nautilus::val<T> overallIndex;
    nautilus::val<T> rolloverValue;
    Callback callback;
};

}
