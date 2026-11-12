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

#include <Sources/AsyncFunction.hpp>

#include <cstddef>
#include <initializer_list>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/RuntimeValue.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

AsyncValues::AsyncValues(std::vector<RuntimeValue> values) : storage(std::make_shared<const std::vector<RuntimeValue>>(std::move(values)))
{
}

AsyncValues::AsyncValues(const std::initializer_list<RuntimeValue> values) : AsyncValues(std::vector<RuntimeValue>{values})
{
}

size_t AsyncValues::size() const
{
    return storage->size();
}

bool AsyncValues::empty() const
{
    return size() == 0;
}

RuntimeValueView AsyncValues::operator[](const size_t index) const&
{
    PRECONDITION(index < storage->size(), "Async result index {} is out of bounds for {} values", index, storage->size());
    return (*storage)[index].view();
}

}
