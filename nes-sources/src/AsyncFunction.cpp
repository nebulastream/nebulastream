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

boost::asio::awaitable<SharedRuntimeValues> invokeOnce(AsyncFunction& function, SharedRuntimeValues arguments)
{
    auto prepared = function.prepare(std::move(arguments));
    PRECONDITION(prepared != nullptr, "Async function preparation must produce a prepared call");
    co_return co_await prepared->executeAttempt();
}

SharedRuntimeValues::SharedRuntimeValues(std::vector<RuntimeValue> values)
    : storage(std::make_shared<const std::vector<RuntimeValue>>(std::move(values)))
{
}

SharedRuntimeValues::SharedRuntimeValues(const std::initializer_list<RuntimeValue> values)
    : SharedRuntimeValues(std::vector<RuntimeValue>{values})
{
}

size_t SharedRuntimeValues::size() const
{
    return storage ? storage->size() : 0;
}

bool SharedRuntimeValues::empty() const
{
    return size() == 0;
}

RuntimeValueView SharedRuntimeValues::operator[](const size_t index) const&
{
    PRECONDITION(index < size(), "Async result index {} is out of bounds for {} values", index, size());
    return (*storage)[index].view();
}

}
