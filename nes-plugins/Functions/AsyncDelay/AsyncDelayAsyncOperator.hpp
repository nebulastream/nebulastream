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
#include <string_view>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncInputAccessor.hpp>

namespace NES
{
struct AsyncDelayAsyncOperator
{
    AsyncDelayAsyncOperator(AsyncInputAccessor value, AsyncInputAccessor millis);
    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getWrittenFields() const;
    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext& context, const AsyncRowHandle& row) const;

private:
    friend struct Reflector<AsyncDelayAsyncOperator>;
    friend struct Unreflector<AsyncDelayAsyncOperator>;
    AsyncInputAccessor valueInput;
    AsyncInputAccessor millisInput;
};

template <>
struct Reflector<AsyncDelayAsyncOperator>
{
    Reflected operator()(const AsyncDelayAsyncOperator&, const ReflectionContext&) const;
};

template <>
struct Unreflector<AsyncDelayAsyncOperator>
{
    AsyncDelayAsyncOperator operator()(const Reflected&, const ReflectionContext&) const;
};

static_assert(AsyncOperatorConcept<AsyncDelayAsyncOperator>);
}
