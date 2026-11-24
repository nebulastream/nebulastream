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
#include <Sources/AsyncInputAccessor.hpp>

#include <cstddef>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace detail
{
struct AsyncInputContext
{
    std::vector<DataType> types;
};
}

AsyncRowHandle::AsyncRowHandle(
    std::shared_ptr<const detail::AsyncInputContext> inputContext, std::function<RuntimeValue(size_t)> reader, const size_t size)
    : inputContext(std::move(inputContext)), reader(std::move(reader)), count(size)
{
    PRECONDITION(this->reader, "An async row needs an input reader");
}

size_t AsyncRowHandle::size() const
{
    return count;
}

AsyncInputAccessor::AsyncInputAccessor(std::shared_ptr<const detail::AsyncInputContext> inputContext, const size_t index)
    : inputContext(std::move(inputContext)), index(index)
{
}

DataType AsyncInputAccessor::getDataType() const
{
    return inputContext->types.at(index);
}

RuntimeValue AsyncInputAccessor::operator()(const AsyncRowHandle& row) const
{
    PRECONDITION(inputContext == row.inputContext, "Async input accessor belongs to another invocation input context");
    PRECONDITION(index < row.count, "Async input accessor is outside this row's inputs");
    return row.reader(index);
}

AsyncInputAccessors::AsyncInputAccessors() : AsyncInputAccessors(std::vector<DataType>{})
{
}

AsyncInputAccessors::AsyncInputAccessors(std::vector<DataType> types)
    : inputContext(std::make_shared<const detail::AsyncInputContext>(detail::AsyncInputContext{std::move(types)}))
{
}

const std::vector<DataType>& AsyncInputAccessors::getTypes() const
{
    return inputContext->types;
}

size_t AsyncInputAccessors::size() const
{
    return inputContext->types.size();
}

AsyncInputAccessor AsyncInputAccessors::operator[](const size_t index) const
{
    PRECONDITION(index < size(), "Async input accessor is outside its input signature");
    return AsyncInputAccessor{inputContext, index};
}

AsyncRowHandle AsyncInputAccessors::createRowHandle(std::function<RuntimeValue(size_t)> reader) const
{
    return AsyncRowHandle{inputContext, std::move(reader), size()};
}

AsyncRowHandle AsyncInputAccessors::createRowHandle(SharedRuntimeValues values) const
{
    const auto count = values.size();
    return AsyncRowHandle{inputContext, [values = std::move(values)](const size_t index) { return values[index].toOwned(); }, count};
}

SharedRuntimeValues AsyncInputAccessors::read(const AsyncRowHandle& row) const
{
    PRECONDITION(inputContext == row.inputContext, "Async row belongs to another invocation input context");
    PRECONDITION(row.size() == size(), "Async row does not match its input signature");
    std::vector<RuntimeValue> values;
    values.reserve(size());
    for (size_t index = 0; index < size(); ++index)
    {
        values.push_back((*this)[index](row));
    }
    return SharedRuntimeValues{std::move(values)};
}

Reflected Reflector<AsyncInputAccessors>::operator()(const AsyncInputAccessors& inputs, const ReflectionContext& context) const
{
    return context.reflect(inputs.getTypes());
}

AsyncInputAccessors Unreflector<AsyncInputAccessors>::operator()(const Reflected& data, const ReflectionContext& context) const
{
    return AsyncInputAccessors{context.unreflect<std::vector<DataType>>(data)};
}
}
