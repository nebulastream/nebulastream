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

#include <cstddef>
#include <functional>
#include <memory>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Util/Reflection.hpp>

namespace NES
{
namespace detail
{
struct AsyncInputContext;
}

class AsyncInputAccessor;
class AsyncInputAccessors;

/// An owning handle to one invocation's ready inputs. Only accessors from its input context can read it.
/// A provider hides the row's physical representation and retains anything needed by its reads.
class AsyncRowHandle
{
public:
    [[nodiscard]] size_t size() const;

private:
    friend class AsyncInputAccessor;
    friend class AsyncInputAccessors;
    AsyncRowHandle(std::shared_ptr<const detail::AsyncInputContext> inputContext, std::function<RuntimeValue(size_t)> reader, size_t size);
    std::shared_ptr<const detail::AsyncInputContext> inputContext;
    std::function<RuntimeValue(size_t)> reader;
    size_t count;
};

/// A typed input function for one immutable input context, independent of any buffer layout.
class AsyncInputAccessor
{
public:
    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] RuntimeValue operator()(const AsyncRowHandle& row) const;

private:
    friend class AsyncInputAccessors;
    AsyncInputAccessor(std::shared_ptr<const detail::AsyncInputContext> inputContext, size_t index);
    std::shared_ptr<const detail::AsyncInputContext> inputContext;
    size_t index;
};

/// Factory-supplied input functions. Copies share their input context; fresh construction creates a distinct context.
/// Reflection transports the signature, not a live callback or a row. Readers are supplied locally when creating row handles.
class AsyncInputAccessors
{
public:
    AsyncInputAccessors();
    explicit AsyncInputAccessors(std::vector<DataType> types);

    [[nodiscard]] const std::vector<DataType>& getTypes() const;
    [[nodiscard]] size_t size() const;
    [[nodiscard]] AsyncInputAccessor operator[](size_t index) const;
    [[nodiscard]] AsyncRowHandle createRowHandle(std::function<RuntimeValue(size_t)> reader) const;
    [[nodiscard]] AsyncRowHandle createRowHandle(SharedRuntimeValues values) const;
    [[nodiscard]] SharedRuntimeValues read(const AsyncRowHandle& row) const;

private:
    std::shared_ptr<const detail::AsyncInputContext> inputContext;
};

template <>
struct Reflector<AsyncInputAccessors>
{
    Reflected operator()(const AsyncInputAccessors& inputs, const ReflectionContext& context) const;
};

template <>
struct Unreflector<AsyncInputAccessors>
{
    AsyncInputAccessors operator()(const Reflected& data, const ReflectionContext& context) const;
};
}
