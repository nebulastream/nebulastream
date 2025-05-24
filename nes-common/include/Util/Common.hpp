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
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>
#include <ErrorHandling.hpp>

namespace NES
{
/// Used by all `Node`s to control the verbosity of their `operator<<` method.
enum class VerbosityLevel : uint8_t
{
    /// Print everything.
    Debug = 0,
    /// Print only the name of the Node/Function, to make the queryplan printing easier to read.
    QueryPlan = 1,
};
/// Check the iword array of the passed stream for the set `VerbosityLevel` using the allocated index.
VerbosityLevel getVerbosityLevel(std::ostream& os);
/// Set the iword array of the passed stream to the passed `VerbosityLevel` using the allocated index.
void setVerbosityLevel(std::ostream& os, const VerbosityLevel& level);
/// The index for the `VerbosityLevel` into any stream's iword array.
/// The index may only be allocated once, otherwise it will change and we won't be able to set and retrieve it correctly.
int getIwordIndex();

inline std::ostream& operator<<(std::ostream& os, const VerbosityLevel& level)
{
    setVerbosityLevel(os, level);
    return os;
}
}

namespace NES::Util
{


/// check if the given object is an instance of the specified type.
template <typename Out, typename In>
bool instanceOf(const std::shared_ptr<In>& obj)
{
    return std::dynamic_pointer_cast<Out>(obj).get() != nullptr;
}

/// check if the given object is an instance of the specified type.
template <typename Out, typename In>
bool instanceOf(const In& obj)
{
    return dynamic_cast<Out*>(&obj);
}

template <typename Derived, typename Base>
bool instanceOf(const std::unique_ptr<Base>& ptr)
{
    return dynamic_cast<const Derived*>(ptr.get()) != nullptr;
}

/// check if the given object is an instance of the specified type.
template <typename Out, typename In>
bool instanceOfConst(const In& obj)
{
    return dynamic_cast<Out*>(const_cast<std::remove_const_t<In>*>(&obj));
}

/// cast the given object to the specified type.
template <typename Out, typename In>
std::shared_ptr<Out> as(const std::shared_ptr<In>& obj)
{
    if (auto ptr = std::dynamic_pointer_cast<Out>(obj))
    {
        return ptr;
    }
    throw InvalidDynamicCast("Invalid dynamic cast: from {} to {}", std::string(typeid(In).name()), std::string(typeid(Out).name()));
}

/// cast the given object to the specified type.
template <typename Out, typename In>
Out as(const In& obj)
{
    return *dynamic_cast<Out*>(&obj);
}

/// cast the given object to the specified type.
template <typename Out, typename In>
Out asConst(const In& obj)
{
    return *dynamic_cast<Out*>(const_cast<std::remove_const_t<In>*>(&obj));
}

/// cast the given object to the specified type.
template <typename Out, typename In>
std::shared_ptr<Out> as_if(const std::shared_ptr<In>& obj)
{
    return std::dynamic_pointer_cast<Out>(obj);
}


template <typename T, typename S>
std::unique_ptr<T> dynamic_pointer_cast(std::unique_ptr<S>&& ptr) noexcept
{
    if (auto* converted = dynamic_cast<T*>(ptr.get()))
    {
        ptr.release();
        return std::unique_ptr<T>(converted);
    }
    return nullptr;
}
}
