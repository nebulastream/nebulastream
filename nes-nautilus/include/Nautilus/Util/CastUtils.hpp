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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_UTIL_CASTUTILS_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_UTIL_CASTUTILS_HPP_
#include <memory>
#include <typeinfo>

/**
 * This file provides simple and fast type checks and casts.
 */
namespace NES::Nautilus {

/**
 * @brief Provides an identifier for runtime types.
 */
class TypeIdentifier {
  public:
    /**
     * @brief Factory method to create a new type identifier for a specific template type. The template type has to be derived from typed.
     * @tparam T derived from typed.
     * @return TypeIdentifier
     */
    template<class T>
    static TypeIdentifier create() {
        auto& typeId = typeid(T);
        return TypeIdentifier(typeId);
    }

    template<class name>
    bool isType() const {
        return this == &name::type;
    }

    /**
     * @brief Returns the runtime type info
     * @return std::type_info&
     */
    const std::type_info& getTypeInfo() const;

  private:
    TypeIdentifier(const std::type_info& typeInfo);
    const std::type_info& typeInfo;
};

template<typename T, typename U>
concept IsBase = std::is_base_of<T, U>::value;
template<typename T, typename U>
concept IsSame = std::is_same<T, U>::value;

class Typed;
template<typename T>
concept IsTyped = requires(T&) { IsBase<Typed, T> == true; };

template<typename T>
concept HasTypeIdentifier = requires(T&) {
    { std::remove_pointer<T>::type::type } -> IsSame<const TypeIdentifier&>;
};

/**
 * @brief If a class inherits from Typed it enables the custom casting system and allows the isa and cast methods.
 */
class Typed {
  public:
    Typed(const TypeIdentifier* typeIdentifier);
    inline const TypeIdentifier* getTypeIdentifier() const { return typeIdentifier; };

    template<HasTypeIdentifier T>
    bool isType() const {
        return &T::type == typeIdentifier;
    }

  private:
    const TypeIdentifier* typeIdentifier;
};

template<HasTypeIdentifier T>
T* cast(Typed* typed) {
    return static_cast<T*>(typed);
};

template<HasTypeIdentifier T>
T* cast(Typed& typed) {
    return static_cast<T*>(&typed);
};

template<HasTypeIdentifier T, IsTyped U>
std::shared_ptr<T> cast(std::shared_ptr<U> typed) {
    return std::static_pointer_cast<T>(typed);
};

template<class T>
struct is_unique_ptr : std::false_type {};

template<class T, class D>
struct is_unique_ptr<std::unique_ptr<T, D>> : std::true_type {};

template<class T>
struct is_shared_ptr : std::false_type {};

template<class T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type {};

template<HasTypeIdentifier T, IsTyped U>
bool isa(U& typed) {
    if constexpr (is_unique_ptr<U>::value) {
        return &T::type == typed->getTypeIdentifier();
    } else if constexpr (is_shared_ptr<U>::value) {
        return &T::type == typed->getTypeIdentifier();
    } else if constexpr (std::is_pointer<U>::value) {
        return &T::type == typed->getTypeIdentifier();
    } else {
        return &T::type == typed.getTypeIdentifier();
    }
};

template<HasTypeIdentifier T>
T* cast_if(Typed* typed) {
    if (isa<T>(typed)) {
        return cast<T>(typed);
    }
    return nullptr;
};

}// namespace NES::Nautilus

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_UTIL_CASTUTILS_HPP_
