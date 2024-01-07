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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_CASTING_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_CASTING_HPP_
#include <memory>
#include <type_traits>
namespace NES {

// polyfill because concepts do not exist on all platforms yet.
template<typename _From, typename _To>
concept convertible_to = std::is_convertible_v<_From, _To> && requires { static_cast<_To>(std::declval<_From>()); };

class TypeCastable {
  public:
    enum class Kind : uint8_t {
        IntegerValue,
        FloatValue,
        BooleanValue,
        TraceValue,
        DoubleValue,
        MemRef,
    };
    TypeCastable(Kind k) : kind(k) {}
    Kind getKind() const { return kind; }

  private:
    const Kind kind;
};

template<typename T>
concept GetType = requires(T a) {
    { T::type } -> convertible_to<TypeCastable::Kind>;
};

template<class X, class Y>
    requires(std::is_base_of<Y, X>::value == false)
inline constexpr bool instanceOf(const std::unique_ptr<Y>&) {
    return false;
}

template<GetType X, class Y>
    requires(std::is_base_of<Y, X>::value == true)
inline bool instanceOf(const std::unique_ptr<Y>& y) {
    return X::type == y->getKind();
}

template<GetType X>
inline bool instanceOf(const TypeCastable& y) {
    return X::type == y.getKind();
}

}// namespace NES

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_CASTING_HPP_
