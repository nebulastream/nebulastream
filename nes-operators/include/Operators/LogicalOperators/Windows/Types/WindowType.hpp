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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_TYPES_WINDOWTYPE_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_TYPES_WINDOWTYPE_HPP_

#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <vector>

namespace NES::Windowing {

class WindowType : public std::enable_shared_from_this<WindowType>{
  public:
    explicit WindowType();

    virtual ~WindowType() = default;

    /**
     * @brief Checks if the current window is of type WindowType
     * @tparam WindowType
     * @return bool true if window is of WindowType
     */
    template<class WindowType>
    bool instanceOf() {
        if (dynamic_cast<WindowType*>(this)) {
            return true;
        };
        return false;
    };

    /**
     * @brief Dynamically casts the window to a WindowType or throws an error.
     * @tparam WindowType
     * @return returns a shared pointer of the WindowType or throws an error if the type can't be casted.
     */
    template<class WindowType>
    std::shared_ptr<WindowType> as() {
        if (instanceOf<WindowType>()) {
            return std::dynamic_pointer_cast<WindowType>(this->shared_from_this());
        }
        throw std::logic_error("WindowType:: we performed an invalid cast of operator " + this->toString() + " to type "
                               + typeid(WindowType).name());
        return nullptr;
    }

    virtual std::string toString() const = 0;

    /**
     * @brief Check equality of this window type with the input window type
     * @param otherWindowType : the other window type to compare with
     * @return true if equal else false
     */
    virtual bool equal(WindowTypePtr otherWindowType) = 0;

    /**
     * @brief Infer stamp of the window type
     * @param schema : the schema of the window
     * @return true if success else false
     */
    virtual bool inferStamp(const SchemaPtr& schema) = 0;
    virtual bool inferStampOther(const SchemaPtr& schema) { return inferStamp(schema); }

    // TODO this will be implemented for all window types with issue #4624
    virtual std::size_t hash() const {
        const auto str = toString();
        return std::hash<std::string>()(str);
    };
};

}// namespace NES::Windowing

#endif // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_TYPES_WINDOWTYPE_HPP_
