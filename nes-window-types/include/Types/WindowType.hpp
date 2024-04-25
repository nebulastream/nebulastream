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
#include <API/Schema.hpp>

#include <memory>
#include <vector>

namespace NES::Windowing {

class WindowType;
using WindowTypePtr = std::shared_ptr<WindowType>;

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
        }
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

    /**
     * @brief Get the hash of the window type
     *
     * This function computes a hash value uniquely identifying the window type including characteristic attributes.
     * The hash value is different for different WindowTypes and same WindowTypes with different attributes.
     * Especially a SlidingWindow of same size and slide returns a different hash value,
     * than a TumblingWindow with the same size.
     *
     * @return the hash of the window type
     */
    virtual uint64_t hash() const = 0;
};

}// namespace NES::Windowing

#endif // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_TYPES_WINDOWTYPE_HPP_
