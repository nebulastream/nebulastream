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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWTYPES_WINDOWTYPE_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWTYPES_WINDOWTYPE_HPP_

#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <vector>

namespace NES::Windowing {

class WindowType {
  public:
    explicit WindowType();

    virtual ~WindowType() = default;

    /**
     * @return true if this is a timebased window
     */
    virtual bool isTimeBasedWindowType();

    /**
    * @return true if this is a contentbased window
    */
    virtual bool isContentBasedWindowType();

    virtual std::string toString() = 0;

    /**
     * @brief Check equality of this window type with the input window type
     * @param otherWindowType : the other window type to compare with
     * @return true if equal else false
     */
    virtual bool equal(WindowTypePtr otherWindowType) = 0;

    /**
       * Cast the current window type as a time based window type
       * @return a shared pointer of TimeBasedWindowType
       */
    static TimeBasedWindowTypePtr asTimeBasedWindowType(std::shared_ptr<WindowType> windowType);

    /**
       * Cast the current window type as a content based window type
       * @return a shared pointer of ContentBasedWindowType
       */
    static ContentBasedWindowTypePtr asContentBasedWindowType(std::shared_ptr<WindowType> windowType);

    /**
     * @brief Infer stamp of the window type
     * @param schema : the schema of the window
     * @return true if success else false
     */
    virtual bool inferStamp(const SchemaPtr& schema) = 0;
};

}// namespace NES::Windowing

#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWTYPES_WINDOWTYPE_HPP_
