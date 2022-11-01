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

#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <memory>

namespace NES::Runtime::Execution::MemoryProvider {
    RowMemoryProvider::RowMemoryProvider(Runtime::MemoryLayouts::MemoryLayoutPtr rowMemoryLayoutPtr) : 
        rowMemoryLayoutPtr(rowMemoryLayoutPtr) {
            //todo assert that MemoryLayoutPtr is RowLayoutPtr
            // - if correct layout is confirmed here, we can change dynamic_pointer_cast to static_pointer_cast
            // - could delete constructor and use static 'constructor' function to enable error handling
        };
    
    MemoryLayouts::MemoryLayoutPtr RowMemoryProvider::getMemoryLayoutPtr() {
        return rowMemoryLayoutPtr;
    }

    // MemoryLayouts::RowLayoutPtr RowMemoryProvider::getRowMemoryLayoutPtr() {
    //     return std::dynamic_pointer_cast<Runtime::MemoryLayouts::RowLayout>(rowMemoryLayoutPtr);
    // }

} //namespace