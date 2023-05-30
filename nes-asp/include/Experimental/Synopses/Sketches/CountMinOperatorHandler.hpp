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

#ifndef NES_COUNTMINOPERATORHANDLER_HPP
#define NES_COUNTMINOPERATORHANDLER_HPP

#include <Runtime/Execution/OperatorHandler.hpp>
#include <Nautilus/Interface/Fixed2DArray/Fixed2DArray.hpp>


namespace NES::ASP {

class CountMinOperatorHandler : public Runtime::Execution::OperatorHandler {

  public:
    /**
     * @brief Initializes the fixed2DArray
     * @param numRows
     * @param numCols
     * @param entrySize
     */
    void setup(uint64_t numRows, uint64_t numCols, uint64_t entrySize);



  private:
    std::unique_ptr<Nautilus::Interface::Fixed2DArray> fixed2DArray;

};
} // namespace NES::ASP

#endif //NES_COUNTMINOPERATORHANDLER_HPP
