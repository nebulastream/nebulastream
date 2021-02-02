/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_API_WINDOW_DistributionCharacteristic_HPP_
#define NES_INCLUDE_API_WINDOW_DistributionCharacteristic_HPP_
#include <memory>

namespace NES::Windowing {

class DistributionCharacteristic;
typedef std::shared_ptr<DistributionCharacteristic> DistributionCharacteristicPtr;

/**
 * @brief The time stamp characteristic represents if an window is in event or processing time.
 */
class DistributionCharacteristic {
  public:
    /**
     * @brief The type as enum.
     */
    enum Type { Complete, Slicing, Combining, Merging };
    explicit DistributionCharacteristic(Type type);

    /**
     * @brief Factory to create central window that do slcicing and combining
     * @return DistributionCharacteristicPtr
     */
    static DistributionCharacteristicPtr createCompleteWindowType();

    /**
     * @brief Factory to to create a window slicer
     * @return
     */
    static DistributionCharacteristicPtr createSlicingWindowType();

    /**
    * @brief Factory to create a window combiner
    * @return
    */
    static DistributionCharacteristicPtr createCombiningWindowType();

    /**
    * @brief Factory to create a window merger
    * @return
    */
    static DistributionCharacteristicPtr createMergingWindowType();

    /**
     * @return The DistributionCharacteristic type.
     */
    Type getType();

    /**
   * @return The DistributionCharacteristic type.
   */
    std::string toString();

  private:
    Type type;
};

}// namespace NES::Windowing
#endif//NES_INCLUDE_API_WINDOW_DistributionCharacteristic_HPP_
