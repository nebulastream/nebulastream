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

#ifndef NES_NES_COMMON_INCLUDE_UTIL_COMPRESSIONMETHODS_HPP_
#define NES_NES_COMMON_INCLUDE_UTIL_COMPRESSIONMETHODS_HPP_

#include <string>
namespace NES {

/**
 * @brief This class provides methods for compressing and decompressing data. For now, this class is intended to be used
 * with the statistic framework. But this can be extended to other use cases as well.
 */
class CompressionMethods {
  public:
    /**
     * @brief This method compresses the data using run length encoding.
     * @param data
     * @return Compressed string
     */
    static std::string runLengthEncoding(const std::string& data);

    /**
     * @brief This method decompresses the data using run length decoding.
     * @param data
     * @return Decompressed string
     */
    static std::string runLengthDecoding(const std::string& data);
};

}// namespace NES

#endif//NES_NES_COMMON_INCLUDE_UTIL_COMPRESSIONMETHODS_HPP_
