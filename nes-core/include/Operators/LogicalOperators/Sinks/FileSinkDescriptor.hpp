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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <string>

class SinkMedium;
namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical file sink
 */
class FileSinkDescriptor : public SinkDescriptor {
  public:
    /**
     * @brief Factory method to create a new file sink descriptor
     * @param schema output schema of this sink descriptor
     * @param filePath the path to the output file
     * @return descriptor for file sink
     */
    static SinkDescriptorPtr create(std::string fileName, std::string sinkFormat, const std::string& append);

    /**
     * @brief Factory method to create a new file sink descriptor as default
     * @param schema output schema of this sink descriptor
     * @param filePath the path to the output file
     * @return descriptor for file sink
     */
    static SinkDescriptorPtr create(std::string fileName);

    /**
     * @brief Get the file name where the data is to be written
     */
    const std::string& getFileName() const;

    std::string toString() override;
    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;

    std::string getSinkFormatAsString();

    bool getAppend() const;

  private:
    explicit FileSinkDescriptor(std::string fileName, std::string sinkFormat, bool append);
    std::string fileName;
    std::string sinkFormat;
    bool append;
};

using FileSinkDescriptorPtr = std::shared_ptr<FileSinkDescriptor>;
}// namespace NES

#endif  // NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_
