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

#ifndef NES_CONNECTORS_INCLUDE_SINKS_MEDIUMS_FILESINK_HPP_
#define NES_CONNECTORS_INCLUDE_SINKS_MEDIUMS_FILESINK_HPP_

#include <SinkMedium.hpp>
#include <Util/FaultToleranceType.hpp>

#include <cstdint>
#include <memory>
#include <string>

namespace NES {

/**
 * @brief this class implements the File sing
 */
class FileSink : public SinkMedium {
  public:
    /**
     * @brief constructor that creates an empty file sink using a schema
     * @param schema of the print sink
     * @param format in which the data is written
     * @param filePath location of file on sink server
     * @param modus of writting (overwrite or append)
     * @param numberOfOrigins: number of origins of a given query
     */
    explicit FileSink(SinkFormatPtr format,
                      Runtime::NodeEnginePtr nodeEngine,
                      uint32_t numOfProducers,
                      const std::string& filePath,
                      bool append,
                      QueryId queryId,
                      DecomposedQueryPlanId querySubPlanId,
                      uint64_t numberOfOrigins = 1);

    /**
     * @brief dtor
     */
    ~FileSink() override;

    /**
     * @brief method to override virtual setup function
     * @Note currently the method does nothing
     */
    void setup() override;

    /**
     * @brief method to override virtual shutdown function
     * @Note currently the method does nothing
     */
    void shutdown() override;

    /**
     * @brief method to write a TupleBuffer
     * @param a tuple buffers pointer
     * @return bool indicating if the write was complete
     */
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;

    /**
     * @brief override the toString method for the file output sink
     * @return returns string describing the file output sink
     */
    std::string toString() const override;

    /**
     * @brief get file path
     */
    std::string getFilePath() const;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType() override;

    /**
     * @brief method to return if the sink is appended
     * @return bool indicating append
     */
    bool getAppend() const;

    /**
     * @brief method to return if the sink is append or overwrite
     * @return string of mode
     */
    std::string getAppendAsString() const;

  private:
    /**
     * @brief method to write a TupleBuffer to a local file system file
     * @param a tuple buffers pointer
     * @return bool indicating if the write was complete
     */
    bool writeDataToFile(Runtime::TupleBuffer& inputBuffer);

  protected:
    std::string filePath;
    std::ofstream outputFile;
    bool append{false};
};
using FileSinkPtr = std::shared_ptr<FileSink>;
}// namespace NES

#endif// NES_CONNECTORS_INCLUDE_SINKS_MEDIUMS_FILESINK_HPP_
