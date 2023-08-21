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

#ifndef NES_CORE_INCLUDE_SINKS_MEDIUMS_FILESINK_HPP_
#define NES_CORE_INCLUDE_SINKS_MEDIUMS_FILESINK_HPP_

#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/FaultToleranceType.hpp>

#include <cstdint>
#include <memory>
#include <string>

#ifdef ENABLE_ARROW_BUILD
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#endif

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
     * @param faultToleranceType: fault tolerance type of a query
     * @param numberOfOrigins: number of origins of a given query
     * @param addTimestamp: bool to add a timestamp when writing to file as text
     */
    explicit FileSink(SinkFormatPtr format,
                      Runtime::NodeEnginePtr nodeEngine,
                      uint32_t numOfProducers,
                      const std::string& filePath,
                      bool append,
                      QueryId queryId,
                      QuerySubPlanId querySubPlanId,
                      FaultToleranceType faultToleranceType = FaultToleranceType::NONE,
                      uint64_t numberOfOrigins = 1,
                      bool addTimestamp = false);

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

    /**T
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

#ifdef ENABLE_ARROW_BUILD
    /**
     * @brief method to write a TupleBuffer to an Arrow File. Arrow opens its own filestream to write the file and therefore
     * we require and abstraction to be able to obtain this object, or otherwise we need to write our own Arrow writer.
     * @param a tuple buffers pointer
     * @return bool indicating if the write was complete
     */
    bool writeDataToArrowFile(Runtime::TupleBuffer& inputBuffer);

    /**
     * @brief method to write a TupleBuffer to an Arrow File. Arrow opens its own filestream to write the file and therefore
     * we require and abstraction to be able to obtain this object, or otherwise we need to write our own Arrow writer.
     * @param a tuple buffers pointer
     * @return bool indicating if the write was complete
     */
    arrow::Status openArrowFile(std::shared_ptr<arrow::io::FileOutputStream> arrowFileOutputStream,
                                std::shared_ptr<arrow::Schema> arrowSchema,
                                std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter);
#endif
};
using FileSinkPtr = std::shared_ptr<FileSink>;
}// namespace NES

#endif// NES_CORE_INCLUDE_SINKS_MEDIUMS_FILESINK_HPP_
