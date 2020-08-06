#ifndef INCLUDE_CSVSOURCE_H_
#define INCLUDE_CSVSOURCE_H_

#include <Sources/DataSource.hpp>
#include <fstream>
#include <string>

namespace NES {
class TupleBuffer;

/**
 * @brief this class implement the CSV as an input source
 */
class CSVSource : public DataSource {
  public:
    /**
   * @brief constructor of CSV sou1rce
   * @param schema of the source
   * @param path to the csv file
   * @param delimiter inside the file, default ","
   * @param number of buffers to create
   */
    explicit CSVSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& file_path,
                       const std::string& delimiter, size_t numBuffersToProcess, size_t frequency);

    /**
     * @brief override the receiveData method for the csv source
     * @return returns a buffer if available
     */
    std::optional<TupleBuffer> receiveData() override;

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    void fillBuffer(TupleBuffer&);

    /**
     * @brief override the toString method for the csv source
     * @return returns string describing the binary source
     */
    const std::string toString() const override;

    /**
     * @brief Get source type
     * @return source type
     */
    SourceType getType() const override;

    /**
     * @brief Get file path for the csv file
     */
    const std::string getFilePath() const;

    /**
     * @brief Get the csv file delimiter
     */
    const std::string getDelimiter() const;

  private:
    std::string filePath;
    size_t tupleSize;
    std::string delimiter;
    size_t currentPosInFile;
};

typedef std::shared_ptr<CSVSource> CSVSourcePtr;
}// namespace NES

#endif /* INCLUDE_CSVSOURCE_H_ */
