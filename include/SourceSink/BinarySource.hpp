#ifndef INCLUDE_BINARYSOURCE_H_
#define INCLUDE_BINARYSOURCE_H_

#include <SourceSink/DataSource.hpp>
#include <fstream>

namespace NES {

/**
 * @brief this class provides a binary file as source
 */
class BinarySource : public DataSource {
  public:
    /**
     * @brief constructor for binary source
     * @param schema of the data source
     * @param file path
     */
    BinarySource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& file_path);

    /**
     * @brief override the receiveData method for the binary source
     * @return returns a buffer if available
     */
    std::optional<TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the binary source
     * @return returns string describing the binary source
     */
    const std::string toString() const override;

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    void fillBuffer(TupleBuffer&);

    SourceType getType() const override;

    const std::string& getFilePath() const;

  private:
    //this one only required for serialization
    BinarySource();
    std::ifstream input;
    std::string file_path;

    int file_size;
    size_t tuple_size;
};

typedef std::shared_ptr<BinarySource> BinarySourcePtr;

}// namespace NES
#endif /* INCLUDE_BINARYSOURCE_H_ */
