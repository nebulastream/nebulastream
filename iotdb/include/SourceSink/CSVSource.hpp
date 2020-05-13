#ifndef INCLUDE_CSVSOURCE_H_
#define INCLUDE_CSVSOURCE_H_

#include <SourceSink/DataSource.hpp>
#include <fstream>
#include <string>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>

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
    CSVSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& file_path,
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
    const std::string toString() const;

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
    CSVSource();

    std::string filePath;
    size_t tupleSize;
    std::string delimiter;
    size_t currentPosInFile;
    /**
     * @brief method for serialization, all listed variable below are added to the
     * serialization/deserialization process
     */
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar,
                   const unsigned int version) {
        ar& boost::serialization::base_object<DataSource>(*this);
        ar& filePath;
        ar& tupleSize;
        ar& delimiter;
        ar& generatedTuples;
        ar& generatedBuffers;
    }
};

typedef std::shared_ptr<CSVSource> CSVSourcePtr;
}  // namespace NES
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(NES::CSVSource)
#endif /* INCLUDE_CSVSOURCE_H_ */
