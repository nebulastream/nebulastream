#ifndef PRINTSINK_HPP
#define PRINTSINK_HPP

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include <iostream>
#include <SourceSink/DataSink.hpp>

namespace NES {

/**
 * @brief this class provides a print sink
 */
class PrintSink : public DataSink {
  public:

    /**
     * @brief Default constructor
     * @Note the default output will be written to cout
     */
    PrintSink(std::ostream& pOutputStream = std::cout);

    /**
       * @brief Default constructor
       * @Note the default output will be written to cout
       * @param schema of the written buffer tuples
       */
    PrintSink(SchemaPtr pSchema, std::ostream& pOutputStream = std::cout);

    /**
     * @brief destructor
     * @Note this is required by some tests
     * TODO: find out why this is required
     */
    ~PrintSink();

    /**
     * @brief setup method for print sink
     * @Note required due to derivation but does nothing
     */
    void setup() override;

    /**
     * @brief shutdown method for print sink
     * @Note required due to derivation but does nothing
     */
    void shutdown() override;

    /**
     * @brief method to write the content of a tuple buffer to output console
     * @param tuple buffer to write
     * @return bool indicating success of the write
     */
    bool writeData(const TupleBufferPtr input_buffer) override;

    /**
     * @brief override the toString method for the print sink
     * @return returns string describing the print sink
     */
    const std::string toString() const override;

    SinkType getType() const override;

  private:
    std::ostream& outputStream;
    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(DataSink);
    }
};
}  // namespace NES

#endif // PRINTSINK_HPP
