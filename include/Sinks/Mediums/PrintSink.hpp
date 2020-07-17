#ifndef PRINTSINK_HPP
#define PRINTSINK_HPP

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include <Sinks/Mediums/SinkMedium.hpp>
#include <iostream>

namespace NES {

/**
 * @brief this class provides a print sink
 */
class PrintSink : public SinkMedium {
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
    bool writeData(TupleBuffer& input_buffer) override;

    /**
     * @brief override the toString method for the print sink
     * @return returns string describing the print sink
     */
    const std::string toString() const override;

    /**
     * @brief Get sink type
     */
    std::string getMediumAsString() override;

  private:
    std::ostream& outputStream;
};
typedef std::shared_ptr<PrintSink> PrintSinkPtr;
}// namespace NES

#endif// PRINTSINK_HPP
