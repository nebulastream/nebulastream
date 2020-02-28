#ifndef NES_INCLUDE_NETWORK_PACKETHEADER_HPP_
#define NES_INCLUDE_NETWORK_PACKETHEADER_HPP_

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <string>

namespace NES {
/**
 * @brief This class is the header of the internal transmission used by the InputGate.
 */
class PacketHeader {
  public:
    /**
     * @brief Constructor for boost serialization.
     */
    PacketHeader() = default;

    /**
     * @brief Constructor for the PacketHeader
     * @param tupleCount The number of tuples for the follow up buffer
     * @param tupleSize The size of each tuple in the buffer.
     * @param sourceId The sourceId to which the buffer belongs to.
     */
    PacketHeader(size_t tupleCount, size_t tupleSize, std::string sourceId);

    // equal operators
    bool operator==(const PacketHeader& rhs) const;
    bool operator!=(const PacketHeader& rhs) const;

    /**
     * @brief getter for number of tuples
     * @return number of tuples
     */
    size_t getTupleCount() const;

    /**
     * @brief setter for number of tuples
     * @param tupleCount number of tuples
     */
    void setTupleCount(size_t tupleCount);

    /**
     * @brief getter for size of tuples
     * @return the size of tuples
     */
    size_t getTupleSize() const;

    /**
     * @brief setter for size of tuples
     * @param tupleSize
     */
    void setTupleSize(size_t tupleSize);

    /**
     * @brief getter for the sourceId
     * @return the sourceId
     */
    const std::string& getSourceId() const;

    /**
     * @brief setter for sourceId
     * @param sourceId the sourceId
     */
    void setSourceId(const std::string& sourceId);

    /**
     * @brief a toString method to return the fields of the PacketHeader in a pretty way.
     * @return the pretty string of the PacketHeader.
     */
    std::string toString();

  private:
    size_t tupleCount{};
    size_t tupleSize{};
    std::string sourceId;

    /**
   * @brief method for serialization, all listed variable below are added to the
   * serialization/deserialization process
   */
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar,
                   const unsigned int version) {
        ar & tupleCount;
        ar & tupleSize;
        ar & sourceId;
    }
};
}

#endif //NES_INCLUDE_NETWORK_PACKETHEADER_HPP_
