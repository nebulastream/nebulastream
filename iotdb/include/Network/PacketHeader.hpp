#ifndef NES_INCLUDE_NETWORK_PACKETHEADER_HPP_
#define NES_INCLUDE_NETWORK_PACKETHEADER_HPP_

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <string>

namespace NES {
class PacketHeader {
  public:
    PacketHeader() = default;
    PacketHeader(size_t tupleCount, size_t tupleSize, std::string sourceId);
    bool operator==(const PacketHeader& rhs) const;
    bool operator!=(const PacketHeader& rhs) const;

  private:
    size_t tupleCount{};
  public:
    size_t getTupleCount() const;
    void setTupleCount(size_t tupleCount);
    size_t getTupleSize() const;
    void setTupleSize(size_t tupleSize);
    const std::string& getSourceId() const;
    void setSourceId(const std::string& sourceId);
  private:
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
