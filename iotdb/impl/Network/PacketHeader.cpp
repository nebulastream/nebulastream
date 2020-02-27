#include <Network/PacketHeader.hpp>

namespace NES {
PacketHeader::PacketHeader(size_t tupleCount, size_t tupleSize, std::string sourceId)
    : tupleCount(tupleCount), tupleSize(tupleSize), sourceId(std::move(sourceId)) {
}
bool PacketHeader::operator==(const PacketHeader& rhs) const {
    return tupleCount == rhs.tupleCount &&
        tupleSize == rhs.tupleSize &&
        sourceId == rhs.sourceId;
}

bool PacketHeader::operator!=(const PacketHeader& rhs) const {
    return !(rhs == *this);
}
size_t PacketHeader::getTupleCount() const {
    return tupleCount;
}
void PacketHeader::setTupleCount(size_t tupleCount) {
    PacketHeader::tupleCount = tupleCount;
}
size_t PacketHeader::getTupleSize() const {
    return tupleSize;
}
void PacketHeader::setTupleSize(size_t tupleSize) {
    PacketHeader::tupleSize = tupleSize;
}
const std::string& PacketHeader::getSourceId() const {
    return sourceId;
}
void PacketHeader::setSourceId(const std::string& sourceId) {
    PacketHeader::sourceId = sourceId;
}

}

