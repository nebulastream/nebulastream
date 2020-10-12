#include <Windowing/Runtime/SliceMetaData.hpp>

namespace NES {

SliceMetaData::SliceMetaData(uint64_t startTs, uint64_t endTs) : startTs(startTs), endTs(endTs){};

uint64_t SliceMetaData::getEndTs() {
    return endTs;
}

uint64_t SliceMetaData::getStartTs() {
    return startTs;
}

}// namespace NES