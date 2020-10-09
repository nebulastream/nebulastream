#ifndef NES_INCLUDE_WINDOWING_SLICEMETADATA_HPP_
#define NES_INCLUDE_WINDOWING_SLICEMETADATA_HPP_
#include <cstdint>
namespace NES{

/**
* SliceMetaData stores the meta data of a slice to identify if a record can be assigned to a particular slice.
*/
class SliceMetaData {
  public:
    SliceMetaData(uint64_t start_ts, uint64_t end_ts) : start_ts(start_ts), end_ts(end_ts) {}

    /**
    * The start timestamp of this slice
    */
    uint64_t getStartTs() {
        return start_ts;
    }

    /**
     * The end timestamp of this slice
     */
    uint64_t getEndTs() {
        return end_ts;
    }

  private:
    uint64_t start_ts;
    uint64_t end_ts;
};


}
#endif//NES_INCLUDE_WINDOWING_SLICEMETADATA_HPP_
