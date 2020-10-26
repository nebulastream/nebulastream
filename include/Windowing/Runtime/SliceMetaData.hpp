#ifndef NES_INCLUDE_WINDOWING_SLICEMETADATA_HPP_
#define NES_INCLUDE_WINDOWING_SLICEMETADATA_HPP_
#include <cstdint>
namespace NES::Windowing {

/**
* SliceMetaData stores the meta data of a slice to identify if a record can be assigned to a particular slice.
*/
class SliceMetaData {
  public:
    /**
     * @brief Construct a new slice meta data object.
     * @param startTs
     * @param endTs
     */
    SliceMetaData(uint64_t startTs, uint64_t endTs);

    /**
    * @brief The start timestamp of this slice
    */
    uint64_t getStartTs();

    /**
     * @brief The end timestamp of this slice
     */
    uint64_t getEndTs();

  private:
    uint64_t startTs;
    uint64_t endTs;
};

}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_SLICEMETADATA_HPP_
