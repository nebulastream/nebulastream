#ifndef NES_NES_CORE_INCLUDE_NETWORK_DETAIL_ULIMITNUMFDCHANGER_HPP_
#define NES_NES_CORE_INCLUDE_NETWORK_DETAIL_ULIMITNUMFDCHANGER_HPP_

namespace NES::Network::detail {
namespace UlimitNumFdChanger {

    /// change ulimit to allow number of higher zmq socket
    void InitUlimitNumFdChanger() __attribute__((constructor));

    /// restore ulimit to previous values
    void DinitUlimitNumFdChanger();

    static int oldSoftNumFileLimit;
    static int oldHardNumFileLimit;
};
}// namespace NES::Network::detail

#endif//NES_NES_CORE_INCLUDE_NETWORK_DETAIL_ULIMITNUMFDCHANGER_HPP_
