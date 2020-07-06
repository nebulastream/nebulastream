#ifndef NES_INCLUDE_EXCEPTIONS_NESNETWORKERROR_HPP_
#define NES_INCLUDE_EXCEPTIONS_NESNETWORKERROR_HPP_
#include <Network/NesPartition.hpp>
#include <Network/NetworkMessage.hpp>
#include <stdexcept>

/**
 * @brief This exception represents a network error
 */
namespace NES {

class NesNetworkError : public std::runtime_error {
  public:
    explicit NesNetworkError(NES::Network::Messages::ErrMessage& msg) : msg(msg), std::runtime_error("") {
    }

    const NES::Network::Messages::ErrMessage& getErrorMessage() const {
        return msg;
    }

  private:
    const NES::Network::Messages::ErrMessage msg;
};

}// namespace NES
#endif//NES_INCLUDE_EXCEPTIONS_NESNETWORKERROR_HPP_
