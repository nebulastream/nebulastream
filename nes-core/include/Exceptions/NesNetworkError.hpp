/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_CORE_INCLUDE_EXCEPTIONS_NESNETWORKERROR_HPP_
#define NES_CORE_INCLUDE_EXCEPTIONS_NESNETWORKERROR_HPP_
#include<Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Network/NetworkMessage.hpp>
#include <stdexcept>

/**
 * @brief This exception represents a network error
 */
namespace NES {

class NesNetworkException : public Exceptions::RuntimeException {
  public:
    explicit NesNetworkException(NES::Network::Messages::ErrMessage& msg)
        : msg(msg), Exceptions::RuntimeException("Network error") {}

    const NES::Network::Messages::ErrMessage& getErrorMessage() const { return msg; }

  private:
    const NES::Network::Messages::ErrMessage msg;
};

}// namespace NES
#endif// NES_CORE_INCLUDE_EXCEPTIONS_NESNETWORKERROR_HPP_
