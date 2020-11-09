/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_I2C_BUS_HPP_
#define NES_INCLUDE_I2C_BUS_HPP_

#include <linux/i2c-dev.h>
#include <linux/i2c.h>
#include <sys/ioctl.h>

#include <Sensors/GenericBus.hpp>

namespace NES {
namespace Sensors {

class I2CBus : public GenericBus {
  public:
    /**
     * @brief constructor only with a path as argument
     * @param filename, the path to read the file from
     */
    I2CBus(const char* filename);

    /**
     * destructor
     */
    ~I2CBus();

  private:
    /**
     * @brief overrides the initBus method
     * @return true if address is in the bus and controllable
     */
    bool initBus(int address) override;

    /**
     * @brief overrides the writeData method
     * @return the status code of ioctl for I2C
     */
    bool writeData(int address, int size, unsigned char* buffer) override;

    /**
     * @brief overrides the readData method
     * @return the status code of ioctl for I2C
     */
    bool readData(int address, int size, unsigned char* buffer) override;

    /**
     * Perform a r/w operation on top of raw I2C / SMBus driver
     * @param address the register addres we're interested in
     * @param readWriteOperation the operation to perform from I2C
     * @param size the size of the buffer to read/write
     * @param buffer the data
     * @return operation status
     */
    int rawI2CRdrw(uint8_t address, uint8_t readWriteOperation, uint8_t size, unsigned char* buffer);
};

typedef std::shared_ptr<I2CBus> I2CBusPtr;

}//namespace Sensors
}//namespace NES
#endif//NES_INCLUDE_I2C_BUS_HPP_