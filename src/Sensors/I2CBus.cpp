#include <Sensors/I2CBus.hpp>
#include <Util/Logger.hpp>
#include <cstring>
#include <fcntl.h>

namespace NES {
namespace Sensors {

I2CBus::I2CBus(const char *filename)
    : GenericBus(filename, BusType::I2C){
    NES_INFO("I2CBus: Creating bus");
}

I2CBus::~I2CBus() {
    NES_DEBUG("I2CBus: Destroying bus");
}

bool I2CBus::initBus(int address) {
    if ((this->file = open(this->fileName, O_RDWR)) < 0) {
        // Failed to open bus
        // TODO: use proper logging
        return false;
    }

    if (ioctl(this->file, I2C_SLAVE, address) < 0) {
        // Failed to control sensor
        // TODO: use proper logging
        return false;
    }

    return true;
}

bool I2CBus::writeData(int address, int size, unsigned char *buffer) {
    return !(rawI2CRdrw(address, I2C_SMBUS_WRITE, size, buffer) < 0);
}

bool I2CBus::readData(int address, int size, unsigned char* buffer) {
    return !(rawI2CRdrw(address, I2C_SMBUS_READ, size, buffer) < 0);
}

int I2CBus::rawI2CRdrw(uint8_t address, uint8_t readWriteOperation, uint8_t size, unsigned char* buffer) {
// envelope data for ioctl
    struct i2c_smbus_ioctl_data ioctlData;
    // the actual data to send over i2c
    union i2c_smbus_data smbusData;

    // the return status
    int returnStatus;

    if (size > I2C_SMBUS_BLOCK_MAX) {
        // TODO: replace with NES_ERROR
        return -1;
    }

    // First byte is always the size to write and to receive
    // https://github.com/torvalds/linux/blob/master/drivers/i2c/i2c-core-smbus.c
    // (See i2c_smbus_xfer_emulated CASE:I2C_SMBUS_I2C_BLOCK_DATA)
    smbusData.block[0] = size;

    if (readWriteOperation != I2C_SMBUS_READ) {
        // prepare for reading
        memcpy(&smbusData.block[1], buffer, size);
    }

    // prepare ioctlData
    ioctlData.read_write = readWriteOperation;// type of operation
    ioctlData.command = address;     // chip address to read
    ioctlData.size = I2C_SMBUS_I2C_BLOCK_DATA;
    ioctlData.data = &smbusData;// buffer with data or buffer to fill (depends on type of rw)

    returnStatus = ioctl(this->file, I2C_SMBUS, &ioctlData);
    if (returnStatus < 0) {
        // I2C r/w operation failed
        return returnStatus;
    }

    if (readWriteOperation == I2C_SMBUS_READ) {
        // read from bus
        memcpy(buffer, &smbusData.block[1], size);
    }

    // less that zero indicates errors from ioctl
    return returnStatus;
}

}//namespace Sensors
}//namespace NES