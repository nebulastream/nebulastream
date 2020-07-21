#include <Sensors/SensorBus.hpp>
#include <cstring>
#include <fcntl.h>

namespace NES {

/**
 * @brief perform raw I2C/SMBus operation, we need a buffer to fill the data (read)
 * or to read from (write) to an `address`, behind a bus located in `file`
 * @param file the sensor bus file descriptor
 * @param address the chip address to write or read from
 * @param readWrite the type of the operation, found in i2c-dev
 * @param size the size of data to read or write
 * @param buffer the buffer to fill or read from
 * @return output of ioctl (less than zero in cases of failure)
 */
int SensorBus::rawI2CRdrw(int file, uint8_t address, uint8_t readWrite, uint8_t size, unsigned char* buffer) {
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

    if (readWrite != I2C_SMBUS_READ) {
        // prepare for reading
        memcpy(&smbusData.block[1], buffer, size);
    }

    // prepare ioctlData
    ioctlData.read_write = readWrite;// type of operation
    ioctlData.command = address;     // chip address to read
    ioctlData.size = I2C_SMBUS_I2C_BLOCK_DATA;
    ioctlData.data = &smbusData;// buffer with data or buffer to fill (depends on type of rw)

    returnStatus = ioctl(file, I2C_SMBUS, &ioctlData);
    if (returnStatus < 0) {
        // I2C r/w operation failed
        return returnStatus;
    }

    if (readWrite == I2C_SMBUS_READ) {
        // read from bus
        memcpy(buffer, &smbusData.block[1], size);
    }

    // less that zero indicates errors from ioctl
    return returnStatus;
}

bool SensorBus::read(int file, int address, int size, unsigned char* buffer) {
    // less that zero indicates errors
    return !(SensorBus::rawI2CRdrw(file, address, I2C_SMBUS_READ, size, buffer) < 0);
}

bool SensorBus::write(int file, int address, int size, unsigned char* buffer) {
    // less that zero indicates errors
    return !(SensorBus::rawI2CRdrw(file, address, I2C_SMBUS_WRITE, size, buffer) < 0);
}

bool SensorBus::initBus(int& file, const char* filename, int address) {
    if ((file = open(filename, O_RDWR)) < 0) {
        // Failed to open bus
        // TODO: use proper logging
        return false;
    }

    if (ioctl(file, I2C_SLAVE, address) < 0) {
        // Failed to control sensor
        // TODO: use proper logging
        return false;
    }

    return true;
}

}// namespace NES
