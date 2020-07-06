#include <Util/SensorBus.hpp>
#include <cstring>
#include <fcntl.h>

namespace NES {

int SensorBus::raw_i2c_rdrw(int file, uint8_t address, uint8_t read_write, uint8_t size, unsigned char *buffer) {
    // envelope data for ioctl
    struct i2c_smbus_ioctl_data ioctl_data;
    // the actual data to send over i2c
    union i2c_smbus_data smbus_data;

    // the return status
    int return_status;

    if(size > I2C_SMBUS_BLOCK_MAX) {
        // TODO: replace with NES_ERROR
        return -1;
    }

    // First byte is always the size to write and to receive
    // https://github.com/torvalds/linux/blob/master/drivers/i2c/i2c-core-smbus.c
    // (See i2c_smbus_xfer_emulated CASE:I2C_SMBUS_I2C_BLOCK_DATA)
    smbus_data.block[0] = size;

    if(read_write != I2C_SMBUS_READ) {
        // prepare for reading
        memcpy(&smbus_data.block[1], buffer, size);
    }

    // prepare ioctl_data
    ioctl_data.read_write = read_write;
    ioctl_data.command = address;
    ioctl_data.size = I2C_SMBUS_I2C_BLOCK_DATA;
    ioctl_data.data = &smbus_data;

    return_status = ioctl(file, I2C_SMBUS, &ioctl_data);
    if (return_status < 0) {
        // I2C r/w operation failed
        return return_status;
    }

    if (read_write == I2C_SMBUS_READ) {
        // read from bus
        memcpy(buffer, &smbus_data.block[1], size);
    }

    return return_status;
}

bool SensorBus::read(int file, int address, int size, unsigned char *buffer) {
    return !(SensorBus::raw_i2c_rdrw(file, address, I2C_SMBUS_READ, size, buffer) < 0);
}

bool SensorBus::write(int file, int address, int size, unsigned char *buffer) {
    return !(SensorBus::raw_i2c_rdrw(file, address, I2C_SMBUS_WRITE, size, buffer) < 0);
}

bool SensorBus::init_bus(int& file, const char *filename, int address) {
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
