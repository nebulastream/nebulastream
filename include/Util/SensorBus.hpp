#ifndef SENSOR_BUS_HPP
#define SENSOR_BUS_HPP

#include <string>
#include <linux/i2c.h>
#include <linux/i2c-dev.h>
#include <sys/ioctl.h>

namespace NES {

class SensorBus {
  public:
    /**
     * Initialize the file and check if the address behind the file exists
     * @param file the file-descriptor to be allocated
     * @param filename the path of the bus
     * @param address the register address we're interested in
     * @return the new file descriptor
     */
    static bool init_bus(int& file, const char* filename, int address);

    /**
     * Write the buffer to file `file` at address `address`, using `length`.
     * @param file the file descriptor for the bus to write to
     * @param address the register address we're interested in
     * @param size the size of the data
     * @param buffer the data
     */
    static bool write(int file, int address, int size, unsigned char* buffer);

    /**
     * Read `buffer` from `address` under bus located at `file`, using `length`.
     * @param file the file descriptor for the bus to read from
     * @param address the register address we're interested in
     * @param size the size of the data
     * @param buffer the data
     */
    static bool read(int file, int address, int size, unsigned char* buffer);

  private:
    /**
     * Perform a r/w operation on top of raw I2C / SMBus
     * @param file the file descriptor to perform the operation on
     * @param address the register addres we're interested in
     * @param read_write the operation to perform from I2C
     * @param size the size of the buffer to read/write
     * @param buffer the data
     * @return operation status
     */
    static int raw_i2c_rdrw(int file, uint8_t address, uint8_t read_write, uint8_t size, unsigned char* buffer);
};

} // namespace NES

#endif /* SENSOR_BUS_HPP */
