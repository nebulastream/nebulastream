#ifndef NES_INCLUDE_GENERIC_BUS_HPP_
#define NES_INCLUDE_GENERIC_BUS_HPP_

#include <memory>
#include <string>

namespace NES {
namespace Sensors {

/**
 * @brief types of sensor buses we suppprt
 */
enum BusType {
    I2C,
    SPI,
    UART
};

/**
 * @brief Utility class for performing I/O on top of known sensor buses.
 */
class GenericBus {
  public:
    /**
     * @brief use `filename` and `type` to define a new sensor bus
     * @param filename, the path of the file to open
     * @param type, the type of bus allowed in BusType
     */
    GenericBus(const char* filename, BusType type);

    /**
     * @brief virtual destructor
     */
    virtual ~GenericBus() = 0;

    /**
     * @brief test if `address` is accessable from the bus
     * @param address, the chip address of a sensor
     * @return true if `address` is accessable
     */
    bool init(int address);

    /**
     * @brief generic write, uses `writeData` underneath
     * @param address, the address to write to
     * @param size, the size of data we want to write
     * @param buffer, the data container
     * @return the result of `writedata`
     */
    bool write(int address, int size, unsigned char* buffer);

    /**
     * @brief generic read, uses `readData` underneath
     * @param address, the address to read from
     * @param size, the size of data we want to read
     * @param buffer, the data container
     * @return the result of `readdata`
     */
    bool read(int address, int size, unsigned char* buffer);

    /**
     * @brief return the necessary type of bus
     * @return BusType
     */
    BusType getType();

  protected:
    /**
     * @brief the file descriptor
     */
    int file;

    /**
     * @brief the path of the file on disk
     */
    const char* fileName;

    /**
     * @brief the type of the current bus
     */
    BusType busType;

  private:
    /**
     * Initialize the file and check if the address behind the file exists
     * @param address the register address we're interested in
     * @return true if the `address` is controllable
     */
    virtual bool initBus(int address) = 0;

    /**
     * Write the buffer to file `file` at address `address`, using `size`.
     * @param address the register address we're interested in
     * @param size the size of the data
     * @param buffer the data
     * @return true if ioctl succeeds and is larger than 0
     */
    virtual bool writeData(int address, int size, unsigned char* buffer) = 0;

    /**
     * Read the buffer from file `file` at address `address`, using `size`.
     * @param address the register address we're interested in
     * @param size the size of the data
     * @param buffer the data
     * @return true if ioctl succeeds and is larger than 0
     */
    virtual bool readData(int address, int size, unsigned char* buffer) = 0;
};

typedef std::shared_ptr<GenericBus> GenericBusPtr;

}//namespace Sensors
}//namespace NES
#endif//NES_INCLUDE_GENERIC_BUS_HPP_
