
#ifndef NES_INCLUDE_NETWORK_OUTPUTGATE_HPP_
#define NES_INCLUDE_NETWORK_OUTPUTGATE_HPP_

namespace NES {

/**
 * @brief Method stubs for OutputGate. WIP
 */
class OutputGate {
  public:
    OutputGate() = default;
    ~OutputGate();

    void setup();
    void shutdown();
  private:
    bool connect();
    bool disconnect();
};
} // namespace NES

#endif //NES_INCLUDE_NETWORK_OUTPUTGATE_HPP_
