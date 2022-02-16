
#ifndef NES_NES_CORE_INCLUDE_COMMON_GEOGRAPHICALLOCATION_H_
#define NES_NES_CORE_INCLUDE_COMMON_GEOGRAPHICALLOCATION_H_
#include <CoordinatorRPCService.grpc.pb.h>

namespace NES {

/**
 * @brief a representation of geographical location used to specify the fixed location of field nodes
 * and the changing location of mobile devices
 */
class GeographicalLocation {

  public:
    GeographicalLocation(double latitude, double longitude);

    GeographicalLocation(Coordinates coord);

    explicit GeographicalLocation(std::tuple<double, double>);

    static GeographicalLocation fromString(const std::string coordinates);

    explicit operator std::tuple<double, double>();

    operator Coordinates*();

    bool operator==(const GeographicalLocation& other) const;

    double getLatitude() const;

    double getLongitude() const;

    /**
     * @brief checks if the tuple represents valid coordinates (abs(lat) < 90 and abs(lng) < 180)
     * @param coordinates: tuple in the format <latitude, longitude>
     * @return true if input was valid geocoordinates
     */
    static bool checkValidityOfCoordinates(double latitude, double longitude);

  private:
    double latitude;
    double longitude;
};

struct CoordinatesOutOfRangeException : public std::exception {
    const char * what () const noexcept;
};

struct InvalidCoordinateFormatException : public std::exception {
    const char * what () const noexcept;
};

}

#endif//NES_NES_CORE_SRC_COMMON_DATATYPES_GEOGRAPHICALLOCATION_H_
