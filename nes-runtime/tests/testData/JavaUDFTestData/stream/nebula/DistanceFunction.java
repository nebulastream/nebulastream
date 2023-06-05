package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.
import java.io.Serializable;
import stream.nebula.DistancePojo;

/**
 * This class implements the MapFunction interface to add a double value to a Double input value.
 */
public class DistanceFunction implements MapFunction<DistancePojo, Double> {
    public static final double EQUATORIAL_RADIUS = 6378137.0;
    public static final double POLAR_RADIUS = 6356752.3142;
    public static final double INVERSE_FLATTENING = 298.257223563;

     public static double vincentyDistance(double lat1, double long1, double lat2, double long2) {
            double f = 1 / INVERSE_FLATTENING;
            double L = Math.toRadians(lat2 - long1);
            double U1 = Math.atan((1 - f) * Math.tan(Math.toRadians(lat1)));
            double U2 = Math.atan((1 - f) * Math.tan(Math.toRadians(lat2)));
            double sinU1 = Math.sin(U1), cosU1 = Math.cos(U1);
            double sinU2 = Math.sin(U2), cosU2 = Math.cos(U2);

            double lambda = L, lambdaP, iterLimit = 100;

            double cosSqAlpha = 0, sinSigma = 0, cosSigma = 0, cos2SigmaM = 0, sigma = 0, sinLambda = 0, sinAlpha = 0, cosLambda = 0;
            do {
                sinLambda = Math.sin(lambda);
                cosLambda = Math.cos(lambda);
                sinSigma = Math.sqrt((cosU2 * sinLambda) * (cosU2 * sinLambda)
                        + (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda)
                        * (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda));
                if (sinSigma == 0)
                    return 0; // co-incident points
                cosSigma = sinU1 * sinU2 + cosU1 * cosU2 * cosLambda;
                sigma = Math.atan2(sinSigma, cosSigma);
                sinAlpha = cosU1 * cosU2 * sinLambda / sinSigma;
                cosSqAlpha = 1 - sinAlpha * sinAlpha;
                if (cosSqAlpha != 0) {
                    cos2SigmaM = cosSigma - 2 * sinU1 * sinU2 / cosSqAlpha;
                } else {
                    cos2SigmaM = 0;
                }
                double C = f / 16 * cosSqAlpha * (4 + f * (4 - 3 * cosSqAlpha));
                lambdaP = lambda;
                lambda = L
                        + (1 - C)
                        * f
                        * sinAlpha
                        * (sigma + C * sinSigma
                        * (cos2SigmaM + C * cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM)));
            } while (Math.abs(lambda - lambdaP) > 1e-12 && --iterLimit > 0);

            if (iterLimit == 0)
                return 0; // formula failed to converge

            double uSq = cosSqAlpha
                    * (Math.pow(EQUATORIAL_RADIUS, 2) - Math.pow(POLAR_RADIUS, 2))
                    / Math.pow(POLAR_RADIUS, 2);
            double A = 1 + uSq / 16384 * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)));
            double B = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)));
            double deltaSigma = B
                    * sinSigma
                    * (cos2SigmaM + B
                    / 4
                    * (cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM) - B / 6 * cos2SigmaM
                    * (-3 + 4 * sinSigma * sinSigma)
                    * (-3 + 4 * cos2SigmaM * cos2SigmaM)));
            double s = POLAR_RADIUS * A * (sigma - deltaSigma);

            return s;
        }


    /**
     * Adds the instanceVariable to the given value and returns the result.
     *
     * @param value the value to add the instanceVariable to.
     * @return the result of adding the instanceVariable to the given value.
     */
    @Override
    public Double map(DistancePojo value) {
        double result = vincentyDistance(value.lat1, value.long1, value.lat2, value.long2);
        return result;
    }
}