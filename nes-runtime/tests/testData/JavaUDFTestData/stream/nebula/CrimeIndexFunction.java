package stream.nebula;
// IMPORTANT: If you make changes to this file, be sure to run buildJar.sh _and_ reload the cmake project to update the JAR file.

import java.io.Serializable;

import stream.nebula.CityDataPojo;

/**
 * A {@link MapFunction} implementation that adds an instance variable to the input Integer value.
 */
public class CrimeIndexFunction implements MapFunction<CityDataPojo, Double> {

    /**
     * Adds the instance variable to the input Integer value.
     *
     * @param value The input Integer value to which the instance variable is added.
     * @return The result of adding the instance variable to the input Integer value.
     */
    @Override
    public Double map(CityDataPojo city) {
        return city.total_population + city.total_adult_population * 2.0 + (city.number_of_robberies * -2000.0);
    }
}