# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED,VARSIZED,VARSIZED,VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["52.48", "33.10", "0.00", "52.90", "34.20", "0.11"], ["10.05", "60.75", "0.00", "52.90", "34.20", "0.11"], ["88.10", "12.33", "0.00", "88.10", "12.33", "0.00"], ["30.0", "30.0", "0.0", "32.0", "31.5", "1.2"], ["-0.5", "34.0", "0.0", "2.5", "34.0", "0.0"], ["70.12", "40.40", "0.00", "71.00", "41.00", "2.50"]]
# real-udf: origin=streaming id=trivadis_ball_possession requires= wrapped=1 ported=0
# Wrapper: case_entry builds the player sensor and ball (x, y, z) tuples from the string coordinates of the events and returns the result ((True, dist) or False) as JSON text.
# Imports: the original module's imports that this code uses.

import json
import math

# Source: https://github.com/TrivadisPF/soccer-demo/blob/923f22191af516c207f34881ea3eaef174d82583/src/faust/fbBallPossession/fbBallPossession.py#L10-L18
# Repository: https://github.com/TrivadisPF/soccer-demo (commit 923f22191af516c207f34881ea3eaef174d82583)
# License: Apache-2.0
# System: Faust
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def ballPossession(sensorId, ballId, distance=3):
    dist = euclidianDistance((ballId[0], ballId[1], ballId[2]),(sensorId[0], sensorId[1], sensorId[2]))
    if dist < 3:
        return((True, dist))
    else:
        return(False)


def euclidianDistance(ball_set, player_set):
    return(math.sqrt((float(ball_set[0]) - float(player_set[0]))**2 + (float(ball_set[1]) - float(player_set[1]))**2 + (float(ball_set[2]) - float(player_set[2]))**2))


def case_entry(sensor_x, sensor_y, sensor_z, ball_x, ball_y, ball_z):
    return json.dumps(ballPossession((sensor_x, sensor_y, sensor_z), (ball_x, ball_y, ball_z)))
