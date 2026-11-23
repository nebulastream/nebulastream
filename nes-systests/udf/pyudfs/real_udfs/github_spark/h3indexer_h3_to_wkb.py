# codon-fuzz: entry=h3_to_wkb args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["8928308280fffff"], ["8828308281fffff"], ["872830828ffffff"], ["85283473fffffff"], ["8a2a1072b59ffff"], ["891fb46622fffff"]]
# real-udf: origin=github-spark id=h3indexer_h3_to_wkb requires=h3,shapely wrapped=0 ported=0
# Note: needs the h3 3.x API (h3_to_geo_boundary).
# Source: https://github.com/amazon-science/h3-indexer/blob/dc822ed87a79942e49031f3c727e9fa7c08257e9/src/h3_indexer/spark/spark_udfs.py#L24-L45
# Repository: amazon-science/h3-indexer @ commit dc822ed87a79942e49031f3c727e9fa7c08257e9
# License: Apache-2.0
# Copied verbatim from src/h3_indexer/spark/spark_udfs.py (L24-L45); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=BinaryType())

import binascii
import h3
from shapely.geometry import Polygon


def h3_to_wkb(h3_index: str) -> str:
    """
    Convert H3 index to WKB geometry bytes.

    :param h3_index: H3 cell index as string.
    :return: WKB representation of the H3 cell boundary as bytes.
    """
    # Get the boundary coordinates
    coords = h3.h3_to_geo_boundary(h3_index)

    # H3 returns [lat, lng] but Shapely expects [lng, lat]
    # Also need to close the polygon by repeating first point
    boundary = [[coord[1], coord[0]] for coord in coords]
    boundary.append(boundary[0])  # Close the polygon

    # Create Shapely polygon
    polygon = Polygon(boundary)

    # Convert to WKB and then to hexadecimal string
    wkb_bytes = polygon.wkb
    return "0x" + binascii.hexlify(wkb_bytes).decode("ascii")
