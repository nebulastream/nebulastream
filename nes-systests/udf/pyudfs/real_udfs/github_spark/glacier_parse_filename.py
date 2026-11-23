# codon-fuzz: entry=parse_filename args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["kZ7bQ1xYv0aJp3sNf9TmEw", "<m><v>4</v><p>UGhvdG9zLzIwMTQvU3VtbWVyL0lNR18wMDAxLmpwZw==</p><lm>20140812T101500Z</lm></m>"], ["Hc4uR8oPm2LwXy6Qa1BnZt", "<m><v>2</v><p>RG9jdW1lbnRzXFxUYXhlc1xcMjAxM1xccmV0dXJuLnBkZg==</p><lm>20140105T080000Z</lm></m>"], ["P0qWe9RtY7uIo5pAs3DfGh", "<ArchiveMetadata><Path>QmFja3Vwcy9zZXJ2ZXIwMS9ldGMudGFyLmd6</Path></ArchiveMetadata>"], ["Jk2LmN4bVc6Xz8QwEr0TyU", "{\"Path\": \"Backups/2019/db-dump.sql.gz\", \"UTC\": \"2019-05-01T02:00:00Z\"}"], ["Aa1Ss2Dd3Ff4Gg5Hh6Jj7Kk", "backup-2020-03-01.tar.gz"], ["Zz9Xx8Cc7Vv6Bb5Nn4Mm3Ll", "   "], ["Qq1Ww2Ee3Rr4Tt5Yy6Uu7Ii", "<m><v>4</v><p>UGhvdG9z</m>"]]
# real-udf: origin=github-spark id=glacier_parse_filename requires=defusedxml wrapped=0 ported=0
# Note: the last row is malformed XML and exercises the error path (00error/<id>); logging goes to stderr.
# Source: https://github.com/aws-solutions-library-samples/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/blob/a0f24cc85a6064f832db19c371b26122751d40df/source/solution/infrastructure/glue_helper/scripts/archive_naming.py#L19-L31
# Repository: aws-solutions-library-samples/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3 @ commit a0f24cc85a6064f832db19c371b26122751d40df
# License: Apache-2.0
# Copied verbatim from source/solution/infrastructure/glue_helper/scripts/archive_naming.py (L15-L16, L34-L67, L19-L31); only the module's own import lines that the code uses are prepended.

import json
import logging
import os
import re
from base64 import b64decode
from defusedxml.minidom import parseString


logger = logging.getLogger()
logger.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))


def parse_filename(archive_id: str, archive_description: str) -> str:
    try:
        filename = parse_description(archive_description.strip()).strip()
    except Exception as e:
        logger.error(
            f"Failed to parse archive description: {archive_description}. Error: {e}"
        )
        filename = f"00error/{archive_id}"
    else:
        if not filename:
            filename = f"00undefined/{archive_id}"

    return filename.replace(r"\\", "/")


def parse_description(archive_description: str) -> str:
    """
    Parses the archive description and returns the filename.
    """
    # FastGlacier v2/3/4
    if re.search(r"<m>.*<\/m>", archive_description):
        return parse_fast_glacier(archive_description, "m", "p")

    # FastGlacier v1
    if re.search(r"<ArchiveMetadata>.*</ArchiveMetadata>", archive_description):
        return parse_fast_glacier(archive_description, "ArchiveMetadata", "Path")

    # JSON
    if re.search(r"{\s*\\*\".*}\s*", archive_description):
        try:
            json_object = json.loads(archive_description)
            path = json_object.get("Path") or json_object.get("path")
            if isinstance(path, str):
                return path
        except Exception as _:
            logger.info(f"Failed to parse JSON: {archive_description}")
    return archive_description


def parse_fast_glacier(archive_description: str, metadata: str, path: str) -> str:
    """
    Parse the Fast Glacier XML archive description and return the filename.
    """
    json_obj = parseString(archive_description.strip("'").strip('"'))
    return b64decode(
        json_obj.getElementsByTagName(metadata)[0]
        .getElementsByTagName(path)[0]
        .firstChild.data  # type: ignore
    ).decode("utf-8", errors="ignore")
