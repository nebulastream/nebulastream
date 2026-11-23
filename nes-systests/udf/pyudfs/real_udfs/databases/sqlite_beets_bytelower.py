# codon-fuzz: entry=bytelower args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["The Beatles"], ["/Music/Pink Floyd/The Wall/01 In The Flesh.flac"], ["ÄRZTE"], ["AC/DC - Back In Black"], [""], ["Sigur Rós"]]
# real-udf: origin=databases id=sqlite_beets_bytelower requires= wrapped=0 ported=0
# Source: https://github.com/beetbox/beets/blob/458390ea297496ea7f0a8629e5453dee59b722d9/beets/dbcore/db.py#L1237
# Commit: 458390ea297496ea7f0a8629e5453dee59b722d9
# License: MIT
# Nested function inside Database.add_functions; dedented.

import re
from typing import Any, AnyStr

def bytelower(bytestring: AnyStr | None) -> AnyStr | None:
    """A custom ``bytelower`` sqlite function so we can compare
    bytestrings in a semi case insensitive fashion.

    This is to work around sqlite builds are that compiled with
    ``-DSQLITE_LIKE_DOESNT_MATCH_BLOBS``. See
    ``https://github.com/beetbox/beets/issues/2172`` for details.
    """
    if bytestring is not None:
        return bytestring.lower()

    return bytestring
