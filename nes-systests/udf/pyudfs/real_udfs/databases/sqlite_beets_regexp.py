# codon-fuzz: entry=regexp args=VARSIZED,VARSIZED returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Abbey Road (Remastered 2009)", "\\(Remaster"], ["01 Come Together.mp3", "^\\d{2} "], ["Live at Wembley", "(?i)live"], ["Dark Side of the Moon", "^The "], ["track_07.flac", "\\.(mp3|ogg)$"], ["1969", "^19[6-7]\\d$"]]
# real-udf: origin=databases id=sqlite_beets_regexp requires= wrapped=0 ported=0
# Source: https://github.com/beetbox/beets/blob/458390ea297496ea7f0a8629e5453dee59b722d9/beets/dbcore/db.py#L1232
# Commit: 458390ea297496ea7f0a8629e5453dee59b722d9
# License: MIT
# Nested function inside Database.add_functions; dedented.

import re
from typing import Any, AnyStr

def regexp(value: Any, pattern: str) -> bool:
    if isinstance(value, bytes):
        value = value.decode()
    return re.search(pattern, str(value)) is not None
