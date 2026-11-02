# OpenCV Codon plugin

This plugin provides the embedded `cv2.codon` compatibility module and its native OpenCV-backed C functions. It depends only on the Codon plugin descriptor ABI and the host-provided `seq_alloc` symbol; it does not link against NebulaStream.

The currently exposed operations are `imdecode`, `imencode`, `cvtColor`, `applyColorMap`, `resize`, `warpAffine`, `normalize`, and `rectangle`.
