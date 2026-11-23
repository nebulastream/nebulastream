# codon-fuzz: entry=page_category args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["/Default.aspx"], ["/images/beagle.JPG"], ["/css/site.css?v=3"], ["/"], ["/products/plants"], ["-"], ["/downloads/catalog.pdf"], ["/scripts/app.min.js"], ["/favicon.ico"], ["/archive.tar.gz"]]
# real-udf: origin=github-spark id=w3c_page_category requires= wrapped=0 ported=0
# Source: https://github.com/AhmedIkram05/w3c-etl-pipeline/blob/9b30e751e8f4dadd4c9eca2e26628b7f30a123bd/pipeline/spark/jobs/utils/transformations.py#L45-L62
# Repository: AhmedIkram05/w3c-etl-pipeline @ commit 9b30e751e8f4dadd4c9eca2e26628b7f30a123bd
# License: MIT
# Copied verbatim from pipeline/spark/jobs/utils/transformations.py (L25-L42, L45-L62); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

_EXTENSION_CATEGORIES = {
    "aspx": "Dynamic Page",
    "asp": "Dynamic Page",
    "html": "Static Page",
    "htm": "Static Page",
    "shtml": "Static Page",
    "jpg": "Image",
    "jpeg": "Image",
    "png": "Image",
    "gif": "Image",
    "bmp": "Image",
    "ico": "Icon",
    "css": "Stylesheet",
    "js": "Script",
    "txt": "Text File",
    "pdf": "Document",
    "no_extension": "Directory",
}


def page_category(uri_stem):
    """Classify a URI stem into a human-friendly page category.

    Extracts the file extension from *uri_stem*, then maps it to a
    category (Static Page, Image, Script, etc.).  URI stems without
    an extension are classified as "Directory".
    """
    # NOTE: Python type hints intentionally omitted on UDF signatures
    # to suppress PySpark 4.x "Cannot infer the eval type" warnings;
    # the returnType=StringType() decorator arg carries the schema.
    if not uri_stem or uri_stem.strip() in ("-", "Unknown", ""):
        return "Unknown"
    stem = uri_stem.split("?")[0]
    if "." in stem:
        ext = stem.rsplit(".", 1)[-1].lower().strip()
        return _EXTENSION_CATEGORIES.get(ext, "Other")
    return "Directory"
