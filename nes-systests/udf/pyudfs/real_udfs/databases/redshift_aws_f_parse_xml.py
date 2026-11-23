# codon-fuzz: entry=f_parse_xml args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["<data><country name=\"Liechtenstein\"><rank>1</rank><year>2008</year><gdppc>141100</gdppc></country></data>"], ["<data><country name=\"Singapore\"><rank>4</rank><year>2011</year></country><country name=\"Panama\"><rank>68</rank></country></data>"], ["<?xml version=\"1.0\"?><data><country name=\"Germany\"><rank>12</rank><neighbor name=\"Austria\" direction=\"S\"/></country></data>"], ["<data><country name=\"New Zealand\"><rank>7</rank></country></data>"], ["<data>\n  <country name=\"Brazil\">\n    <rank>23</rank>\n  </country>\n</data>"]]
# real-udf: origin=databases id=redshift_aws_f_parse_xml requires= wrapped=0 ported=0
# Source: https://github.com/awslabs/amazon-redshift-udfs/blob/16fc7a773592315b41a4bd0b3711c26bd2fff51a/python-udfs/f_parse_xml%28varchar%29/function.sql#L8
# Commit: 16fc7a773592315b41a4bd0b3711c26bd2fff51a
# License: Apache-2.0
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.
# Removed from master in 2026 (Redshift Python UDF deprecation); pinned to the last commit that has it.

def f_parse_xml(xml):
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml)
    for country in root.findall('country'):
        rank = country.find('rank').text
        name = country.get('name')
        return name + ':' + rank
