#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import argparse
import os
import tempfile
import zipfile
import re

def replace_keywords1(xml_in, xml_out, extracted_keywords):
    with open(xml_in, "r", encoding="utf-8") as f:
        lines = f.readlines()


    out_lines = []
    keywords2 = []
    for line in lines: 
        if "<keywords2 " in line:
            # Extract keywords attribute
            start = line.index('keywords="') + len('keywords="')
            end = line.index('"', start)
            original_keywords = line[start:end].split(";")

            for token in original_keywords:
                token = token.strip()
                keywords2.append(token)
            extracted_keywords = [kw for kw in extracted_keywords if kw not in keywords2]
            break
    for line in lines:
        if "<keywords " in line:
            # Extract keywords attribute
            start = line.index('keywords="') + len('keywords="')
            end = line.index('"', start)
            original_keywords = line[start:end].split(";")

            specials = []
            for token in original_keywords:
                token = token.strip()
                if not token.isalpha() and not token.replace("_", "").isalpha():
                    specials.append(token)

            # create keywords
            new_line = line[:start] + ";".join(specials) + ";\n\t"
            char_count = 0
            for kw in extracted_keywords:
                new_line = new_line + kw + ";" 
                char_count += len(kw) + 1
                if char_count > 80:
                    new_line = new_line + "\n\t"
                    char_count = 0
            new_line = new_line + line[end:]
            
            out_lines.append(new_line)
        else:
            out_lines.append(line)
    # Write output
    if xml_out:
        with open(xml_out, "w", encoding="utf-8") as f:
            f.writelines(out_lines)
    else:
        sys.stdout.writelines(out_lines)

def extract_keywords(filepath):
    keywords = set()
    in_section = False

    with open(filepath, "r", encoding="ascii") as f:
        for line in f:
            line = line.strip()

            if line.startswith("/// Start of the keywords list"):
                in_section = True
                continue
            if line.startswith("/// End of the keywords list"):
                break
            if not in_section:
                continue
            
            if not line or line.startswith("///"):
                continue

            words = re.findall(r"'([^']+)'", line)
            for keyword in words:
                keywords.add(keyword.lower())

    return sorted(list(keywords))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.description = (
        "This script updates the XML file used for syntax highlighting with keywords extracted from the grammar file. "
        "It can either process the XML directly or the ZIP file containing all the Clion settings."
    )
    parser.add_argument("-grammar", required=True, help="Grammar file (.g4)")
    parser.add_argument("-xml", default=None, help="XML file (.xml)")
    parser.add_argument("-zip", default=None, help="ZIP file containing Clion settings (.zip)")
    parser.add_argument("-output", default=None, help="Output file (XML or ZIP, default: stdout for XML)")
    args = parser.parse_args()
    if not args.grammar.endswith(".g4"):
        sys.exit("Error: Grammar file must end with .g4")
    
    if not args.xml and not args.zip:
        sys.exit("Error: Either -xml or -zip flag must be provided")
    
    if args.xml and args.zip:
        sys.exit("Error: Cannot use both -xml and -zip flags")
    
    keyword_list = extract_keywords(args.grammar)
    
    if args.zip:
        # Handle ZIP input
        if not args.output or not args.output.endswith(".zip"):
            sys.exit("Error: When input is ZIP, output must be a ZIP file")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Unzip to temp dir
            with zipfile.ZipFile(args.zip, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # XML file path inside the zip
            xml_file = os.path.join(temp_dir, 'filetypes', 'System-level test.xml')
            if not os.path.exists(xml_file):
                sys.exit("Error: XML file not found at expected path in ZIP")
            
            # Process XML
            replace_keywords1(xml_file, xml_file, keyword_list)
            
            # Zip back to output
            with zipfile.ZipFile(args.output, 'w', zipfile.ZIP_DEFLATED) as zip_out:
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, temp_dir)
                        zip_out.write(file_path, arcname)
    else:
        # Handle direct XML input
        if not args.xml.endswith(".xml"):
            sys.exit("Error: XML file must end with .xml")
        replace_keywords1(args.xml, args.output, keyword_list)