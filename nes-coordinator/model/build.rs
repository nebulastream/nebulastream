/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

//! Generates the Rust view of the project's error codes from the same list the
//! C++ enum is generated from, so the two cannot name different numbers.

use std::path::Path;

/// The list of every error the system defines, as `EXCEPTION(Name, code, "text")` entries.
const DEFINITIONS: &str = "../../nes-common/include/ExceptionDefinitions.inc";

/// Reads one entry, or nothing when the line is a comment, a guard or blank.
fn parse_entry(line: &str) -> Option<(String, u16, String)> {
    let rest = line.trim().strip_prefix("EXCEPTION(")?;
    let body = rest.split_once(')')?.0;
    let mut parts = body.splitn(3, ',');
    let name = parts.next()?.trim().to_string();
    let code = parts.next()?.trim().parse().ok()?;
    let text = parts.next()?.trim().trim_matches('"').to_string();
    Some((name, code, text))
}

fn main() {
    println!("cargo::rerun-if-changed={DEFINITIONS}");

    let definitions = std::fs::read_to_string(DEFINITIONS)
        .unwrap_or_else(|e| panic!("failed to read {DEFINITIONS}: {e}"));
    let entries: Vec<_> = definitions.lines().filter_map(parse_entry).collect();
    assert!(
        !entries.is_empty(),
        "{DEFINITIONS} defined no exceptions, so its format has changed"
    );

    let mut out = String::from(
        "// Generated from ExceptionDefinitions.inc by build.rs. Do not edit.\n\
         \n\
         /// An error the system defines, identified by the code it reports.\n\
         /// Generated from the list the C++ `ErrorCode` enum is generated from, so a code means\n\
         /// the same thing on both sides of the bridge.\n\
         #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]\n\
         #[repr(u16)]\n\
         #[allow(dead_code, clippy::doc_markdown)]\n\
         pub enum ErrorCode {\n",
    );
    for (name, code, text) in &entries {
        out.push_str(&format!("    /// {text}\n    {name} = {code},\n"));
    }
    out.push_str("}\n\n");

    out.push_str(
        "impl ErrorCode {\n\
         \x20   /// The code a number stands for.\n\
         \x20   /// A number this list does not define reads as `UnknownException`, which cannot happen while\n\
         \x20   /// both sides are generated from the same file and is reported rather than assumed away.\n\
         \x20   #[must_use]\n\
         \x20   pub const fn from_code(code: u16) -> Self {\n\
         \x20       match code {\n",
    );
    for (name, code, _) in &entries {
        out.push_str(&format!("            {code} => Self::{name},\n"));
    }
    out.push_str(
        "            _ => Self::UnknownException,\n\
         \x20       }\n\
         \x20   }\n\
         }\n",
    );

    let dir = std::env::var("OUT_DIR").expect("OUT_DIR is set by cargo");
    std::fs::write(Path::new(&dir).join("error_code.rs"), out)
        .expect("failed to write error_code.rs");
}
