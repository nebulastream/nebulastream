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

use anyhow::Result;
use clap::ValueEnum;
use model::statement::StatementResult;
use serde::Serialize;
use std::io::Write;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    Auto,
    Json,
    Table,
}

pub struct Output<'a, W: Write> {
    out: &'a mut W,
    json: bool,
}

impl<'a, W: Write> Output<'a, W> {
    pub fn new(out: &'a mut W, format: Format, stdout_is_terminal: bool) -> Self {
        let json = match format {
            Format::Json => true,
            Format::Table => false,
            Format::Auto => !stdout_is_terminal,
        };
        Self { out, json }
    }

    pub const fn is_json(&self) -> bool {
        self.json
    }

    pub fn result(&mut self, result: &StatementResult) -> Result<()> {
        if self.json {
            self.json(result)
        } else {
            writeln!(self.out, "{result}")?;
            Ok(())
        }
    }

    pub fn json(&mut self, value: &impl Serialize) -> Result<()> {
        serde_json::to_writer_pretty(&mut *self.out, value)?;
        writeln!(self.out)?;
        Ok(())
    }

    pub fn text(&mut self, text: &str) -> Result<()> {
        writeln!(self.out, "{text}")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_follows_the_terminal() {
        let mut sink = Vec::new();
        assert!(Output::new(&mut sink, Format::Auto, false).is_json());
        assert!(!Output::new(&mut sink, Format::Auto, true).is_json());
        assert!(Output::new(&mut sink, Format::Json, true).is_json());
        assert!(!Output::new(&mut sink, Format::Table, false).is_json());
    }

    #[test]
    fn a_result_is_json_or_a_table() {
        let result = StatementResult::Workers(vec![]);
        let mut json = Vec::new();
        Output::new(&mut json, Format::Json, true)
            .result(&result)
            .unwrap();
        assert_eq!(
            String::from_utf8(json).unwrap(),
            "{\n  \"Workers\": []\n}\n"
        );
        let mut table = Vec::new();
        Output::new(&mut table, Format::Table, false)
            .result(&result)
            .unwrap();
        assert!(String::from_utf8(table).unwrap().starts_with('+'));
    }
}
