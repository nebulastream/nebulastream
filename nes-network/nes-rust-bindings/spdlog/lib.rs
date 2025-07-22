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
use cxx::SharedPtr;
use std::collections::HashMap;
use std::fmt::Write;
use tracing::span::Attributes;
use tracing::warn;
use tracing::{Event, Id, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

#[cxx::bridge]
mod ffi {
    enum Level {
        Debug,
        Info,
        Warn,
        Error,
        Fatal,
    }

    extern "Rust" {
        fn initialize_logging(logger: SharedPtr<SpdLogger>);
    }

    // C++ functions we'll call from Rust
    unsafe extern "C++" {
        include!("LoggerBindings.hpp");
        type SpdLogger;
        fn log(log: &SharedPtr<SpdLogger>, level: i32, file: &str, line_number: u32, message: &str);
        fn mdc_insert(key: &str, message: &str);
        fn mdc_remove(key: &str);
    }
}
unsafe impl Send for ffi::SpdLogger {}
unsafe impl Sync for ffi::SpdLogger {}
pub struct SpdlogLayer {
    logger: SharedPtr<ffi::SpdLogger>,
}

impl SpdlogLayer {
    pub fn new(logger: SharedPtr<ffi::SpdLogger>) -> Self {
        SpdlogLayer { logger }
    }

    fn convert_level(level: &tracing::Level) -> i32 {
        match *level {
            tracing::Level::TRACE => 0,
            tracing::Level::DEBUG => 1,
            tracing::Level::INFO => 2,
            tracing::Level::WARN => 3,
            tracing::Level::ERROR => 4,
        }
    }
}

// Helper to extract the message field
struct MessageExtractor<'a>(&'a mut String);

impl tracing::field::Visit for MessageExtractor<'_> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            *self.0 = value.to_owned();
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            write!(self.0, "{:?}", value).unwrap();
        }
    }
}

struct PopulateSpanExtension<'a>(&'a mut HashMap<&'static str, String>);
impl tracing::field::Visit for PopulateSpanExtension<'_> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name(), value.to_string());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name(), format!("{:?}", value).to_string());
    }
}

impl<S> Layer<S> for SpdlogLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    // Sync tracings span with spdlogs mdc
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        {
            let mut values = HashMap::new();
            let mut visitor = PopulateSpanExtension(&mut values);
            attrs.record(&mut visitor);
            span.extensions_mut().insert(values);
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut message = String::new();
        let mut visitor = MessageExtractor(&mut message);
        event.record(&mut visitor);

        // Extract metadata
        let metadata = event.metadata();
        let file = metadata.file().unwrap_or("");
        let line = metadata.line().unwrap_or(0);
        let level = Self::convert_level(metadata.level());

        ffi::log(&self.logger, level, file, line, message.as_str());
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            // Walk up the span hierarchy and insert all parent span fields
            let mut spans_to_insert = Vec::new();

            // Collect all spans from current to root
            let mut current = Some(span);
            while let Some(span_ref) = current {
                if let Some(fields) = span_ref.extensions().get::<HashMap<&'static str, String>>() {
                    spans_to_insert.push(fields.clone());
                }
                current = span_ref.parent();
            }

            // Insert in reverse order (root to current) so that child values override parent values
            for fields in spans_to_insert.iter().rev() {
                for (k, v) in fields {
                    ffi::mdc_insert(k, v);
                }
            }
        }
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(fields) = span.extensions().get::<HashMap<&'static str, String>>() {
                for k in fields.keys() {
                    ffi::mdc_remove(k);
                }
            }
        }
    }
}

fn initialize_logging(logger: SharedPtr<ffi::SpdLogger>) {
    // We want to propagate rust panics to terminate the application.
    std::panic::set_hook(Box::new(|panic_info| {
        eprintln!("panic: {panic_info}");
        std::process::exit(1);
    }));

    // Create spdlog layer
    let spdlog_layer = SpdlogLayer::new(logger);

    // Set the subscriber globally without calling init()
    let subscriber = tracing_subscriber::registry().with(spdlog_layer);
    if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
        warn!("Could not initialize rust logger: {e:?}");
    }
}
