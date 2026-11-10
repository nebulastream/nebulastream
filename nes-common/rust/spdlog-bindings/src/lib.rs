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
        Trace,
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
        fn log(
            log: &SharedPtr<SpdLogger>,
            level: Level,
            file: &str,
            line_number: u32,
            message: &str,
        );
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

    /// convert from `tracing` log-level to the cxx-bridge enum type
    fn convert_level(level: &tracing::Level) -> ffi::Level {
        match *level {
            tracing::Level::TRACE => ffi::Level::Trace,
            tracing::Level::DEBUG => ffi::Level::Debug,
            tracing::Level::INFO => ffi::Level::Info,
            tracing::Level::WARN => ffi::Level::Warn,
            tracing::Level::ERROR => ffi::Level::Error,
        }
    }
}

// Helper to extract the message field
#[derive(Default)]
struct MessageExtractor {
    message: String,
    fields: String,
}

impl MessageExtractor {
    fn render(event: &Event<'_>) -> String {
        let mut visitor = Self::default();
        event.record(&mut visitor);
        if visitor.message.is_empty() {
            return visitor.fields.trim_start().to_owned();
        }
        visitor.message + &visitor.fields
    }
}

impl tracing::field::Visit for MessageExtractor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            value.clone_into(&mut self.message);
        } else {
            write!(self.fields, " {}={value}", field.name()).unwrap();
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            write!(self.message, "{value:?}").unwrap();
        } else {
            write!(self.fields, " {}={value:?}", field.name()).unwrap();
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
        let message = MessageExtractor::render(event);

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
        // TODO: There is actually a problem here. If a span entering a span overwrites a key, the key will be removed from the parent span.
        //       However the fix would have a grater performance impact, and its currently not worth it, as there are no duplicate keys.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct Capture(Arc<Mutex<Vec<String>>>);

    impl<S: Subscriber> Layer<S> for Capture {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            self.0.lock().unwrap().push(MessageExtractor::render(event));
        }
    }

    #[test]
    fn event_fields_are_appended_to_the_message() {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(Capture(lines.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(from = %"Started", to = 3, name = "w", "transition");
            tracing::info!("plain {}", 1);
            tracing::info!(key = 7);
        });
        assert_eq!(
            *lines.lock().unwrap(),
            ["transition from=Started to=3 name=w", "plain 1", "key=7"]
        );
    }
}
