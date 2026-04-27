//! Per-`IORuntime` config registry.
//!
//! Stores opaque, name-keyed configuration blobs (JSON strings by convention)
//! attached from outside the runtime — typically from C++ at worker startup.
//! Services read their config back by name when they're constructed on
//! demand via [`crate::ServiceRegistry::get_or_init`].
//!
//! This registry deliberately knows nothing about config schemas: each
//! service crate parses its own blob (e.g., `serde_json::from_str::<MyCfg>`)
//! so that `nes-io-runtime` stays free of feature-specific dependencies.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
pub struct ConfigRegistry {
    entries: Mutex<HashMap<String, String>>,
}

impl ConfigRegistry {
    /// Attach a config blob under `name`, returning the previous value if one
    /// was registered.
    pub fn attach(
        &self,
        name: impl Into<String>,
        config: impl Into<String>,
    ) -> Option<String> {
        self.entries
            .lock()
            .expect("ConfigRegistry mutex poisoned")
            .insert(name.into(), config.into())
    }

    /// Returns the config blob attached under `name`, if any.
    pub fn get(&self, name: &str) -> Option<String> {
        self.entries
            .lock()
            .expect("ConfigRegistry mutex poisoned")
            .get(name)
            .cloned()
    }

    /// Remove the config under `name`, returning what was there.
    pub fn remove(&self, name: &str) -> Option<String> {
        self.entries
            .lock()
            .expect("ConfigRegistry mutex poisoned")
            .remove(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attach_then_get() {
        let r = ConfigRegistry::default();
        assert!(r.attach("svc", r#"{"x":1}"#).is_none());
        assert_eq!(r.get("svc").as_deref(), Some(r#"{"x":1}"#));
    }

    #[test]
    fn get_missing_returns_none() {
        let r = ConfigRegistry::default();
        assert!(r.get("nope").is_none());
    }

    #[test]
    fn attach_replaces_returns_previous() {
        let r = ConfigRegistry::default();
        r.attach("svc", "first");
        let prev = r.attach("svc", "second").expect("previous");
        assert_eq!(prev, "first");
        assert_eq!(r.get("svc").as_deref(), Some("second"));
    }

    #[test]
    fn remove_clears_entry() {
        let r = ConfigRegistry::default();
        r.attach("svc", "v");
        assert_eq!(r.remove("svc").as_deref(), Some("v"));
        assert!(r.get("svc").is_none());
    }
}
