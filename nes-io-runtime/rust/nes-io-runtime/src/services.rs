//! Per-`IORuntime` service registry.
//!
//! Sources/sinks running on the same `IORuntime` look up shared services
//! (network sender/receiver, etc.) by type. The registry hands out `Arc<T>`
//! and stores `Weak<T>` internally, so a service auto-shuts-down when its
//! last user drops their `Arc`.
//!
//! See `ioruntime-services.md` at the repo root for the design rationale.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

struct Slot {
    cell: Mutex<Option<Weak<dyn Any + Send + Sync>>>,
}

#[derive(Default)]
pub struct ServiceRegistry {
    slots: Mutex<HashMap<TypeId, Arc<Slot>>>,
}

impl ServiceRegistry {
    /// Returns the registered `Arc<T>` if one is live in the registry.
    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        let slot = self
            .slots
            .lock()
            .expect("ServiceRegistry mutex poisoned")
            .get(&TypeId::of::<T>())
            .cloned()?;
        let guard = slot.cell.lock().expect("ServiceRegistry slot mutex poisoned");
        let weak = guard.as_ref()?;
        let strong = weak.upgrade()?;
        Some(strong.downcast::<T>().expect("ServiceRegistry type mismatch"))
    }

    /// Returns the registered `Arc<T>`, calling `init` to construct one if no
    /// live entry exists.
    ///
    /// Concurrent callers requesting the *same* `T` serialize on the slot; the
    /// loser of the race observes the winner's value rather than running
    /// `init` again. Calls for *different* types do not block each other.
    pub fn get_or_init<T, F>(&self, init: F) -> Arc<T>
    where
        T: Any + Send + Sync + 'static,
        F: FnOnce() -> Arc<T>,
    {
        self.try_get_or_init::<T, _, std::convert::Infallible>(|| Ok(init()))
            .expect("infallible init")
    }

    /// Like [`get_or_init`] but `init` may return an error. On error the slot
    /// is left empty so a later caller can retry initialization.
    pub fn try_get_or_init<T, F, E>(&self, init: F) -> Result<Arc<T>, E>
    where
        T: Any + Send + Sync + 'static,
        F: FnOnce() -> Result<Arc<T>, E>,
    {
        let slot = self.slot_for::<T>();
        let mut guard = slot.cell.lock().expect("ServiceRegistry slot mutex poisoned");
        if let Some(weak) = guard.as_ref() {
            if let Some(strong) = weak.upgrade() {
                return Ok(strong.downcast::<T>().expect("ServiceRegistry type mismatch"));
            }
        }
        let strong = init()?;
        let as_any: Arc<dyn Any + Send + Sync> = strong.clone();
        *guard = Some(Arc::downgrade(&as_any));
        Ok(strong)
    }

    /// Insert (or replace) the `Arc<T>` in the registry.
    ///
    /// Returns the previous live entry, if one was registered, so the caller
    /// can shut it down explicitly.
    pub fn insert<T>(&self, service: Arc<T>) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        let slot = self.slot_for::<T>();
        let mut guard = slot.cell.lock().expect("ServiceRegistry slot mutex poisoned");
        let prev = guard
            .as_ref()
            .and_then(Weak::upgrade)
            .map(|arc| arc.downcast::<T>().expect("ServiceRegistry type mismatch"));
        let as_any: Arc<dyn Any + Send + Sync> = service.clone();
        *guard = Some(Arc::downgrade(&as_any));
        prev
    }

    fn slot_for<T: Any + 'static>(&self) -> Arc<Slot> {
        self.slots
            .lock()
            .expect("ServiceRegistry mutex poisoned")
            .entry(TypeId::of::<T>())
            .or_insert_with(|| {
                Arc::new(Slot {
                    cell: Mutex::new(None),
                })
            })
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Foo(u32);
    struct Bar(&'static str);

    #[test]
    fn get_returns_none_when_empty() {
        let reg = ServiceRegistry::default();
        assert!(reg.get::<Foo>().is_none());
    }

    #[test]
    fn get_or_init_constructs_once() {
        let reg = ServiceRegistry::default();
        let calls = Arc::new(Mutex::new(0u32));
        let mk = || {
            let calls = calls.clone();
            move || {
                *calls.lock().unwrap() += 1;
                Arc::new(Foo(42))
            }
        };
        let a = reg.get_or_init::<Foo, _>(mk());
        let b = reg.get_or_init::<Foo, _>(mk());
        assert_eq!(a.0, 42);
        assert_eq!(b.0, 42);
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(*calls.lock().unwrap(), 1);
    }

    #[test]
    fn different_types_dont_collide() {
        let reg = ServiceRegistry::default();
        let foo = reg.get_or_init::<Foo, _>(|| Arc::new(Foo(1)));
        let bar = reg.get_or_init::<Bar, _>(|| Arc::new(Bar("x")));
        assert_eq!(foo.0, 1);
        assert_eq!(bar.0, "x");
    }

    #[test]
    fn dropping_last_arc_allows_reinit() {
        let reg = ServiceRegistry::default();
        let first = reg.get_or_init::<Foo, _>(|| Arc::new(Foo(1)));
        assert_eq!(first.0, 1);
        drop(first);
        // Slot still exists but is stale — next call constructs anew.
        let second = reg.get_or_init::<Foo, _>(|| Arc::new(Foo(2)));
        assert_eq!(second.0, 2);
    }

    #[test]
    fn insert_returns_previous_live_entry() {
        let reg = ServiceRegistry::default();
        let first = Arc::new(Foo(1));
        let prev = reg.insert::<Foo>(first.clone());
        assert!(prev.is_none());

        let second = Arc::new(Foo(2));
        let prev = reg.insert::<Foo>(second.clone()).expect("expected previous");
        assert_eq!(prev.0, 1);
        assert!(Arc::ptr_eq(&prev, &first));

        let live = reg.get::<Foo>().expect("live entry");
        assert_eq!(live.0, 2);
        assert!(Arc::ptr_eq(&live, &second));
    }

    #[test]
    fn try_get_or_init_error_does_not_poison_slot() {
        let reg = ServiceRegistry::default();
        let err: Result<Arc<Foo>, &'static str> =
            reg.try_get_or_init::<Foo, _, _>(|| Err("nope"));
        assert_eq!(err.unwrap_err(), "nope");

        let ok = reg
            .try_get_or_init::<Foo, _, &'static str>(|| Ok(Arc::new(Foo(7))))
            .expect("retry succeeds");
        assert_eq!(ok.0, 7);
    }

    #[test]
    fn get_after_drop_returns_none() {
        let reg = ServiceRegistry::default();
        let arc = reg.get_or_init::<Foo, _>(|| Arc::new(Foo(1)));
        drop(arc);
        assert!(reg.get::<Foo>().is_none());
    }
}
