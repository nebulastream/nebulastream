pub mod query;
pub mod sink;
pub mod source;
pub mod worker;

pub use sea_orm::Set;
use sea_orm::Condition;

pub trait IntoCondition {
    fn into_condition(self) -> Condition;
}

#[cfg(feature = "testing")]
pub trait Generate: Sized + std::fmt::Debug {
    fn generate() -> proptest::strategy::BoxedStrategy<Self>;
}

