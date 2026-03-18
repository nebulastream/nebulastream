pub async fn supervised<F: Future<Output = ()>>(fut: F) -> bool {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;
    AssertUnwindSafe(fut).catch_unwind().await.is_err()
}
