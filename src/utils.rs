use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

pub async fn retry<TFutureFn, TFuture, TReturn, TError>(
    mut func: TFutureFn,
    max_attempts: u8,
    wait: Duration,
) -> Result<TReturn, TError>
where
    TFutureFn: FnMut() -> TFuture,
    TFuture: Future<Output = Result<TReturn, TError>>,
{
    let mut attempts = 0;
    loop {
        match func().await {
            Ok(result) => {
                return Ok(result);
            }
            Err(e) => {
                if attempts >= max_attempts {
                    return Err(e);
                }
                attempts += 1;
                sleep(wait).await
            }
        }
    }
}
