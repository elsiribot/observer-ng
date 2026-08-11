use std::time::Duration;

use fmo_core::gateway_poll::ping_gateway;

#[tokio::test]
async fn ping_unreachable_host_returns_false() {
    // Reserved-for-documentation IP that black-holes: connect times out fast.
    let (reachable, latency) = ping_gateway("http://192.0.2.1:9", Duration::from_millis(300)).await;
    assert!(!reachable);
    assert!(latency.is_none());
}

#[tokio::test]
async fn ping_reachable_host_returns_true_with_latency() {
    // Spin up a throwaway local HTTP listener.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            if let Ok((mut sock, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await;
            }
        }
    });
    let (reachable, latency) =
        ping_gateway(&format!("http://{addr}"), Duration::from_secs(2)).await;
    assert!(reachable);
    assert!(latency.is_some());
}
