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

use crate::common::TestServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

#[tokio::test]
async fn serves_over_tcp_and_shuts_down_on_request() {
    let server = TestServer::start().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let serving = tokio::spawn(server::serve(listener, server.router.clone(), async move {
        let _ = stopped.await;
    }));

    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /v1/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8_lossy(&response);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with(r#"{"status":"ok"}"#), "{response}");

    stop.send(()).unwrap();
    serving.await.unwrap().unwrap();
}
