// Copyright 2025 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use bytes::Bytes;
use prometheus::{IntCounter, register_int_counter};
use std::time::Duration;

use pingora::prelude::Opt;
use pingora_core::server::Server;
use pingora_core::services::listening::Service;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::{Error, Result};
use pingora_http::ResponseHeader;
use pingora_proxy::{ProxyHttp, Session};

// Example static metric
lazy_static::lazy_static! {
    static ref REQ_METRIC: IntCounter =
        register_int_counter!("req_counter", "Number of requests").unwrap();
}

// Example auth check
fn check_auth(req: &pingora_http::RequestHeader) -> bool {
    req.headers.get("Authorization").map(|v| v.as_bytes()) == Some(b"password")
}

/// Per-request context for sharing state
#[derive(Default)]
pub struct RetryCtx {
    pub retries: u32,
}

/// The gateway type
pub struct IridiumGateway;

#[async_trait]
impl ProxyHttp for IridiumGateway {
    type CTX = RetryCtx;
    fn new_ctx(&self) -> Self::CTX {
        Self::CTX::default()
    }

    // Handle incoming request, checking authorization for protected URIs.
    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool> {
        let header = session.req_header();
        if header.uri.path().starts_with("/bar") && !check_auth(header) {
            let result = session
                .respond_error_with_body(403, Bytes::from_static(b""))
                .await;
            if let Err(err) = result {
                log::error!("error sending 403 response: {}", err)
            }
            return Ok(true);
        }
        Ok(false)
    }

    // Determine whether to retry when connecting to upstream fails
    fn fail_to_connect(
        &self,
        _session: &mut Session,
        _peer: &HttpPeer,
        ctx: &mut Self::CTX,
        error: Box<Error>,
    ) -> Box<Error> {
        ctx.retries += 1;
        if ctx.retries <= 3 {
            let mut retry_err = error;
            retry_err.set_retry(true);
            return retry_err;
        }
        error
    }

    // Determine which upstream peer to send requests to, delaying when a retry is detected
    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        if ctx.retries > 0 {
            let delay_ms = Duration::from_millis(u64::pow(10, ctx.retries + 1));
            log::info!("retry w/ back-off ms: {delay_ms:?}");
            tokio::time::sleep(delay_ms).await;
        }
        let path = session.req_header().uri.path();
        let addr = if path.starts_with("/foo") {
            ("1.0.0.1", 443)
        } else {
            ("1.1.1.1", 443)
        };
        log::info!("connecting to upstream: {addr:?} for path: {path}");
        let mut peer = HttpPeer::new(addr, true, "one.one.one.one".to_string());
        peer.options.connection_timeout = Some(Duration::from_secs(10));
        Ok(Box::new(peer))
    }

    // Modify response headers from upstream before sending downstream
    async fn response_filter(
        &self,
        _session: &mut Session,
        response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        response.insert_header("Server", "IridiumGateway").unwrap();
        response.remove_header("alt-svc");
        Ok(())
    }

    // Log request summary and collect metrics
    async fn logging(&self, session: &mut Session, _error: Option<&Error>, ctx: &mut Self::CTX) {
        let status = session
            .response_written()
            .map_or(0, |resp| resp.status.as_u16());
        let summary = self.request_summary(session, ctx);
        log::info!("{}, status: {}", summary, status);
        REQ_METRIC.inc();
    }
}

// RUST_LOG=INFO cargo run --bin gateway
// curl 127.0.0.1:6191 -H "Host: one.one.one.one"
// curl 127.0.0.1:6191/foo/ -H "Host: one.one.one.one"
// curl 127.0.0.1:6191/bar/ -H "Host: one.one.one.one" -I -H "Authorization: password"
// curl 127.0.0.1:6191/bar/ -H "Host: one.one.one.one" -I -H "Authorization: bad"
// For metrics
// curl 127.0.0.1:6192/
fn main() {
    env_logger::init();

    let mut server = Server::new(Some(Opt::parse_args())).unwrap();
    server.bootstrap();

    let mut proxy = pingora_proxy::http_proxy_service(&server.configuration, IridiumGateway);
    proxy.add_tcp("0.0.0.0:6191");
    server.add_service(proxy);

    let mut prometheus_service = Service::prometheus_http_service();
    prometheus_service.add_tcp("0.0.0.0:6192");
    server.add_service(prometheus_service);

    server.run_forever();
}
