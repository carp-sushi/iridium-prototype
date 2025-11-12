use async_trait::async_trait;
use pingora_cache::lock::CacheLock;
use pingora_cache::predictor::Predictor;
use prometheus::{IntCounter, register_int_counter};
use std::time::{Duration, SystemTime};

use pingora::cache::{
    CacheKey, CacheMeta, ForcedInvalidationKind, HitHandler, MemCache, NoCacheReason,
    RespCacheable, eviction::lru::Manager,
};
use pingora::prelude::Opt;
use pingora::server::Server;
use pingora::services::listening::Service;
use pingora::upstreams::peer::HttpPeer;
use pingora::{Error, ErrorType, Result};
use pingora_http::ResponseHeader;
use pingora_proxy::{ProxyHttp, Session};

// How long items are considered fresh when cached.
const CACHE_TTL_SECS: u64 = 300; // 5 minutes

// Max backoff duration
const MAX_DELAY_MS: u64 = 10000; // 10 seconds

// Cache activation idempotency key.
const CACHE_KEY: &str = "IridiumCacheKey";

// LRU sizing
const CACHE_SIZE_LIMIT: usize = 1024 * 1024; // 1MB
const CACHE_SHARD_CAPACITY: usize = CACHE_SIZE_LIMIT * 100; // MB

// Base for how long locks can be held.
const CACHE_LOCK_SECS: u64 = 30;

// Lazy initialized statics: request counter metric and in-memory cache (not production grade).
lazy_static::lazy_static! {
    static ref REQ_COUNTER: IntCounter =
        register_int_counter!("req_counter", "Number of requests").unwrap();

    // TODO: Find a production grade, durable cache that works with pingora.
    static ref CACHE_STORAGE: MemCache = MemCache::new();

    // Cache eviction manager (10 shard LRU).
    static ref CACHE_MANAGER: Manager<10> =
        Manager::<10>::with_capacity(CACHE_SIZE_LIMIT, CACHE_SHARD_CAPACITY);

    // Cache predictor (10 shards, each with the capacity for remembering 100 uncachable keys).
    static ref CACHE_PREDICTOR: Predictor<10> = Predictor::<10>::new(100, None);

    // Cache locking with timeout (how long a writer can hold onto a particular lock).
    static ref CACHE_LOCK: CacheLock = CacheLock::new(Duration::from_secs(CACHE_LOCK_SECS));
}

/// Per-request context for sharing state
#[derive(Default)]
pub struct RetryCtx {
    pub retries: u8,
}

/// The gateway type
pub struct IridiumGateway;

impl IridiumGateway {
    // Customize upstream selection based on URI path
    fn select_upstream(&self, path: &str) -> (&str, u16) {
        if path.starts_with("/bar") {
            ("1.0.0.1", 443)
        } else {
            ("1.1.1.1", 443)
        }
    }

    // Example auth check
    fn check_auth(&self, req: &pingora_http::RequestHeader) -> bool {
        req.headers.get("Authorization").map(|v| v.as_bytes()) == Some(b"password")
    }

    // Determine backoff time and delay for that duration
    async fn backoff_delay(&self, retries: u8) {
        let exp = retries as u32;
        let delay_ms = Duration::from_millis(u64::pow(10, exp).clamp(10, MAX_DELAY_MS));
        log::info!("retry after back-off of: {delay_ms:?}");
        tokio::time::sleep(delay_ms).await;
    }
}

#[async_trait]
impl ProxyHttp for IridiumGateway {
    type CTX = RetryCtx;
    fn new_ctx(&self) -> Self::CTX {
        Default::default()
    }

    // Handle incoming request, checking authorization for protected URIs.
    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool> {
        let header = session.req_header();
        if header.uri.path().starts_with("/bar") && !self.check_auth(header) {
            if let Err(err) = session.respond_error(403).await {
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
        let mut retry_err = error;
        retry_err.set_retry(true); // Defaults to 16 tries, can be changed in config
        retry_err
    }

    // Determine which upstream peer to send requests to, delaying when a retry is detected
    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        if ctx.retries > 0 {
            self.backoff_delay(ctx.retries).await;
        }
        let path = session.req_header().uri.path();
        let addr = self.select_upstream(path);
        log::info!("connecting to upstream {addr:?} for path {path}");
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
        REQ_COUNTER.inc();
        log::info!(
            "{}, status: {}, requests: {}",
            summary,
            status,
            REQ_COUNTER.get()
        );
    }

    // Decide if the request is cacheable and what cache backend to use.
    // Ideally only `session.cache` should be modified here.
    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        if std::env::var("IRIDIUM_FORCE_CACHE_DISABLE").is_ok() {
            return Ok(());
        }
        if session.req_header().headers.contains_key(CACHE_KEY) {
            log::info!("request_cache_filter: enabling session cache");
            session.cache.enable(
                &*CACHE_STORAGE,
                Some(&*CACHE_MANAGER),
                Some(&*CACHE_PREDICTOR),
                Some(&*CACHE_LOCK),
                None,
            );
        }
        Ok(())
    }

    // Decide if the response is cacheable
    fn response_cache_filter(
        &self,
        _session: &Session,
        resp: &ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<RespCacheable> {
        log::info!("response_cache_filter: decide if the response is cacheable");
        if !resp.status.is_success() {
            log::info!("uncacheable: not a success response");
            return Ok(RespCacheable::Uncacheable(NoCacheReason::Custom(
                "Only success responses are cacheable",
            )));
        }
        log::info!("cacheable");
        let now = SystemTime::now();
        let fresh_until = now
            .checked_add(Duration::from_secs(CACHE_TTL_SECS))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let cache_meta = CacheMeta::new(fresh_until, now, 0, 0, resp.clone());
        Ok(RespCacheable::Cacheable(cache_meta))
    }

    // Generate a cache key for a request when caching is enabled.
    fn cache_key_callback(&self, session: &Session, _ctx: &mut Self::CTX) -> Result<CacheKey> {
        log::info!("cache_key_callback: generating cache key");
        let req_header = session.req_header();
        let namespace = req_header
            .headers
            .get(CACHE_KEY)
            .map(|v| v.to_str().unwrap_or_default())
            .unwrap_or_default()
            .trim();
        if namespace.is_empty() {
            return Err(Error::new(ErrorType::InvalidHTTPHeader));
        }
        let primary = format!("{}{}", req_header.method.as_str(), req_header.uri.path());
        log::info!(
            "cache_key: namespace = {}, primary = {}",
            namespace,
            primary
        );
        Ok(CacheKey::new(namespace, primary, ""))
    }

    // This filter is called after a successful cache lookup
    async fn cache_hit_filter(
        &self,
        _session: &mut Session,
        meta: &CacheMeta,
        _hit_handler: &mut HitHandler,
        is_fresh: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<ForcedInvalidationKind>>
    where
        Self::CTX: Send + Sync,
    {
        log::info!(
            "cache_hit_filter: successful cache lookup; is_fresh = {}, age = {:?}",
            is_fresh,
            meta.age()
        );
        Ok(None)
    }
}

// RUST_LOG=INFO target/debug/gateway --conf conf.yaml
//
// curl 127.0.0.1:6191 -H "Host: one.one.one.one"
// curl 127.0.0.1:6191 -H "Host: one.one.one.one" -H "IridiumCacheKey: one.one.one.one"
// curl 127.0.0.1:6191/bar/ -H "Host: one.one.one.one" -I -H "Authorization: password"
//
// For metrics
// curl 127.0.0.1:6192/
//
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
