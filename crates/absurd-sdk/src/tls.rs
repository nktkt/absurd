//! TLS plumbing. The default build uses `NoTls`. Enable the `rustls` or
//! `native-tls` Cargo feature to upgrade — the connection pool picks the
//! configured backend automatically.

use crate::error::{AbsurdError, Result};
use deadpool_postgres::{Manager, ManagerConfig};
use tokio_postgres::Config;

/// Build a Manager using whichever TLS backend is configured. Falls back to
/// `NoTls` when no TLS feature is enabled.
pub(crate) fn build_manager(cfg: Config, mgr_cfg: ManagerConfig) -> Result<Manager> {
    #[cfg(feature = "rustls")]
    {
        let tls = rustls_connector()?;
        return Ok(Manager::from_config(cfg, tls, mgr_cfg));
    }
    #[cfg(all(feature = "native-tls", not(feature = "rustls")))]
    {
        let tls = native_tls_connector()?;
        return Ok(Manager::from_config(cfg, tls, mgr_cfg));
    }
    #[cfg(not(any(feature = "rustls", feature = "native-tls")))]
    {
        Ok(Manager::from_config(cfg, tokio_postgres::NoTls, mgr_cfg))
    }
}

#[cfg(feature = "rustls")]
fn rustls_connector() -> Result<tokio_postgres_rustls::MakeRustlsConnect> {
    use rustls::RootCertStore;

    let mut roots = RootCertStore::empty();
    let result = rustls_native_certs::load_native_certs();
    for cert in result.certs {
        let _ = roots.add(cert);
    }
    if roots.is_empty() {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    let crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(crypto))
}

#[cfg(all(feature = "native-tls", not(feature = "rustls")))]
fn native_tls_connector() -> Result<postgres_native_tls::MakeTlsConnector> {
    let connector = native_tls::TlsConnector::new()
        .map_err(|e| AbsurdError::other(format!("native-tls: {e}")))?;
    Ok(postgres_native_tls::MakeTlsConnector::new(connector))
}

// Suppress unused-import warnings when no TLS features are enabled.
#[allow(dead_code)]
fn _suppress_unused(_: AbsurdError) {}
