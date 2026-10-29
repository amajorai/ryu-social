//! Resolution and capability-cache tests for [`super::ProviderRegistry`].

use super::*;

fn account(id: &str, platform: Platform) -> ProviderAccount {
    ProviderAccount {
        id: id.to_string(),
        platform,
        label: None,
        external_id: None,
    }
}

fn credentials() -> ProviderCredentials {
    ProviderCredentials::default()
}

fn managed_registry(credentials: ProviderCredentials) -> ProviderRegistry {
    ProviderRegistry::from_credentials_with_router(
        reqwest::Client::new(),
        credentials,
        ProviderRouter::for_test(
            crate::state::PLUGIN_ID,
            reqwest::Client::new(),
            "http://127.0.0.1:1/providers/call",
            "http://127.0.0.1:1/providers/status",
        ),
    )
}

#[test]
fn nothing_configured_resolves_to_the_honest_fallback_not_the_fake() {
    let registry = ProviderRegistry::from_credentials(reqwest::Client::new(), credentials());
    for platform in Platform::ALL {
        assert_eq!(
            registry.provider_for(platform).id(),
            ProviderId::Unconfigured,
            "{platform}"
        );
    }
}

#[test]
fn a_direct_adapter_beats_the_broker_for_its_own_platform_only() {
    let registry = managed_registry(ProviderCredentials {
        bluesky_handle: Some("me.bsky.social".into()),
        bluesky_app_password: Some("pw".into()),
        ..credentials()
    });
    assert_eq!(
        registry.provider_for(Platform::Bluesky).id(),
        ProviderId::Bluesky
    );
    assert_eq!(registry.provider_for(Platform::X).id(), ProviderId::Treg);
}

#[test]
fn half_configured_bluesky_is_ignored_rather_than_half_connected() {
    let registry = ProviderRegistry::from_credentials(
        reqwest::Client::new(),
        ProviderCredentials {
            bluesky_handle: Some("me.bsky.social".into()),
            ..credentials()
        },
    );
    assert_eq!(
        registry.provider_for(Platform::Bluesky).id(),
        ProviderId::Unconfigured
    );
}

#[test]
fn an_account_pin_overrides_platform_resolution_and_degrades_when_unconfigured() {
    let registry = managed_registry(ProviderCredentials {
        use_fake: true,
        account_pins: [
            ("acc_fake".to_string(), ProviderId::Fake),
            // Pinned to an adapter with no credentials: must fall through.
            ("acc_bsky".to_string(), ProviderId::Bluesky),
        ]
        .into_iter()
        .collect(),
        ..credentials()
    });
    assert_eq!(
        registry
            .provider_for_account(&account("acc_fake", Platform::X))
            .id(),
        ProviderId::Fake
    );
    assert_eq!(
        registry
            .provider_for_account(&account("acc_bsky", Platform::X))
            .id(),
        ProviderId::Treg
    );
    assert_eq!(
        registry
            .provider_for_account(&account("acc_other", Platform::X))
            .id(),
        ProviderId::Treg
    );
}

#[tokio::test]
async fn capabilities_are_cached_per_provider_and_invalidatable() {
    let fake = Arc::new(FakeProvider::new());
    let registry = ProviderRegistry::with_provider(fake);
    let first = registry.capabilities_for(Platform::X).await;
    let second = registry.capabilities_for(Platform::X).await;
    assert_eq!(first, second);
    assert!(first.publish);
    // schedule is false for EVERY provider — it is never delegated.
    assert!(!first.schedule);
    registry.invalidate_capabilities().await;
    assert_eq!(registry.capabilities_for(Platform::X).await, first);
}

#[tokio::test]
async fn the_unconfigured_fallback_reports_an_empty_matrix() {
    let registry = ProviderRegistry::from_credentials(reqwest::Client::new(), credentials());
    assert_eq!(
        registry.capabilities_for(Platform::X).await,
        PlatformCapabilities::empty()
    );
}
