from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class TVESourceTarget:
    key: str
    name: str
    domains: tuple[str, ...]
    extractor: str | None
    auth_system: str
    live_status: str
    vod_status: str
    drm_risk: str
    epg_status: str
    status: str
    notes: str

    def to_dict(self) -> dict:
        return asdict(self)


TVE_SOURCE_TARGETS: tuple[TVESourceTarget, ...] = (
    TVESourceTarget(
        key='aenetworks',
        name='A+E Networks',
        domains=('history.com', 'aetv.com', 'mylifetime.com', 'fyi.tv'),
        extractor='aenetworks',
        auth_system='Adobe Pass',
        live_status='working',
        vod_status='probe',
        drm_risk='low',
        epg_status='working',
        status='implemented',
        notes='Native Cox Adobe Pass path is working for live A+E channels; yt-dlp A+E page parsing is currently stale.',
    ),
    TVESourceTarget(
        key='fox',
        name='FOX / Fox Sports / Fox News',
        domains=('fox.com', 'foxsports.com', 'foxnews.com', 'foxweather.com', 'foxbusiness.com'),
        extractor='FOX, FoxSports, foxnews',
        auth_system='Adobe Pass / site-specific',
        live_status='working-partial',
        vod_status='candidate',
        drm_risk='mixed',
        epg_status='partial',
        status='implemented-partial',
        notes='Fox Weather is playable through a FastChannels live proxy. Fox News and Fox Business resolve through their official TVE pages to obtain short-lived Akamai-signed HLS, then FastChannels propagates that token to variants and segments. Fox Sports Product API listings provide real EPG, and FS1/FS2/BTN/FOX Deportes/FOX Soccer Plus resolve through FOX Sports DVP using Cox MVPD auth when configured, with PreviewPass fallback while prototyping. FOX digital resolves only when a current listing exists.',
    ),
    TVESourceTarget(
        key='warner',
        name='Warner Bros. Discovery Cable',
        domains=('tbs.com', 'tntdrama.com', 'trutv.com', 'cnn.com', 'tcm.com'),
        extractor='TBS, CNN',
        auth_system='Adobe Pass / site-specific',
        live_status='candidate',
        vod_status='candidate',
        drm_risk='medium',
        epg_status='needed',
        status='high-priority',
        notes='Potentially valuable bundle, but extractor coverage is uneven and live endpoints need direct probing.',
    ),
    TVESourceTarget(
        key='nbcuniversal',
        name='NBCUniversal',
        domains=('nbc.com', 'usanetwork.com', 'cnbc.com', 'golfchannel.com', 'syfy.com', 'bravotv.com'),
        extractor='NBC, NBCSports, USANetwork, CNBCVideo',
        auth_system='Adobe Pass / site-specific',
        live_status='candidate',
        vod_status='candidate',
        drm_risk='medium-high',
        epg_status='needed',
        status='candidate',
        notes='Likely broad TVE coverage, but NBC live streams often need per-brand endpoint work and may be DRM-protected.',
    ),
    TVESourceTarget(
        key='discovery',
        name='Discovery / HGTV / Food Network',
        domains=('go.discovery.com', 'hgtv.com', 'foodnetwork.com', 'tlc.com', 'investigationdiscovery.com'),
        extractor='GoDiscovery, HGTVUsa, FoodNetwork, TLC, InvestigationDiscovery',
        auth_system='Adobe Pass / Discovery auth',
        live_status='unknown',
        vod_status='candidate',
        drm_risk='medium',
        epg_status='needed',
        status='candidate',
        notes='yt-dlp coverage is mostly VOD-oriented; live source discovery is the open question.',
    ),
    TVESourceTarget(
        key='espn',
        name='ESPN / WatchESPN',
        domains=('espn.com', 'watch.espn.com'),
        extractor='ESPN, WatchESPN',
        auth_system='Adobe Pass / ESPN',
        live_status='candidate',
        vod_status='candidate',
        drm_risk='high',
        epg_status='needed',
        status='candidate',
        notes='High-value but likely a harder DRM/auth target than A+E or FOX.',
    ),
    TVESourceTarget(
        key='cbs',
        name='CBS / CBS Sports / CBS News',
        domains=('cbs.com', 'cbsnews.com', 'cbssports.com'),
        extractor='CBS, cbsnews, cbssports',
        auth_system='site-specific',
        live_status='mixed',
        vod_status='candidate',
        drm_risk='medium-high',
        epg_status='needed',
        status='candidate',
        notes='CBS News may be open/FAST-like; CBS and CBS Sports require separate entitlement probing.',
    ),
    TVESourceTarget(
        key='abc_disney',
        name='ABC / Disney / Freeform',
        domains=('abc.com', 'abcnews.com', 'disneynow.com', 'freeform.com'),
        extractor='abcnews, Disney',
        auth_system='Adobe Pass / Disney',
        live_status='unknown',
        vod_status='candidate',
        drm_risk='high',
        epg_status='needed',
        status='candidate',
        notes='Extractor coverage exists for parts of the ecosystem, but ABC/Disney TVE live support needs fresh probing.',
    ),
    TVESourceTarget(
        key='sports_leagues',
        name='League Sites',
        domains=('nba.com', 'nfl.com'),
        extractor='nba, nfl.com',
        auth_system='site-specific',
        live_status='unknown',
        vod_status='candidate',
        drm_risk='high',
        epg_status='needed',
        status='candidate',
        notes='Likely useful for authenticated VOD/replays before linear channels.',
    ),
    TVESourceTarget(
        key='public_news',
        name='Public / News Adjacent',
        domains=('pbs.org', 'c-span.org', 'bloomberg.com'),
        extractor='pbs, CSpan, Bloomberg',
        auth_system='mixed / often no TVE',
        live_status='mixed',
        vod_status='candidate',
        drm_risk='low',
        epg_status='source-dependent',
        status='parallel-track',
        notes='May produce legal streams without TVE credentials; keep separate from Adobe Pass work.',
    ),
)


def tve_source_catalog() -> list[dict]:
    return [target.to_dict() for target in TVE_SOURCE_TARGETS]
