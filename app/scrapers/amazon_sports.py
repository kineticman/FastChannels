"""Amazon Sports: live and upcoming Prime Video sports events as event channels.

Discovery is Amazon's personalized sports storefront (/gp/video/sports and its
paged event carousels). Everything else about an event comes from Prime
Video's sports GraphQL gateway, which accepts the same web session cookies:

  - liveSchedule: Amazon's predicted start and end, and LIVE/UPCOMING state.
  - titleActionsCues: whether this account is entitled, through what (Prime,
    free with ads, an add-on), or which subscriptions would unlock it.
  - titleDataView: canPlay and Amazon's reasons an event can't play.
  - liveEntityClassification: SPORT and LEAGUE ids, named via getSportsEntity.

Detail pages are fetched only for events that are live and playable, for the
live action's playback id and envelope. Playback reuses Amazon Free's live PRS
and Widevine license path; the envelope is per event.

There is no sign-in here: the account is the Amazon Prime Free source's.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from bs4 import BeautifulSoup

from .amazon_prime_free import AmazonPrimeFreeScraper
from .base import BaseScraper, ChannelData, ProgramData, EVENT_CHANNEL_TAG

logger = logging.getLogger(__name__)

ACCOUNT_SOURCE = 'amazon_prime_free'
# What this source borrows from the Amazon Free source's config.
_ACCOUNT_KEYS = ('cookie_header', 'amazon_email', 'prs_device_id')

EVENT_END_GRACE_SECONDS = 2 * 3600
EVENT_PREVIEW_CACHE_KEY = 'amazon_sports_event_preview'
ENTITY_NAMES_CACHE_KEY = 'amazon_sports_entity_names'
# Without an Amazon prediction, an event is shown for this long.
_FALLBACK_EVENT_HOURS = 3
# Amazon lists games weeks out; like ESPN's, an event channel appears this
# long before its start.
_EVENT_LOOKAHEAD_HOURS = 48
# An event and a linear station airing of it start together (to the minute,
# when measured); this allows for a late listing.
_STATION_MATCH_SECONDS = 30 * 60

_LUMINA_URL = 'https://atv-ps.amazon.com/cdp/lumina/playerChromeResources/v1'
_GRAPHQL_URL = 'https://atv-ps.amazon.com/cdp/sportsgraphqlgateway/graphql'
_TITLE_BATCH = 20
# Back-to-back gateway calls tripped "exceeded the allowed request limit".
_GRAPHQL_PAUSE_SECONDS = 1.2
# titleActionsCues v3 is UnauthorizedDatum for the web client; v2 is allowed.
# titleDataView v5/v6 are the only readable versions.
_TITLE_FIELDS = (
    'liveSchedule(request: {version: 1}) { ... on ALiveScheduleV1Output { '
    'livelinessEnum predictedStartTime predictedEndTime actualStartTime isMultiDay } } '
    'titleActionsCues(request: {version: 2}) { ... on ATitleActionsCuesV2Output { '
    'entitlement { isEntitled entitlementDetailsList { benefitName entitlementType } } '
    'acquisition { svodAcquisitions { offers { benefitId benefitName isFreeTrialOffered } } } } } '
    'titleDataView(request: {version: 6}) { ... on ATitleDataViewV6Output { '
    'consumption { eligibility { canPlay nonPlayableConsumptionReasons { reason } } } } } '
    'liveEntityClassification(request: {version: 1}) { ... on ALiveEntityClassificationV1Output { '
    'classificationsByDimensions { dimensionTerm classifications { classificationId } } } }'
)
# Reasons that rights and live state already explain. Anything else (a
# blackout or regional block, say) is surfaced in the event's description.
_EXPECTED_REASONS = frozenset({'NO_ENTITLEMENT', 'LIVE_EVENT_IN_FUTURE'})
# How an entitled event is labelled for the access filter, by entitlementType.
# Amazon names free-with-ads events' benefit "Prime", which reads as if Prime
# were needed.
_ENTITLED_LABELS = {
    'FREE': 'Free',
    'AD_SUPPORTED': 'Free with ads',
    'THIRD_PARTY_SUBSCRIPTION_FREE_WITH_ADS': 'Free with ads',
    'PRIME_SUBSCRIPTION': 'Prime',
}
OTHER = 'Other'
# Amazon tags some events with sport ids its sports gateway can't name
# (volleyball, softball, billiards, horse racing...). Those are named from
# the titles and synopses of every event sharing the id; the most matched
# wins. Whole words, case-insensitive; field hockey is checked before hockey.
_SPORT_KEYWORDS = (
    (r'volleyball', 'Volleyball'), (r'softball', 'Softball'), (r'baseball|mlb', 'Baseball'),
    (r'basketball|w?nba', 'Basketball'), (r'field hockey', 'Field Hockey'), (r'hockey|nhl|ahl|echl', 'Ice Hockey'),
    (r'billiards?|snooker', 'Billiards'), (r'horse racing|thoroughbred|day at the races', 'Horse Racing'),
    (r'lumberjacks?|timbersports', 'Timbersports'), (r'padel', 'Padel'), (r'pickleball', 'Pickleball'),
    (r'lacrosse', 'Lacrosse'), (r'rugby', 'Rugby'), (r'cricket', 'Cricket'), (r'squash', 'Squash'),
    (r'tennis', 'Tennis'), (r'golf', 'Golf'), (r'boxing', 'Boxing'), (r'darts', 'Darts'), (r'bowling', 'Bowling'),
    (r'cycling|mountain bike|mtb', 'Cycling'), (r'soccer|mls|futbol', 'Soccer'), (r'football|nfl', 'Football'),
)
_SPORT_PATTERNS = [(re.compile(rf'\b(?:{pattern})\b', re.I), name) for pattern, name in _SPORT_KEYWORDS]


def _infer_sport(texts: list[str]) -> str | None:
    counts: dict[str, int] = {}
    for text in texts:
        for pattern, name in _SPORT_PATTERNS:
            if pattern.search(text or ''):
                counts[name] = counts.get(name, 0) + 1
                break
    return max(counts, key=counts.get) if counts else None


def amazon_account_config() -> dict:
    """The Amazon Free source's account keys, or {} outside an app context or
    before that source has signed in."""
    try:
        from ..models import Source
        source = Source.query.filter_by(name=ACCOUNT_SOURCE).first()
    except Exception:
        return {}
    config = (source.config or {}) if source else {}
    return {key: config[key] for key in _ACCOUNT_KEYS if config.get(key)}


def _with_account(config: dict | None) -> dict:
    return {**(config or {}), **amazon_account_config()}


# ── storefront and detail parsing ─────────────────────────────────────────

def _hydration(html: str) -> dict:
    tag = BeautifulSoup(html, 'html.parser').find('script', id='dv-web-page-hydration-data')
    if not tag or not tag.string:
        raise ValueError('Amazon hydration data missing; session may need renewal')
    return json.loads(tag.string)['init']['preparations']['body']


def _detail_actions(html: str) -> tuple[dict, list[dict]]:
    tag = BeautifulSoup(html, 'html.parser').find('script', id='dv-web-page-hydration-data')
    if not tag or not tag.string:
        raise ValueError('Amazon detail hydration data missing')
    state = json.loads(tag.string)['init']['preparations']['body']['atf']['state']
    headers = state['detail']['headerDetail']
    key = next(iter(headers))
    node = (state.get('action') or {}).get('atf', {}).get(key, {})
    children = ((node.get('playbackActions') or {}).get('main') or {}).get('children') or []
    return dict(headers[key]), children


def _entitlement_message(cues: dict) -> str | None:
    for key in ('focusMessage', 'highValueMessage', 'productSummaryMessage',
                'buyboxMessage', 'informationalMessage'):
        value = cues.get(key) or {}
        message = value.get('message') if isinstance(value, dict) else None
        if isinstance(message, str) and message.strip():
            return message.strip()
    return None


def _provider(cues: dict) -> str | None:
    """Public storefront logo identity, used to tell broadcasters apart."""
    logo = (cues.get('providerLogo') or {}).get('imageUrl') or ''
    for marker, kind in (('/channel-id/', 'channel'), ('/benefit-id/', 'benefit')):
        if marker in logo:
            parts = logo.split(marker, 1)[1].split('/')
            value = parts[1] if kind == 'benefit' and len(parts) > 1 else parts[0]
            if value:
                return value.split('?', 1)[0]
    return None


def _fixture_title(title: str) -> str | None:
    normalized = ' '.join((title or '').casefold().split())
    # Generic show names and "teams TBA" are unsafe to merge by title.
    if re.search(r'\b(?:vs\.?|at)\b', normalized) and 'tba' not in normalized:
        return normalized
    return None


def _chosen_live_action(actions: list[dict]) -> dict | None:
    """The page's one live offer; distinct offers on one page need a choice
    this source doesn't guess at."""
    eligible = [action for action in actions
                if action.get('playbackID') and action.get('playbackEnvelope')
                and action.get('videoMaterialType') in ('Live', 'LiveStreaming')]
    eligible.sort(key=lambda action: (
        action.get('playbackStatus') != 'LiveStreamWatchNow',
        (action.get('label') or '').casefold() != 'watch live',
        (action.get('label') or '').casefold() != 'watch from beginning'))
    offers = {(action.get('playbackID'), action.get('benefitId')) for action in eligible}
    return eligible[0] if len(offers) == 1 else None


def _group_offers(offers: dict[str, dict]) -> list[list[str]]:
    """Conservatively group separately listed broadcasts of one fixture."""
    groups: list[list[str]] = []
    for asin, offer in sorted(offers.items(), key=lambda item: (
            _fixture_title(item[1]['title']) or '',
            item[1]['start'] or datetime.max.replace(tzinfo=timezone.utc), item[0])):
        title = _fixture_title(offer['title'])
        start = offer['start']
        provider = offer['provider_id']
        matched = None
        if title and start and provider:
            for group in groups:
                first = offers[group[0]]
                if (_fixture_title(first['title']) == title and first['start']
                        and abs((start - first['start']).total_seconds()) <= 30 * 60
                        and provider not in {offers[other]['provider_id'] for other in group}
                        and (not offer['venue'] or not first['venue']
                             or offer['venue'].casefold() == first['venue'].casefold())):
                    matched = group
                    break
        if matched is None:
            groups.append([asin])
        else:
            matched.append(asin)
    return groups


def _graphql_time(value: str | None) -> datetime | None:
    # Sports GraphQL times look like 2026-10-28T03:30:00+0000.
    try:
        return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S%z').astimezone(timezone.utc) if value else None
    except ValueError:
        return None


def _event_art(lumina: dict, card: dict) -> str | None:
    """A 16:9 image with the event's branding. Amazon's 4:3 boxart is
    sometimes a blank placeholder, and featured cards carry only an 8:3
    banner, so those come last."""
    card_url = lambda key: (card.get(key) or {}).get('url')
    return (lumina.get('coverImage') or card_url('cover') or lumina.get('heroImage')
            or card_url('poster2x3') or lumina.get('boxartImage') or card_url('boxart') or card_url('hero'))


def _event_description(title: str, sport: str | None, league: str | None, access: list[str]) -> str:
    """For events Amazon doesn't describe: what it is and how it streams."""
    league, sport = (league or '').strip(), (sport or '').strip()
    # "UTR Pro Tennis Tour" already names its sport; "GPAC" wouldn't.
    label = league if sport and sport.casefold() in league.casefold() else ' '.join(x for x in (league, sport) if x)
    parts = [f'{label}: {title}' if label else title]
    parts.append(f'Streaming on Prime Video ({access[0]})' if access else 'Streaming on Prime Video')
    return '. '.join(parts) + '.'


def _event_epg_categories(meta: dict, source_label: str) -> str:
    """Keep the sport, league and subscription searchable in XMLTV categories."""
    values = ('Sports Event', meta.get('sport'), meta.get('league'), source_label, *(meta.get('access') or [])[:1])
    result, seen = [], set()
    for value in values:
        label = (value or '').strip()
        if label and label.casefold() not in seen:
            result.append(label)
            seen.add(label.casefold())
    return ';'.join(result)


# ── access labels and the event filter ────────────────────────────────────

def access_labels(info: dict) -> list[str]:
    """What an event plays through when entitled, else what would unlock it."""
    if info.get('entitled'):
        labels = [_ENTITLED_LABELS.get(item.get('type')) or item.get('benefit')
                  for item in info.get('entitled_via') or []]
        return list(dict.fromkeys(label for label in labels if label)) or ['Free']
    return list(dict.fromkeys(offer['name'] for offer in info.get('unlock_offers') or [] if offer.get('name')))


def clean_event_filter(value) -> dict:
    """Validate the saved filter. access_mode 'entitled' follows what Amazon
    says this account can play, so a new add-on is picked up on its own."""
    if value is None:
        return {'access_mode': 'entitled', 'access': [], 'sport_mode': 'all', 'sports': {}}
    if not isinstance(value, dict):
        raise ValueError('Invalid event filter')
    access_mode = value.get('access_mode', 'entitled')
    if access_mode not in ('entitled', 'selected'):
        raise ValueError('Subscriptions must be "What my account can play" or "Only the ones I pick"')
    access = value.get('access', [])
    if not isinstance(access, list) or len(access) > 100 or any(
            not isinstance(name, str) or not name.strip() or len(name) > 100 for name in access):
        raise ValueError('Invalid subscription')
    if value.get('sport_mode', 'all') not in ('all', 'selected'):
        raise ValueError('Event sports must be All or Only selected')
    raw_sports = value.get('sports', {})
    if not isinstance(raw_sports, dict) or len(raw_sports) > 100:
        raise ValueError('Too many event sports')
    sports = {}
    for name, rule in raw_sports.items():
        if not isinstance(name, str) or not name.strip() or len(name) > 100 or not isinstance(rule, dict):
            raise ValueError('Invalid event sport')
        mode = rule.get('league_mode')
        if mode not in ('all', 'selected'):
            raise ValueError('Event leagues must be All or Only selected')
        leagues = rule.get('leagues', [])
        if not isinstance(leagues, list) or len(leagues) > 100 or any(
                not isinstance(league, str) or not league.strip() or len(league) > 100 for league in leagues):
            raise ValueError('Invalid event league')
        sports[name.strip()] = {'league_mode': mode,
                                'leagues': list(dict.fromkeys(league.strip() for league in leagues))}
    return {'access_mode': access_mode, 'access': list(dict.fromkeys(name.strip() for name in access)),
            'sport_mode': value.get('sport_mode', 'all'), 'sports': sports}


def event_matches_filter(event: dict, config: dict) -> bool:
    """`event` is a preview summary entry: access, entitled, sport, league."""
    policy = clean_event_filter((config or {}).get('event_filter'))
    if policy['access_mode'] == 'entitled':
        if not event.get('entitled'):
            return False
    else:
        picked = {name.casefold() for name in policy['access']}
        if not any(label.casefold() in picked for label in event.get('access') or []):
            return False
    if policy['sport_mode'] == 'selected':
        sport = (event.get('sport') or OTHER).casefold()
        rule = next((rule for name, rule in policy['sports'].items() if name.casefold() == sport), None)
        if not rule:
            return False
        if rule['league_mode'] != 'all' and (event.get('league') or OTHER).casefold() not in {
                league.casefold() for league in rule['leagues']}:
            return False
    return True


class AmazonSportsScraper(AmazonPrimeFreeScraper, BaseScraper):
    source_name = 'amazon_sports'
    source_aliases = ()
    display_name = 'Amazon Sports'
    is_beta = True
    # Live events get their playback envelope on first play (resolve()), so
    # the schedule only needs to keep the 48-hour window current and extend
    # events that run over.
    scrape_interval = 120
    config_required = True
    # Event streams are checked by Amazon at play time; there is no liveness
    # endpoint that works without each event's envelope.
    stream_audit_enabled = False
    audit_requires_config = []
    license_url = 'https://atv-ps.amazon.com/playback/drm/GetWidevineLicense'
    # Envelope-bound manifests; keep well inside the envelope's life.
    _STREAM_URL_TTL = 90
    phase_timeouts = {'init': 30, 'bootstrap': 60, 'channels': 300, 'epg': 60}

    SPORTS_URL = 'https://www.amazon.com/gp/video/sports'
    CAROUSELS = ('Sports for you', 'Sports with a subscription')

    # The sign-in lives on the Amazon Free source; event_filter is saved by
    # the source card's picker.
    config_schema = []

    def __init__(self, config=None):
        super().__init__(_with_account(config))
        self._events: dict[str, dict] = {}

    # ── discovery ──

    def _candidates(self) -> dict[str, dict]:
        response = self.get(self.SPORTS_URL)
        if not response or response.status_code != 200:
            raise RuntimeError('Amazon sports page unavailable')
        body = _hydration(response.text)
        containers = body.get('containers') or []
        main = [c for c in containers if c.get('title') in self.CAROUSELS]
        if len(main) != len(self.CAROUSELS):
            raise RuntimeError('Amazon sports carousels missing; refusing a partial scrape')
        params = {**self.PAGINATE_DEFAULT_PARAMS, 'pageId': 'Sports',
                  'widgetScheme': 'web-explore-v46', 'variant': 'desktopLinux'}
        params['dynamicFeatures'] = [*params['dynamicFeatures'],
                                     'LinearFavoritesSupported', 'RecordingCardSupported']
        headers = {**self.PAGINATE_HEADERS, 'Referer': self.SPORTS_URL}
        events = {}

        def add_entities(entities):
            for entity in entities:
                if entity.get('entityType') != 'EVENT' or not entity.get('titleID'):
                    continue
                asin = entity['titleID']
                old = events.get(asin)
                score = lambda item: (
                    (item.get('liveInfo') or {}).get('status') in ('LIVE', 'UPCOMING'),
                    bool((item.get('entitlementCues') or {}).get('entitlementType')),
                    bool((item.get('images') or {}).get('boxart')))
                if old is None or score(entity) > score(old):
                    events[asin] = entity

        for container in containers:
            entities = container.get('entities') or []
            if not any(entity.get('entityType') == 'EVENT' for entity in entities):
                continue
            add_entities(entities)
            token = container.get('paginationServiceToken')
            target = container.get('paginationTargetId')
            index = container.get('paginationStartIndex')
            if not token or not target or not isinstance(index, int):
                if container in main:
                    raise RuntimeError('Amazon sports pagination seed missing')
                continue
            for _ in range(12):
                query = {**params, 'paginationTargetId': target, 'serviceToken': token,
                         'startIndex': index,
                         'journeyIngressContext': container.get('journeyIngressContext', '')}
                # Amazon now and then answers a page with no entities but more
                # to come; the same request moments later is complete.
                for attempt in range(3):
                    if attempt:
                        time.sleep(attempt)
                    page = self.get(self.PAGINATE_URL, params=query, headers=headers)
                    if not page or page.status_code != 200:
                        raise RuntimeError('Amazon sports pagination failed; refusing a partial scrape')
                    try:
                        data = page.json()
                    except ValueError as exc:
                        raise RuntimeError('Amazon sports pagination was not JSON') from exc
                    entities = data.get('entities')
                    if not isinstance(entities, list):
                        raise RuntimeError('Amazon sports pagination changed shape')
                    if entities or not data.get('hasMoreItems'):
                        break
                add_entities(entities)
                if not data.get('hasMoreItems'):
                    break
                if not entities:
                    raise RuntimeError('Amazon sports pagination stalled')
                next_index = data.get('startIndex')
                if not isinstance(next_index, int) or next_index <= index:
                    raise RuntimeError('Amazon sports pagination did not advance')
                index = next_index
                token = (data.get('pagination') or {}).get('serviceToken') or token
            else:
                raise RuntimeError('Amazon sports pagination exceeded page limit')
        if not events:
            raise RuntimeError('Amazon sports event catalog empty; refusing a destructive scrape')
        return events

    def _fetch_detail(self, asin: str) -> tuple[dict, list[dict]] | None:
        for attempt in range(3):
            try:
                response = self.get(f'https://www.amazon.com/gp/video/detail/{asin}')
                if response and response.status_code == 200:
                    return _detail_actions(response.text)
            except Exception as exc:
                logger.warning('[amazon-sports] detail attempt %d failed for %s: %s',
                               attempt + 1, asin, type(exc).__name__)
            if attempt < 2:
                time.sleep(0.4 * (attempt + 1))
        return None

    # ── sports GraphQL ──

    def _graphql(self, query: str) -> dict:
        response = self.session.post(
            _GRAPHQL_URL, timeout=30, json={'query': query},
            params={'deviceTypeID': 'AOAGZA014O5RE', 'deviceId': self._prs_device_id,
                    'firmware': '1', 'uxLocale': self._ux_locale, 'osLocale': self._ux_locale,
                    'timeZoneId': 'America/New_York', 'softwareVersion': '3'},
            headers={'Accept': 'application/json', 'Origin': 'https://www.amazon.com',
                     'Referer': 'https://www.amazon.com/'})
        return response.json().get('data') or {}

    def _catalog_metadata(self, asin: str) -> tuple[str | None, dict]:
        """Storefront cards carry the catalog ASIN; GraphQL wants the GTI,
        which Lumina's catalog metadata exposes in its image URLs. The same
        response has the event's images, for every event; featured cards
        carry only a wide banner."""
        params = {'deviceID': self._prs_device_id, 'deviceTypeID': 'AOAGZA014O5RE',
                  'gascEnabled': 'false', 'marketplaceID': self._marketplace_id,
                  'uxLocale': self._ux_locale, 'desiredResources': 'catalogMetadataV2',
                  'entityId': asin, 'firmware': '1', 'widgetScheme': 'pvplayer-web-v6'}
        response = self.get(_LUMINA_URL, params=params,
                            headers={'Origin': 'https://www.amazon.com', 'Referer': 'https://www.amazon.com/'})
        if not response:
            return None, {}
        match = re.search(r'amzn1\.dv\.gti\.[0-9a-f-]{36}', response.text)
        try:
            images = ((response.json().get('resources') or {}).get('catalogMetadataV2') or {}).get('images') or {}
        except ValueError:
            images = {}
        return (match.group(0) if match else None), images

    def _station_airings(self) -> dict[str, list[tuple[datetime, datetime, str]]]:
        """Airings on Amazon's linear stations by normalized title. Some events
        are a slot on a provider's station (Ryz Sports, FloSports, Unbeaten,
        Tennis Channel 2...): that stream shows the next programme once the
        slot ends, while Amazon still calls the event live past it."""
        # Station pagination records each station's envelope in memory; keep
        # this source's own.
        own_envelopes = self._channel_pe
        try:
            page = self.get(self.LIVE_TV_URL)
            if not page:
                return {}
            stations = self._extract_initial_stations(page.text)
            seed = self._extract_pagination_seed(page.text)
            if seed:
                for station in self._paginate_stations(seed).values():
                    station_id = self._station_id(station)
                    if station_id and station_id not in stations:
                        stations[station_id] = station
        except Exception as exc:
            logger.warning('[amazon-sports] station schedules unavailable: %s', type(exc).__name__)
            return {}
        finally:
            self._channel_pe = own_envelopes
        airings: dict[str, list[tuple[datetime, datetime, str]]] = {}
        for station in stations.values():
            for airing in station.get('schedule') or []:
                title = ' '.join(((airing.get('metadata') or {}).get('title') or '').casefold().split())
                try:
                    start = datetime.fromtimestamp(int(airing['start']) / 1000, tz=timezone.utc)
                    end = datetime.fromtimestamp(int(airing['end']) / 1000, tz=timezone.utc)
                except (KeyError, TypeError, ValueError):
                    continue
                if title and end > start:
                    airings.setdefault(title, []).append((start, end, (station.get('name') or '').strip()))
        return airings

    def _entity_names(self, ids: set[str]) -> dict[str, str]:
        """Sport and league names for classification ids. Ids are stable, so
        names are kept across scrapes and only new ids are looked up."""
        names = dict(self.cache.get(ENTITY_NAMES_CACHE_KEY) or {})
        pending = sorted(i for i in ids if i not in names)
        for offset in range(0, len(pending), 25):
            time.sleep(_GRAPHQL_PAUSE_SECONDS)
            chunk = pending[offset:offset + 25]
            query = '{ ' + ' '.join(
                f'e{i}: getSportsEntity(getSportsEntityInput: {{id: "{entity_id}", useGenericTypes: true}}) '
                '{ ... on GenericSport { name } ... on GenericSportsOrganization { name } '
                '... on GenericCompetitionGroup { name } }'
                for i, entity_id in enumerate(chunk)) + ' }'
            try:
                data = self._graphql(query)
            except Exception as exc:
                logger.warning('[amazon-sports] entity name batch failed: %s', type(exc).__name__)
                continue
            for i, entity_id in enumerate(chunk):
                # Some ids resolve to nothing; remember that too.
                names[entity_id] = ((data.get(f'e{i}') or {}).get('name') or '').strip()
        if pending:
            self._update_cache(ENTITY_NAMES_CACHE_KEY, names)
        return names

    def _title_info(self, asins: list[str], texts: dict[str, str]) -> dict[str, dict]:
        """Amazon's predicted window, account rights and sport/league per
        event. Best-effort: a missing entry falls back to the card's signals,
        an unclassified sport and a fixed window."""
        with ThreadPoolExecutor(max_workers=4) as pool:
            catalog = dict(zip(asins, pool.map(self._catalog_metadata, asins)))
        by_gti = {gti: asin for asin, (gti, _) in catalog.items() if gti}
        titles = {}
        pending = list(by_gti)
        for offset in range(0, len(pending), _TITLE_BATCH):
            if offset:
                time.sleep(_GRAPHQL_PAUSE_SECONDS)
            chunk = pending[offset:offset + _TITLE_BATCH]
            query = '{ ' + ' '.join(
                f't{i}: getTitle(getTitleInput: {{id: "{gti}"}}) {{ ... on GtiTitle {{ {_TITLE_FIELDS} }} }}'
                for i, gti in enumerate(chunk)) + ' }'
            try:
                data = self._graphql(query)
            except Exception as exc:
                logger.warning('[amazon-sports] title batch failed: %s', type(exc).__name__)
                continue
            for i, gti in enumerate(chunk):
                title = data.get(f't{i}') or {}
                if not title:
                    continue
                cues = title.get('titleActionsCues') or {}
                entitlement = cues.get('entitlement') or {}
                offers = ((cues.get('acquisition') or {}).get('svodAcquisitions') or {}).get('offers') or []
                eligibility = (((title.get('titleDataView') or {}).get('consumption') or {})
                               .get('eligibility')) or {}
                reasons = [item.get('reason') for item in eligibility.get('nonPlayableConsumptionReasons') or []
                           if item.get('reason')]
                unexpected = sorted(set(reasons) - _EXPECTED_REASONS)
                if unexpected:
                    logger.warning('[amazon-sports] %s: unexpected non-playable reason(s) %s',
                                   by_gti[gti], ', '.join(unexpected))
                dimensions = {
                    item.get('dimensionTerm'): [c.get('classificationId') for c in item.get('classifications') or []
                                                if c.get('classificationId')]
                    for item in ((title.get('liveEntityClassification') or {})
                                 .get('classificationsByDimensions') or [])}
                titles[by_gti[gti]] = {
                    'gti': gti,
                    'images': catalog[by_gti[gti]][1],
                    'schedule': title.get('liveSchedule') or {},
                    'entitled': entitlement.get('isEntitled'),
                    'entitled_via': [{'benefit': item.get('benefitName') or None, 'type': item.get('entitlementType')}
                                     for item in entitlement.get('entitlementDetailsList') or []],
                    'unlock_offers': [{'benefit_id': item.get('benefitId'), 'name': item.get('benefitName'),
                                       'free_trial': item.get('isFreeTrialOffered')}
                                      for item in offers if item.get('benefitName')],
                    'can_play': eligibility.get('canPlay'),
                    'blocked_reasons': reasons,
                    'sport_ids': dimensions.get('SPORT') or [],
                    'league_ids': dimensions.get('LEAGUE') or [],
                }
        names = self._entity_names({i for t in titles.values() for i in t['sport_ids'] + t['league_ids']})
        for info in titles.values():
            info['sport'] = next((names[i] for i in info['sport_ids'] if names.get(i)), None)
            info['league'] = next((names[i] for i in info['league_ids'] if names.get(i)), None)
        # Sport ids Amazon can't name, from the text of every event tagged with them.
        unnamed: dict[str, list[str]] = {}
        for asin, info in titles.items():
            if not info['sport']:
                for sport_id in info['sport_ids']:
                    unnamed.setdefault(sport_id, []).append(texts.get(asin, ''))
        inferred = {sport_id: _infer_sport(event_texts) for sport_id, event_texts in unnamed.items()}
        for asin, info in titles.items():
            if not info['sport']:
                info['sport'] = (next((inferred[i] for i in info['sport_ids'] if inferred.get(i)), None)
                                 or _infer_sport([texts.get(asin, '')]))
        logger.info('[amazon-sports] title info for %d/%d events: %d with a predicted end, '
                    '%d entitled, %d with a sport',
                    len(titles), len(asins),
                    sum(1 for t in titles.values() if t['schedule'].get('predictedEndTime')),
                    sum(1 for t in titles.values() if t['entitled']),
                    sum(1 for t in titles.values() if t['sport']))
        return titles

    # ── channels / EPG ──

    def fetch_channels(self) -> list[ChannelData]:
        if not self._cookie_header:
            raise RuntimeError('Sign in to Amazon on the Amazon Prime Free source first')
        cards = {asin: card for asin, card in self._candidates().items()
                 if (card.get('liveInfo') or {}).get('status') in ('LIVE', 'UPCOMING')}
        if not cards:
            raise RuntimeError('Amazon sports page has no live or upcoming events')
        titles = self._title_info(list(cards), {
            asin: f"{card.get('title') or ''}\n{card.get('synopsis') or ''}" for asin, card in cards.items()})

        offers = {}
        for asin, card in cards.items():
            info = titles.get(asin) or {}
            cues = card.get('entitlementCues') or {}
            schedule = info.get('schedule') or {}
            offers[asin] = {
                'card': card,
                'title': card.get('title') or asin,
                'start': _graphql_time(schedule.get('predictedStartTime')),
                'end': _graphql_time(schedule.get('predictedEndTime')),
                'live': (schedule.get('livelinessEnum') or (card.get('liveInfo') or {}).get('status')) == 'LIVE',
                'venue': (card.get('liveInfo') or {}).get('venue'),
                'provider_id': _provider(cues),
                'card_entitlement': cues.get('entitlementType'),
                'card_message': _entitlement_message(cues),
                'entitled': info.get('entitled'),
                'entitled_via': info.get('entitled_via') or [],
                'unlock_offers': info.get('unlock_offers') or [],
                'can_play': info.get('can_play'),
                'blocked_reasons': info.get('blocked_reasons') or [],
                'access': access_labels(info) if info else [],
                'sport': info.get('sport'),
                'league': info.get('league'),
                'images': info.get('images') or {},
            }

        def has_rights(asin):
            offer = offers[asin]
            return bool(offer['entitled'] or offer['card_entitlement'] == 'Entitled')

        station_airings = self._station_airings()

        # Only live, playable events need their detail page, for the live
        # action's playback id and envelope.
        live = [asin for asin in offers if offers[asin]['live'] and has_rights(asin)]
        details = {}
        with ThreadPoolExecutor(max_workers=4) as pool:
            for asin, result in zip(live, pool.map(self._fetch_detail, live)):
                if result:
                    details[asin] = result
                else:
                    logger.warning('[amazon-sports] detail page failed for live event %s; not playable this scrape', asin)

        now = datetime.now(timezone.utc)
        horizon = now + timedelta(hours=_EVENT_LOOKAHEAD_HOURS)
        previous_ids = self.cache.get('sports_group_ids') or {}
        # Station guides drop an airing once it ends, which is exactly when an
        # event still called live needs its slot; keep slots between scrapes.
        previous_slots = self.cache.get('sports_station_slots') or {}
        station_slots = {}
        group_ids, used_group_ids = {}, set()
        channels, envelopes, metadata, broadcasts, preview = [], {}, {}, {}, []
        included_ids, candidate_ids = set(), set()
        for group in _group_offers(offers):
            # Events further out are left for a later scrape.
            if all(offers[asin]['start'] and offers[asin]['start'] > horizon for asin in group):
                continue
            group_id = next((previous_ids[asin] for asin in group
                             if previous_ids.get(asin) and previous_ids[asin] not in used_group_ids), None)
            if group_id is None:
                group_id = next((asin for asin in sorted(group) if asin not in used_group_ids), None)
            if group_id is None:
                group_id = 'sports-group-' + hashlib.sha1(','.join(sorted(group)).encode()).hexdigest()[:16]
            used_group_ids.add(group_id)
            candidate_ids.add(group_id)
            actions = {asin: _chosen_live_action(details[asin][1]) if asin in details else None for asin in group}
            # An actionable broadcast first, then one this account can play.
            # The choice holds for manifest and license requests until the
            # next scrape; a playback session never switches broadcasts.
            selected_asin = min(group, key=lambda asin: (not actions[asin], not has_rights(asin), asin))
            for asin in group:
                group_ids[asin] = group_id
            broadcasts[group_id] = [
                {'asin': asin, 'provider_id': offers[asin]['provider_id'],
                 'card_entitlement': offers[asin]['card_entitlement'],
                 'entitled': offers[asin]['entitled'], 'access': offers[asin]['access'],
                 'actionable': bool(actions[asin])}
                for asin in group]
            selected = offers[selected_asin]
            action = actions[selected_asin]
            # Every listing of the game counts toward the filter.
            access = list(dict.fromkeys(label for asin in group for label in offers[asin]['access']))
            entitled = any(has_rights(asin) for asin in group)
            # What this account plays the game through, as opposed to what
            # would unlock another broadcaster's listing of it.
            playable_access = list(dict.fromkeys(label for asin in group if has_rights(asin)
                                                 for label in offers[asin]['access']))
            start = selected['start']
            end = selected['end'] if selected['end'] and start and selected['end'] > start else None
            if start and not end:
                end = start + timedelta(hours=_FALLBACK_EVENT_HOURS)
            if not start:
                continue
            slot = min(((abs((slot_start - start).total_seconds()), slot_start, slot_end, station)
                        for slot_start, slot_end, station
                        in station_airings.get(' '.join(selected['title'].casefold().split()), [])),
                       default=None)
            station = None
            if slot and slot[0] <= _STATION_MATCH_SECONDS:
                # The stream is the station's: its slot is the event.
                _, start, end, station = slot
            elif group_id in previous_slots:
                kept = previous_slots[group_id]
                start, end, station = (datetime.fromisoformat(kept['start']), datetime.fromisoformat(kept['end']),
                                       kept['station'])
            elif selected['live']:
                # An event running past its prediction stays listed until
                # the next scrape can look again.
                end = max(end, now + timedelta(minutes=self.scrape_interval + 30))
            if station:
                station_slots[group_id] = {'start': start.isoformat(), 'end': end.isoformat(), 'station': station}
            summary = {'access': access, 'entitled': entitled, 'playable_access': playable_access,
                       'sport': selected['sport'] or OTHER, 'league': selected['league'] or OTHER,
                       'end': int(end.timestamp())}
            preview.append(summary)
            if not event_matches_filter(summary, self.config):
                continue
            included_ids.add(group_id)
            if action:
                envelopes[group_id] = {'pe': action['playbackEnvelope'], 'expiry': 0,
                                       'asin': selected_asin, 'fetched_at': time.time()}
            card = selected['card']
            art = _event_art(selected['images'], card.get('images') or {})
            description = (card.get('synopsis') or '').strip()
            # Some synopses only repeat the title.
            if not description or description.casefold() == selected['title'].strip().casefold():
                description = _event_description(selected['title'], selected['sport'], selected['league'], access)
            if not has_rights(selected_asin):
                needs = list(dict.fromkeys(offer['name'] for offer in selected['unlock_offers']))
                access_note = 'Not playable with this Amazon account.'
                if needs:
                    access_note += f" Needs {' or '.join(needs)}."
                description = f'{access_note} {description}'.strip()
            unexpected = [reason for reason in selected['blocked_reasons'] if reason not in _EXPECTED_REASONS]
            if unexpected:
                description = f"Amazon reports this event can't play: {', '.join(unexpected)}. {description}".strip()
            metadata[group_id] = {
                'playback_id': action.get('playbackID') if action else None,
                'title': selected['title'], 'description': description, 'art': art,
                'start': start.isoformat(), 'end': end.isoformat(), 'live': selected['live'],
                'playable': has_rights(selected_asin), 'station': station,
                'selected_asin': selected_asin, 'broadcast_asins': group,
                'entitled': selected['entitled'], 'entitled_via': selected['entitled_via'],
                'unlock_offers': selected['unlock_offers'], 'can_play': selected['can_play'],
                'blocked_reasons': selected['blocked_reasons'], 'access': access,
                'sport': selected['sport'], 'league': selected['league'],
            }
            label = selected['league'] or selected['sport'] or self.display_name
            channels.append(ChannelData(
                source_channel_id=group_id,
                name=f'{label} - {selected["title"]}'[:256],
                stream_url=f'primefree://{group_id}',
                logo_url=art,
                category='Sports',
                stream_type='dash',
                guide_key=group_id,
                tags=[EVENT_CHANNEL_TAG, self.display_name, *access[:1]],
                description=description,
                gracenote_mode='off',
                retire_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS),
                delete_at=end + timedelta(seconds=EVENT_END_GRACE_SECONDS + 24 * 3600),
            ))

        # A completed event inventory can retire old event rows on this scrape.
        self.hide_missing_event_channels = True
        # Filtered-out events leave the output now; their rows keep manual
        # edits until their delete deadline.
        self.excluded_channel_ids = frozenset(candidate_ids - included_ids)
        self._events = metadata
        self._channel_pe = envelopes
        self._update_cache('channel_pe', envelopes)
        self._update_cache('sports_events', metadata)
        self._update_cache('sports_broadcasts', broadcasts)
        self._update_cache('sports_group_ids', group_ids)
        self._update_cache('sports_station_slots', station_slots)
        # The source card's subscription and sport/league picker reads this.
        self._update_cache(EVENT_PREVIEW_CACHE_KEY, {'fetched_at': int(time.time()),
                                                     'window_hours': _EVENT_LOOKAHEAD_HOURS, 'events': preview})
        logger.info('[amazon-sports] %d events, %d games, %d channels after filters, %d playable now',
                    len(cards), len(candidate_ids), len(channels), len(envelopes))
        return channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        programs = []
        for channel in channels:
            meta = self._events.get(channel.source_channel_id) or {}
            if not meta.get('start') or not meta.get('end'):
                continue
            # Amazon's predicted window, often including its studio coverage.
            # A display window only: retire_at, not this, ends the channel.
            league = meta.get('league')
            programs.append(ProgramData(
                channel.source_channel_id, meta['title'],
                datetime.fromisoformat(meta['start']), datetime.fromisoformat(meta['end']),
                description=meta.get('description') or None, poster_url=meta.get('art'),
                category=_event_epg_categories(meta, self.display_name),
                episode_title=league if league and league.casefold() != meta['title'].casefold() else None,
                is_live=True, episode_id=channel.source_channel_id))
        return programs

    # ── playback ──

    @classmethod
    def _event(cls, config: dict, channel_id: str) -> dict:
        return (config.get('sports_events') or {}).get(channel_id) or {}

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        playback_id = cls._event(config, channel_id).get('playback_id') if channel_id else None
        # The watch-page builder has Source.config but not source_cache; it
        # needs a nonempty URL to expose the local license proxy. The proxy
        # itself gets the cache and substitutes the actual playback id.
        return super().get_license_url(_with_account(config), playback_id or channel_id)

    @classmethod
    def _channel_pe_for(cls, config: dict, title_id: str) -> str | None:
        entry = (config.get('channel_pe') or {}).get(title_id) or {}
        # A stale or missing envelope waits for the next scrape; minting one
        # separately for the manifest and the license would mix sessions.
        if time.time() - float(entry.get('fetched_at') or 0) > 7200:
            return None
        return entry.get('pe')

    @classmethod
    def _get_live_playback_resources(cls, config: dict, title_id: str, pe: str | None = None) -> dict | None:
        playback_id = cls._event(config, title_id).get('playback_id')
        if not playback_id:
            return None
        # Envelopes are keyed by channel; the parent would look one up by playback id.
        pe = pe or cls._channel_pe_for(config, title_id)
        return super()._get_live_playback_resources(_with_account(config), playback_id, pe)

    @classmethod
    def _get_session_handoff_token(cls, config: dict, channel_id: str) -> str | None:
        return super()._get_session_handoff_token(_with_account(config), channel_id)

    @classmethod
    def prepare_license_request(cls, challenge: bytes, config: dict, channel_id: str | None = None,
                                sht: str | None = None) -> tuple[bytes, dict]:
        return super().prepare_license_request(challenge, _with_account(config), channel_id, sht)

    def resolve(self, raw_url: str) -> str:
        if raw_url.startswith('primefree://'):
            self._ensure_live_action(raw_url[len('primefree://'):])
        return super().resolve(raw_url)

    def _ensure_live_action(self, group_id: str) -> None:
        """A game that kicked off after the last scrape has no envelope yet:
        fetch its live action from the detail page now. It goes in the source
        cache, where the license proxy reads it, so the manifest and license
        use the same session."""
        state = {**self.config, **self.cache}
        if self._channel_pe_for(state, group_id):
            return
        meta = self._event(state, group_id)
        start = datetime.fromisoformat(meta['start']) if meta.get('start') else None
        if (not meta.get('playable') or not meta.get('selected_asin')
                or (start and start > datetime.now(timezone.utc) + timedelta(minutes=10))):
            return
        # Don't fetch the page on every manifest poll of an event that isn't on yet.
        misses = dict(self.cache.get('sports_action_misses') or {})
        if time.time() - float(misses.get(group_id) or 0) < 60:
            return
        detail = self._fetch_detail(meta['selected_asin'])
        action = _chosen_live_action(detail[1]) if detail else None
        if not action:
            logger.info('[amazon-sports] no live action yet for %s', group_id)
            self._update_cache('sports_action_misses', {**misses, group_id: time.time()})
            return
        entry = {'pe': action['playbackEnvelope'], 'expiry': 0,
                 'asin': meta['selected_asin'], 'fetched_at': time.time()}
        self._channel_pe[group_id] = entry
        self._update_cache('channel_pe', {**(self.cache.get('channel_pe') or {}), group_id: entry})
        self._update_cache('sports_events', {**(self.cache.get('sports_events') or {}),
                                             group_id: {**meta, 'playback_id': action['playbackID'], 'live': True}})
        logger.info('[amazon-sports] fetched live action for %s at play time', group_id)

    def _resolve_live(self, title_id: str, pe: str | None = None) -> str | None:
        data = self._get_live_playback_resources({**self.config, **self.cache}, title_id, pe)
        result = ((data or {}).get('livePlaybackUrls') or {}).get('result') or {}
        url_sets = result.get('urlSets') or []
        if not isinstance(url_sets, list):
            return None
        default_id = result.get('defaultUrlSetId')
        for item in sorted(url_sets, key=lambda x: x.get('urlSetId') != default_id):
            url = ((item.get('urls') or {}).get('manifest') or {}).get('url')
            if isinstance(url, str) and url.startswith('https://'):
                return url
        return None
