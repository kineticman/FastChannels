import copy
import logging
import socket
import unicodedata
from abc import ABC, abstractmethod
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

_SPANISH_LANGUAGE_MARKERS = (
    'en espanol',
    'espanol',
    'español',
    'spanish',
    'latino',
    'latina',
    'latinos',
    'latinas',
    'noticias',
    'deportes',
    'novelas',
    'telenovela',
    'telemundo',
    'univision',
    'canela',
    'estrella',
    'azteca',
    'nuevo latino',
    'siempre latino',
    # Spanish-language channel names / phrases
    'cine de',       # "Cine de Horror", "Cine de Accion"
    'todo cine',     # "Todo Cine"
    'lamusica',      # "LaMúsica TV" (accent stripped by fold)
    'tv amor',       # "TV Amor"
    'multimedios',   # Mexican broadcast network
    'bandamax',      # Mexican music channel
    'ritmoson',      # Latin music channel
    'tele hit',      # Mexican pop music channel
    # Spanish channel names / title words
    'cuando',        # "Cuando Los Angeles Caen"
    'unica',         # "ÚNICA TV" (accent stripped)
    'vas o no',      # "Vas O No Vas USA"
    'en alerta',     # "C4 en Alerta"
    'apostaria',     # "Apostarías por Mí" (accent stripped)
    'naturaleza',    # "Naturaleza Salvaje"
    'atresplayer',   # AtresmediaPlayer (Spanish broadcaster)
    'el rey',        # El Rey Network (Robert Rodriguez's Spanish channel)
    'pitufo',        # "Pitufo TV" (Los Pitufos = The Smurfs, Spanish)
    'vix',           # ViX / Vix+ (Univision/Televisa FAST service)
    'caracol',       # Caracol TV (Colombian network)
    'tudn',          # Telemundo/Univision Deportes Network
    'exitos',        # "Éxitos del Momento" (accent stripped by fold)
    'parejas',       # "Grandes Parejas" (Spanish for couples)
    'ahora',         # "Aqui y Ahora" (Spanish for now)
    'lo mejor de',   # "Lo Mejor de Liga" (Spanish for the best of)
    'desimpedidos',  # Brazilian Portuguese football channel
    # Additional Spanish words / brand names found during language audit
    'accion',        # fold of acción — "FreeTV Acción", "FILMEX Acción"
    'cinepolis',     # Cinépolis (Mexican cinema chain)
    'crimen',        # Spanish for crime — "Crimen", "Todo Crimen"
    'filmex',        # FILMEX Spanish-language movie service
    'lucha',         # Lucha Libre — "Lucha Plus", "Lucha Libre AAA"
    'saborear',      # "Saborear TV" (Mexican food/lifestyle)
    'sangre',        # Spanish for blood — "Runtime Sangre Fría"
    'sureno',        # fold of sureño — "FreeTV Sureño"
    # Common unambiguous Spanish words
    'miedo',         # Spanish for fear — "Mi Miedo Canal"
    'pelicula',      # fold of película — Spanish for movie
    'corazon',       # fold of corazón — Spanish for heart
    'tvoai',         # TVOAI Channel (Spanish-language Roku brand)
    'astro ciencia', # multi-word to avoid substring collisions on the common word "ciencia"
)


def fold_language_hint(value: str | None) -> str:
    if not value:
        return ''
    normalized = unicodedata.normalize('NFKD', value)
    ascii_only = ''.join(ch for ch in normalized if not unicodedata.combining(ch))
    return ascii_only.casefold()


def infer_language_from_metadata(*values: str | None, default: str = 'en') -> str:
    for value in values:
        folded = fold_language_hint(value)
        if any(marker in folded for marker in _SPANISH_LANGUAGE_MARKERS):
            return 'es'
    return default


# Explicit language signals → ISO 639 code, for scrapers that expose a rich
# description (e.g. FreeCast).  Matched as substrings against the accent-folded,
# lowercased name + description; the FIRST match in this ordered tuple wins, so
# keep the most specific markers first.
#
# These are deliberately *explicit language statements* or unambiguous
# language-specific brands — never bare country/region names.  A channel can be
# English while being about a foreign place ("Discovering China", "True
# African", "Arirang … English language general entertainment channel from
# S. Korea"), so country words would mislabel them.
_LANGUAGE_PHRASE_MARKERS = (
    ('hausa', 'ha'),
    ('yoruba', 'yo'),
    ('haitian creole', 'ht'),
    ('creole language', 'ht'),
    ('kreyol', 'ht'),
    ('russian-language', 'ru'),
    ('russian language', 'ru'),
    ('francophone', 'fr'),
    ('french-language', 'fr'),
    ('french language', 'fr'),
    ('francais', 'fr'),          # "3ABN Français" (name)
    ('mandarin', 'zh'),
    ('cantonese', 'zh'),
    ('chinese-language', 'zh'),
    ('chinese language', 'zh'),
    ('filipino', 'tl'),
    ('tagalog', 'tl'),
    ('korean-language', 'ko'),
    ('korean language', 'ko'),
    ('arabic', 'ar'),
    ('arab world', 'ar'),
    ('arab movie', 'ar'),
    ('arab musical', 'ar'),
    ('arab series', 'ar'),
    ('al arabiya', 'ar'),        # brand (desc never says "Arabic")
    ('asharq', 'ar'),            # brand (pan-Arab news)
    # NOTE: Portuguese is intentionally NOT inferred from country references
    # ("from Brazil", "Samba"): those are bilingual/Latin-music false positives
    # (e.g. TRACE Latina lists Samba among Salsa/Reggaeton; 3ABN Latino is
    # Spanish+Portuguese).  Add 'pt' only on an explicit "Portuguese-language"
    # style statement if one ever appears.
    # Spanish phrases the name-based pass (_SPANISH_LANGUAGE_MARKERS) misses
    # because they live in the description rather than the channel name.
    ('spanish speaking', 'es'),
    ('spanish-speaking', 'es'),
    ('spanish language', 'es'),
    ('puerto rican', 'es'),
)

# Unicode script → ISO code, for scripts that map cleanly to a single language
# in practice.  Latin is intentionally excluded (ambiguous across en/es/fr/pt…).
_SCRIPT_LANGUAGE_HINTS = (
    ('CYRILLIC', 'ru'),
    ('ARABIC', 'ar'),
    ('HANGUL', 'ko'),
    ('CJK', 'zh'),
    ('DEVANAGARI', 'hi'),
)
_SCRIPT_MIN_CHARS = 4   # ignore a stray symbol; require a real run of script


def _dominant_script_language(text: str | None) -> str | None:
    """Return an ISO code if `text` is dominated by a single non-Latin script.

    Catches descriptions written in the target language (e.g. 3ABN Russia's
    Cyrillic blurb) where no English language phrase is present.
    """
    if not text:
        return None
    counts: dict[str, int] = {}
    for ch in text:
        if not ch.isalpha():
            continue
        char_name = unicodedata.name(ch, '')
        for script, code in _SCRIPT_LANGUAGE_HINTS:
            if script in char_name:
                counts[code] = counts.get(code, 0) + 1
                break
    if not counts:
        return None
    code, n = max(counts.items(), key=lambda kv: kv[1])
    return code if n >= _SCRIPT_MIN_CHARS else None


def infer_language(name: str | None, description: str | None = None,
                   default: str = 'en') -> str:
    """ISO 639 language code from a channel's name + description.

    Richer than `infer_language_from_metadata` (which only distinguishes
    Spanish from the default): tries explicit language phrases, then non-Latin
    script detection on the description, then finally the es/en name heuristic.
    """
    folded = f'{fold_language_hint(name)} {fold_language_hint(description)}'
    for marker, code in _LANGUAGE_PHRASE_MARKERS:
        if marker in folded:
            return code
    script_code = _dominant_script_language(description)
    if script_code:
        return script_code
    return infer_language_from_metadata(name, default=default)


_SSL_HANDSHAKE_MARKERS = (
    'handshake failure',
    'handshake_failure',
    'sslv3 alert',
    'ssl alert',
    'tlsv1 alert',
)


def is_ssl_handshake_failure(exc: Exception) -> bool:
    """Return True when the exception chain contains an SSL handshake rejection.

    Unlike DNS failures or connection timeouts, an SSL handshake alert is
    sent by the server and indicates a persistent protocol mismatch — not a
    transient network blip.  The audit uses this to mark channels as dead
    rather than silently skipping them.
    """
    seen = set()
    current = exc
    while current and id(current) not in seen:
        seen.add(id(current))
        if any(marker in str(current).lower() for marker in _SSL_HANDSHAKE_MARKERS):
            return True
        current = current.__cause__ or current.__context__
    return False


def is_transient_network_error(exc: Exception) -> bool:
    network_types = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        socket.gaierror,
        ConnectionError,
        TimeoutError,
        OSError,
    )
    network_markers = (
        'name resolution',
        'failed to resolve',
        'temporary failure in name resolution',
        'network is unreachable',
        'err_name_not_resolved',
        'max retries exceeded',
        'failed to establish a new connection',
    )

    seen = set()
    current = exc
    while current and id(current) not in seen:
        seen.add(id(current))
        text = str(current).lower()
        if isinstance(current, network_types) and any(marker in text for marker in network_markers):
            return True
        if any(marker in text for marker in network_markers):
            return True
        current = current.__cause__ or current.__context__
    return False


def merge_config_updates(existing: dict | None, updates: dict | None) -> dict:
    """Recursively merge scraper config updates into existing config JSON."""
    merged = copy.deepcopy(existing or {})
    for key, value in (updates or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_config_updates(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def format_http_reason(prefix: str, status_code: int, detail: str | None = None) -> str:
    reason = f"{prefix} (HTTP {status_code})"
    if detail:
        return f"{reason}: {detail}"
    return reason


class StreamDeadError(Exception):
    """Raised by audit_resolve() when a channel is confirmed dead (not a transient error)."""


class ScrapeSkipError(Exception):
    """Raised when a scraper should skip the current run without treating it as a hard failure."""


class ConfigField:
    """Declares a single config field a scraper needs from the UI."""
    def __init__(self, key: str, label: str, field_type: str = 'text',
                 required: bool = False, secret: bool = False,
                 placeholder: str = '', help_text: str = '', default=None,
                 options: list | None = None, multiple: bool = False):
        self.key         = key          # key in source.config JSON
        self.label       = label        # human label in UI
        self.field_type  = field_type   # 'text' | 'password' | 'select' | 'toggle' | 'number'
        self.required    = required
        self.secret      = secret       # never echo back in API responses
        self.placeholder = placeholder
        self.help_text   = help_text
        self.default     = default
        self.options     = options or []  # [{'value': ..., 'label': ...}] for select fields
        self.multiple    = multiple

    def to_dict(self):
        d = {
            'key':         self.key,
            'label':       self.label,
            'field_type':  self.field_type,
            'required':    self.required,
            'secret':      self.secret,
            'placeholder': self.placeholder,
            'help_text':   self.help_text,
            'default':     self.default,
        }
        if self.options:
            d['options'] = self.options
        if self.multiple:
            d['multiple'] = self.multiple
        return d


class ChannelData:
    def __init__(self, source_channel_id, name, stream_url, logo_url=None,
                 slug=None, category=None, language='en', country='US',
                 stream_type='hls', number=None, gracenote_id=None,
                 guide_key=None, tags=None, description=None,
                 gracenote_mode=None):
        self.source_channel_id = source_channel_id
        self.name        = name
        self.stream_url  = stream_url
        self.logo_url    = logo_url
        self.slug        = slug or name.lower().replace(' ', '-')
        self.category    = category
        self.language    = language
        self.country     = country
        self.stream_type = stream_type
        self.number      = number
        self.gracenote_id = gracenote_id
        # Initial gracenote routing mode for NEW channels only ('auto'|'manual'|
        # 'off'); None defaults to 'auto'. Existing channels keep their stored
        # mode so a re-scrape never overrides a user's choice. 'off' stores the
        # id but keeps the channel in the standard (non-Gracenote) M3U.
        self.gracenote_mode = gracenote_mode
        self.guide_key   = guide_key
        self.tags        = tags or []  # list of raw tag/group strings from source
        self.description = description  # optional long-form channel description


class ProgramData:
    def __init__(self, source_channel_id, title, start_time, end_time,
                 description=None, poster_url=None, category=None, rating=None,
                 episode_title=None, season=None, episode=None,
                 original_air_date=None, is_live=None, program_type=None,
                 series_id=None, episode_id=None):
        self.source_channel_id = source_channel_id
        self.title        = title
        self.start_time   = start_time
        self.end_time     = end_time
        self.description  = description
        self.poster_url   = poster_url
        self.category     = category
        self.rating       = rating
        self.episode_title = episode_title
        self.season       = season
        self.episode      = episode
        self.original_air_date = original_air_date
        self.is_live      = is_live
        self.program_type = program_type  # "movie", "episode", or None
        self.series_id    = series_id    # stable source-level series identifier
        self.episode_id   = episode_id   # stable source-level episode/content identifier


class BaseScraper(ABC):
    source_name:     str = None
    source_aliases:  tuple[str, ...] = ()
    display_name:    str = None
    scrape_interval: int = 360
    min_scrape_interval: int = 30
    max_scrape_interval: int = 10080
    stream_audit_enabled: bool = False  # opt-in; enable Stream Audit (health + DRM scan) for this source
    # 'full' = descriptions + posters + episode metadata; 'basic' = titles + times only.
    # Used by the resolve-duplicates priority key to prefer richer EPG when breaking ties.
    epg_quality: str = 'full'
    audit_requires_config: list[str] = []  # config keys that must be non-empty for the audit to run
    kodi_props: dict[str, str] = {}  # extra #KODIPROP lines emitted per-channel in M3U output
    license_url: str = None  # DRM license server URL; enables /play/<source>/license proxy endpoint
    config_required: bool = False      # True if source won't return useful channels without user configuration
    is_premium: bool = False           # True for paid/subscription services — shown as a badge in the admin UI
    source_category: str = 'fast'     # 'fast' | 'premium' | 'specialty' | 'drm'
    channel_refresh_hours: int = 0   # 0 = refresh channels every run; >0 = only refresh channels after N hours
    channel_miss_threshold: int = 3  # missed scrapes before is_active=False; override per scraper
    rehome_by_guide_key: bool = False  # when True, re-use existing DB rows whose guide_key matches an incoming channel whose uuid changed

    # Per-phase wall-clock limits (seconds). Overriding in a subclass replaces
    # the entire dict — set all keys you need, not just the ones you're changing.
    # epg=900 covers per-channel-per-day scrapers (Plex, TCL) even under VPN latency.
    # Set SCRAPE_EPG_TIMEOUT env var to override the epg ceiling at runtime.
    phase_timeouts: dict = {
        'init':      30,
        'bootstrap': 60,
        'channels':  120,
        'epg':       900,
    }

    # Declare config fields your scraper needs.
    # The admin UI auto-renders these — no template changes needed for new scrapers.
    # Example:
    #   config_schema = [
    #       ConfigField('username', 'Username', placeholder='email@example.com'),
    #       ConfigField('password', 'Password', field_type='password', secret=True),
    #   ]
    config_schema: list[ConfigField] = []

    def __init__(self, config: dict = None):
        # Scrapers mutate config at runtime to queue persisted tokens/caches.
        # Work on a deep copy so SQLAlchemy-backed JSON objects are not mutated
        # in-place before the caller explicitly saves pending updates.
        self.config  = copy.deepcopy(config or {})
        self._pending_config_updates: dict = {}
        # Large regenerable caches live in the source_cache table, NOT in config,
        # so Source-entity loads (incl. report joins) never deserialize them.
        # Lazily loaded on first `self.cache` access (see the property below) so the
        # play/resolve hot path doesn't pay a DB join for sources that never use a
        # cache (xumo, distro, pluto, tubi, …) — they just never touch self.cache.
        self._cache: dict | None = None
        self._pending_cache_updates: dict = {}
        self._progress_cb = None   # optional callable(phase, done, total) set by worker
        self.session = requests.Session()
        self._configure_session(self.session)
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; FastChannels/1.0)'
        })

    def _retry_config(self) -> Retry:
        return Retry(
            total=3,
            connect=3,
            read=2,
            status=2,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=None,
            raise_on_status=False,
        )

    def _configure_session(self, session: requests.Session) -> None:
        adapter = HTTPAdapter(max_retries=self._retry_config())
        session.mount('https://', adapter)
        session.mount('http://', adapter)

    def new_session(self, *, headers: dict | None = None, cookies: dict | None = None) -> requests.Session:
        session = requests.Session()
        self._configure_session(session)
        session.headers.update(headers or dict(self.session.headers))
        session.cookies.update(cookies or self.session.cookies.get_dict())
        return session

    def _update_config(self, key: str, value) -> None:
        """Queue a config key/value to be persisted by the worker after this run.
        Also updates self.config so the value is usable within the current run."""
        self.config[key] = value
        self._pending_config_updates[key] = value

    @property
    def cache(self) -> dict:
        """Lazily-loaded {cache_key: value} from the source_cache table for this
        scraper's source. The DB query is deferred to first access so scrapers that
        never use a cache pay nothing on the per-request play/resolve path."""
        if self._cache is None:
            self._cache = self._load_source_cache()
        return self._cache

    # Cache keys that are large and only needed on the scrape/EPG path — never on the
    # play/resolve hot path. Excluded from the eager `self.cache` load so a tune doesn't
    # deserialize them; fetch on demand via load_lazy_cache_key() where they ARE needed.
    LAZY_CACHE_KEYS: frozenset = frozenset()

    def _load_source_cache(self) -> dict:
        """Read this source's source_cache rows into a dict.

        Caches live outside Source.config so Source-entity loads never pay to
        deserialize them. Guarded: with no DB/app context (e.g. ad-hoc
        instantiation) the cache simply stays empty and the scraper re-fetches.
        Large EPG-only caches (LAZY_CACHE_KEYS) are skipped here and loaded on demand.
        """
        try:
            from app.config_store import load_source_cache_by_name
            return load_source_cache_by_name(
                self.source_name, exclude=self.LAZY_CACHE_KEYS or None) or {}
        except Exception:
            logger.debug("[%s] could not load source cache; starting empty",
                         getattr(self, 'source_name', '?'), exc_info=True)
            return {}

    def load_lazy_cache_key(self, key: str):
        """Fetch one cache key on demand (for LAZY_CACHE_KEYS excluded from the eager
        load) and merge it into self.cache. Call before reading a large EPG-only cache."""
        if key in self.cache:
            return self.cache[key]
        try:
            from app.config_store import load_source_cache_by_name
            value = load_source_cache_by_name(self.source_name, keys=[key]).get(key)
        except Exception:
            logger.debug("[%s] could not load cache key %s", self.source_name, key, exc_info=True)
            value = None
        self.cache[key] = value
        return value

    def _update_cache(self, key: str, value) -> None:
        """Queue a cache key/value to be persisted (to the source_cache table)
        by the worker/play path after this run. Also updates self.cache so the
        value is usable within the current run. Mirrors _update_config but for
        large regenerable caches that must stay out of the config blob."""
        self.cache[key] = value
        self._pending_cache_updates[key] = value

    def pre_run_setup(self) -> None:
        """Called by the worker before fetch_channels/fetch_epg.
        Override to perform auth or any setup that queues config updates
        (e.g. capturing tokens) so they can be persisted before the long scrape."""
        pass

    @abstractmethod
    def fetch_channels(self) -> list[ChannelData]: ...

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        return []

    def resolve(self, raw_url: str) -> str:
        """Override to resolve raw stored URLs to playable URLs at request time."""
        return raw_url

    @classmethod
    def license_request_headers(cls, config: dict) -> dict:
        """Headers to attach when proxying a DRM license request to the license server.
        Override in scrapers that require auth on their license server."""
        return {}

    @classmethod
    def get_license_url(cls, config: dict, channel_id: str | None = None) -> str | None:
        """Returns the full license server URL (with any config-dependent query params).
        Override in scrapers that need dynamic or per-channel license URLs."""
        return cls.license_url

    @classmethod
    def process_license_response(cls, response_bytes: bytes) -> bytes:
        """Transform the raw license server response before returning it to the client.
        Override in scrapers whose license server returns a non-standard format (e.g. JSON)."""
        return response_bytes

    @classmethod
    def get_kodi_props(cls, base_url: str) -> dict[str, str]:
        """Returns #KODIPROP key/value pairs for M3U output.
        If the scraper has a license_url, injects the proxy license_key automatically."""
        props = dict(cls.kodi_props)
        if cls.license_url and 'inputstream.adaptive.license_key' not in props:
            props['inputstream.adaptive.license_key'] = (
                f'{base_url}/play/{cls.source_name}/license||R{{SSM}}|'
            )
        return props

    @classmethod
    def get_kodi_props_for_channel(cls, base_url: str, source_channel_id: str) -> dict[str, str] | None:
        """Per-channel Kodi props override. Return None to use class-level get_kodi_props."""
        return None

    @classmethod
    def prepare_license_request(
        cls, challenge: bytes, config: dict, channel_id: str | None = None, **kwargs
    ) -> tuple[bytes, dict]:
        """Returns (body, headers) for a license request.
        Override in scrapers that need auth headers or challenge body transforms."""
        return challenge, cls.license_request_headers(config)

    def run(self) -> tuple[list[ChannelData], list[ProgramData]]:
        channels = self.fetch_channels()
        programs = self.fetch_epg(channels)
        return channels, programs

    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        try:
            r = self.session.get(url, timeout=30, **kwargs)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if is_transient_network_error(e):
                logger.warning(f'[{self.source_name}] transient GET failure for {url}: {e}')
            else:
                logger.error(f'[{self.source_name}] GET {url} failed: {e}')
            return None
