import copy
import logging
import socket
from abc import ABC, abstractmethod
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


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


class StreamDeadError(Exception):
    """Raised by audit_resolve() when a channel is confirmed dead (not a transient error)."""


class ScrapeSkipError(Exception):
    """Raised when a scraper should skip the current run without treating it as a hard failure."""


class ConfigField:
    """Declares a single config field a scraper needs from the UI."""
    def __init__(self, key: str, label: str, field_type: str = 'text',
                 required: bool = False, secret: bool = False,
                 placeholder: str = '', help_text: str = '', default=None):
        self.key         = key          # key in source.config JSON
        self.label       = label        # human label in UI
        self.field_type  = field_type   # 'text' | 'password' | 'select' | 'toggle' | 'number'
        self.required    = required
        self.secret      = secret       # never echo back in API responses
        self.placeholder = placeholder
        self.help_text   = help_text
        self.default     = default

    def to_dict(self):
        return {
            'key':         self.key,
            'label':       self.label,
            'field_type':  self.field_type,
            'required':    self.required,
            'secret':      self.secret,
            'placeholder': self.placeholder,
            'help_text':   self.help_text,
            'default':     self.default,
        }


class ChannelData:
    def __init__(self, source_channel_id, name, stream_url, logo_url=None,
                 slug=None, category=None, language='en', country='US',
                 stream_type='hls', number=None, gracenote_id=None):
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


class ProgramData:
    def __init__(self, source_channel_id, title, start_time, end_time,
                 description=None, poster_url=None, category=None, rating=None,
                 episode_title=None, season=None, episode=None):
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


class BaseScraper(ABC):
    source_name:     str = None
    source_aliases:  tuple[str, ...] = ()
    display_name:    str = None
    scrape_interval: int = 360
    stream_audit_enabled: bool = False  # opt-in; enable Stream Audit (health + DRM scan) for this source
    channel_refresh_hours: int = 0   # 0 = refresh channels every run; >0 = only refresh channels after N hours

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

    def pre_run_setup(self) -> None:
        """Called by the worker before fetch_channels/fetch_epg.
        Override to perform auth or any setup that queues config updates
        (e.g. capturing tokens) so they can be persisted before the long scrape."""
        pass

    @abstractmethod
    def fetch_channels(self) -> list[ChannelData]: ...

    def fetch_epg(self, channels: list[ChannelData]) -> list[ProgramData]:
        return []

    def resolve(self, raw_url: str) -> str:
        """Override to resolve raw stored URLs to playable URLs at request time."""
        return raw_url

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
