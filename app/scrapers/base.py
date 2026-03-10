import logging
from abc import ABC, abstractmethod
from typing import Optional

import requests

logger = logging.getLogger(__name__)


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
        self.config  = config or {}
        self._pending_config_updates: dict = {}
        self._progress_cb = None   # optional callable(phase, done, total) set by worker
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; FastChannels/1.0)'
        })

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
            logger.error(f'[{self.source_name}] GET {url} failed: {e}')
            return None
