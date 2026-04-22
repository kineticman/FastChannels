import hashlib
import os
from datetime import datetime, timedelta, timezone
from .extensions import db

_LOGO_CACHE_ROOT = '/data/logo_cache/logos'


def _logo_display_url(raw_url: str | None) -> str | None:
    """Return a server-relative proxy URL for a logo so browsers never hit upstream CDNs."""
    if not raw_url:
        return None
    if raw_url.startswith('/'):
        return raw_url  # already local
    key = hashlib.md5(raw_url.encode()).hexdigest()
    ext = next((e for e in ('jpg', 'jpeg', 'png', 'gif') if f'.{e}' in raw_url.lower()), 'jpg')
    if os.path.exists(os.path.join(_LOGO_CACHE_ROOT, key)):
        return f'/logos/{key}.{ext}'
    return f'/images/proxy/logo/{key}.{ext}'


class Source(db.Model):
    __tablename__ = 'sources'

    id              = db.Column(db.Integer, primary_key=True)
    name            = db.Column(db.String(64), unique=True, nullable=False)
    display_name    = db.Column(db.String(128), nullable=False)
    scrape_interval = db.Column(db.Integer, default=360)
    is_enabled      = db.Column(db.Boolean, default=True)
    last_scraped_at = db.Column(db.DateTime(timezone=True))
    last_audited_at = db.Column(db.DateTime(timezone=True))
    last_error      = db.Column(db.Text)
    config          = db.Column(db.JSON, default=dict)
    chnum_start     = db.Column(db.Integer, nullable=True)   # starting tvg-chno in combined /m3u output
    epg_only        = db.Column(db.Boolean, default=False)   # if True: excluded from M3U output

    channels = db.relationship('Channel', backref='source', lazy='dynamic',
                                cascade='all, delete-orphan')

    def next_scrape_at(self):
        """Return the datetime when this source is next due to be scraped, or None if never scraped."""
        if self.last_scraped_at is None:
            return None
        last = self.last_scraped_at
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        return last + timedelta(minutes=self.scrape_interval or 360)

    def __repr__(self):
        return f'<Source {self.name}>'

    def to_dict(self):
        return {
            'id':             self.id,
            'name':           self.name,
            'display_name':   self.display_name,
            'scrape_interval': self.scrape_interval,
            'is_enabled':     self.is_enabled,
            'last_scraped_at': self.last_scraped_at.isoformat() if self.last_scraped_at else None,
            'last_audited_at': self.last_audited_at.isoformat() if self.last_audited_at else None,
            'last_error':     self.last_error,
            'channel_count':  self.channels.filter_by(is_active=True).count(),
            'chnum_start':    self.chnum_start,
            'epg_only':       self.epg_only,
        }


class Channel(db.Model):
    __tablename__ = 'channels'

    id                = db.Column(db.Integer, primary_key=True)
    source_id         = db.Column(db.Integer, db.ForeignKey('sources.id'), nullable=False)
    source_channel_id = db.Column(db.String(256))
    name              = db.Column(db.String(256), nullable=False)
    slug              = db.Column(db.String(256))
    logo_url          = db.Column(db.Text)
    logo_url_pinned   = db.Column(db.Boolean, default=False, nullable=False)  # True when user has manually pinned a logo URL
    stream_url        = db.Column(db.Text)
    stream_type       = db.Column(db.String(16), default='hls')
    category          = db.Column(db.String(128))
    category_override = db.Column(db.String(128), nullable=True)  # set by user; beats all auto logic
    language          = db.Column(db.String(16), default='en')
    language_override = db.Column(db.String(16), nullable=True)   # set by user; beats scraper value
    country           = db.Column(db.String(8), default='US')
    tags              = db.Column(db.Text, nullable=True)          # comma-separated raw tags/groups from source
    number            = db.Column(db.Integer)
    number_pinned     = db.Column(db.Boolean, default=False, nullable=False)  # True when user has manually set/locked this channel number
    gracenote_id      = db.Column(db.String(32), nullable=True)   # e.g. EP012345678; set by scraper or user
    gracenote_locked  = db.Column(db.Boolean, default=False, nullable=False)  # True when user manually sets/locks Gracenote ID
    gracenote_mode    = db.Column(db.String(16), default='auto', nullable=False)  # auto | manual | off
    guide_key         = db.Column(db.String(256), nullable=True)  # provider-specific guide lookup key (e.g. Plex gridKey)
    description       = db.Column(db.Text, nullable=True)         # optional channel description from scraper
    disable_reason    = db.Column(db.String(64), nullable=True)  # e.g. 'DRM'; set by play proxy
    stream_info       = db.Column(db.JSON, nullable=True)        # populated by audit/inspect: max_resolution, video_codec, has_4k, variants
    is_duplicate      = db.Column(db.Boolean, default=False)  # set by user — manual duplicate label (does not disable)
    is_active         = db.Column(db.Boolean, default=True)   # set by scraper — channel exists upstream
    is_enabled        = db.Column(db.Boolean, default=True)   # set by user — include in M3U/EPG
    last_seen_at      = db.Column(db.DateTime(timezone=True), nullable=True)
    missed_scrapes    = db.Column(db.Integer, default=0, nullable=False)
    created_at        = db.Column(db.DateTime(timezone=True),
                                  default=lambda: datetime.now(timezone.utc))
    updated_at        = db.Column(db.DateTime(timezone=True),
                                  default=lambda: datetime.now(timezone.utc),
                                  onupdate=lambda: datetime.now(timezone.utc))

    programs = db.relationship('Program', backref='channel', lazy='dynamic',
                                cascade='all, delete-orphan')

    __table_args__ = (
        db.UniqueConstraint('source_id', 'source_channel_id', name='uq_source_channel'),
        db.Index('idx_channels_source_id', 'source_id'),
        db.Index('idx_channels_active', 'is_active', 'is_enabled'),
    )

    def __repr__(self):
        return f'<Channel {self.name}>'

    def to_dict(self):
        return {
            'id':               self.id,
            'source_id':        self.source_id,
            'source_name':      self.source.name if self.source else None,
            'name':             self.name,
            'slug':             self.slug,
            'logo_url':         self.logo_url,
            'logo_display_url': _logo_display_url(self.logo_url),
            'logo_url_pinned':  bool(self.logo_url_pinned),
            'stream_url':       self.stream_url,
            'stream_type':      self.stream_type,
            'category':          self.category,
            'category_override': self.category_override,
            'language':          self.language,
            'language_override': self.language_override,
            'country':          self.country,
            'number':           self.number,
            'number_pinned':    bool(self.number_pinned),
            'gracenote_id':     self.gracenote_id,
            'gracenote_locked': self.gracenote_locked,
            'gracenote_mode':   self.gracenote_mode or 'auto',
            'guide_key':        self.guide_key,
            'description':      self.description,
            'is_active':        self.is_active,
            'disable_reason':   self.disable_reason,
            'is_duplicate':     self.is_duplicate,
            'is_enabled':       self.is_enabled,
            'last_seen_at':     self.last_seen_at.isoformat() if self.last_seen_at else None,
            'missed_scrapes':   self.missed_scrapes or 0,
        }


class Program(db.Model):
    __tablename__ = 'programs'

    id            = db.Column(db.Integer, primary_key=True)
    channel_id    = db.Column(db.Integer, db.ForeignKey('channels.id'), nullable=False)
    title         = db.Column(db.String(512), nullable=False)
    description   = db.Column(db.Text)
    start_time    = db.Column(db.DateTime(timezone=True), nullable=False)
    end_time      = db.Column(db.DateTime(timezone=True), nullable=False)
    poster_url    = db.Column(db.Text)
    category      = db.Column(db.String(128))
    rating        = db.Column(db.String(16))
    episode_title = db.Column(db.String(256))
    season        = db.Column(db.Integer)
    episode       = db.Column(db.Integer)
    original_air_date = db.Column(db.Date, nullable=True)

    __table_args__ = (
        db.Index('idx_programs_channel_id', 'channel_id'),
        db.Index('idx_programs_end_time', 'end_time'),
        db.Index('idx_programs_start_time', 'start_time'),
    )

    def __repr__(self):
        return f'<Program {self.title} @ {self.start_time}>'


class Feed(db.Model):
    """
    A named, filtered sub-feed that exposes its own /m3u and /epg.xml URLs.
    Filters are stored as a JSON dict and passed directly to generate_m3u()
    / generate_xmltv() at request time — no denormalisation needed.

    Filter keys (all optional):
      sources      list[str]  — Source.name values to include
      categories   list[str]  — channel category strings
      languages    list[str]  — ISO 639-1 codes
      max_channels int        — cap on channels returned
    """
    __tablename__ = 'feeds'

    id          = db.Column(db.Integer, primary_key=True)
    slug        = db.Column(db.String(64), unique=True, nullable=False)   # URL-safe, permanent
    name        = db.Column(db.String(128), nullable=False)
    description = db.Column(db.Text, default='')
    filters     = db.Column(db.JSON, default=dict)
    chnum_start = db.Column(db.Integer, nullable=True)   # starting tvg-chno for this feed's M3U output
    is_enabled  = db.Column(db.Boolean, default=True)
    created_at  = db.Column(db.DateTime(timezone=True),
                            default=lambda: datetime.now(timezone.utc))
    updated_at  = db.Column(db.DateTime(timezone=True),
                            default=lambda: datetime.now(timezone.utc),
                            onupdate=lambda: datetime.now(timezone.utc))

    def channel_count(self) -> int:
        from .generators.m3u import _build_channel_query, feed_to_query_filters
        return _build_channel_query(feed_to_query_filters(self.filters or {})).count()

    def __repr__(self):
        return f'<Feed {self.slug}>'

    def to_dict(self, base_url: str = '') -> dict:
        base_url = (base_url or '').rstrip('/')
        return {
            'id':          self.id,
            'slug':        self.slug,
            'name':        self.name,
            'description': self.description,
            'filters':     self.filters or {},
            'chnum_start': self.chnum_start,
            'is_enabled':  self.is_enabled,
            'created_at':  self.created_at.isoformat() if self.created_at else None,
            'updated_at':  self.updated_at.isoformat() if self.updated_at else None,
            # Convenience URLs for the client / admin UI
            'm3u_url':     f'{base_url}/feeds/{self.slug}/m3u',
            'epg_url':     f'{base_url}/feeds/{self.slug}/epg.xml',
            'gracenote_url': f'{base_url}/feeds/{self.slug}/m3u/gracenote',
        }


class FeedChannelNumber(db.Model):
    """Persistent feed-specific channel number assignments (sticky tvg-chno for feeds)."""
    __tablename__ = 'feed_channel_numbers'

    feed_id    = db.Column(db.Integer, db.ForeignKey('feeds.id',    ondelete='CASCADE'), primary_key=True)
    channel_id = db.Column(db.Integer, db.ForeignKey('channels.id', ondelete='CASCADE'), primary_key=True)
    number     = db.Column(db.Integer, nullable=False)

    __table_args__ = (db.Index('ix_feed_channel_numbers_feed_id', 'feed_id'),)


class AppSettings(db.Model):
    """Single-row global settings table (always id=1)."""
    __tablename__ = 'app_settings'

    id                   = db.Column(db.Integer, primary_key=True)
    global_chnum_start   = db.Column(db.Integer, nullable=True)  # master tvg-chno start for ungrouped sources
    channels_dvr_url     = db.Column(db.Text, nullable=True)     # e.g. http://192.168.1.x:8089
    public_base_url      = db.Column(db.Text, nullable=True)     # e.g. http://192.168.1.x:5523
    timezone_name        = db.Column(db.String(64), nullable=True)  # IANA timezone, e.g. America/New_York
    gracenote_auto_fill  = db.Column(db.Boolean, nullable=False, default=True)  # scrapers auto-assign Gracenote IDs
    gracenote_map_url          = db.Column(db.Text, nullable=True)      # remote community CSV URL (defaults to built-in Gist)
    gracenote_contribution_url = db.Column(db.Text, nullable=True)      # webhook URL for submitting community contributions
    last_contribution_at       = db.Column(db.DateTime, nullable=True)  # server-side rate-limit: last successful submission

    @staticmethod
    def _env_int(name: str) -> int | None:
        raw = (os.environ.get(name) or '').strip()
        if not raw:
            return None
        try:
            value = int(raw)
        except ValueError:
            return None
        return value if value > 0 else None

    @staticmethod
    def _env_str(name: str) -> str | None:
        raw = (os.environ.get(name) or '').strip().rstrip('/')
        return raw or None

    @classmethod
    def env_global_chnum_start(cls) -> int | None:
        return cls._env_int('MASTER_CHANNEL_NUMBER_START')

    @classmethod
    def env_public_base_url(cls) -> str | None:
        return cls._env_str('FASTCHANNELS_SERVER_URL')

    @classmethod
    def env_channels_dvr_url(cls) -> str | None:
        return cls._env_str('CHANNELS_DVR_SERVER_URL')

    def effective_global_chnum_start(self) -> int | None:
        # Primary source is now the default Feed's chnum_start column.
        # AppSettings.global_chnum_start is legacy; schema migration copies it
        # to the feed row on first boot after upgrade.
        default_feed = Feed.query.filter_by(slug='default').first()
        if default_feed and default_feed.chnum_start is not None:
            return default_feed.chnum_start
        return self.env_global_chnum_start()

    def effective_public_base_url(self) -> str | None:
        value = (self.public_base_url or '').strip().rstrip('/')
        return value or self.env_public_base_url()

    def effective_channels_dvr_url(self) -> str | None:
        value = (self.channels_dvr_url or '').strip().rstrip('/')
        return value or self.env_channels_dvr_url()

    _DEFAULT_GRACENOTE_MAP_URL = (
        'https://gist.githubusercontent.com/kineticman/'
        '87765d469610233f894c9c225cb4f2ca/raw/gistfile1.txt'
    )
    _DEFAULT_CONTRIBUTION_URL = (
        'https://hook.us2.make.com/op063u88o0mx9noggvv9wgx4gass96iv'
    )

    def effective_gracenote_map_url(self) -> str:
        return (self.gracenote_map_url or '').strip() or self._DEFAULT_GRACENOTE_MAP_URL

    def effective_gracenote_contribution_url(self) -> str:
        return (self.gracenote_contribution_url or '').strip() or self._DEFAULT_CONTRIBUTION_URL

    def effective_timezone_name(self) -> str:
        from .timezone_utils import current_timezone_name
        return current_timezone_name(self.timezone_name)

    @classmethod
    def get(cls):
        """Return the single settings row, creating it if absent."""
        row = cls.query.get(1)
        if row is None:
            row = cls(id=1)
            db.session.add(row)
            db.session.commit()
        return row


class TvtvProgramCache(db.Model):
    """
    Rolling 3-day cache of tvtv.us guide data for all indexed FAST stations.
    Refreshed nightly via the background worker.  Used by the Gracenote
    Suggestions helper and available for future EPG enrichment.
    """
    __tablename__ = 'tvtv_program_cache'

    id          = db.Column(db.Integer, primary_key=True)
    station_id  = db.Column(db.String(32),  nullable=False)
    lineup      = db.Column(db.String(64),  nullable=False)
    program_id  = db.Column(db.String(32),  nullable=True)
    title       = db.Column(db.String(512), nullable=False)
    subtitle    = db.Column(db.String(512), nullable=True)
    start_time  = db.Column(db.DateTime(timezone=True), nullable=False)
    end_time    = db.Column(db.DateTime(timezone=True), nullable=False)
    fetched_at  = db.Column(db.DateTime(timezone=True), nullable=False,
                            default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint('station_id', 'start_time', name='uq_tvtv_station_start'),
        db.Index('idx_tvtv_station_start', 'station_id', 'start_time'),
        db.Index('idx_tvtv_end_time',      'end_time'),
    )
