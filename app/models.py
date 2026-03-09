from datetime import datetime, timezone
from .extensions import db


class Source(db.Model):
    __tablename__ = 'sources'

    id              = db.Column(db.Integer, primary_key=True)
    name            = db.Column(db.String(64), unique=True, nullable=False)
    display_name    = db.Column(db.String(128), nullable=False)
    scrape_interval = db.Column(db.Integer, default=360)
    is_enabled      = db.Column(db.Boolean, default=True)
    last_scraped_at = db.Column(db.DateTime(timezone=True))
    last_error      = db.Column(db.Text)
    config          = db.Column(db.JSON, default=dict)
    chnum_start     = db.Column(db.Integer, nullable=True)   # starting tvg-chno in combined /m3u output

    channels = db.relationship('Channel', backref='source', lazy='dynamic',
                                cascade='all, delete-orphan')

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
            'last_error':     self.last_error,
            'channel_count':  self.channels.filter_by(is_active=True).count(),
            'chnum_start':    self.chnum_start,
        }


class Channel(db.Model):
    __tablename__ = 'channels'

    id                = db.Column(db.Integer, primary_key=True)
    source_id         = db.Column(db.Integer, db.ForeignKey('sources.id'), nullable=False)
    source_channel_id = db.Column(db.String(256))
    name              = db.Column(db.String(256), nullable=False)
    slug              = db.Column(db.String(256))
    logo_url          = db.Column(db.Text)
    stream_url        = db.Column(db.Text)
    stream_type       = db.Column(db.String(16), default='hls')
    category          = db.Column(db.String(128))
    language          = db.Column(db.String(16), default='en')
    country           = db.Column(db.String(8), default='US')
    number            = db.Column(db.Integer)
    gracenote_id      = db.Column(db.String(32), nullable=True)   # e.g. EP012345678; set by scraper or user
    disable_reason    = db.Column(db.String(64), nullable=True)  # e.g. 'DRM'; set by play proxy
    is_active         = db.Column(db.Boolean, default=True)   # set by scraper — channel exists upstream
    is_enabled        = db.Column(db.Boolean, default=True)   # set by user — include in M3U/EPG
    created_at        = db.Column(db.DateTime(timezone=True),
                                  default=lambda: datetime.now(timezone.utc))
    updated_at        = db.Column(db.DateTime(timezone=True),
                                  default=lambda: datetime.now(timezone.utc),
                                  onupdate=lambda: datetime.now(timezone.utc))

    programs = db.relationship('Program', backref='channel', lazy='dynamic',
                                cascade='all, delete-orphan')

    __table_args__ = (
        db.UniqueConstraint('source_id', 'source_channel_id', name='uq_source_channel'),
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
            'stream_url':       self.stream_url,
            'stream_type':      self.stream_type,
            'category':         self.category,
            'language':         self.language,
            'country':          self.country,
            'number':           self.number,
            'gracenote_id':     self.gracenote_id,
            'is_active':        self.is_active,
            'disable_reason':   self.disable_reason,
            'is_enabled':       self.is_enabled,
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
