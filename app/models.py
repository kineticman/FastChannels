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
            'is_active':        self.is_active,
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
