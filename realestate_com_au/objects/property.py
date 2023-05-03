from dataclasses import dataclass, field, astuple, fields
from typing import List, Optional
from sqlalchemy import Column, Text, Integer, Float, DATE, VARCHAR, ForeignKey, Boolean, Index, Table
from sqlalchemy.dialects.mysql import JSON, TINYINT, MEDIUMINT, TINYTEXT, DOUBLE
from sqlalchemy.orm import registry, relationship, composite
from sqlalchemy.ext.hybrid import hybrid_property
from datetime import datetime, timedelta, date as dt_date
import re
from more_itertools import windowed

from realestate_com_au.objects.mixins import DFMixin
from realestate_com_au.utils import strp_date_or_none, float_or_none, positive_float_or_none, positive_int_or_none

mapper_registry = registry()

    # def to_pd_dict(self):
    #     result = {}
    #     for name, field in self.__dataclass_fields__.items():
    #         value = self.__dict__[name]
    #         if value and 'datetime.date' in str(field.type):
    #             value = datetime(*value.timetuple()[:6])
    #         result[name] = value
    #     return result

    # @classmethod
    # def to_df_args(cls, instances):
    #     columns = cls.__dataclass_fields__.keys()
    #     return {
    #         "data": [ instance.get_row_df(columns) for instance in instances ],
    #         "columns": [field.name for field in columns]
    #     }

@mapper_registry.mapped
@dataclass
class PropertyEvent(DFMixin):
    __table__ = Table(
        "realestate_timeline",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        Column("property_id", ForeignKey("realestate_property.id")),
        Column("event_type", VARCHAR(32)),
        Column("date", DATE),
        Column("value", Integer),
        Column("text", TINYTEXT),
    )

    id: int
    property_id: Optional[int] = None
    event_type: Optional[str] = None
    date: Optional[dt_date] = None
    value: Optional[int] = None
    text: Optional[str] = None

    def from_json(data, property_id=None):
        return PropertyEvent(
            id=None,
            property_id=property_id,
            event_type=data["eventType"],
            date=strp_date_or_none(data["date"], "%Y-%m-%d"),
            value=positive_int_or_none(data["price"]),
        )


@mapper_registry.mapped
@dataclass
class PropertyTrend(DFMixin):
    __table__ = Table(
        "realestate_trend",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        # Column("property_id", ForeignKey("realestate_property.id")),
        Column("fullSuburb", TINYTEXT),
        Column("bedrooms", VARCHAR(32)), # '4+', 'ALL'
        Column("property_type", VARCHAR(32)), # 'house', 'unit'
        Column("trend_type", VARCHAR(32)), # medianRentalPrice, medianSoldPrice,
        Column("interval", VARCHAR(32)), # 'monthly', 'yearly'
        Column("ingest", DATE),
        Column("count", Integer),
        Column("interval_end", DATE),
        Column("value", Integer),
    )

    id: int
    full_suburb: Optional[str] = None
    # property_id: Optional[int] = None
    bedrooms: Optional[str] = None
    property_type: Optional[str] = None
    trend_type: Optional[str] = None
    interval: Optional[str] = None
    ingest: Optional[dt_date] = None
    count: Optional[int] = None
    interval_end: Optional[dt_date] = None
    value: Optional[int] = None

    @classmethod
    def from_json(cls, full_suburb, data):
        bedrooms = data.get("bedrooms")
        property_type = data.get("propertyType")
        rent_ingest = data.get("rentDataIngestDateDisplay")
        if rent_ingest:
            rent_ingest = strp_date_or_none(rent_ingest, "%d %b %Y")
        sold_ingest = data.get("soldDataIngestDateDisplay")
        if sold_ingest:
            sold_ingest = strp_date_or_none(sold_ingest, "%d %b %Y")
        result = []
        for trend_type, trends in data.get('trends', {}).items():
            if 'Rent' in trend_type:
                ingest = rent_ingest
            elif 'Sold' in trend_type:
                ingest = sold_ingest
            for trends in trends:
                if 'bedrooms' in trends:
                    bedrooms = trends.pop('bedrooms')
                for interval, trends in trends.items():
                    if type(trends) != list:
                        continue
                    for trend in trends:
                        result.append(PropertyTrend(
                            id=None,
                            full_suburb=full_suburb,
                            # property_id=None,
                            bedrooms=bedrooms,
                            property_type=property_type,
                            trend_type=trend_type,
                            interval=interval,
                            ingest=ingest,
                            count=trend.get('count'),
                            interval_end=strp_date_or_none(trend.get('intervalEnd'), "%Y-%m-%d"),
                            value=trend.get('value'),
                        ))
        return result


@mapper_registry.mapped
@dataclass
class MediaItem(DFMixin):
    __table__ = Table(
        "realestate_media",
        mapper_registry.metadata,
        Column("path", VARCHAR(256), primary_key=True),
        # not unique relationship
        # Column("property_id", ForeignKey("realestate_property.id")),
        Column("media_type", VARCHAR(32)),
        Column("server", TINYTEXT),
        Column("date", DATE)
    )

    path: str
    property_id: Optional[int] = None
    media_type: Optional[str] = None
    server: Optional[str] = None
    date: Optional[dt_date] = None

    @hybrid_property
    def url(self):
        return self.server + '1144x888-format=webp' + self.path

    def from_templated(url, media_type):
        server, path = url.split('{size}')
        return MediaItem(
            path=path,
            media_type=media_type,
            server=server,
        )

    def from_json(data):
        return MediaItem(
            path=data["uri"],
            media_type=data["name"],
            server=data["server"],
        )

# separates subunits
re_sub_sep = re.compile(r"[/,;]", re.IGNORECASE)
re_and_sep = re.compile(r"[&+]", re.IGNORECASE)
re_tok_sep = re.compile(re_sub_sep.pattern + r'|' + re_and_sep.pattern, re.IGNORECASE)
re_multi_sep = re.compile(r"[&+-]", re.IGNORECASE)

re_tok_delim = re.compile(r"\s+|(?=" + re_tok_sep.pattern + r")|(?<=" + re_tok_sep.pattern + r")")
# A
re_al = re.compile(r"[A-Z]", re.IGNORECASE)
# A | A&B | A-C | A&B&C
re_al_multi = re.compile(
    r"(" + re_al.pattern + r"(" + re_multi_sep.pattern + re_al.pattern + r")*)", re.IGNORECASE)
# 1
re_num = re.compile(r"\d+", re.IGNORECASE)
# A/1
# re_alnum = re.compile(re_al.pattern + r"/" + re_num.pattern, re.IGNORECASE)
# 1 | 1A | A
re_numal = re.compile(r"(" + re_al.pattern + r"?" + re_num.pattern + re_al.pattern + r"?|"+ re_al.pattern + r")", re.IGNORECASE)
# splits 1A => 1, A
re_num_delim = re.compile(r"(?<=\d)(?=" + re_al.pattern + r")")
# 1 | 1&2 | 1-3 | 1&1A | 1A&B | 1A-C | 1&2&4 | A&B&D
re_numal_multi = re.compile(
    r"(" + re_numal.pattern + r"(" + re_multi_sep.pattern + re_numal.pattern + r")*)", re.IGNORECASE)


# 1A | 1A/2 | 1/2A
re_numal_subprem = re.compile(
    r"(" + re_numal.pattern + r"(" + re_sub_sep.pattern + re_numal.pattern + r")?)", re.IGNORECASE)
# 1A | 1A&B/2-16 | 1A&B/2/3-5
re_numal_subprem_multi = re.compile(
    r"((?P<subprem>" + re_numal_multi.pattern + r")" + \
        r"(" + re_sub_sep.pattern + r"(?P<middle>" + re_numal.pattern + r"))?" + \
        r"(" + re_sub_sep.pattern + r"(?P<numal>" + re_numal_multi.pattern + r"))?)", re.IGNORECASE)
re_slash = re.compile(r"/", re.IGNORECASE)
re_to = re.compile(r"-", re.IGNORECASE)
re_and = re.compile(r"(and|" + re_and_sep.pattern + r")", re.IGNORECASE)
re_proposed = re.compile(r"proposed|p", re.IGNORECASE)
re_lot = re.compile(r"l|lot", re.IGNORECASE)
re_subprem = re.compile(r"(" + re_lot.pattern + r"|pl|fl|flat|u|unit|v|villa|sold|ptn|apt)", re.IGNORECASE)
# LOT1 | LOT1A | LOTA
re_subprem_numal = re.compile(r"(?P<subprem>" + re_subprem.pattern + r")(?P<numal>" + re_numal.pattern + r")", re.IGNORECASE)
# LOT1-2
re_subprem_numal_multi = re.compile(r"(?P<subprem>" + re_subprem.pattern + r")(?P<numal>" + re_numal_multi.pattern + r")", re.IGNORECASE)
# (REAR)
re_comment = re.compile(r"\(.*\)", re.IGNORECASE)

@dataclass(eq=True, unsafe_hash=True, order=True)
class FullAddress:
    # short_address: str
    # subpremise: Optional[str]
    street_number: str
    street_name: str
    suburb: str
    state: str
    postcode: str

    def __composite_values__(self):
        # return self.street_number, self.street_name, self.suburb, self.state, self.postcode
        return astuple(self)

    def __iter__(self):
        return iter(astuple(self))

    def __repr__(self):
        # subprem, numal = FullAddress.split_street_number(self.street_number)
        # if subprem:
        #     number = f"{self.subpremise}/{number}"
        return f"{self.street_number} {self.street_name}, {self.suburb}, {self.state} {self.postcode}"

    def street_number_close(a, b):
        _, num_a = FullAddress.split_street_number(a)
        _, num_b = FullAddress.split_street_number(b)
        if num_a and num_b:
            if not num_a.intersection(num_b):
                return False
        elif num_a != num_b:
            return False
        return True

    def close(self, other):
        for f in fields(self):
            self_v, other_v = getattr(self, f.name), getattr(other, f.name)
            if f.name == 'street_number':
                self_subprem, self_num = FullAddress.split_street_number(self_v)
                other_subprem, other_num = FullAddress.split_street_number(other_v)
                # print(f"self_subprem: {self_subprem}, other_subprem: {other_subprem}")
                # if self_subprem != other_subprem:
                #     return False
                # print(f"self_num: {self_num}, other_num: {other_num}")
                # if self_num != other_num:
                #     return False
                if self_num and other_num:
                    if not self_num.intersection(other_num):
                        return False
                elif self_num != other_num:
                    return False
                # if any(map(lambda x: re_))
            elif self_v != other_v:
                return False
        return True

    def split_street_number(street_number):
        subprem = set()
        if (match := re_numal.fullmatch(street_number)):
            tokens = re_num_delim.split(street_number)
            # print(f"re_numal match @ {street_number}: {tokens}")
            if len(tokens) > 1:
                subprem, street_number = set(tokens[1]), tokens[0]
        elif (match := re_numal_subprem_multi.fullmatch(street_number)):
            match = match.groupdict()
            # print(f"re_numal_subprem_multi match @ {street_number}: {match}")
            street_number = match.get('subprem')
            if match.get('middle'):
                subprem = street_number
                street_number = match.get('middle')
                if match.get('numal'):
                    subprem = f"{subprem}/{street_number}"
                    street_number = match.get('numal')
            elif match.get('numal'):
                subprem = street_number
                street_number = match.get('numal')
            if not street_number:
                subprem, street_number = set(), subprem
        if type(subprem) == str:
            if re_numal_multi.fullmatch(subprem):
                tokens = re_multi_sep.split(subprem)
                # print(f"re_numal_multi match @ {subprem} / {street_number}: {tokens}")
                subprem = set(tokens)
        if type(street_number) == str:
            if re_numal_multi.fullmatch(street_number):
                tokens = re_multi_sep.split(street_number)
                # print(f"re_numal_multi match @ {street_number}: {tokens}")
                street_number = set(tokens)
            else:
                street_number = set(street_number)
        return subprem, street_number




    def split_short_address(short_address):
        """
        edge cases:
            - lot 1-3 5
            - 1,55 Mars Street
            - lot1-4 175
            - SOLD1/77 Surrey Rd
            - proposed lot 3, 5-8
            - 43&43A
            - 1/67 {LEY,BOW,THE,HAY}
            - 15 York Street + 28 Angelo Street
            - A and B/149 Manning Road
            - G01/2
            - L 1 250 Spencer Street
            - 38-42/2
            - 202&203&204 Melville Parade
            - PTN 1 & PTN 2 / 7 Lisa Place
            - Lot 1, 2 & 3, 11 Argyle Stret
            - unit 10A/62
            - 5A& 5B
            - 5 11 15 Canterbury Terrace
            - 12A,B &C Farnham Street
            - 33/189 Swansea Street (E)
            - C3/62 Great Eastern Highway
            - 201 (rear) Bishopgate Street
            - Apt 418, 1 Kyle Way
        """
        number_tokens = []
        tokens = [*filter(None, (re_tok_delim.split(short_address)))]

        # match
        multi = False
        i = 0
        # first pass: group together tokens
        while i < len(tokens)-1:
            # print(short_address, f"| first {i}:", tokens)
            if False: # this just makes it easier to reorder rules
                pass
            # p/l => lot
            elif i < len(tokens)-2 and re_proposed.fullmatch(tokens[i]) \
                    and re_slash.fullmatch(tokens[i+1]) \
                    and re_lot.fullmatch(tokens[i+2]):
                tokens.pop(i)  # kill "proposed"
                tokens.pop(i)  # kill "/"
                tokens[i] = f"lot"
            # u1 => u 1, u1A => u 1A, uA => u A, u1-2 -> u 1-2
            elif i < len(tokens)-1 and re_subprem_numal_multi.fullmatch(tokens[i]):
                match = re_subprem_numal_multi.fullmatch(tokens[i]).groupdict()
                tokens[i] = match['numal']
                tokens.insert(i, match['subprem'])
            # X - Y => X-Y
            elif i < len(tokens)-2 and re_to.fullmatch(tokens[i+1]):
                tokens[i] = f"{tokens.pop(i)}{tokens[i]}{tokens.pop(i+1)}"
            # X / Y => X/Y ; X , Y => X/Y
            elif i < len(tokens)-2 and re_sub_sep.fullmatch(tokens[i+1]):
                tokens[i] = f"{tokens.pop(i)}/{tokens.pop(i+1)}"
            # X & Y => X&Y
            elif i < len(tokens)-2 and re_and.fullmatch(tokens[i+1]):
                tokens[i] = f"{tokens.pop(i)}&{tokens.pop(i+1)}"
            # A 1 => 1A
            # elif i < len(tokens)-1 and re_al.fullmatch(tokens[i]) and re_num.fullmatch(tokens[i+1]):
            #     tokens[0] = f"{tokens[1]}{tokens.pop(i)}"
            # 1 A => 1A
            elif i < len(tokens)-1 and re_num.fullmatch(tokens[i]) and re_al_multi.fullmatch(tokens[i+1]):
                tokens[i] = f"{tokens.pop(i)}{tokens[i]}"
            # proposed lot => lot
            elif i < len(tokens)-1 and re_proposed.fullmatch(tokens[i]) \
                    and re_subprem.fullmatch(tokens[i+1]):
                tokens.pop(i)  # kill "proposed"
            # sub X / Y => X/Y
            elif i < len(tokens)-3 and re_subprem.fullmatch(tokens[i]) \
                    and re_numal_multi.fullmatch(tokens[i+1]) \
                    and re_sub_sep.fullmatch(tokens[i+2]) \
                    and re_numal_multi.fullmatch(tokens[i+3]):
                tokens.pop(i)  # kill sub
                tokens.pop(i+1)  # kill /
                tokens[i] = f"{tokens.pop(i)}/{tokens[i]}" # X/Y
            # sub X Y => X/Y
            elif i < len(tokens)-2 and re_subprem.fullmatch(tokens[i]) \
                    and re_numal_multi.fullmatch(tokens[i+1]) \
                    and re_numal_multi.fullmatch(tokens[i+2]):
                tokens.pop(i) # kill sub
                tokens[i] = f"{tokens.pop(i)}/{tokens[i]}"
            # sub X => X
            elif i < len(tokens)-1 and re_subprem.fullmatch(tokens[i]) \
                    and re_numal_subprem_multi.fullmatch(tokens[i+1]):
                tokens.pop(i)
            else:
                i += 1
        i = 0
        while i < len(tokens)-1:
            # print(short_address, f"| second {i}:", tokens, "|", number_tokens)
            if False:  # this makes it easier to reorder these statements
                pass
            # A/N => NA
            # elif re_alnum.fullmatch(tokens[i]):
            #     match = re_alnum.fullmatch(tokens[i]).groupdict()
            #     number_tokens.append(f"{match['num']}{match['al']}")
            # X/Y
            elif re_numal_subprem.fullmatch(tokens[i]):
                number_tokens.append(tokens[i])
            # X/Y-Z
            elif re_numal_subprem_multi.fullmatch(tokens[i]):
                multi = True
                number_tokens.append(tokens[i])
            # X-Y
            elif re_numal.fullmatch(tokens[i]):
                number_tokens.append(tokens[i])
            elif re_numal_multi.fullmatch(tokens[i]):
                multi = True
                number_tokens.append(tokens[i])
            elif re_comment.fullmatch(tokens[i]):
                number_tokens.append(tokens[i])
            else:
                break
            i += 1
        # if tokens and not number_tokens and '/' in tokens[0]:
        #     slash_tokens = tokens[0].split('/')
        #     slash_prefix = '/'.join(slash_tokens[:-1])
        #     if re_numal_subprem_multi.match(slash_prefix):
        #         number_tokens.append(slash_prefix)
        #         tokens[0] = slash_tokens[-1]
        if not number_tokens:
            print(f"failed to split {short_address!r}")
        route_substitutions = {
            'st': 'street',
            'ave': 'avenue',
            'wy': 'way',
            'pl': 'place',
            'tce': 'terrace',
            'hwy': 'highway',
            'rd': 'road',
            'esp': 'esplanade',
            'ct': 'court',
            # these are the wrong way around
            'crescent': 'cres'
        }
        if tokens and tokens[-1].lower() in route_substitutions:
            tokens[-1] = route_substitutions[tokens[-1].lower()]
        return ' '.join(number_tokens).upper(), ' '.join(tokens[i:]).title(), multi

    def from_short_address(short_address, *args, **kwargs):
        kwargs['street_number'], kwargs['street_name'], _ = FullAddress.split_short_address(
            short_address)
        return FullAddress( *args, **kwargs )


@ mapper_registry.mapped
@ dataclass
class Property(DFMixin):
    __table__ = Table(
        "realestate_property",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        Column("actual_id", Integer, unique=True, index=True),
        Column("url", TINYTEXT),
        Column("suburb", TINYTEXT),
        Column("state", VARCHAR(3)),
        Column("postcode", MEDIUMINT(unsigned=True)),
        Column("street_number", TINYTEXT),
        Column("street_name", TINYTEXT),
        Column("property_type", VARCHAR(32)),
        Column("off_market", Boolean),
        Column("bedrooms", TINYINT(unsigned=True)),
        Column("bathrooms", TINYINT(unsigned=True)),
        Column("parking_spaces", TINYINT(unsigned=True)),
        Column("studies", TINYINT(unsigned=True)),
        Column("building_size", Float),
        Column("land_size", Float),
        Column("latitude", DOUBLE),
        Column("longitude", DOUBLE),
        Column("year_built", Integer),
        Column("geocode", JSON),
        Column("avm_data", JSON),
        Index("idx_address", "street_number", "street_name", "suburb",
              "state", "postcode", unique=True),
    )

    id: Optional[int] = field(init=False)
    actual_id: Optional[int] = None
    url: Optional[str] = None
    address: Optional[FullAddress] = None
    property_type: Optional[str] = None
    off_market: Optional[bool] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    parking_spaces: Optional[int] = None
    studies: Optional[int] = None
    building_size: Optional[int] = None
    land_size: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    year_built: Optional[int] = None
    geocode: Optional[dict] = field(default_factory=dict)
    avm_data: Optional[dict] = field(default_factory=dict)
    # partial: Optional[bool] = True
    timeline: List[PropertyEvent] = field(default_factory=list)
    # media: List[MediaItem] = field(default_factory=list)

    @property
    def _street_number(self):
        return self.address.street_number

    @property
    def _street_name(self):
        return self.address.street_name

    @property
    def _suburb(self):
        return self.address.suburb

    @property
    def _postcode(self):
        return self.address.postcode

    __mapper_args__ = {  # type: ignore
        "properties": {
            "timeline": relationship("PropertyEvent", cascade="all, delete-orphan"),
            # "media": relationship("MediaItem", cascade="all, delete"),
            "address": composite(
                FullAddress,
                __table__.c.street_number,
                __table__.c.street_name,
                __table__.c.suburb,
                __table__.c.state,
                __table__.c.postcode
            ),
            "listings": relationship("Listing", back_populates="property"),
        }
    }

    def merge(self, other, force=False):
        # merge other.timeline into self.timeline
        self.timeline = other.timeline
        # for oe in other.timeline:
        #     found = False
        #     for se in self.timeline:
        #         if se.event_type == oe.event_type \
        #                 and se.price == oe.price \
        #                 and abs(oe.date - se.date) < timedelta(days=7):
        #             found = True
        #             break
        #     if not found:
        #         self.timeline.append(oe)

        # merge other.media into self.media
        # for om in other.media:
        #     found = False
        #     for sm in self.media:
        #         if sm.url == om.url:
        #             found = True
        #             break
        #     if not found:
        #         # print(f"adding media {om} not found in {self.media}")
        #         self.media.append(om)
        #         om.property_id = self.id

        # merge other fields
        keys = {*other.__dataclass_fields__.keys()}.union({*self.__dataclass_fields__.keys()}) - \
            {'id', 'timeline', 'media'}
        for k in keys:
            if other.__dict__[k] is not None and (force or not self.__dict__.get(k)):
                setattr(self, k, other.__dict__[k])

    def __repr__(self):
        return f"Property(id={self.id!r}, address={self.address!r})"

    def from_json(data):
        def identity(x): return x
        kwargs = {}
        for k, v, f in [
            ('actual_id', 'propertyId', identity),
            ('url', 'canonicalUrl', identity),
            ('property_type', 'propertyType', identity),
            ('off_market', 'offMarket', identity),
            ('bedrooms', 'bedrooms', positive_int_or_none),
            ('bathrooms', 'bathrooms', positive_int_or_none),
            ('building_size', 'floorArea', positive_float_or_none),
            ('land_size', 'landArea', positive_float_or_none),
            ('year_built', 'yearBuilt', positive_int_or_none),
            ('latitude', 'lat', float_or_none),
            ('longitude', 'lon', float_or_none),
            ('avm_data', 'avmData', identity),
        ]:
            try:
                kwargs[k] = f(data.get(v))
            except Exception as e:
                raise ValueError(
                    f"could no parse key {k} = {data[v]}:\n{e}")
        kwargs['timeline'] = [
            PropertyEvent.from_json(t) for t in data.get('propertyTimeline', [])
        ]
        postcode = data.get('fullSuburb').split(' ')[-1]
        kwargs['address'] = FullAddress.from_short_address(
            data.get('longStreetAddress'),
            suburb=data.get('suburb'),
            state=data.get('state'),
            postcode=positive_int_or_none(postcode)
        )
        if kwargs['land_size'] and kwargs['land_size'] > 1_000_000:
            del kwargs['land_size']
        # filter dumb price values
        if kwargs.get('avm_data') and kwargs.get('avm_data', {}).get('range', {}).get('max'):
            stupid_threshold = 5 * kwargs['avm_data']['range']['max']
            for e in kwargs['timeline']:
                if e.value and e.value > stupid_threshold:
                    e.value = None
        # kwargs['media'] = [
        #     MediaItem.from_json(m) for m in data.get('allImages', [])
        # ]
        # kwargs['partial'] = False
        return Property(**kwargs)
