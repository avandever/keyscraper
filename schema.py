from enum import Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    types,
)
from sqlalchemy.orm import relationship
from typing import Iterable, List
import uuid


ICONSEP1 = "."
ICONSEP2 = ":"
Base = declarative_base()


def generate_uuid() -> str:
    return str(uuid.uuid4())


# These show up like "Enhance AADDRR."
class Icon(Enum):
    CAPTURE = "PT"
    AEMBER = "A"
    DRAW = "R"
    DAMAGE = "D"


class IdList(types.TypeDecorator):
    impl=String(37*37)
    cache_ok = True

    def __init__(self, sep=","):
        self.sep = sep

    def process_bind_param(self, value, dialect):
        if value is not None:
            return self.sep.join(map(str, value))

    def process_result_value(self, value, dialect):
        if value is not None:
            if value == "":
                return []
            return list(map(str, value.split(self.sep)))


class Card(Base):
    """
    This represents a card as designed, including its text and verious stats,
    and the url for its image. Most of the columns map directly to one of the
    fields in the Master Vault JSON api, so that calling Card(**obj) on one of
    those dictionaries will work.
    """
    __tablename__ = "card"
    id = Column(String(36), primary_key=True)
    card_title = Column(String(64))
    house = Column(String(20))
    card_type = Column(String(20))
    front_image = Column(String(256))
    card_text = Column(String(512))
    traits = Column(String(64))
    amber = Column(Integer)
    power = Column(Integer)
    armor = Column(Integer)
    rarity = Column(String(10))
    flavor_text = Column(String(512))
    card_number = Column(Integer)
    expansion = Column(Integer)
    is_maverick = Column(Boolean)
    is_anomaly = Column(Boolean)
    is_enhanced = Column(Boolean)
    is_non_deck = Column(Boolean, default=False, nullable=False)
    deck_id_list = Column(IdList(","))
    enhanced_card = relationship("EnhancedCard", uselist=False, back_populates="card")

    def __repr__(self):
        """Nice string representation of the card."""
        return f"<Card({self.card_title})>"


class Deck(Base):
    """
    This represents a deck, including various stats about it from the Master
    Vault. This object does not inherently contain a list of cards, however, and
    instead builds that list from the DeckCard assoc table.
    """
    __tablename__ = "deck"
    id = Column(String(36), primary_key=True)
    name = Column(String(256))
    expansion = Column(Integer)
    power_level = Column(Integer)
    chains = Column(Integer)
    wins = Column(Integer)
    losses = Column(Integer)
    card_id_list = Column(IdList(","))

    @classmethod
    def cards_by_id(cls, ids: List[str]) -> List[Card]:
        return cls.query(Card).filter(Card.id.in_(ids)).all()

    @property
    def cards(self) -> List[Card]:
        return self.cards_by_id(self.card_id_list)

    def __repr__(self):
        return f"<Deck({self.name} / {self.id})>"


class CardWithEnhance(Base):
    __tablename__ = "cards_with_enhance"
    uuid = Column(String(64), primary_key=True, default=generate_uuid)
    # expansion + card_number is functionally a unique id
    expansion = Column(Integer)
    card_number = Column(Integer)
    card_text = Column(String(512))
    aembers = Column(Integer)
    captures = Column(Integer)
    draws = Column(Integer)
    damages = Column(Integer)


class EnhancedCard(Base):
    __tablename__ = "enhanced_cards"
    uuid = Column(String(64), primary_key=True, default=generate_uuid)
    id = Column(String(36), ForeignKey(Card.id), primary_key=True)
    card_title = Column(String(64))
    house = Column(String(20))
    expansion = Column(Integer)
    card_number = Column(Integer)
    amber = Column(Integer)
    possible_icon_groups = Column(String(256))
    card = relationship("Card", uselist=False, back_populates="enhanced_card")

    def set_possible_icons(self, icon_groups: Iterable[List[Icon]]):
        result = ICONSEP2.join(
            ICONSEP1.join(icon.value for icon in icons)
            for icons in icon_groups
        )
        self.possible_icon_groups = result

    def get_possible_icons(self) -> Iterable[List[Icon]]:
        for group in self.possible_icon_groups.split(ICONSEP2):
            yield [Icon(icon) for icon in group]
