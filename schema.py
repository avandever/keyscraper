from enum import Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    ForeignKey,
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


class DeckCard(Base):
    """
    These assoc objects represent an individual card in a deck. They serve as a
    two-way mapping, so that a deck can have a list of cards, while each card
    can have a list of decks in which it is found. However, this results in a
    pretty large table on disk (30GB including indices on my instance), so it
    may need to be changed.
    """
    __tablename__ = "deck_cards"
    uuid = Column(String, primary_key=True, default=generate_uuid)
    is_legacy = Column(Boolean)
    deck_id = Column(String, ForeignKey("deck.id"), primary_key=True, index=True)
    card_id = Column(String, ForeignKey("card.id"), primary_key=True, index=True)
    deck = relationship("Deck", back_populates="card_assocs")
    card = relationship("Card", back_populates="deck_assocs")


class Card(Base):
    """
    This represents a card as designed, including its text and verious stats,
    and the url for its image. Most of the columns map directly to one of the
    fields in the Master Vault JSON api, so that calling Card(**obj) on one of
    those dictionaries will work.
    """
    __tablename__ = "card"
    id = Column(String, primary_key=True)
    card_title = Column(String)
    house = Column(String)
    card_type = Column(String)
    front_image = Column(String)
    card_text = Column(String)
    traits = Column(String)
    amber = Column(Integer)
    power = Column(Integer)
    armor = Column(Integer)
    rarity = Column(String)
    flavor_text = Column(String)
    card_number = Column(Integer)
    expansion = Column(Integer)
    is_maverick = Column(Boolean)
    is_anomaly = Column(Boolean)
    is_enhanced = Column(Boolean)
    deck_assocs = relationship("DeckCard", back_populates="card")
    enhanced_card = relationship("EnhancedCard", uselist=False, back_populates="card")

    @property
    def decks(self) -> List[Base]:
        """
        Construct a list of decks containing this card, built from the DeckCard
        assoc table.
        """
        return [assoc.deck for assoc in self.deck_assocs]

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
    id = Column(String, primary_key=True)
    name = Column(String)
    expansion = Column(Integer)
    power_level = Column(Integer)
    chains = Column(Integer)
    wins = Column(Integer)
    losses = Column(Integer)
    card_assocs = relationship("DeckCard", back_populates="deck")

    @property
    def cards(self) -> List[Card]:
        """
        Construct a list of Cards associated with this deck, built from the
        DeckCard assoc table.
        """
        return [assoc.card for assoc in self.card_assocs]


class CardWithEnhance(Base):
    __tablename__ = "cards_with_enhance"
    uuid = Column(String, primary_key=True, default=generate_uuid)
    # expansion + card_number is functionally a unique id
    expansion = Column(Integer)
    card_number = Column(Integer)
    card_text = Column(String)
    aembers = Column(Integer)
    captures = Column(Integer)
    draws = Column(Integer)
    damages = Column(Integer)


class EnhancedCard(Base):
    __tablename__ = "enhanced_cards"
    uuid = Column(String, primary_key=True, default=generate_uuid)
    id = Column(String, ForeignKey(Card.id), primary_key=True)
    card_title = Column(String)
    house = Column(String)
    expansion = Column(Integer)
    card_number = Column(Integer)
    amber = Column(Integer)
    possible_icon_groups = Column(String)
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
