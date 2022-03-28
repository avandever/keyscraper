import csv
import json
import logging
import os
import random
from aiohttp_requests import requests
import sqlite3
import shutil
from typing import Callable, Dict, Iterable, List, Set, TextIO
from sqlalchemy import create_engine, select
from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    Session,
)
import time
from collections import Counter
from schema import (
    Base,
    Card,
    Deck,
)
from exceptions import (
    DeckNotFoundError,
    InternalServerError,
    RequestThrottled,
    UnknownDBDriverException,
)


def keys_from_dok_dump(dok_dump: TextIO) -> Iterable[str]:
    reader = csv.reader(dok_dump)
    first_row = next(reader)
    id_idx = first_row.index("keyforge_id")
    logging.debug(f"keyforge_id index is {id_idx}")
    for row in reader:
        yield row[id_idx]


APIBASE = "http://www.keyforgegame.com/api/decks/"
HOUSES = os.path.join(APIBASE, "houses", "")
DECK_PARAMS = {"links": "cards,notes"}
SEARCH_PARAMS = {
    "page_size": 10,
    "search": "",
    "power_level": "0,11",
    "chains": "0,24",
    "ordering": "-date",
}


async def get_houses_data() -> Dict:
    response = await requests.get(HOUSES)
    return await response.json()


def randip() -> str:
    third = random.randint(1, 253)
    fourth = random.randint(1, 253)
    return f"192.168.{third}.{fourth}"


async def get_decks_from_page(page: int) -> Iterable[str]:
    params = SEARCH_PARAMS.copy()
    params["page"] = page
    headers = {"X-Forwarded-For": randip()}
    response = await requests.get(APIBASE, params=params, headers=headers)
    try:
        data = await response.json()
    except json.decoder.JSONDecodeError:
        logging.error(f"raw response: {response}")
        raise
    if "code" in data:
        if data["code"] == 429:
            raise RequestThrottled(data["message"] + data["detail"])
        # "Internal Server Error" - means page does not exist
        elif data["code"] == 0:
            raise InternalServerError(data["message"] + data["detail"])
        else:
            logging.error(f"Unrecognized json response {data}")
    return [deck["id"] for deck in data["data"]]


async def get_deck_id_by_name(name: str) -> str:
    params = SEARCH_PARAMS.copy()
    params["search"] = name
    response = await requests.get(APIBASE, params=params)
    data = await response.json()
    if not data["data"]:
        raise DeckNotFoundError(f"Found no decks with name {name}")
    return data["data"][0]["id"]


async def get_deck(deck_id: str) -> Dict:
    deck_url = os.path.join(APIBASE, deck_id, "")
    headers = {"X-Forwarded-For": randip()}
    response = await requests.get(deck_url, params=DECK_PARAMS, headers=headers)
    data = await response.json()
    if "code" in data:
        if data["code"] == 429:
            logging.debug(f"Throttle: {data}")
            after = getattr(response, "retry-after")
            if after is not None:
                logging.debug(f"Retry-after is {after}")
            else:
                logging.debug(f"no retry-after. {dir(response)}")
            raise RequestThrottled(data["message"] + data["detail"])
        else:
            logging.error(
                f"Unrecognized json response {data} while trying to "
                f"fetch deck with id {deck_id}"
            )
    return data


def add_card(card: Dict, session: Session) -> None:
    logging.info(f"Adding card: {card}")
    try:
        int(card["card_number"])
    except ValueError:
        card["card_number"] = card["card_number"][1:]
    new_card = Card(**card)
    session.add(new_card)
    session.commit()
    record_known_card_id(card["id"])


def process_cards(deck: Dict, session: Session) -> None:
    cards = deck["_linked"]["cards"]
    for card in cards:
        if not is_card_known(session, card["id"]):
            add_card(card, session)


card_cache = {}


def maybe_add_deck(deck: Dict, session: Session) -> None:
    global card_cache
    data = deck["data"]
    if session.query(Deck).filter_by(id=data["id"]).count() > 0:
        return
    new_deck = Deck(
        id=data["id"],
        name=data["name"],
        expansion=data["expansion"],
        power_level=data["power_level"],
        chains=data["chains"],
        wins=data["wins"],
        losses=data["losses"],
        card_id_list = data["_links"]["cards"]
    )
    for card_id in data["_links"]["cards"]:
        if card_id in card_cache.keys():
            card = card_cache[card_id]
        else:
            card = session.query(Card).filter_by(id=card_id).one()
            card_cache[card_id] = card
        is_legacy = (card.expansion < new_deck.expansion)
        if not card.deck_id_list:
            card.deck_id_list = []
        card.deck_id_list.append(new_deck.id)
    session.add(new_deck)
    session.commit()


def get_known_cards(session: Session) -> List[str]:
    result = session.query(Card, Card.id).order_by(Card.id)
    known_card_numbers = [row.id for row in result.all()]
    return known_card_numbers


known_card_ids = set()


def is_card_known(session: Session, card_id: str) -> bool:
    global known_card_ids
    if card_id in known_card_ids:
        return True
    known_card_ids.update(get_known_cards(session))
    return card_id in known_card_ids
    

def get_all_cards(session: Session) -> List[Card]:
    result = session.query(Card).order_by(Card.card_number)
    return result.all()


def record_known_card_id(card_id: str) -> None:
    global known_card_ids
    known_card_ids.add(card_id)


async def get_card_images(
    cards: List[Card],
    image_dir: str,
    group_by: List[str] = None,
) -> None:
    for card in cards:
        await get_card_image(card, image_dir, group_by)


def build_image_dir(base_dir: str, card: Card, group_by: List[str] = None) -> str:
    if group_by is None:
        return base_dir
    path_bits = [base_dir]
    path_bits.append("-".join(group_by))
    for attr in group_by:
        path_bits.append(str(getattr(card, attr)))
    return os.path.join(*path_bits)


async def get_card_image(card: Card, image_dir: str, group_by: List[str] = None) -> None:
    group_by_path = build_image_dir(image_dir, card, group_by)
    output_file = os.path.join(image_dir, os.path.basename(card.front_image))
    group_by_link = os.path.join(group_by_path,
                                 os.path.basename(card.front_image))
    if not os.path.exists(output_file):
        img = await requests.get(card.front_image)
        with open(output_file, "wb") as fh:
            fh.write(await img.content.read())
    if not os.path.exists(group_by_link):
        os.makedirs(os.path.dirname(group_by_link), exist_ok=True)
        #os.symlink(link_target, group_by_link)
        shutil.copy(output_file, group_by_link)


class SessionFactory:
    def __init__(
        self,
        driver: str = "sqlite",
        path: str = "keyforge_cards.sqlite",
        host: str = "localhost",
        port: int = None,
        user: str = None,
        password: str = None,
        database: str = "keyforge_decks",
    ):
        uri_bits = [driver, "://"]
        if driver == "sqlite":
            uri_bits.append("/")
            uri_bits.append(path)
        elif driver in ["postgresql", "mysql"]:
            if user is not None:
                uri_bits.append(user)
                if password is not None:
                    uri_bits.append(":")
                    uri_bits.append(password)
            uri_bits.append("@")
            uri_bits.append(host)
            if port is not None:
                uri_bits.append(":")
                uri_bits.append(port)
            uri_bits.append("/")
            uri_bits.append(database)
        else:
            raise UnknownDBDriverException(f"Unrecognized DB Driver: {driver}")
        self.uri = "".join(uri_bits)

    def __call__(self, future: bool = False) -> Session:
        engine = create_engine(self.uri, echo=False)
        Base.metadata.create_all(engine)
        session_factory = sessionmaker(bind=engine)
        return scoped_session(session_factory)()


def loop_report(
    session_factory: SessionFactory,
    sleep_seconds: int = 60,
    running_window: int = 10,
) -> None:
    session = session_factory()
    first_count = None
    current_count = 0
    runs = 0
    last_n_runs = []
    while True:
        previous_count = current_count
        session.commit()
        current_count = session.query(Deck).count()
        cards_by_expansion = Counter(session.query(Card.expansion).all())
        if len(last_n_runs) == running_window:
            last_n_runs.pop(0)
        if previous_count >= 0:
            last_n_runs.append(current_count - previous_count)
        if first_count is None:
            first_count = current_count
        runs += 1
        if runs >= 1:
            all_time_avg = (current_count - first_count) / runs
            running_avg = sum(last_n_runs) / len(last_n_runs)
            print(f"Current count: {current_count}")
            print(f"Average increase: {all_time_avg} / {sleep_seconds}s")
            print(f"Average over last {running_window} runs: {running_avg}")
            print(f"Card counts: {cards_by_expansion}")
            print(f"===================================================")
        time.sleep(sleep_seconds)
