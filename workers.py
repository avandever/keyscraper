import aiohttp
import asyncio
from asyncio import Lock, Queue
import json
from utils import (
    get_decks_from_page,
    get_deck,
    maybe_add_deck,
    process_cards,
    SessionFactory,
)
from exceptions import (
    InternalServerError,
    RequestThrottled,
)
from schema import Deck
import logging
import os
import requests
from sqlalchemy.exc import DatabaseError
import time
from typing import Set
import random

page_one_stopper = "/tmp/stop_page_one_loop"


class PageProcessor:
    def __init__(self, in_q: Queue, out_q: Queue, known_deck_ids: Set[str]):
        self.in_q = in_q
        self.out_q = out_q
        self.known_deck_ids = known_deck_ids
        self.counter = 0
        self.counter_lock = Lock()

    async def __call__(self, iname: str) -> None:
        skipped_decks = 0
        ise_in_a_row = 0
        logging.debug(f"{iname}:Starting up")
        while True:
            page = await self.in_q.get()
            self.in_q.task_done()
            if page % 10 == 0:
                logging.info(f"{iname}:Getting page {page}")
            try:
                decks = await get_decks_from_page(page)
                if ise_in_a_row > 0:
                    logging.info(
                        f"{iname}:Had {ise_in_a_row} InternalServerErrors. "
                        "Clearing."
                    )
                ise_in_a_row = 0
            except InternalServerError:
                ise_in_a_row += 1
                if ise_in_a_row > 5:
                    logging.exception(
                        f"{iname}:Got InternalServerError 5x in a row. STOPPING"
                    )
                    return
                continue
            except RequestThrottled:
                logging.exception(
                    f"{iname}:RequestThrottled bubbled up from page {page}"
                )
                continue
            except json.decoder.JSONDecodeError:
                logging.error(f"{iname}:JSONDecodeError getting page {page}")
                continue
            except KeyError:
                logging.exception(f"{iname}:KeyError getting page {page}")
                continue
            except Exception:
                logging.exception(f"{iname}:Exception getting page {page}")
                continue
            for deck_id in decks:
                if deck_id in self.known_deck_ids:
                    continue
                await self.out_q.put(deck_id)
                self.known_deck_ids.add(deck_id)


class PageOneTailer:
    def __init__(
        self,
        loop_interval: int,
        out_q: Queue,
        known_deck_ids: Set[str],
        stopper: str = page_one_stopper,
    ):
        self.loop_interval = loop_interval
        self.out_q = out_q
        self.known_deck_ids = known_deck_ids
        self.stopper = stopper

    async def __call__(self):
        name = "p1_tailer"
        decks_added = 0
        logging.debug(f"{name}:Starting up")
        while True and not os.path.exists(self.stopper):
            loop_start = time.time()
            end_iter = False
            page = 1
            while not end_iter:
                decks = await get_decks_from_page(page)
                page += 1
                for deck_id in decks:
                    if deck_id in self.known_deck_ids:
                        end_iter = True
                        continue
                    await self.out_q.put(deck_id)
                    self.known_deck_ids.add(deck_id)
                    decks_added += 1
                    logging.debug(f"{name}:found {decks_added} decks so far")
            logging.debug(f"{name}:Page One Getter checked {page - 1} pages this iteration")
            loop_end = time.time()
            to_sleep = loop_start + self.loop_interval - loop_end
            if to_sleep > 0:
                logging.debug(f"{name}:Page One Getter sleeping for {to_sleep}")
                await asyncio.sleep(to_sleep)
            else:
                logging.debug(f"{name}:Page One Getter not sleeping")
        os.remove(self.stopper)


class DeckFetcher:
    def __init__(self, in_q: Queue, out_q: Queue):
        self.in_q = in_q
        self.out_q = out_q
        self.counter = 0
        self.counter_lock = Lock()

    async def __call__(self, iname: str) -> None:
        logging.debug(f"{iname}:Deck getter starting up")
        while True:
            deck_id = await self.in_q.get()
            self.in_q.task_done()
            try:
                deck = await get_deck(deck_id)
                while self.out_q.qsize() > 100:
                    logging.debug(f"{iname}:Sleeping for db queue")
                    asyncio.sleep(30 + random.randint(0, 30))
                await self.out_q.put(deck)
            except requests.exceptions.ConnectionError:
                logging.error(f"{iname}:ConnectionError getting deck {deck_id}")
            except json.decoder.JSONDecodeError:
                logging.error(f"{iname}:JSON error on deck {deck_id}")
            except aiohttp.client_exceptions.ServerDisconnectedError:
                logging.exception(f"{iname}:Got disconnected on deck {deck_id}")
            except Exception:
                logging.exception(f"{iname}:Uncaught exception on deck {deck_id}!")
            async with self.counter_lock:
                self.counter += 1
                if self.counter % 1 == 0:
                    logging.debug(f"{iname}:{self.counter} decks retrieved")


class DeckInserter:
    def __init__(self, q: Queue, session_factory: SessionFactory):
        self.q = q
        self.session_factory = session_factory
        self.counter = 0
        self.counter_lock = Lock()


    async def __call__(self, iname: str) -> None:
        logging.debug(f"{iname}:Starting up")
        session = self.session_factory()
        while True:
            deck = await self.q.get()
            try:
                process_cards(deck, session)
                maybe_add_deck(deck, session)
            except DatabaseError:
                deck_id = deck["data"]["id"]
                logging.exception(f"{iname}:DBError processing deck {deck_id}")
                session = self.session_factory()
            except Exception:
                logging.exception(f"Uncaught, deck dump: {deck}")
            async with self.counter_lock:
                self.counter += 1
                if self.counter % 1 == 0:
                    logging.debug(f"{iname}:{self.counter} decks processed")
