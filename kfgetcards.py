#!/usr/bin/env python3.7
#import asyncclick as click
import click
import configparser
from utils import (
    get_all_cards,
    get_card_images,
    keys_from_dok_dump,
    loop_report,
    SessionFactory,
)
from schema import Deck
from workers import (
    PageProcessor,
    PageOneTailer,
    DeckFetcher,
    DeckInserter,
)
import logging
import click_log
import asyncio
from asyncio import create_task, Queue, Task
from typing import Iterable, List, Set

click_log.basic_config()


@click.group()
@click_log.simple_verbosity_option()
@click.option("-c", "--config", type=str, default="kfcards.ini")
@click.pass_context
def cli(ctx, config):
    ctx.ensure_object(dict)
    cparser = configparser.ConfigParser()
    cparser.read(config)
    ctx.obj["CONFIG"] = cparser
    ctx.obj["SESSION_FACTORY"] = SessionFactory(**cparser["db"])
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


@cli.command()
@click.option("--db-file", type=str, default="keyforge_cards.sqlite")
@click.option("-s", "--sleep-seconds", type=int, default=60)
@click.option("-w", "--running-window", type=int, default=10)
@click.option("--db-driver", type=str, default="")
@click.option("--db-host", type=str, default="")
@click.option("--db-user", type=str, default="")
@click.option("--db-pass", type=str, default="")
@click.pass_context
def report_loop(ctx, db_file, sleep_seconds, running_window, db_driver: str,
                db_host: str, db_user: str, db_pass: str,
):
    asyncio.run(loop_report(
        ctx.obj["SESSION_FACTORY"],
        sleep_seconds,
        running_window,
    ))


@cli.command()
@click.option("--image-dir", default="keyforge-images")
@click.pass_context
def get_images(ctx, image_dir: str):
    session = ctx.obj["SESSION_FACTORY"]()
    cards = get_all_cards(session)
    get_card_images(
        cards,
        image_dir,
        group_by=["expansion", "house", "rarity", "card_type"],
    )


@cli.command()
@click.argument("dok_csv", type=click.File("r"))
@click.option("--deck-fetchers", default=1)
@click.option("--db-workers", default=1)
@click.pass_context
def get_from_dok_dump(ctx, dok_csv, deck_fetchers: int, db_workers: int):
    asyncio.run(_get_from_dok_dump(ctx, dok_csv, deck_fetchers, db_workers))

async def _get_from_dok_dump(ctx, dok_csv, deck_fetchers: int, db_workers: int):
    session_factory = ctx.obj["SESSION_FACTORY"]
    session = session_factory()
    known_deck_ids = {x[0] for x in session.query(Deck.id).all()}
    logging.info(f"main:Starting with {len(known_deck_ids)} decks in db.")
    deck_queue = Queue()
    db_queue = Queue()
    tasks = []
    counters = {"fetched": 0, "processed": 0}
    skipped_decks = 0
    for deck_id in keys_from_dok_dump(dok_csv):
        if deck_id in known_deck_ids:
            skipped_decks += 1
        else:
            await deck_queue.put(deck_id)
    logging.info(f"main:Skipped {skipped_decks} known decks")
    logging.info(f"main:Inserted {deck_queue.qsize()} decks")
    tasks.extend(await start_deck_fetchers(
        deck_fetchers,
        deck_queue,
        db_queue,
    ))
    tasks.extend(await start_db_inserters(
        db_workers,
        db_queue,
        session_factory,
    ))
    await deck_queue.join()
    await db_queue.join()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@cli.command()
@click.argument("decks", type=str, nargs=-1)
@click.pass_context
def get_decks_by_id(ctx, decks: List[str]):
    asyncio.run(_get_decks_by_id(ctx, decks))

async def _get_decks_by_id(ctx, decks: List[str]):
    deck_queue = Queue()
    logging.debug(f"main:Fetching {len(decks)} decks")
    for deck in decks:
        await deck_queue.put(deck)
    db_queue = Queue()
    tasks = []
    tasks.extend(await start_deck_fetchers(
        2,
        deck_queue,
        db_queue
    ))
    tasks.extend(await start_db_inserters(
        2,
        db_queue,
        ctx.obj["SESSION_FACTORY"],
    ))
    await deck_queue.join()
    await db_queue.join()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def start_page_fetchers(
    workers: int,
    in_q: Queue,
    out_q: Queue,
    known_deck_ids: Set[str],
) -> Iterable[Task]:
    fetcher = PageProcessor(in_q, out_q, known_deck_ids)
    return [create_task(fetcher(f"pager-{x}")) for x in range(workers)]


async def start_deck_fetchers(
    workers: int,
    in_q: Queue,
    out_q: Queue,
) -> Iterable[Task]:
    fetcher = DeckFetcher(in_q, out_q)
    return [create_task(fetcher(f"fetcher-{x}")) for x in range(workers)]


async def start_db_inserters(
    workers: int,
    in_q: Queue,
    session_factory: SessionFactory,
) -> Iterable[Task]:
    inserter = DeckInserter(in_q, session_factory)
    return [create_task(inserter(f"inserter-{x}")) for x in range(workers)]


@cli.command()
@click.option("--start-page", type=int, default=1)
@click.option("--max-pages", type=int, default=1)
@click.option("--page-workers", default=1)
@click.option("--deck-fetchers", default=1)
@click.option("--db-workers", default=1)
@click.option("--reverse/--no-reverse", default=False)
@click.option("-i", "--page-one-interval", type=int, default=0)
@click.pass_context
def get(ctx, start_page: int, max_pages: int, page_workers: int,
        deck_fetchers: int, db_workers: int, reverse: bool,
        page_one_interval: int,
):
    asyncio.run(
        _get(
            ctx, start_page, max_pages, page_workers, deck_fetchers,
            db_workers, reverse, page_one_interval,
        )
    )

async def _get(ctx, start_page: int, max_pages: int,
               page_workers: int, deck_fetchers: int,
               db_workers: int, reverse: bool,
               page_one_interval: int,
 ):
    session_factory = ctx.obj["SESSION_FACTORY"]
    session = session_factory()
    known_deck_ids = {x[0] for x in session.query(Deck.id).all()}
    logging.info(f"main:Starting with {len(known_deck_ids)} decks in db.")
    page_queue = Queue()
    deck_queue = Queue()
    db_queue = Queue()
    tasks = []
    tasks.extend(await start_page_fetchers(
        page_workers,
        page_queue,
        deck_queue,
        known_deck_ids,
    ))
    tasks.extend(await start_deck_fetchers(
        deck_fetchers,
        deck_queue,
        db_queue,
    ))
    tasks.extend(await start_db_inserters(
        db_workers,
        db_queue,
        session_factory,
    ))
    pages = range(start_page, max_pages + 1)
    if reverse:
        pages = reversed(pages)
    for page in pages:
        await page_queue.put(page)
    if page_one_interval:
        tailer = PageOneTailer(page_one_interval, deck_queue, known_deck_ids)
        page_one_task = create_task(tailer())
    await page_queue.join()
    if page_one_interval:
        await page_one_task
    await deck_queue.join()
    await db_queue.join()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@cli.command()
@click.option("--deck-fetchers", default=1)
@click.option("--db-workers", default=1)
@click.option("-i", "--interval", type=int, default=300)
@click.pass_context
def tail(ctx, deck_fetchers: int, db_workers: int, interval: int):
    asyncio.run(_tail(ctx, deck_fetchers, db_workers, interval))

async def _tail(ctx, deck_fetchers: int, db_workers: int, interval: int):
    session_factory = ctx.obj["SESSION_FACTORY"]
    session = session_factory()
    known_deck_ids = {x[0] for x in session.query(Deck.id).all()}
    logging.info(f"main:Starting tailer with {len(known_deck_ids)} decks in db.")
    deck_queue = Queue()
    db_queue = Queue()
    tasks = []
    tasks.extend(await start_deck_fetchers(
        deck_fetchers,
        deck_queue,
        db_queue,
    ))
    tasks.extend(await start_db_inserters(
        db_workers,
        db_queue,
        session_factory,
    ))
    tailer = PageOneTailer(interval, deck_queue, known_deck_ids)
    tailer_task = create_task(tailer())
    await tailer_task
    await deck_queue.join()
    await db_queue.join()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    #cli(obj={}, _anyio_backend="asyncio")
    cli(obj={})
