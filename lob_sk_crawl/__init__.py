import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from lxml import etree

from pipeline.crawl import tree
from pipeline.crawl.scheduler import Runtime, Scheduler
from pipeline.types import DMIN
from pipeline.utils import http

# constants for scheduler
EARLIEST_DATE = datetime(2011, 4, 1)
PAGE_SIZE = 50

# url components
ROOT_URL = "https://www.sasklobbyistregistry.ca/search-the-registry/"

# max records in active date range
MAX_RECORDS_IN_DATE_RANGE = 0


def seed(scheduler: Scheduler) -> tree.Edge | None:
    path = Path(__file__).parent.resolve()

    if scheduler.runtime == Runtime.HIST:
        with open(path / "adv.json") as file:
            req = json.load(file)
        req["json"]["start"] = scheduler.indexer.page_start - 1
    elif scheduler.runtime == Runtime.IDX:
        raise NotImplementedError(f"IDX Runtime not supported in lob sk")
    elif scheduler.runtime == Runtime.DATE:
        if (
            MAX_RECORDS_IN_DATE_RANGE <= scheduler.indexer.max_idx
            and scheduler.seeds > 0
        ):
            return None
        with open(path / "adv.json") as file:
            req = json.load(file)
        req["json"]["PostedFromDate"] = scheduler.calendar.from_date.date().isoformat()
        req["json"]["PostedToDate"] = scheduler.calendar.to_date.date().isoformat()

    return tree.Edge(
        label="search_results",
        req=http.Request(**req),
        p_rid="",
        p_rdate=DMIN,
    )


def sections(data: tree.Data) -> list[tree.Data]:
    label, text = data
    if label == "search_results":
        d = json.loads(text)
        global MAX_RECORDS_IN_DATE_RANGE
        MAX_RECORDS_IN_DATE_RANGE = d["recordsTotal"]
        results = d["data"]
        return [tree.Data("result", json.dumps(r)) for r in results]
    if label == "main":
        return [data]


def parse(
    data: tree.Data, p_rid: str, p_rdate: datetime
) -> tuple[str, datetime, list[tree.Edge]]:
    label, text = data
    if label == "result":
        d = json.loads(text)
        path = Path(__file__).parent.resolve()
        rid = d["Url"]
        rdate = datetime.utcfromtimestamp(0)
        with open(path / "main.json") as file:
            req = json.load(file)
        req["url"] += rid
        edges = [
            tree.Edge(p_rid=rid, p_rdate=rdate, label="main", req=http.Request(**req))
        ]
        return rid, rdate, edges
    if label == "main":
        root = etree.HTML(text)
        reg = root.xpath("//label[contains(text(),'Registration Number:')]/following-sibling::p/text()")[0].strip()
        rid = reg
        posted = root.xpath("//label[contains(text(),'Posted Date:')]/following-sibling::p/text()")[0].strip()
        posteddt = datetime.strptime(posted, '%Y-%m-%d')
        rdate = posteddt
        return rid, rdate, []
