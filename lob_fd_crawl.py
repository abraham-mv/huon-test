import json
import re
from datetime import datetime
from functools import reduce
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from lxml import etree

from pipeline.crawl import tree
from pipeline.crawl.runtime import Runtime
from pipeline.crawl.scheduler import Scheduler
from pipeline.types import DMIN
from pipeline.utils import http
from pipeline.utils.log import get_logger

# constants for scheduler
EARLIEST_DATE = datetime(2008, 7, 2)
PAGE_SIZE = 50

# url components
ROOT_URL = "https://lobbycanada.gc.ca"
# max records in active date range
MAX_RECORDS_IN_DATE_RANGE = 0

LOGGER = get_logger(__name__, False)


def _is_hist(data: tree.Data) -> bool:
    """Utility function that checks whether the given html (as Data) is
    from the Advanced (hist) or Recent (dated) interface."""
    label, text = data
    root = etree.HTML(text)
    # XPATH for search results in adv; valid for result or page thereof
    hxp = "//a[@class='nodecoration']"  # list-group-item nodecoration in on the html of the dated page
    if "result" in label:
        if root.xpath(hxp):
            return True
        else:
            return False
    else:
        raise ValueError(f"Invalid label {label}")


def fetch_page_content(request_data) -> str:
    response = requests.post(
        url=request_data["url"],
        headers=request_data["headers"],
        data=request_data["data"],
    )
    return response.text


# def is_empty(content: str) -> bool:
#     # Basic check, might need refining based on actual empty pages' structure?
#     return not content.strip()


def seed(scheduler: Scheduler) -> tree.Edge | None:
    """This site uses two interfaces for historical/idx ("Advanced Search"),
    and date searches ("Recent Registrations").

    In the former, we just supply page start as an integer.

    In the latter, we need to supply date ranges, and paginate within. Each range
    supports 2,500 possible results. **NOTE: we do not ensure provided date range
    misses no records.** We could, but not too useful a feature: we can just run
    historical crawl if we really need to.

    These date ranges have pagination within them. Because you can search by date,
    you will not encounter a date past from/to_date (they're in DESC order in thise
    case) so you have to instead track the pages remaining.
    """
    path = Path(__file__).parent.resolve()

    if scheduler.runtime == Runtime.HIST:
        with open(path / "adv.json") as file:
            raise NotImplementedError("NOT DONE")
            d = json.load(file)
            # page_start will be set to -c, if supplied, otherwise 1;
            # -1 since site is 0-indexed
            d["data"]["registrationDocsStart"] = scheduler.indexer.page_start - 1

    elif scheduler.runtime == Runtime.IDX:
        raise NotImplementedError("IDX Runtime not supported in lob fd")
    else:
        # we finish when we've reached the max records point, and its not the first seed call
        if (
            MAX_RECORDS_IN_DATE_RANGE <= scheduler.indexer.max_idx
            and scheduler.seeds > 0
        ):
            return None
        with open(path / "rcnt.json") as file:
            d = json.load(file)
        d["params"]["fromDate"] = scheduler.calendar.from_date.date().isoformat()
        d["params"]["toDate"] = scheduler.calendar.to_date.date().isoformat()
        d["params"]["pg"] = scheduler.indexer.page_number
        with requests.session() as session:
            res = requests.request(**d)
        results = section_results(tree.Data(label="results", data=res.text))
        return reduce(lambda a, b: a + b, [_edges(item) for item in results])


def section_results(data: tree.Data) -> list[tree.Data]:
    label, text = data
    root = etree.HTML(text)
    if _is_hist(data):
        # xp = "//a[@class='nodecoration']"
        xp = "//li[@class='list-group-item mrgn-bttm-md']"
    # if not hist mode, also checking remaining pages in date range
    else:
        xp = "//li[@class='list-group-item']"
        page_string = root.xpath(
            "//header[@class='panel-heading']/h2[@class='panel-title']/text()"
        )
        # if not page string, there are no results for the given date range;
        # check max number of records in the date range
        if page_string:
            cap = re.search(
                r"(?P<start>\d{1,3}(?:,\d{3})*?)-(?P<curr>\d{1,3}(?:,\d{3})*?) of (?P<max>\d{1,3}(?:,\d{3})*)(?![\d,])",
                page_string[0],
            )
            LOGGER.debug(f"cap match object: {cap}")
            global MAX_RECORDS_IN_DATE_RANGE
            MAX_RECORDS_IN_DATE_RANGE = int(cap["max"].replace(",", ""))
    results = root.xpath(xp)
    return [tree.Data("result", etree.tostring(result).decode()) for result in results]


def sections(data: tree.Data) -> list[tree.Data]:
    """The only sectioning in this crawl is the search results page. We must account
    for both the advanced and recent search interfaces, though."""
    # label, (page, text) = list(data.items())[0]  no longer works
    text = data.data
    if data.label == "main":
        return [tree.Data("main", text)]
    if data.label == "regpage":
        return [tree.Data("regpage", text)]
    else:
        raise ValueError("Unrecognized label")





def parse(
    data: tree.Data, p_rid: str, p_rdate: datetime
) -> tuple[str, datetime, list[tree.Edge]]:
    text = data.data
    if data.label == "main":
        rid = _rid(text)
        rdate = _rdate(text)
        edges = _edges(text)
        return rid, rdate, edges
    if data.label == "regpage":
        return p_rid, p_rdate, []
    else:
        raise ValueError("Unrecognized label")


def _rid(data: tree.Data) -> str:
    """We use the record links to get cno, vn.
    Links are same structure/location in adv/rcnt.
    regid refers to the version"""

    label, text = data
    root = etree.HTML(text)
    #selected = root.xpath("//select[contains(@id, 'regId')]/option[@selected='selected']")

    reg = root.xpath("//a[contains(@href, 'regId=')]")
    href_value = reg[0].get('href')
    regId_value = href_value.split("regId=")[1].split("&")[0]
    cno_value = href_value.split("cno=")[1].split("&")[0]
    return f"{cno_value}-{regId_value}"


def _rdate(data: tree.Data) -> datetime:
    """
    adv will return the starting date of the registration, or the "occured" date
     if it's a monthly report.
    rcnt will return the current datetime. (check with martin)
    """
    label, text = data
    root = etree.HTML(data.data)
    date_string = root.xpath("//div[contains(@class, 'small')]/strong/text()")
    date_str = date_string[1].strip()
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return date_obj


def _edges(data: tree.Data) -> list[tree.Edge]:
    """
    vwrG_href corresponds to the href for adv
    clntSmmry_href corresponds to the href for rcnt
    returns label "main", which is the page for that entry with further detail
    """
    root = etree.HTML(data.data)
    vwRg_href = root.xpath("//a[contains(@href, 'vwRg')]/@href")
    clntSmmry_href = root.xpath("//a[contains(@href, 'clntSmmry')]/@href")

    # Determine which href is present
    if vwRg_href:
        href = vwRg_href[0]
    elif clntSmmry_href:
        href = clntSmmry_href[0]
    else:
        raise ValueError("Expected href not found in the provided HTML content.")
    return [
        tree.Edge(
            label="regpage",
            req=http.Request(
                method="GET",
                url=urljoin(ROOT_URL, href),
            ),
            p_rid=_rid(data),
            p_rdate=_rdate(data),
        )
    ]
