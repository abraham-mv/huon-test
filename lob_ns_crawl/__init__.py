from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from lxml import etree

from pipeline.crawl import tree
from pipeline.crawl.scheduler import Scheduler
from pipeline.types import DMIN
from pipeline.utils.http import Request, SSLManager
from pipeline.utils.log import get_logger



SSLManager()._allow_legacy_renegotiation()
ROOT_URL = "https://novascotia.ca/"

logger = get_logger(__name__, debug=False)



def seed(scheduler: Scheduler) -> tree.Edge | None:
    """This is for the "historical" run.
    For now it's pretty straightforward to get pages,
    and i'll rejig it to specify dates for the scheduler
    later after chat w Martin"""
    if scheduler.indexer.page_start >= 1900:
        return None
    i = scheduler.indexer.page_start - 1
    req = Request(
        method="GET",
        url=f"https://novascotia.ca/sns/Lobbyist/search.asp?page={i}",
    )

    return tree.Edge(
        label="search_results",
        req=req,
        p_rid="",
        p_rdate=DMIN,
    )


def sections(data: tree.Data) -> list[tree.Data]:
    label, text = data
    utf8_encoded_data = text.encode('utf-8')
    soup = BeautifulSoup(utf8_encoded_data, "lxml")
    # label, text = data
    # soup = BeautifulSoup(text, "lxml")
    # this next if statement selects all the links for the 1-25, 26-50 etc sections at the top
    if label == "search_results":
        table = soup.find("table", class_=lambda x: x and "innertable" in x)
        tr_tags = table.find_all("tr")[1:]
        return [tree.Data("sec_results", str(tag)) for tag in tr_tags]

    if label == "reg":
        return [data]

    raise ValueError(f"Unrecognized label: {label}")


def parse(
    data: tree.Data, p_rid: str, p_rdate: datetime
) -> tuple[str, datetime, list[tree.Edge]]:
    """
    outputs the results page for each section of results (i.e. 1-25, 26-50, etc)
    
    the new label is "sec_results", meaning second results, returns the second landing
    page of results. maybe a different name would be better. idk.

    michaela thats the most vague commend u've ever written

    the new label "reg" points to each registration page. One more loop is required to get
    date info for the rdate.
    """

    edges_to_return = []

    if data.label == "sec_results":
        soup = BeautifulSoup(data.data, "lxml")
        a_tag = soup.find_all("a", href=True)
        href1 = a_tag[0]["href"]
        rid = (
                href1.split("regid=")[1].split("&")[0] if "regid=" in href1 else None
            )
        edges_to_return = [
                tree.Edge(
                    label="reg",
                    req=Request(
                        method="GET",
                        url=urljoin(ROOT_URL, href1),
                    ),
                    p_rid=rid,
                    p_rdate=p_rdate,
                )
            ]
        return rid, p_rdate, edges_to_return

    if data.label == "reg":
        soup = BeautifulSoup(data.data, "lxml")
        rdate_tag_row = soup.find("td", string="Last date of any changes").find_parent(
            "tr"
        )
        rdate_row = rdate_tag_row.find_next_sibling("tr")
        rdate_str = (rdate_row.find("td").get_text().strip())  
        rdate = datetime.strptime(rdate_str, "%d-%B-%Y") 
        return p_rid, rdate, []
