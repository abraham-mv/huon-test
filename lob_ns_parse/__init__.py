from pipeline.database.models.crawl import Pages, CrawledRecord
from pipeline.database.models.lob import (LobAffiliate, LobCategory, LobFund,
                                          LobLobbyist, LobMeta, LobOffice,
                                          LobOrg, LobRep, LobSubject,
                                          LobTarget)
from pipeline.types import DMAX, DMIN
from lxml import etree
from datetime import datetime
from bs4 import BeautifulSoup

def meta(data: CrawledRecord) -> LobMeta:
    main = data["reg"][0]
    xmls = data["reg"][1]
    root = etree.HTML(xmls)
    sdate1 = root.xpath("(//tr[td/strong[text()='Initial registration date']]/following-sibling::tr/td[1])[1]/text()")
    sdate2 = sdate1[0] 
    sdate_dt = datetime.strptime(sdate2, '%d-%B-%Y')
    edate1 = root.xpath("(//tr[td/strong[text()='Initial registration date']]/following-sibling::tr/td[2])[1]/text()")
    edate2 = edate1[0]
    edate_dt = datetime.strptime(edate2, '%d-%B-%Y')
    rnum = root.xpath("//tr[td/strong[contains(text(), 'Registration Number')]]/following-sibling::tr[1]/td[1]/text()")[0]
    statuscheck = root.xpath("//tr[td/strong[contains(text(), 'Status')]]/following-sibling::tr[1]/td[2]/text()")[0].strip()
    status = False if statuscheck == "Inactive" else True
    return LobMeta(
        rid=main.rid,
        s="ns",
        added=main.retrieved,
        cid=main.cid,
        rnum=rnum,
        active=status,
        start_date=sdate_dt,
        end_date=edate_dt,
    )


def org(data: CrawledRecord) -> LobOrg:
    main = data["reg"][0]
    xmls = data["reg"][1]
    root = etree.HTML(xmls) 
    name = root.xpath("//td/strong[text()='Lobbying on behalf of (Name of Client)']/following-sibling::text()[1]")[0]
    return LobOrg(rid=main.rid, name=name)


def rep(data: CrawledRecord) -> LobRep:
    main = data["reg"][0]
    xmls = data["reg"][1]
    root = etree.HTML(xmls) 
    last = root.xpath("//strong[contains(text(), \"Lobbyist's Last Name\")]/following-sibling::text()")[1].strip()
    first = root.xpath("//strong[contains(text(), \"Lobbyist's First Name\")]/following-sibling::text()")[0].strip()
    name = f"{first} {last}"
    return LobRep(rid=main.rid, name=name)


def fund(data: CrawledRecord) -> LobFund:
    #haven't verified this w a page that has funds table
    main = data["reg"][0]
    xmls = data["reg"][1]
    root = etree.HTML(xmls) 
    funds_list = []
    table = root.xpath("//table[contains(@class, 'table-striped') and contains(@class, 'table-bordered')]")
    
    if table:
        rows = table[0].xpath(".//tr[position() > 1]")
        for row in rows:
            columns = row.xpath(".//td")
            if len(columns) >= 2: 
                source = columns[0].text.strip()
                amount = columns[1].text.strip()
                amountf = float(amount)
                print(f"source: {source}, amount: {amount}")
                funds_list.append(LobFund(rid=main.rid, source=source, amount=amountf))
    else:
        print("Table not found")
        funds_list.append(LobFund(rid=main.rid, source="NA", amount=0.0))

    return funds_list


def affiliate(data: CrawledRecord) -> LobAffiliate:
    #haven't tested on something with a table
    main = data["reg"][0]
    xmls = data["reg"][1]
    root = etree.HTML(xmls) 
    affiliates_list = []
    table = root.xpath("//table[@class='innertable' and contains(., 'Other Beneficiaries of Lobbying Activites')]")
    if table:
        rows = table[0].xpath(".//tr[bgcolor='#FFFFFF']")
        for row in rows:
            name = row.xpath(".//td[1]/text()")[0].strip()
            print(name)
            affiliates_list.append(LobAffiliate(rid=main.rid, name=name))
    else:
        print("Table not found")
        affiliates_list.append(LobAffiliate(rid=main.rid, name="NA"))
    return affiliates_list


def lobbyist(data: CrawledRecord) -> LobLobbyist:
    main = data["reg"][0]
    xmls = data["reg"][1]
    root = etree.HTML(xmls) 
    last = root.xpath("//strong[contains(text(), \"Lobbyist's Last Name\")]/following-sibling::text()")[1].strip()
    first = root.xpath("//strong[contains(text(), \"Lobbyist's First Name\")]/following-sibling::text()")[0].strip()
    name = f"{first} {last}"
    return [LobLobbyist(rid=main.rid, name=name)]


def office(data: CrawledRecord) -> LobOffice:
    #NA for nova scotia data
    #couldn't return empty list so saving as NA
    main = data["reg"][0]
    return [
        LobOffice(
            rid=main.rid, name="NA", office="NA", start_date=DMIN, end_date=DMAX
        )
    ]

def subject(data: CrawledRecord) -> LobSubject:
    main = data["reg"][0]
    xmls = data["reg"][1]
    root = etree.HTML(xmls) 
    subjects_list = []
    subjects = root.xpath("//strong[contains(text(), 'II. Subject Matter')]/ancestor::tr/following-sibling::tr[1]/td/table[@cellpadding='0' and @class='innertable' and @cellspacing='2' and @border='0' and @width='100%']//tr/td/text()")
    for subject in subjects:
        subject_text = subject.strip()
        subjects_list.append(LobSubject(rid=main.rid, name=subject_text))
    return subjects_list    


def category(data: CrawledRecord) -> LobCategory:
    main = data["reg"][0]
    xmls = data["reg"][1]
    root = etree.HTML(xmls) 
    outcome = root.xpath("//strong[contains(text(), 'I. Description')]/ancestor::tr/following-sibling::tr[1]/td/text()")[1]
    return [LobCategory(rid=main.rid, category="", outcome=outcome)]


def target(data: CrawledRecord) -> LobTarget:
    main = data["reg"][0]
    xmls = data["reg"][1]
    root = etree.HTML(xmls) 
    targets_list = []
    targets = root.xpath("//strong[contains(text(), 'III. Lobby Targets')]/ancestor::tr/following-sibling::tr[1]/td/table[@cellpadding='0' and @class='innertable' and @cellspacing='2' and @border='0' and @width='100%']//tr/td/text()")
    for target in targets:
        target_text = target.strip()
        targets_list.append(LobTarget(rid=main.rid, name=target_text))
    return targets_list
