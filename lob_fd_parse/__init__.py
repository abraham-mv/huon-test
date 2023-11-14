from pipeline.database.models.crawl import Pages
from pipeline.database.models.lob import (LobAffiliate, LobCategory, LobFund,
                                          LobLobbyist, LobMeta, LobOffice,
                                          LobOrg, LobRep, LobSubject,
                                          LobTarget)
from pipeline.types import DMAX, DMIN
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime


def meta(data: dict[Pages, bytes]) -> LobMeta:
    breakpoint()
    main = data["result"][0]
    html_content = data['result'][1].decode('utf-8')   
    soup = BeautifulSoup(html_content, 'html.parser')
    registration_tag = soup.find(string=lambda x: "Registration status:" in x)
    if registration_tag:
        next_strong = registration_tag.find_next('strong')
        status = True if next_strong and "Active" in next_strong.get_text(strip=True) else False
    else:
        status = False

    #START_DATE: also using BS
    initial_date_tag = soup.find(string=lambda x: "Initial registration start date:" in x)

    if initial_date_tag:
        next_strong = initial_date_tag.find_next('strong')
        if next_strong:
            date_string = next_strong.get_text(strip=True)
            start_date = datetime.strptime(date_string, '%Y-%m-%d')
        else:
            start_date = None
    else:
        start_date = None

    #RNUM
    registration_num_tag = soup.find(string=lambda x: "Registration Number:" in x)

    if registration_num_tag:
        next_strong = registration_num_tag.find_next('strong')
        rnum = next_strong.get_text(strip=True) if next_strong else None
    else:
        rnum = "SAMPLE_RNUM"

    #END DATE: might have to go into the drop down list?
    return LobMeta(
        rid=main.rid,
        s="fd",
        added=main.retrieved,
        cid=main.cid,
        rnum=rnum,
        active=status,
        start_date=start_date,
        end_date=DMAX,
    )


def org(data: dict[Pages, bytes]) -> LobOrg:
    main = data["main"][0]
    html_content = data['main'][1].decode('utf-8')
    match = re.search(r'Client name:\\r\\n\s*<strong>([^<]*)</strong>', html_content)
    if not match:
        match = re.search(r'In-house Organization name:\\r\\n\s*<strong>([^<]*)</strong>', html_content)
    if match:
        name = match.group(1).replace('\\r\\n', '').strip()
    else:
        name = "Not found"
    return LobOrg(rid=main.rid, name=name)


def rep(data: dict[Pages, bytes]) -> LobRep:
    main = data["main"][0]
    html_content = data['main'][1].decode('utf-8')
    soup = BeautifulSoup(html_content, 'html.parser')
    h3_tag = soup.find("h3", class_="h4 brdr-bttm", string=re.compile("Client representative"))
    if h3_tag and h3_tag.find_next_sibling("p"):
        name = h3_tag.find_next_sibling("p").get_text(strip=True)
        name = name.replace('\\r\\n', ' ').strip()
    else:
        name = "Not found"
    return LobRep(rid=main.rid, name=name)


def fund(data: dict[Pages, bytes]) -> list[LobFund]:
    #The html example I'm running with doesn't have any funding yet tho
    #have to use another one to test
    #Changed source to source1 because it was giving me issues in terminal
    main = data["main"][0]
    html_content = data['main'][1].decode('utf-8')
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find("table", class_="table table-striped table-bordered")
    funds_list = []
    if table:
        for row in table.find_all("tr")[1:]:
            columns = row.find_all("td")
            source1 = columns[0].get_text(strip=True)
            source1 = source1.replace('\\r\\n', ' ').strip()
            amount = columns[1].get_text(strip=True)
            amount = amount.replace('\\r\\n', ' ').strip()
            amount = float(amount.replace("$", "").replace(",", ""))
            print(f"Source: {source1}, Amount: {amount}")
            funds_list.append(LobFund(rid=main.rid, source=source1, amount=amount))
    else:
        print("Table not found")
        funds_list.append(LobFund(rid=main.rid, source="NA", amount=0.0))
    
    return funds_list


def affiliate(data: dict[Pages, bytes]) -> list[LobAffiliate]:
    main = data["main"][0]
    html_content = data['main'][1].decode('utf-8')
    soup = BeautifulSoup(html_content, 'html.parser')

    header = soup.find('h3', class_='h4 brdr-bttm', string='Subsidiary Beneficiary Information')
    names = []
    if header:
        ul = header.find_next_sibling('ul')
        if ul:
            for li in ul.find_all('li', recursive=False):
                name = li.text.strip()  
                names.append(LobAffiliate(rid=main.rid, name=name))
        else:
            names.append(LobAffiliate(rid=main.rid, name="Not applicable"))
    else:
        names.append(LobAffiliate(rid=main.rid, name="Not applicable"))
    
    return names



def lobbyist(data: dict[Pages, bytes]) -> list[LobLobbyist]:
    #Right now, "name" will return the full name, a bunch of space and then their position. i.e. "HUW WILLIAMS,                                                               Consultant"
    #Keeping it for now
    main = data["main"][0]
    html_content = data['main'][1].decode('utf-8')
    match = re.search(r'Responsible Officer Name:\\r\\n\s*<strong>([^<]*)</strong>', html_content)
    if not match:
        match = re.search(r'Lobbyist name:\\r\\n\s*<strong>([^<]*)</strong>', html_content)
    if match:
        name = match.group(1).replace('\\r\\n', '').strip()
    else:
        name = "SAMPLE_NAME"
    return [LobLobbyist(rid="SAMPLE", name=name)]


def office(data: dict[Pages, bytes]) -> list[LobOffice]:
    return [
        LobOffice(
            rid="SAMPLE", name="SAMPLE", office="SAMPLE", start_date=DMIN, end_date=DMAX
        )
    ]


def subject(data: dict[Pages, bytes]) -> list[LobSubject]:
    main = data["main"][0]
    html_content = data['main'][1].decode('utf-8')
    # Extract subject matters
    subject_matters_match = re.findall(r'<li>([^<]*)</li>', re.search(r'Subject Matters\\r\\n\s*</h3>\\r\\n\s*<ul>(.*?)</ul>', html_content, re.DOTALL).group(1))
    subject_matters = [sm.strip() for sm in subject_matters_match]
    subjects_list = []
    for sub in subject_matters:
        subjects_list.append(LobSubject(rid=main.rid, name=sub))

    return subjects_list



def category(data: dict[Pages, bytes]) -> list[LobCategory]:
    main = data["main"][0]
    html_content = data['main'][1].decode('utf-8')
    soup = BeautifulSoup(html_content, 'html.parser')
    categories = soup.find_all("h4", class_="h5 text-primary")
    results = []
    for cat in categories:
        cat_name = cat.get_text(strip=True)
        cat_name = cat_name.replace('\\r\\n', '').strip()
        outcomes_list = cat.find_next_sibling("ul")
        if outcomes_list:
            for outcome_item in outcomes_list.find_all("li"):
                outcome_text = outcome_item.get_text(strip=True)
                results.append(LobCategory(rid=main.rid, category=cat_name, outcome=outcome_text))
    return results



def target(data: dict[Pages, bytes]) -> list[LobTarget]:
    main = data["main"][0]
    html_content = data['main'][1].decode('utf-8')
    target_match = re.findall(r'<li>([^<]*)</li>', re.search(r'Government Institutions\\r\\n\s*</h3>\\r\\n\s*<ul>(.*?)</ul>', html_content, re.DOTALL).group(1))
    targets = [t.strip() for t in target_match]
    targets_list = []
    for target in targets:
        target = target.replace('\\r\\n', '').replace('\\r', '').replace('\\n', '').strip()
        target = re.sub(r'\s+', ' ', target).strip()
        name = target
        targets_list.append(LobTarget(rid=main.rid, name=name))

    return targets_list
