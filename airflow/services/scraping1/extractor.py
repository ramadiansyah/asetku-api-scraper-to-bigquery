import pandas as pd
import requests

from bs4 import BeautifulSoup
from datetime import datetime

def extract():
    url = "https://www.asetku.co.id/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    
    for idx, el in enumerate(soup.find_all("div", class_="amount")):
        print(idx, el.get_text(strip=True))

    url = "https://www.asetku.co.id/api/admin/public/official/report"
    resp = requests.get(url).json()

    data = resp["data"]

    df = pd.DataFrame([{
        "totalFunding": data["totalFunding"],
        "averageLastMonths": data["averageLastMonths"],
        "totalBorrowersUsers": data["totalBorrowersUsers"],
        "activeUserCnt": data["activeUserCnt"],
        "unwithdrawAmt": data["unwithdrawAmt"],
        "tkbRate": data["tkbRate"],
        "tkb0": data["tkb0"],
        "tkb30": data["tkb30"],
        "tkb60": data["tkb60"],
        "lancar": data["lancar"],
        "userRegisteredOfTheYear": data["userRegisteredOfTheYear"],
        "investmentUsersOfAll": data["investmentUsersOfAll"],
        "investmentUsersOfYear": data["investmentUsersOfYear"],
        "investingUsers": data["investingUsers"],
        "borrowUsersOfTheYear": data["borrowUsersOfTheYear"],
        "scraped_at": datetime.utcnow()
    }])

    print(df)
    df.to_csv("data/asetku/extract.csv", index=False)
    