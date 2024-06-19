import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
import lxml
import re
import csv


class FBScrape:
    def __init__(self, fp: str, league: str):
        self.fp = fp
        self.league = league
        self.finished = set()

    def scrape_website(self, url: str, attempts: int = 0):
        # Send GET request to the website
        response = requests.get(url)
        time.sleep(3)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            if attempts > 0:
                print("Success")
            return response
        elif response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            delay_seconds = (
                5  # Default delay in case Retry-After header is not present or invalid
            )
            try:
                delay_seconds = int(retry_after)
            except (ValueError, TypeError):
                pass
            print(f"Received 429. Retrying after {delay_seconds} seconds.")
            time.sleep(delay_seconds)
        elif attempts < 5:
            print("Error:", response.status_code, "Trying again.")
            time.sleep(1)
            return self.scrape_website(url, attempts + 1)
        else:
            # Handle the request error
            print("Error: ", response.status_code, "Failed on:", url)
            return None

    def get_links(self, season: int):
        split = lambda year: str(year) + "-" + str(year + 1)

        if self.league == "Premier League":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/9/{season}/schedule/{season}-Premier-League-Scores-and-Fixtures"
        elif self.league == "La Liga":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/12/{season}/schedule/{season}-La-Liga-Scores-and-Fixtures"
        elif self.league == "Serie A":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/11/{season}/schedule/{season}-Serie-A-Scores-and-Fixtures"
        elif self.league == "Bundesliga":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/20/{season}/schedule/{season}-Bundesliga-Scores-and-Fixturess"
        elif self.league == "Ligue 1":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/13/{season}/schedule/{season}-Ligue-1-Scores-and-Fixtures"
        elif self.league == "Championship":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/10/{season}/schedule/{season}-Championship-Scores-and-Fixtures"
        elif self.league == "MLS":
            season_link = f"https://fbref.com/en/comps/22/{season}/schedule/{season}-Major-League-Soccer-Scores-and-Fixtures"
        elif self.league == "Eredivisie":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/23/{season}/schedule/{season}-Eredivisie-Scores-and-Fixtures"
        elif self.league == "Brasileiro":
            season_link = f"https://fbref.com/en/comps/24/{season}/schedule/{season}-Serie-A-Scores-and-Fixtures"
        elif self.league == "Primeira":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/32/{season}/schedule/{season}-Primeira-Liga-Scores-and-Fixtures"
        elif self.league == "Liga MX":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/31/{season}/schedule/{season}-Liga-MX-Scores-and-Fixtures"
        elif self.league == "Segunda":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/17/{season}/schedule/{season}-Segunda-Division-Scores-and-Fixtures"
        elif self.league == "Belgian Pro League":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/37/{season}/schedule/{season}-Belgian-Pro-League-Scores-and-Fixtures"
        elif self.league == "2 Bundesliga":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/33/{season}/schedule/{season}-2-Bundesliga-Scores-and-Fixtures"
        elif self.league == "Ligue 2":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/33/{season}/schedule/{season}-2-Bundesliga-Scores-and-Fixtures"
        elif self.league == "Argentina":
            season_link = f"https://fbref.com/en/comps/21/{season}/schedule/{season}-Liga-Profesional-Argentina-Scores-and-Fixtures"
        elif self.league == "Serie B":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/18/{season}/schedule/{season}-Serie-B-Scores-and-Fixtures"
        elif self.league == "WSL":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/189/{season}/schedule/{season}-Womens-Super-League-Scores-and-Fixtures"
        elif self.league == "NWSL":
            season_link = f"https://fbref.com/en/comps/182/{season}/schedule/{season}-NWSL-Scores-and-Fixtures"
        elif self.league == "Liga F":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/230/{season}/schedule/{season}-Liga-F-Scores-and-Fixtures"
        elif self.league == "A League":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/196/{season}/schedule/{season}-A-League-Women-Scores-and-Fixtures"
        elif self.league == "Premiere Ligue":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/193/{season}/schedule/{season}-Premiere-Ligue-Scores-and-Fixtures"
        elif self.league == "Frauen Bundesliga":
            season = split(season)
            season_link = f"https://fbref.com/en/comps/183/{season}/schedule/{season}-Frauen-Bundesliga-Scores-and-Fixtures"

        xml = self.scrape_website(season_link)
        if xml is None:
            return None

        xml.encoding = "UTF-8"
        xml = xml.text.strip()
        soup = BeautifulSoup(xml, features="lxml")
        root = "https://fbref.com"
        elems = soup.find_all(attrs={"class": "left", "data-stat": "match_report"})
        links = [
            root + elem.a.get("href")
            for elem in elems
            if "iz" not in elem.get("class") and elem.a.get("href") is not None
        ]
        return links

    def create_csv_header(self) -> None:
        """Writes the header into the csv file. This function functionally resets the file.


        Args:
            fp (str, optional): Destination filepath for training data. Defaults to 'soccer_train.csv'.
        """
        schema = [
            "mid",
            "tie",
            "w_poss",
            "l_poss",
        ]

        with open(self.fp, "w", newline="") as file:
            csv_writer = csv.writer(file)

            csv_writer.writerow(schema)

        print("Header written")

    def write_to_csv(self, row: list) -> None:
        """Writes the data to the csv file.

        Args:
            data (list): a list of lists of data to be written to the csv file.
            fp (str, optional): filepath of output training data. Defaults to "soccer_train.csv".
        """
        with open(self.fp, "a", newline="") as file:
            csv_writer = csv.writer(file)

            csv_writer.writerow(row)

    def scrape_game(self, url: str) -> list:
        xml = self.scrape_website(url)
        if xml is None:
            return None
        xml.encoding = "UTF-8"
        xml = xml.text.strip()
        soup = BeautifulSoup(xml, features="lxml")

        # Get match id
        mid = url.split("/")[-1]

        # Get score
        try:
            scores = soup.find_all(attrs={"class": "score"})
            score = {i: int(score.text) for i, score in enumerate(scores)}
        except:
            print(f"Score not found for {url}")
            return None
        winner = max(score, key=score.get)
        tie = score[0] == score[1]

        # Get possession
        try:
            percents = (
                soup.find(string="Possession")
                .parent.parent.find_next_sibling()
                .find_all(string=re.compile("\d*%"))
            )
            percent = {i: int(percent[:-1]) for i, percent in enumerate(percents)}
        except AttributeError:
            print(f"Possession not found for {url}")
            return None

        row = [mid, tie, percent[winner], percent[1 - winner]]

        return row

    def scrape_season(self, season: str) -> None:
        links = self.get_links(season)

        if links is None:
            print("Links not found")
            return None

        for link in links:
            mid = link.split("/")[-1]
            if mid in self.finished:
                continue

            row = self.scrape_game(link)
            if row is not None:
                self.write_to_csv(row)
            self.finished.add(mid)

        print(f"{season} season scraped")

    def main(self, years: list, reset: bool = False):

        if reset:
            self.create_csv_header()

        with open(self.fp, "r") as f:
            reader = csv.DictReader(f)
            self.finished = set([row["mid"] for row in reader])

        for year in years:
            self.scrape_season(year)
