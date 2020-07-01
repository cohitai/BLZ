import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import preprocessing as pp


class WebScrapper:
    def __init__(self, URL="https://www.berliner-zeitung.de"):
        self.URL = URL

    def _get_main_categories(self):

        """ method returns all url- links in URL as tupled list consists of topic and link.
        :returns
        avg: tuples list. Each tuple has 2 arguments:
                          (topic, url link).
        """
        page = requests.get(self.URL)
        topic_list = []
        soup = BeautifulSoup(page.content, "html.parser").find(class_=
                                                               "m-main-navigation__list")
        soup = soup.findAll('a', attrs={'href': re.compile("^/")})
        for link in soup:
            topic_list.append(link.get('href'))
        complete_link_list = [self.URL + x for x in topic_list]

        return list(zip(topic_list, complete_link_list))

    def _get_all_links(self, categories_and_links):

        """function receives tuples list (topics and
        links) of the main URL and returns all available article in URL.
        Argument: list of tuples.

        :returns a list, all url links of articles available online at URL.
        """

        link_list = []
        for item in categories_and_links:
            page = requests.get(item[1])
            soup = BeautifulSoup(page.content, "html.parser").findAll('a',
                                                                      attrs={'href': re.compile("^/")})
            for link in soup:
                string = link.get('href')

                """here it is being checked whether 
                   the link in soup has the correct form.
                """

                if string.startswith(item[0]) and string[-1].isdigit():
                    link_list.append(string)
        URL_list = [self.URL + x for x in link_list]

        return URL_list

    @staticmethod
    def _create_database(URL_list):

        """function receives a list of urls
        and return a pd database.

        Argument: list of links of articles.

        Returns: a pd Data frame with features:
                'Date','Author','Title','Text','Url','Section'.
        """

        db = []

        for URL_link in URL_list:
            link = URL_link.rsplit('/', 3)[2]
            page = requests.get(URL_link)
            soup = BeautifulSoup(page.content, "html.parser")
            body_text = soup.find_all('p', class_="a-paragraph")
            date = soup.find('p', class_=["a-author date-and-author"]).find(class_=
                                                                            "ld-date-replace")
            author = soup.find('p', class_=["a-author date-and-author"]).find(class_=
                                                                              "ld-author-replace")
            title = soup.title.get_text(" ")
            body = ""
            for line in body_text:
                body += line.get_text(" ") + " "

            db.append([date, author, title, body, URL_link, link])

        return pd.DataFrame(db, columns=['Date', 'Author', 'Title', 'Text', 'Url', 'Section'])

    def create_df(self,save=False, df_path="/home/blz/Desktop/output/df.csv"):
        """main method for scrapping """
        url_list = self._get_all_links(self._get_main_categories())
        df = pp.edit_data_frame(self._create_database(url_list))

        # saving df as a csv file.
        if save:
            print("saving web scrapping file at:",df_path)
            df.to_csv(df_path)

        return df
