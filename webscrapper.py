import logging
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import preprocessing as pp


class WebScrapper:
    """Scrapping Berliner-Zeitung website and creates a dataframe describing articles which are currently online."""

    def __init__(self, output_path, url="https://www.berliner-zeitung.de"):
        self.url = url
        self.output_path = output_path

    def _get_main_categories(self):

        """ method returns all url- links in url as tupled list consists of topic and link.
        :returns
        avg: tuples list. Each tuple has 2 arguments:
                          (topic, url link).
        """
        page = requests.get(self.url)
        topic_list = []
        soup = BeautifulSoup(page.content, "html.parser").find(class_="m-main-navigation__list")
        soup = soup.findAll('a', attrs={'href': re.compile("^/")})
        for link in soup:
            topic_list.append(link.get('href'))
        complete_link_list = [self.url + x for x in topic_list]

        l = list(zip(topic_list, complete_link_list))
        l.append(tuple(('/news', 'https://www.berliner-zeitung.de/news')))
        return l
        # return list(zip(topic_list, complete_link_list))

    def _get_all_links(self, categories_and_links):

        """function receives tuples' list (topics and
        links) from the main;
        :param: list of tuples.
        :returns a list of all online articles in the url.
        """

        link_list = []
        for item in categories_and_links:
            page = requests.get(item[1])
            soup = BeautifulSoup(page.content, "html.parser").findAll('a',
                                                                      attrs={'href': re.compile("^/")})
            for link in soup:
                string = link.get('href')

                """here it is checked whether 
                   the link in soup has the correct form."""

                if string.startswith(item[0]) and string[-1].isdigit():
                    link_list.append(string)
        url_list = [self.url + x for x in link_list]

        return url_list

    @staticmethod
    def _create_database(url_list):

        """function receives a list of urls
        and return a pd database.

        :param: list of links of articles.

        :returns: a Data frame with features:
                'Date','Author','Title','Text','Url','Section'."""

        db = []

        for url_link in url_list:
            try:
                link = url_link.rsplit('/', 3)[2]
                page = requests.get(url_link)
                soup = BeautifulSoup(page.content, "html.parser")
                body_text = soup.find_all('p', class_="a-paragraph")
                date = soup.find('p', class_=["a-author date-and-author"]).find(class_="ld-date-replace")
                author = soup.find('p', class_=["a-author date-and-author"]).find(class_="ld-author-replace")
                title = soup.title.get_text(" ")
                body = ""
                for line in body_text:
                    body += line.get_text(" ") + " "
            except AttributeError:
                continue

            db.append([date, author, title, body, url_link, link])

        return pd.DataFrame(db, columns=['Date', 'Author', 'Title', 'Text', 'Url', 'Section'])

    def create_df(self, save=False):

        """main method for scrapping"""

        url_list = self._get_all_links(self._get_main_categories())
        df = pp.edit_data_frame(self._create_database(url_list))

        # saving df as a csv file.
        if save:
            logging.info("web scrapping is successful, file saved at:{0}".format(self.output_path))
            df.to_csv(self.output_path + "df.csv")

        return df

    @staticmethod
    def load_data_frame(path):

        """function loads an existing data frame."""

        return pd.read_csv(path, index_col=0)
