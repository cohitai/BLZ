import requests
import logging
import json
import pandas as pd
import time
import nltk
import glob
from collections import deque
import csv
from ast import literal_eval
import re
import sqlite3
from sqlite3 import Error
import sys
import os
import preprocessing as pp
csv.field_size_limit(sys.maxsize)
# logging.basicConfig(level=logging.DEBUG)


class LivingDocs:
    """ extracts all data from livingsdocs."""

    # Api Token (production server)
    api_key1 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6InB1YmxpYy1hcGk6cmVhZCIsIm5hbWUiOiJUZXN0IiwicHJvamVjdElkIjoxLCJjaGFubmVsSWQiOjEsInR5cGUiOiJjbGllbnQiLCJqdGkiOiJlMjg1NWM1YS0xNGRiLTRkZTUtYjJlYS0wNTgwM2UwNzkzYTQiLCJjb2RlIjoiZTI4NTVjNWEtMTRkYi00ZGU1LWIyZWEtMDU4MDNlMDc5M2E0IiwiaWF0IjoxNTg5MzU3ODUxfQ.o6nTZdozih2vz9wpEXNJOyh60C9vjzu0ofLukcADiTg"

    def __init__(self, source, target,
                 output_directory):

        self.source = None
        self.target = None
        self.log_file = None
        self.output_path = None
        self.status_id = None
        self.sql_path = None

    def initiate_paths(self, **kwargs):

        """update existing paths"""

        if kwargs.get('log_file_path', None):
            self.log_file = kwargs.get('log_file_path', None)

        if kwargs.get('source_path', None):
            self.source = kwargs.get('source_path', None)

        if kwargs.get('target_path', None):
            self.target = kwargs.get('target_path', None)

        if kwargs.get('output_path', None):
            self.output_path = kwargs.get('output_path', None)

        if kwargs.get('sql_path', None):
            self.sql_path = kwargs.get('sql_path', None)

    def _retrieve_logs(self, pub_event_id):

        """method to retrieve logs rows from the server
        PARAM -----------
        pub_event_id: an integer
        RETURNS a list"""

        url = 'https://api.berliner-zeitung.de/api/v1/publicationEvents?after={1}&limit=1000&access_token={0}'.format(
            self.api_key1, pub_event_id)
        req = requests.get(url)
        l = literal_eval(req.content.decode("UTF-8"))
        items = []

        for i in range(len(l)):
            item = l[i]
            items.append((item['id'], item['documentId'], item['documentType'], item['createdAt'], item['eventType'],
                          item['projectId'], item['channelId'], item['publicationId'], item['contentType']))

        return items

    def create_new_sources_file(self, file_name, pub_event_id=0, replace=True):

        """download log data from Livingsdocs"""

        path = self.output_path+"{0}.csv".format(file_name)

        with open(path, 'w') as file:

            csv_out = csv.writer(file)
            csv_out.writerow(['id', 'documentId', "documentType", "createdAt", 'eventType', 'projectId', 'channelId',
                              'publicationId', 'contentType'])

            pause_counter = 0

            while True:

                items = self._retrieve_logs(pub_event_id)

                for s in items:
                    csv_out.writerow(s)

                # break condition
                if not items:
                    break

                pause_counter += 1
                pub_event_id = items[-1][0]

                if not pause_counter % 50:
                    time.sleep(30)
                    pause_counter = 0

                logging.info("pub_event_id: {0}".format(pub_event_id))

        if replace:
            self.log_file = path
            logging.info("log_file written to {0}:".format(self.log_file))
            logging.info("last publication event: {0}".format(pub_event_id))

    def update_log_file(self, path=None):

        """ log_file update by appending an existing csv log file from Livingdocs server.

        --------
        :param: path to log_file.
        :returns: the last publication event documented at path. """

        if not path:
            path = self.log_file

        logging.info("log file is updated at:{0}".format(path))

        with open(path, 'r') as f:

            # the last id number of a retrieved row in the file.

            n = int(next(reversed(list(csv.reader(f))))[0])
            logging.info("Last event documented:{0}".format(n))

        with open(path, 'a') as file:

            csv_out = csv.writer(file)
            pub_event_id = n

            while True:

                items = self._retrieve_logs(pub_event_id)
                # pub_event_id += 1000

                for s in items:
                    csv_out.writerow(s)

                if not items:
                    break

                pub_event_id = items[-1][0]

        with open(path, 'r') as f:

            logging.info("new last event documented: {0}".format(int(next(reversed(list(csv.reader(f))))[0])))

        return n

    def return_last_file(self, path=None):

        """ function recieves a destination and retrieves the path of the last file by
        self- ordering."""

        if not path:
            path = self.source + "*.csv"
        file_list = glob.glob(path)

        if not file_list:
            return ''
        return sorted(file_list, key=lambda x: int(x.split('_')[-1][:-4]))[-1]

    def get_last_row(self, file):

        """function retrieves the last row."""

        with open(file, 'r') as f:
            return deque(csv.reader(f), 1)[0]

    def get_range(self, path):

        """function retrieves last documentId from most recent csv file."""

        mydata = pd.read_csv(self.return_last_file(path))
        pre = int(mydata.iloc[0, :]["systemdata.documentId"])
        post = int(mydata.iloc[-1, :]["systemdata.documentId"])
        return pre, post

    @staticmethod
    def count_lines(path):

        """function returns number of lines in a csv file"""

        with open(path) as file:
            return sum(1 for row in file)

    @staticmethod
    def relu(x):

        """relu help function"""

        if x >= 0:
            return x
        else:
            return 0

    @staticmethod
    def crop_query(df, log_id=0, unpublish=False):

        """ function reduces df to documentType = "article" and crops it at "id" """

        if not unpublish:
            return df.query('documentType == "article" & id > {0} & eventType != "unpublish"'.format(log_id))
        else:
            return df.query('documentType == "article" & id > {0} & eventType == "unpublish"'.format(log_id))

    def extract_doc(self, docid):

        """function retrieves json object (dict) from document.
        :param docid - integer.
        :returns - json"""

        url = 'https://api.berliner-zeitung.de/blz/v1/print/document?documentId={1}&access_token={0}'.format(
            self.api_key1, docid)
        req = requests.get(url)

        return json.loads(req.content)

    def create_files_database(self):

        """function creates data frame of listed downloaded files. """

        sorted_list = sorted(glob.glob(self.source + "*.csv"), key=lambda file: int(''.join(filter(str.isdigit, file))))
        d = []
        for file in sorted_list:
            d.append(
                {
                    'file': file,
                    'pre': self.get_range(file)[0],
                    'post': self.get_range(file)[1],
                    'count': self.count_lines(file)
                }
            )
        return pd.DataFrame(d)

    @staticmethod
    def match_file_to_docid(d, doc_id):

        """function locates the file that contains doc_id.
        :param d: a DataFrame describing csv files.
        :param doc_id: integer id of a document.
        :returns path of the csv file in the database which contains doc_id."""

        try:
            return d.query('pre <= {0} & post >= {0}'.format(doc_id))["file"].to_list()[0]
        except IndexError:
            return d.query('post <= {0}'.format(doc_id))["file"].to_list()[-1]
        except IndexError:
            return None

    def sizes_list(self, a, b):

        """helper function to create a split.
           :param a - recent file available capacity (int).
           :param b - length of all articles to be stored in the current update (int).

           :returns:  list 'l_sizes' with integers.

           len(l_sizes) = number of files needed for the update:
           its elements are the number of articles to be stored in each file."""

        a = self.relu(a)
        l_sizes = []

        if b == 0:
            return l_sizes

        while b // 1000 >= 0:
            l_sizes.append(min(a, b))
            b -= min(a, b)
            if b == 0:
                return l_sizes
            a = 1000

        return l_sizes

    def get_articles_from_server(self, id_start=0):

        """function retrieves articles information from server and
           saves them as csv's in self.source """

        df1 = pd.DataFrame()

        # generate a generator
        df = pd.read_csv(self.log_file)
        all_articles = set(self.crop_query(df, id_start)["documentId"])
        deleted_articles = set(self.crop_query(pd.read_csv(self.log_file), unpublish=True)["documentId"])
        effective_articles = sorted(all_articles - deleted_articles)
        articles = (self.extract_doc(x) for x in effective_articles)

        i = 0
        for i, item in enumerate(articles):
            i += 1

            try:
                logging.info("current entry keys: {0}".format(item["livingdoc"].keys()))
                logging.info("loop number {0}: ".format(i))
                df2 = pd.DataFrame(pd.json_normalize(item))
                df1 = df1.append(df2, ignore_index=True, sort=False)

            except ConnectionError:
                time.sleep(60)
                logging.info("current entry keys: {0}".format(item["livingdoc"].keys()))
                logging.info("loop number {0}: ".format(i))
                df2 = pd.DataFrame(pd.json_normalize(item))
                df1 = df1.append(df2, ignore_index=True, sort=False)

            except KeyError:
                pass

            if not i % 1000:
                label = int(df1["systemdata.documentId"].head(1))
                df1.to_csv(self.source + "Livingsdoc_" + str(label) + ".csv", encoding='utf-8', index=False)
                df1 = pd.DataFrame()
                time.sleep(60)

        # save tail - residuel information

        if (i % 1000) and i != 0:
            label = int(df1["systemdata.documentId"].head(1))
            df1.to_csv(self.source + "Livingsdoc_" + str(label) + ".csv", encoding='utf-8', index=False)

        self.status_id = self.get_last_row(self.log_file)[0]
        logging.info("Current status of the server:{0}".format(self.status_id))

        ####

        current_time = str(time.time())

    def update_server(self, update_eventid=None):

        """ a method to update the server from a given event id."""

        if update_eventid:
            after = update_eventid
        else:
            after = self.update_log_file()

        logging.info("updating database, starting at event id  = {0}".format(after))

        #
        d = self.create_files_database()

        # exclude empty source directory case
        if d.shape == (0, 0):
            logging.info("directory: {0} is empty, consider using get_articles_from_server instead".format(self.source))
            return
        
        df = pd.read_csv(self.log_file)

        df_re = self.crop_query(df, after)
        df_re = df_re.drop_duplicates(subset='documentId', keep="last")

        # find deleted articles

        deleted_articles = set(self.crop_query(pd.read_csv(self.log_file), unpublish=True)["documentId"])

        n = int(d["post"][-1:])

        logging.info("last document in the database: {0}".format(n))
        #
        mask = df_re['documentId'] > n

        # to append last file

        df_up = df_re[mask]['documentId']
        df_do = df_re[~mask]['documentId']

        # appending new docs

        #
        logging.info("updating new documents")
        #
        articles = sorted(set(df_up) - deleted_articles)

        l = self.sizes_list(a=1000 - int(d["count"][-1:]), b=len(articles))
        dest = self.return_last_file(self.source + "*.csv")
        df1 = pd.read_csv(dest)

        while l:

            logging.info("{0}".format(l))
            x = l.pop(0)
            articles_x = articles[0:x]
            articles = articles[x:]
            articles_ite = (self.extract_doc(x) for x in articles_x)

            i = 0

            for i, item in enumerate(articles_ite):

                i += 1

                #
                if item is None:
                    continue

                try:
                    logging.info("current entry keys: {0}".format(item["livingdoc"].keys()))
                    logging.info("loop number {0}: ".format(i))
                    df2 = pd.DataFrame(pd.json_normalize(item))
                    df1 = df1.append(df2, ignore_index=True, sort=False)

                except ConnectionError:
                    time.sleep(60)
                    logging.info("current entry keys: {0}".format(item["livingdoc"].keys()))
                    logging.info("loop number {0}: ".format(i))
                    df2 = pd.DataFrame(pd.json_normalize(item))
                    df1 = df1.append(df2, ignore_index=True, sort=False)
                except KeyError:
                    pass

            df1.to_csv(dest, encoding='utf-8', index=False)

            if l:
                df1 = pd.DataFrame()
                next_docid = articles[0]
                dest = self.source + "Livingsdocs_{0}.csv".format(next_docid)

        # updating old docs

        #
        logging.info("Updating old documents:")
        #
        l_do = df_do.tolist()
        i = 0
        d = self.create_files_database()

        for DocId in l_do:
            #
            logging.info("updating DocumentId:{0}".format(DocId))
            #
            file = self.match_file_to_docid(d, DocId)
            df1 = pd.read_csv(file)
            item = self.extract_doc(DocId)
            i += 1

            try:
                logging.info("current entry keys: {0}".format(item["livingdoc"].keys()))
                logging.info("loop number {0}: ".format(i))
                df2 = pd.DataFrame(pd.json_normalize(item))
                df1 = df1.append(df2, ignore_index=True, sort=False)

            except ConnectionError:
                time.sleep(60)
                logging.info("current entry keys: {0}".format(item["livingdoc"].keys()))
                logging.info("loop number {0}: ".format(i))
                df2 = pd.DataFrame(pd.json_normalize(item))
                df1 = df1.append(df2, ignore_index=True, sort=False)

            except KeyError:
                pass

            df1 = df1.drop_duplicates(subset='systemdata.documentId', keep="last")
            df1 = df1.sort_values("systemdata.documentId", axis=0)
            #
            logging.info("updating file:{0}".format(file))
            #
            df1.to_csv(file, encoding='utf-8', index=False)

            ####

        self.status_id = self.get_last_row(self.log_file)[0]
        logging.info("Current status of the server:{0}".format(self.status_id))

        ####

    # remove all unpublished files:
        self._remove_deleted()

    @staticmethod
    def extract_text_from_livingdoc_obj(obj):
        start = '\'text\': '
        end = '}}'
        ite = re.finditer(start, obj)
        text_list = [tup.span()[1] for tup in ite]
        text_list_1 = [obj[x: obj[x:].find(end) + x] for x in text_list]
        text_list_2 = "\n".join(
            [term.replace("<strong>", "").replace("</strong>", "").replace("<em>", "").replace("</em>", "") for term
             in text_list_1])
        try:
            return text_list_2[re.search('[a-zA-Z]', text_list_2).span()[0]:]
        except AttributeError:
            return text_list_2

    # obsolete

    @staticmethod
    def json_to_text(obj):

        """recieves a str (obj) in a json structure ;
        returns a string. """

        obj_l = literal_eval(obj)
        xx = list(filter(lambda x: x['component'] == 'lead-p' or x['component'] == 'p', obj_l))
        xxx = []
        for i in range(len(xx)):
            try:
                xxx.append(xx[i]['content']['text'])
            except KeyError:
                pass
        xxxx = [term.replace("<strong>", "").replace("</strong>", "").replace("<em>", "").replace("</em>", "") for term
                in xxx]
        xxxxx = "\n".join([term for term in xxxx]) + "\n"

        return xxxxx

    @staticmethod
    def make_consistent_text(desc, text):

        """adding desc in front of a text unless unnecessary."""

        try:
            if len(desc) < 10:
                return text
        except TypeError:
            return text
        if desc.endswith("..."):
            desc = desc[:-3]

        if text.find(desc) >= 0:
            return text
        else:
            try:
                return "\n".join([desc, text])
            except Exception:
                return text

    @staticmethod
    def find_author(string):

        """find author name within a json string."""

        try:
            rex1 = re.search('author\': \'', string)
            e1 = rex1.end()
            e2 = string[e1:].find('\'}')
            return string[e1:e1 + e2]
        except AttributeError:
            return ''

    def make_operations_on_columns(self, df):

        """ add the text and author columns. """

        df['text'] = df['livingdoc.content'].apply(self.extract_text_from_livingdoc_obj)
        df['text'] = df.apply(lambda row: self.make_consistent_text(row['metadata.description'], row['text']), axis=1)
        df['author'] = df['livingdoc.content'].apply(self.find_author)
        df['url'] = df['metadata.routing.path'].fillna('')
        df["url"] = df['url'].apply(lambda row: "www.berliner-zeitung.de" + row)
        # Warning !! : replace nans with German.
        df['metadata.language.label'].fillna('German', inplace=True)
        df.drop(df[df['metadata.language.label'] != 'German'].index, inplace=True, axis=0)

    @staticmethod
    def create_sorted_list(source):
        return sorted(glob.glob(source), key=lambda x: int(x.split('_')[-1][:-4]))

    def transform(self):

        header = ['systemdata.documentId', 'metadata.category.path', 'metadata.title', 'metadata.description',
                  'metadata.publishDate', 'metadata.language.label', 'text', 'author', 'url']

        sorted_list = self.create_sorted_list(self.source + "*.csv")

        for i in range(len(sorted_list)):
            df = pd.read_csv(sorted_list[i])
            self.make_operations_on_columns(df)
            df[header].rename(columns={"systemdata.documentId": "documentId", "metadata.category.path": "section",
                                       "metadata.title": "title", 'metadata.publishDate': "publishDate",
                                       'metadata.language.label': "language", 'metadata.description': "description"}).to_csv(
                self.target + "Livingsdocs" + "_" + str(df["systemdata.documentId"].iloc[0]) + ".csv", index=False)

    def _remove_deleted(self):

        """method removes articles if an unpublished event exists in log"""

        deleted_articles = sorted(set(self.crop_query(pd.read_csv(self.log_file), unpublish=True)["documentId"]))
        logging.info("There are {0} unpublished articles".format(len(deleted_articles)))
        d = self.create_files_database()

        # resolve the 'republish' bug
        df_logs = pd.read_csv(self.log_file)
        for i in range(len(deleted_articles)):
            if df_logs['eventType'].loc[df_logs['documentId'] == deleted_articles[i]].to_list()[-1] == 'publish':
                continue

            current_path = self.match_file_to_docid(d, deleted_articles[i])
            logging.info("UNPUBLISHED: {0}:{1}".format(deleted_articles[i], current_path))
            if current_path:
                df = pd.read_csv(current_path)
                if deleted_articles[i] in df["systemdata.documentId"].tolist():
                    indexname = df[(df['systemdata.documentId'] == deleted_articles[i])].index
                    df.drop(indexname, inplace=True)
                    df.to_csv(current_path, index=False)
                    logging.info("a drop is made:{0}".format(current_path))

    @staticmethod
    def _create_connection(path):
        """create an SQL .db file"""

        connection = None

        try:
            connection = sqlite3.connect(path)
            logging.info("Connection to SQLite DB successful")
            logging.info("SQL database was created at: {0}".format(path))
        except Error as e:
            print(f"The error '{e}' occurred")

        return connection

    @staticmethod
    def _create_table(conn, create_table_sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            c = conn.cursor()
            c.execute(create_table_sql)
        except Error as e:
            logging.info(e)

    def sql_transform(self, file_name):
        """
        method to transform csv files into a SQL database
        :param file_name : a string
        :return: sql file
        """
        path = self.output_path+file_name

        # overwrite an existing file
        if os.path.exists(path):
            os.remove(path)
            logging.info("file at: {0} already exists! deleting file...".format(path))

        conn = self._create_connection(path)
        with open(self.log_file, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            l = next(csvreader)
            l.pop(5)
            l.pop(5)

        sql_create_Livingdocs_log_table = """ CREATE TABLE IF NOT EXISTS Livingdocs_log (
                                                {0} integer PRIMARY KEY,
                                                {1} integer,
                                                {2} text,
                                                {3} text,
                                                {4} text,
                                                {5} integer,
                                                {6} text,
                                                FOREIGN KEY ({1})
                                                REFERENCES Livingdocs_articles ({1})
                                            ); """.format(*l)

        if conn is not None:

            # create Livingdocs_log table
            self._create_table(conn, sql_create_Livingdocs_log_table)

        with open(self.log_file, 'r') as fin:
            # csv.DictReader uses first line in file for column headings by default
            dr = csv.DictReader(fin)  # comma is default delimiter
            to_db = [(i['id'], i['documentId'], i['documentType'], i['createdAt'], i['eventType'], i['publicationId'], i['contentType']) for i in dr]

        cur = conn.cursor()
        cur.executemany("INSERT INTO Livingdocs_log (id,documentId,documentType,createdAt,eventType,publicationId,contentType) VALUES (?,?,?,?,?,?,?);", to_db)
        conn.commit()

        sorted_list = sorted(glob.glob(self.target+"*.csv"), key=lambda x: int(x.split('_')[-1][:-4]))

        # putting header
        with open(sorted_list[0]) as file:
            csvreader = csv.reader(file)
            l = next(csvreader)

        sql_create_Livingdocs_articles_table = """CREATE TABLE IF NOT EXISTS Livingdocs_articles (
                                                {0} integer PRIMARY KEY,
                                                {1} text,
                                                {2} text,
                                                {3} text,
                                                {4} text,
                                                {5} text,
                                                {6} text,
                                                {7} text,
                                                {8} text
                                            );""".format(*l)

        if conn is not None:
            # create Livingdocs_articles table
            self._create_table(conn, sql_create_Livingdocs_articles_table)

        for file_path in sorted_list:
            with open(file_path, 'r') as file:
                dr = csv.DictReader(file)  # comma is default delimiter
                to_db = [(i['documentId'], i['section'], i['title'], i['description'], i['publishDate'], i['language'], i['text'], i['author'], i['url']) for i in dr]
                cur.executemany("INSERT INTO Livingdocs_articles (documentId,section,title,description,publishDate,language,text,author,url) VALUES (?,?,?,?,?,?,?,?,?);", to_db)
                conn.commit()


        #### Livingdocs images

        header_dict = {"systemdata.documentId": "documentid", "metadata.category.path": "section",
                       "metadata.title": "title", 'metadata.publishDate': "publishdate",
                       "metadata.teaserImage.url": "picture", "systemdata.documentType": "type"}



        sql_create_Livingdocs_images_table = """CREATE TABLE IF NOT EXISTS Livingdocs_images (
                                                        {0} integer PRIMARY KEY,
                                                        {1} text,
                                                        {2} text,
                                                        {3} text,
                                                        {4} text,
                                                        {5} text 
                                                        REFERENCES Livingdocs_articles ({0})     
                                                    );""".format(*list(header_dict.values()))

        if conn is not None:
            # create Livingdocs_images table
            self._create_table(conn, sql_create_Livingdocs_images_table)

        sorted_list = sorted(glob.glob(self.source + "*.csv"), key=lambda x: int(x.split('_')[-1][:-4]))
        l2 = []

        for f_path in sorted_list:
            with open(f_path) as f:
                l1 = next(csv.reader(f))
                if 'metadata.teaserImage.url' in l1:
                    l2.append(f_path)

        for file_path in l2:
            with open(file_path, 'r') as file:
                dr = csv.DictReader(file)  # comma is default delimiter
                to_db = [(i['systemdata.documentId'], i['metadata.category.path'], i['metadata.teaserImage.url'],
                          i['metadata.publishDate'], i['metadata.title'], i["systemdata.documentType"]) for i in dr]
                to_db_red = [x for x in to_db if x[2] != '']
                cur.executemany(
                    "INSERT INTO Livingdocs_images (documentid,section,picture,publishdate,title,type) VALUES (?,?,?,?,?,?);",
                    to_db_red)
                conn.commit()


        self.sql_path = self.output_path+file_name
        conn.close()

    # def create_livingdocs_df(self,list_website):

    def create_livingdocs_df(self, days=30):

        """method to retrieve and create a database directly from the livingdocs database
        :param: list_website, a list of docids of articles currently online.
        :returns: database of last 10 days articles, replacing the one from webscrapper. """

        sql_query_last_n_days = """SELECT * FROM Livingdocs_articles WHERE publishdate BETWEEN datetime('now', '-{0} days') AND datetime('now', '+1 days');""".format(days)
        conn = sqlite3.connect(self.sql_path, check_same_thread=False)
        cur = conn.cursor()
        cur.execute(sql_query_last_n_days)
        data = cur.fetchall()
        df = pd.DataFrame(data,
                          columns=['DocId', 'Section', 'Title','description', 'publishing_date', 'language', 'Text',
                                   'Author', 'Url'])


        df["Full Text"] = df["Title"] + df["Text"]
        df["Tokenized_sents"] = df["Full Text"].apply(nltk.sent_tokenize)
        df["Tokenized_sents"] = df["Tokenized_sents"].apply(pp.clean_text_from_text)
        return df
