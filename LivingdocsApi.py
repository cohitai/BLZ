import requests
import logging
import json
import pandas as pd
import time
import glob
from collections import deque
import csv
from ast import literal_eval
import re

logging.basicConfig(level=logging.DEBUG)


#logging.basicConfig(filename='example.log',level=logging.DEBUG)
#logging.debug('This message should go to the log file')
#logging.info('So should this')
#logging.warning('And this, too')

###### hello 23.6.20 15:50


class LivingDocs:
    # Api Token for the production server
    api_key1 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6InB1YmxpYy1hcGk6cmVhZCIsIm5hbWUiOiJUZXN0IiwicHJvamVjdElkIjoxLCJjaGFubmVsSWQiOjEsInR5cGUiOiJjbGllbnQiLCJqdGkiOiJlMjg1NWM1YS0xNGRiLTRkZTUtYjJlYS0wNTgwM2UwNzkzYTQiLCJjb2RlIjoiZTI4NTVjNWEtMTRkYi00ZGU1LWIyZWEtMDU4MDNlMDc5M2E0IiwiaWF0IjoxNTg5MzU3ODUxfQ.o6nTZdozih2vz9wpEXNJOyh60C9vjzu0ofLukcADiTg"

    def __init__(self, source="/home/blz/Desktop/Livingdocs2/", target="/home/blz/Desktop/Livingsdocs2_files/"
                 , output_directory="/home/blz/Desktop/output/"):

        self.source = source
        self.target = target
        self.log_file = None
        self.output_path = output_directory
        self.status_id = None

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

        path = "/home/blz/Desktop/output/{0}.csv".format(file_name)

        with open(path, 'w') as file:

            csv_out = csv.writer(file)
            csv_out.writerow(['id', 'documentId', "documentType", "createdAt", 'eventType', 'projectId', 'channelId',
                              'publicationId', 'contentType'])

            # start from the first item.
            # pub_event_id = 0

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

                print(pub_event_id)

        if replace:
            self.log_file = path
            print("log_file written to:", self.log_file)
            print("last publication event:", pub_event_id)

    def update_log_file(self, path=None):

        """ log_file update by appending an existing csv log file from Livingdocs server.

        --------

        returns: n: the last publication event. """

        if not path:
            path = self.log_file

        with open(path, 'r') as f:

            # the last id number of a retrieved row in the file.

            n = int(next(reversed(list(csv.reader(f))))[0])
            print("Last id documented:", n)

        with open(path, 'a') as file:

            csv_out = csv.writer(file)
            pub_event_id = n

            while True:

                items = self._retrieve_logs(pub_event_id)
                pub_event_id += 1000

                for s in items:
                    csv_out.writerow(s)

                if not items:
                    break

        with open(path, 'r') as f:

            n = int(next(reversed(list(csv.reader(f))))[0])
            print("New last id documented:", n)

        return n

    def return_last_file(self, path=None):

        """ function recieves a distination and retrieves the path of the last file by
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
        return (pre, post)

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
    def crop_query(df, id=0, unpublish=False):

        """ function reduces df to documentType = "article" and crops it at "id" """
        if not unpublish:
            return df.query('documentType == "article" & id > {0} & eventType != "unpublish"'.format(id))
        else:
            return df.query('documentType == "article" & id > {0} & eventType == "unpublish"'.format(id))

    def extract_doc(self, DocId):

        """function retrieves json type object (dict) from document: DocId"""

        url = 'https://api.berliner-zeitung.de/blz/v1/print/document?documentId={1}&access_token={0}'.format(
            self.api_key1, DocId)
        req = requests.get(url)

        return json.loads(req.content)

    def create_files_database(self):

        """function creates data frame listing the files in source. """

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
    def match_file_to_docid(d, DocId):

        """function finds the file which contains DocId."""

        return d.query('pre <= {0} & post >= {0}'.format(DocId))["file"].to_list()[0]

    def sizes_list(self, a, b):

        """help function to create a split.

           PARAM: a - recent file available capacity (int).
                  b - length of all articles to be stored in the current update (int).

           RETURNS: list 'l' with integers.

           len(l) = number of files needed for the update.
           its elements are the number of articles to be stored in each file.
        """

        a = self.relu(a)
        l = []

        if b == 0:
            return l

        while b // 1000 >= 0:
            l.append(min(a, b))
            b -= min(a, b)
            if b == 0:
                return l
            a = 1000

        return l

    def get_articles_from_server(self, Id_start=0):

        """function extracts articles information from server and
           saves them as csv files in self.source """

        df1 = pd.DataFrame()

        # generate a generator
        df = pd.read_csv(self.log_file)
        all_articles = set(self.crop_query(df, Id_start)["documentId"])
        deleted_articles = set(self.crop_query(pd.read_csv(self.log_file), unpublish=True)["documentId"])
        effective_articles = sorted(all_articles - deleted_articles)
        articles = (self.extract_doc(x) for x in effective_articles)

        i = 0

        for i, item in enumerate(articles):
            i += 1

            try:
                print(item["livingdoc"].keys())
                print(i)
                df2 = pd.DataFrame(pd.json_normalize(item))
                df1 = df1.append(df2, ignore_index=True, sort=False)

            except ConnectionError:
                time.sleep(60)
                print(item["livingdoc"].keys())
                print(i)
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
        print("Current status of the server:", self.status_id)

        ####

        current_time = str(time.time())

        with open(self.output_path + 'status_{0}.txt'.format(current_time), 'w') as f:
            f.write(self.status_id)

    def update_server(self, update_eventid=None):

        """a method to update the server"""

        if update_eventid:
            after = update_eventid
        else:
            after = self.update_log_file()

        print("updating database, id number = ", after)
        #

        d = self.create_files_database()
        df = pd.read_csv(self.log_file)

        df_re = self.crop_query(df, after)
        df_re = df_re.drop_duplicates(subset='documentId', keep="last")

        # find deleted articles

        deleted_articles = sorted(set(self.crop_query(pd.read_csv(self.log_file), unpublish=True)["documentId"]))

        n = int(d["post"][-1:])

        print("last document in the database:", n)
        #
        mask = df_re['documentId'] > n

        # to append last file

        df_up = df_re[mask]['documentId']
        df_do = df_re[~mask]['documentId']

        ## appending new docs ##

        #
        print("updating new documents")
        #
        articles = sorted(set(df_up))

        l = self.sizes_list(a=1000 - int(d["count"][-1:]), b=len(articles))

        dest = self.return_last_file(self.source + "*.csv")
        df1 = pd.read_csv(dest)

        while l:

            print(l)
            x = l.pop(0)
            articles_x = articles[0:x]
            articles = articles[x:]
            articles_ite = (self.extract_doc(x) for x in articles_x)

            i = 0

            for i, item in enumerate(articles_ite):

                i += 1

                try:
                    print(item["livingdoc"].keys())
                    print(i)
                    df2 = pd.DataFrame(pd.json_normalize(item))
                    df1 = df1.append(df2, ignore_index=True, sort=False)

                except ConnectionError:
                    time.sleep(60)
                    print(item["livingdoc"].keys())
                    print(i)
                    df2 = pd.DataFrame(pd.json_normalize(item))
                    df1 = df1.append(df2, ignore_index=True, sort=False)
                except KeyError:
                    pass

            # label = int(df1["systemdata.documentId"].head(1))
            df1.to_csv(dest, encoding='utf-8', index=False)

            if l:
                df1 = pd.DataFrame()
                next_docid = articles[0]
                dest = self.source + "Livingsdocs_{0}.csv".format(next_docid)

        ## updating old docs ##

        #
        print("updating old documents")
        #

        l_do = df_do.tolist()
        i = 0
        d = self.create_files_database()

        for DocId in l_do:
            #
            print("updating DocumentId:", DocId)
            #
            file = self.match_file_to_docid(d, DocId)
            df1 = pd.read_csv(file)
            item = self.extract_doc(DocId)
            i += 1

            try:
                print(item["livingdoc"].keys())
                print(i)
                df2 = pd.DataFrame(pd.json_normalize(item))
                df1 = df1.append(df2, ignore_index=True, sort=False)

            except ConnectionError:
                time.sleep(60)
                print(item["livingdoc"].keys())
                print(i)
                df2 = pd.DataFrame(pd.json_normalize(item))
                df1 = df1.append(df2, ignore_index=True, sort=False)

            except KeyError:
                pass

            df1 = df1.drop_duplicates(subset='systemdata.documentId', keep="last")
            df1 = df1.sort_values("systemdata.documentId", axis=0)
            #
            print("updating file:", file)
            #
            df1.to_csv(file, encoding='utf-8', index=False)

            ####

        self.status_id = self.get_last_row(self.log_file)[0]
        print("Current status of the server:", self.status_id)

        ####

        current_time = str(time.time())

        with open(self.output_path + 'status_{0}.txt'.format(current_time), 'w') as f:
            f.write(self.status_id)

    @staticmethod
    def json_to_text(obj):

        """recieves a str (obj) in a json structure and returns a string. """

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

        """add desc at the beginning of a text unless unnecessary."""

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

        df['text'] = df['livingdoc.content'].apply(self.json_to_text)
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

        header = ['systemdata.documentId', 'metadata.category.path', 'metadata.title',
                  'metadata.publishDate', 'metadata.language.label', 'text', 'author', 'url']

        sorted_list = self.create_sorted_list(self.source + "*.csv")

        for i in range(len(sorted_list)):
            df = pd.read_csv(sorted_list[i])
            self.make_operations_on_columns(df)
            df[header].rename(columns={"systemdata.documentId": "documentId", "metadata.category.path": "section",
                                       "metadata.title": "title", 'metadata.publishDate': "publishDate",
                                       'metadata.language.label': "language"}).to_csv(
                self.target + "Livingsdocs" + str(df["systemdata.documentId"][0]) + ".csv", index=False)
