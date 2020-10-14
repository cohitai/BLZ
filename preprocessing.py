import nltk
import nltk.data
from nltk.stem.snowball import GermanStemmer
import re
from datetime import datetime
import pandas as pd
from itertools import groupby
import os

# Global variable: stop words from a file.

stop_words_path = os.getcwd()+"/stopwords.txt"

with open(stop_words_path, 'r') as file:
    stop_words = [word[0:-1] for word in file]


def edit_data_frame(df):

    """function applies several feature manipulations on the data frame."""

    # extracting Author name from string.

    df["Author"] = df["Author"].apply(lambda x: str(x)[32:-1].split("<")[0])

    # combining Title and Body Text into the single feature "Full_text".

    df["Full_text"] = df["Title"] + df["Text"]

    # extracting features "publishing_date" and "time"; deleting the original "Date" feature.

    df['Date'] = df['Date'].astype('str')
    pattern_date = "\d{1,2}\.\d{1,2}\.\d{2,4}"
    pattern_time = "\d{1,2}\:\d{1,2}"
    df['publishing_date'] = [datetime.strptime(re.findall(pattern_date, d)[0], "%d.%m.%Y") for d in df['Date']]
    df['time'] = [datetime.strptime(re.findall(pattern_time, d)[0], "%H:%M") for d in df['Date']]
    del df["Date"]

    # creating "Tokenized_sents" feature.

    df["Tokenized_sents"] = df["Full_text"].apply(nltk.sent_tokenize)
    df["Tokenized_sents"] = df["Tokenized_sents"].apply(clean_text_from_text)
    df["DocId"] = df["Url"].apply(fetch_docid)

    return df


def load_data_frame():

    """function loads an existing data frame."""

    return pd.read_csv("/home/blz/Desktop/output/df.csv", index_col=0)


def clean_text(path):

    """function cleans text.

       Receiving a path to a text file,
       applies all cleaning functions defined
       below,

       returns the clean text.

       Argument: a string: (path to a text file.)
       Returns: a list of strings, (cleaned text.)
                                                    """

    with open(path) as file1:
        return clean_text_from_text(file1)


def clean_text_from_text(text):

    """function cleans text;
       :param text, a string type ;
       applies all cleaning functions defined below.
       :return  a list of strings- a cleaned text."""

    _clean_text = []

    # All functions below are the basic operations which we use to clean the raw text.

    for line in text:

        token_sent = nltk.word_tokenize(line)
        token_sent = make_lower(token_sent)
        token_sent = replace_dot_followed_by_number(token_sent)
        token_sent = remove_special_chars(token_sent)
        token_sent = replace_hyphen_with_space(token_sent)
        token_sent = remove_numbers(token_sent)
        token_sent = remove_multiple_whitespace(token_sent)
        token_sent = bigrams_replacer(token_sent)
        token_sent = remove_stop_words(token_sent)
        token_sent = remove_empty_words(token_sent)
        token_sent = german_stemmer(token_sent)
        _clean_text.append(token_sent)
    return _clean_text


def make_lower(tok_list):

    """function removes capitalization"""

    return [word.lower() for word in tok_list]


def replace_dot_followed_by_number(tok_list):

    """function removes dots which are followed by a number.  (i.e. '1.' '2.' '5.' )."""

    return [re.sub('\d+\.', '', sent) for sent in tok_list]


def remove_special_chars(tok_list):

    """ function removes all characters
        which are not A-Z,a-z, umlaute, digits,
        whitespaces, or a hyphen."""

    return [re.sub('[^A-ZüÜßäÄöÖa-z0-9 -]+', '', sent) for sent in tok_list]


def replace_hyphen_with_space(tok_list):

    """function replaces hyphen with a whitespace."""

    temp = [sent.replace("-", " ") for sent in tok_list]
    l0 = []
    for w in temp:
        l0 += w.split(" ")
    return l0


def remove_numbers(tok_list):

    """ function removes digits. """

    return [re.sub(r'\d+', '', sent) for sent in tok_list]


def remove_multiple_whitespace(tok_list):

    """function removes multiple whitespaces. """

    return [re.sub(' +', ' ', sent) for sent in tok_list]


def remove_empty_words(tok_list):

    """function removes empty words. """

    return [word for word in tok_list if word]


bi_grams = ["new york", "bundesrepublik deutschland", "baden württemberg", "samsung galaxy",
            "geld sparen", "rheinland pfalz", "herzlichen dank", "wichtige information", "fußball wm",
            "social network", "prenzlauer berg", "humboldt universität", "wissenschaftlicher mitarbeiter"]


def bigrams_replacer(tok_list):

    """function replaces bi- grams by a pair of words."""

    for item in bi_grams:

        # getting the indices of the bi- grams.
        matched_first_list = [i for i, x in enumerate(tok_list) if x == item.split(" ")[0]]
        matched_second_list = [i for i, x in enumerate(tok_list) if x == item.split(" ")[1]]

        # intersection terms to find bi- grams occurrences.
        matched_indices = list_intersection(matched_first_list, [x-1 for x in matched_second_list])

        # boolean if to replace bi-grams by a connected single word when there are matchings.

        if matched_indices:
            for ind in matched_indices[::-1]:
                del tok_list[ind+1]
                tok_list[ind] = "".join(item.split(" "))
        return tok_list


# static
def list_intersection(list_1, list_2):

    """function receives 2 lists ; returns their intersection."""

    list_3 = [value for value in list_1 if value in list_2]

    return list_3


def remove_stop_words(tok_list):

    """function removes all stop-words and words with length smaller than 2"""

    return [word for word in tok_list if word not in stop_words and len(word) > 2]


def german_stemmer(tok_list):

    """function applies a German stemmer on a tokenized list."""

    return [GermanStemmer().stem(word) for word in tok_list]


def fetch_docid(url):

    """function extracts DocId from url"""

    try:
        return [int(''.join(group)) for key, group in groupby(iterable=url, key=lambda e: e.isdigit()) if key][-1]
    except IndexError:
        return None
