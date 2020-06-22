from bs4 import BeautifulSoup
import os


class XML_reader:

    """ class reads XML files from a directory (path) and writes raw text into the directory: new_dir

        Arguments: path - a path to XML files.
                   new_dir - a path to directory where the new files are to be stored.
    """
    def __init__(self, path="/home/blz/Desktop/BLZ_Artikel", new_dir="/home/blz/Desktop/BLZ_Artikel_2"):
        self.path = path
        self.new_dir = new_dir

    def transform_files(self):

        with open(self.path, 'r') as old_file:
            soup = BeautifulSoup(old_file.read())
            for script in soup(["script", "style"]):

                # rip it out

                script.extract()

            # get text

            text = soup.get_text()

        lines = (line.strip() for line in text.splitlines())

        # break multi-headlines into a line each

        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))

        # drop blank lines

        text = '\n'.join(chunk for chunk in chunks if chunk)
        l = '\n'.join(x for x in text.split("\n") if len(x.split()) > 1 and not x.isupper())

        with open(self.new_dir + "/" + os.path.split(self.path)[1][:-3] + "txt", 'w') as new_file:
            print("new file in:", self.new_dir + "/" + os.path.split(self.path)[1][:-3] + "txt")
            new_file.write(l)
