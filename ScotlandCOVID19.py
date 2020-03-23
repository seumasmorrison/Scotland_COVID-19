"""Class for parsing COVID-19 data from the Scottish Government and archived copies of that page through archive.org"""
import urllib

from html2text import html2text
from dateutil import parser
from word2number import w2n
import pansas as pd

class ScotlandCOVID19():
    
    def __init__(self, html: str, archive_copy: bool = False, date_requested: str = None):
        self.html = html
        self.date_requested = date_requested
        self.lines = self.parse_html()
        self.archive_copy = archive_copy
        self.date = self.parse_date()
        self.counts = self.get_counts()
        self.deaths = self.get_number_of_deaths()
        self.tests = self.get_num_tests()
        
    def __repr__(self):
        return "Date: " + str(self.date)[:10] + "\nNumbe of tests concluded to date: " + str(self.tests) + \
            "\nPatient deaths of confirmed cases to date: " + str(self.deaths) + \
            "\n" + str(self.counts)
        
    @classmethod
    def from_latest_government(cls):
        print("Latest")
        html = str(urllib.request.urlopen('https://www.gov.scot/coronavirus-covid-19').read())
        return cls(html)
        
    @classmethod
    def from_date(cls, date: str):
        print(date)
        url =  "https://web.archive.org/web/" + date + \
            "/https://www.gov.scot/coronavirus-covid-19"
        html = str(urllib.request.urlopen(url).read())
        return cls(html, archive_copy=True, date_requested=date)

    def parse_html(self) -> list:
        lines = html2text(self.html).replace('\n','').split('\\n')
        new_lines = []
        for line in lines:
            if line != '':
                new_lines.append(lines)
        return new_lines
    
    def parse_date(self) -> str:
        """Parses the publication date from the line below the published table"""
        for line in self.lines:
            line = ''.join(line)
            if 'updated' in line:
                index = line.find('Last updated')
                if index != -1:
                    substring = line[index + 10: index + 50].split('.')[0][-13:]
                    print(substring)
                    return pd.to_datetime(substring)
        
    def get_number_of_deaths(self) -> int:
        index = self.html.find('Sadly,')
        if index != -1:
            return w2n.word_to_num(self.html[index+7:index+20].split(' ')[0])
        index = self.html.find('patients who')
        if index != -1:
            return int(self.html[index-5:index].split('>')[1])
        return 0
    
    def get_num_tests(self) -> int:
        index = self.html.find('A total of')
        substring = self.html[index+11:index+21].split()[0]
        for char in [';','>']:
            if char in substring:
                substring = substring.split(char)[1]
        return int(substring.replace(',',''))
    
    def get_counts(self) -> pd.DataFrame:
        tables = pd.read_html(self.html)
        if len(tables) >= 3 and self.archive_copy:
            counts = tables[2]
        elif len(tables) > 0: 
            counts = tables[0]
        counts = counts.set_index(counts.columns[0])
        counts.columns.name = counts.columns[0]
        counts.columns = [self.date]
        return counts
