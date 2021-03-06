"""Class for parsing COVID-19 data from the Scottish Government and archived copies of that page through archive.org"""
import os
import urllib
import site

from html2text import html2text
from word2number import w2n

import pandas as pd
import xarray as xr

class ScotlandCOVID19():
    
    def __init__(self, html: str, archive_copy: bool = False, date_requested: str = None):
        self.html = html
        self.date_requested = date_requested
        self.lines = self.parse_html()
        self.archive_copy = archive_copy
        self.date = self.parse_date()
        self.counts = self.get_counts()
        self.deaths = self.get_number_of_deaths()
        try:
            self.tests = self.get_num_tests()
        except:
            print("get_num_tests failed")
            self.tests = None
        
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
            if 'Scottish test n' in line:
                index_date = line.find('h test n')
                print(index_date)
                if index_date != -1:
                    return pd.to_datetime(line[index_date+15:index_date+29])
                
        
    def get_number_of_deaths(self) -> int:
        index = self.html.find('Sadly,')
        if index != -1:
            return w2n.word_to_num(self.html[index+7:index+20].split(' ')[0])
        index = self.html.find('patients who')
        if index != -1:
            try:
                substring = self.html[index-5:index].split('>')[1]
                return int(substring)
            except:
                print(substring)
        return 0
    
    def get_num_tests(self) -> int:
        index = self.html.find('total')
        substring = self.html[index+8:index+21].split()[0]
        print(substring)
        for char in [';','>']:
            if char in substring:
                substring = substring.split(char)[1]
        return int(substring.replace(',','').replace('</',''))
    
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
    
class ScotlandCOVID19Results():
    
    def __init__(self, results):
        deaths = {}
        tests = {}
        counts = []
        for result in results:
            deaths[result.date] = result.deaths
            tests[result.date] = result.tests
            counts.append(result.counts)
        self.deaths = pd.Series(deaths, name='Deaths of confirmed cases')
        self.deaths.index.name = 'date'
        self.deaths.drop_duplicates(inplace=True)
        self.tests = pd.Series(tests, name='Tests concluded')
        self.tests.index.name = 'date'
        self.tests.drop_duplicates(inplace=True)
        self.results = pd.concat([self.deaths, self.tests], axis=1)
        self.counts = pd.concat(counts,axis=1).transpose()
        self.counts = self.counts[self.counts.columns.sort_values()]
        self.counts = self.counts.fillna(0)
        self.counts.iloc[:, 1] = self.counts.iloc[:, 0] + self.counts.iloc[:, 1]
        self.counts.drop('Ayrshire & Arran', axis=1, inplace=True)
        self.counts.rename({self.counts.columns[0]:self.counts.columns[0].replace('\xa0', ' ')},axis=1, inplace=True)
        self.counts = self.counts.astype('int')
        self.counts.index.name = 'date'
        self.counts.columns.name = 'Health board'
        self.counts.drop_duplicates(inplace=True)
        
    def __repr__(self):
        return str(self.results)
    
    def to_netcdf(self, file_name: str = "data/Scotland_COVID-19.nc"):
        dataset = xr.Dataset({'counts': self.counts})
        dataset['deaths'] = xr.DataArray(self.deaths)
        dataset['tests'] = xr.DataArray(self.tests)
        dataset.to_netcdf(file_name)

        
def calculate_percentage_increases(covid_data: pd.DataFrame) -> pd.DataFrame:
    """Approach gives 100% for first confirmed case, not ideal"""
    percentage_increases = []
    for index in range(1, len(covid_data)):
        percentage_increases.append(round(
            (covid_data.iloc[index] - covid_data.iloc[index - 1, :]) /covid_data.iloc[index, :] * 100 ,2))
    percentage_increases = pd.concat(percentage_increases, axis=1)
    percentage_increases.columns = covid_data.index[1:]
    percentage_increases.columns.name = 'Daily % increase'
    percentage_increases.index.name = 'Health Board'
    return percentage_increases

def get_population_for_health_board(health_board_to_council: pd.DataFrame,
                                    council_areas: pd.DataFrame) -> pd.Series:
    """Sum population for council areas that make up each health board"""
    population_health_boards = {}
    for health_board, councils in health_board_to_council.iterrows():
        population_total = 0
        for council in councils:
            if council is not None:
                population_total += int(council_areas.loc[council][1])
        population_health_boards[health_board] = population_total
    return pd.Series(population_health_boards, name='Population mid 2018 ONS estimate')


def get_population_and_density_for_health_board_by_council(health_board_to_council: pd.DataFrame,
                                    council_areas: pd.DataFrame) -> dict:
    results = {}
    weighted_density = {}
    for health_board, councils in health_board_to_council.iterrows():
        
        council_data = []
        population_total = 0
        for council in councils:
            if council is not None:
                council_data.append(pd.Series({
                    'population':council_areas.loc[council][1],
                    'density':council_areas.loc[council][-1]}, name=council))
        results[health_board] = pd.concat(council_data, axis=1).transpose()
        results[health_board][
            'population_weight'] = results[
                health_board].population / results[health_board].population.sum()
        weighted_density[health_board] = (results[
            health_board].population_weight*results[health_board].density).sum()
    return results, weighted_density


def get_council_areas_and_relationship_to_health_board():
    tables = pd.read_html('https://en.wikipedia.org/wiki/Subdivisions_of_Scotland')
    council_areas = tables[2].copy()
    council_areas.set_index('Council area', inplace=True)
    return council_areas, tables[4].copy()


def get_all_recent_counts(all_results):
    """include areas with no confirmed cases"""
    recent_counts = all_results.counts.iloc[-1]
    zero_counts = pd.Series({'Orkney':0, 'Western Isles':0})
    return pd.concat([recent_counts, zero_counts])

def write_covid_pth():
    covid_pth = open(site.getsitepackages()[0]+'/covid.pth', 'w')
    covid_pth.write(os.sep.join(os.path.realpath(__file__).split(os.sep)[0:-1]))
    covid_pth.close()
