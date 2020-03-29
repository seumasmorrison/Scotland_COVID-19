
Data on confirmed COVID-19 cases in Scotland by Health board taken from the Web Archive [WayBackMachine](https://web.archive.org/web/*/https://www. gov.scot/coronavirus-covid-19/) capture of the Scottish Government published health board data published since 2020-03-05 ( broken down by specific health boards ).


```python
pip install html2text geopandas lxml matplotlib pandas word2number xarray 
```

    Requirement already satisfied: html2text in c:\programdata\anaconda3\lib\site-packages (2020.1.16)
    Requirement already satisfied: lxml in c:\programdata\anaconda3\lib\site-packages (4.3.2)
    Requirement already satisfied: matplotlib in c:\programdata\anaconda3\lib\site-packages (3.0.3)
    Requirement already satisfied: pandas in c:\programdata\anaconda3\lib\site-packages (1.0.0)
    Requirement already satisfied: word2number in c:\programdata\anaconda3\lib\site-packages (1.1)
    Requirement already satisfied: numpy>=1.10.0 in c:\programdata\anaconda3\lib\site-packages (from matplotlib) (1.17.0)
    Requirement already satisfied: cycler>=0.10 in c:\programdata\anaconda3\lib\site-packages (from matplotlib) (0.10.0)
    Requirement already satisfied: kiwisolver>=1.0.1 in c:\programdata\anaconda3\lib\site-packages (from matplotlib) (1.0.1)
    Requirement already satisfied: pyparsing!=2.0.4,!=2.1.2,!=2.1.6,>=2.0.1 in c:\programdata\anaconda3\lib\site-packages (from matplotlib) (2.3.1)
    Requirement already satisfied: python-dateutil>=2.1 in c:\programdata\anaconda3\lib\site-packages (from matplotlib) (2.8.0)
    Requirement already satisfied: pytz>=2017.2 in c:\programdata\anaconda3\lib\site-packages (from pandas) (2018.9)
    Requirement already satisfied: six in c:\programdata\anaconda3\lib\site-packages (from cycler>=0.10->matplotlib) (1.12.0)
    Requirement already satisfied: setuptools in c:\programdata\anaconda3\lib\site-packages (from kiwisolver>=1.0.1->matplotlib) (40.8.0)
    Note: you may need to restart the kernel to use updated packages.
    


```python
from datetime import datetime
from pathlib import Path

from html2text import html2text
from dateutil import parser
from word2number import w2n
import geopandas as gpd
import pandas as pd
import urllib
import xarray as xr
```


```python
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
```


```python
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
        self.tests = pd.Series(tests, name='Tests concluded')
        self.tests.index.name = 'date'
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
        
    def __repr__(self):
        return str(self.results)
    
    def to_netcdf(self, file_name: str = 'Scotland_COVID-19.nc'):
        dataset = xr.Dataset({'counts': self.counts})
        dataset['deaths'] = xr.DataArray(self.deaths)
        dataset['tests'] = xr.DataArray(self.tests)
        dataset.to_netcdf(file_name)
```


```python
8679-7886
```




    793




```python
results = []
for date in range(20200306,20200320):
    results.append(ScotlandCOVID19.from_date(str(date)))
results.append(ScotlandCOVID19.from_latest_government())
```

    20200306
     6 March 2020
    20200307
     7 March 2020
    20200308
     8 March 2020
    20200309
     9 March 2020
    20200310
    10 March 2020
    20200311
    11 March 2020
    20200312
    12 March 2020
    20200313
    13 March 2020
    20200314
    14 March 2020
    20200315
    15 March 2020
    20200316
    16 March 2020
    20200317
    17 March 2020
    20200318
    18 March 2020
    20200319
    19 March 2020
    Latest
    20 March 2020
    


```python
results.append(ScotlandCOVID19.from_latest_government())
```

    Latest
    22 March 2020
    


```python
all_results = ScotlandCOVID19Results(results)
```


```python
all_results.to_netcdf()
```


```python
initial_counts = all_results.counts.iloc[0]
```


```python
count_difference.sum() + initial_counts
```




    Health Board
    Ayrshire and Arran            21
    Borders                       10
    Dumfries and Galloway         13
    Fife                          13
    Forth Valley                  27
    Grampian                      20
    Greater Glasgow and Clyde    110
    Highland                       8
    Lanarkshire                   49
    Lothian                       44
    Shetland                      24
    Tayside                       34
    dtype: int64




```python
wiki_data = pd.read_csv('wikipedia_numbers.csv', index_col=0)
```


```python
differences = pd.DataFrame(count_difference.values - wiki_data[4:].values, index = count_difference.index, columns=count_difference.columns)
```


```python
all_results.counts.iloc[-1]
```




    Health board
    Ayrshire and Arran            25
    Borders                       11
    Dumfries and Galloway         16
    Fife                          16
    Forth Valley                  30
    Grampian                      23
    Greater Glasgow and Clyde    130
    Highland                       8
    Lanarkshire                   49
    Lothian                       46
    Shetland                      24
    Tayside                       38
    Name: 2020-03-22 00:00:00, dtype: int32




```python
all_results.counts.diff().iloc[-1]
```




    Health board
    Ayrshire and Arran            4.0
    Borders                       1.0
    Dumfries and Galloway         3.0
    Fife                          3.0
    Forth Valley                  3.0
    Grampian                      3.0
    Greater Glasgow and Clyde    20.0
    Highland                      0.0
    Lanarkshire                   0.0
    Lothian                       2.0
    Shetland                      0.0
    Tayside                       4.0
    Name: 2020-03-22 00:00:00, dtype: float64




```python
all_results.tests.diff()
```




    date
    2020-03-06      NaN
    2020-03-07    155.0
    2020-03-08    277.0
    2020-03-09    144.0
    2020-03-10    133.0
    2020-03-11     82.0
    2020-03-12    576.0
    2020-03-13    422.0
    2020-03-14    401.0
    2020-03-15    525.0
    2020-03-16    655.0
    2020-03-17    351.0
    2020-03-18    845.0
    2020-03-19    681.0
    2020-03-20    778.0
    2020-03-21    709.0
    2020-03-22    420.0
    Name: Tests concluded, dtype: float64




```python
all_results.counts.iloc[-2] - all_results.counts.iloc[-3].transpose()
```




    Health board
    Ayrshire and Arran            4
    Borders                       1
    Dumfries and Galloway         4
    Fife                          3
    Forth Valley                  6
    Grampian                      1
    Greater Glasgow and Clyde    20
    Highland                      0
    Lanarkshire                   8
    Lothian                       5
    Shetland                      0
    Tayside                       4
    dtype: int32




```python
all_results.counts.iloc[-1] - all_results.counts.iloc[-2]
```




    Health board
    Ayrshire and Arran            5
    Borders                       1
    Dumfries and Galloway         3
    Fife                          1
    Forth Valley                  4
    Grampian                      1
    Greater Glasgow and Clyde    19
    Highland                      2
    Lanarkshire                   8
    Lothian                       4
    Shetland                      0
    Tayside                       3
    dtype: int32




```python
count_difference = all_results.counts.diff().fillna(0).astype('int')
```


```python
count_difference.to_csv('daily_difference_in_counts.csv')
```


```python
all_results.counts.to_csv('daily_counts_by_health_board.csv')
```


```python
print("Daily increase in number of test concluded")
all_results.tests.diff()[1:].astype('int')
```

    Daily increase in number of test concluded
    




    date
    2020-03-07    155
    2020-03-08    277
    2020-03-09    144
    2020-03-10    133
    2020-03-11     82
    2020-03-12    576
    2020-03-13    422
    2020-03-14    401
    2020-03-15    525
    2020-03-16    655
    2020-03-17    351
    2020-03-18    845
    2020-03-19    681
    2020-03-20    778
    Name: Tests concluded, dtype: int32


all_results.deaths.plot(color='r', title='Total number of deaths of patients who tested positive for COVID-19', grid=True, figsize=(12, 8))

```python
all_results.counts.plot(figsize=(12,8), title='COVID-19 Confirmed cases by Health board Scotland', grid=True)
```




    <matplotlib.axes._subplots.AxesSubplot at 0x22ad7b3d358>




![png](output_24_1.png)



```python
all_results.counts.sum(axis=1).plot(figsize=(12,8), title='COVID-19 all Confirmed cases Scotland', grid=True)
```




    <matplotlib.axes._subplots.AxesSubplot at 0x22ae9c64080>




![png](output_25_1.png)



```python
print("Daily increase of cases")
all_results.counts.sum(axis=1).diff()
```

    Daily increase of cases
    




    date
    2020-03-06     NaN
    2020-03-07     5.0
    2020-03-08     2.0
    2020-03-09     5.0
    2020-03-10     4.0
    2020-03-11     9.0
    2020-03-12    24.0
    2020-03-13    25.0
    2020-03-14    36.0
    2020-03-15    32.0
    2020-03-16    18.0
    2020-03-17    24.0
    2020-03-18    32.0
    2020-03-19    39.0
    2020-03-20    56.0
    2020-03-21    51.0
    2020-03-22    43.0
    dtype: float64




```python
print("Percentage of positive test daily")
(all_results.counts.sum(axis=1).diff()/all_results.tests.diff() * 100)
```

    Percentage of positive test daily
    




    date
    2020-03-06          NaN
    2020-03-07     3.225806
    2020-03-08     0.722022
    2020-03-09     3.472222
    2020-03-10     3.007519
    2020-03-11    10.975610
    2020-03-12     4.166667
    2020-03-13     5.924171
    2020-03-14     8.977556
    2020-03-15     6.095238
    2020-03-16     2.748092
    2020-03-17     6.837607
    2020-03-18     3.786982
    2020-03-19     5.726872
    2020-03-20     7.197943
    2020-03-21     7.193230
    2020-03-22    10.238095
    dtype: float64




```python
print("% increase cases across Scotland")
round(all_results.counts.sum(axis=1).diff()/all_results.counts.sum(axis=1) * 100, 2)[1:]
```

    % increase cases across Scotland
    




    date
    2020-03-07    31.25
    2020-03-08    11.11
    2020-03-09    21.74
    2020-03-10    14.81
    2020-03-11    25.00
    2020-03-12    40.00
    2020-03-13    29.41
    2020-03-14    29.75
    2020-03-15    20.92
    2020-03-16    10.53
    2020-03-17    12.31
    2020-03-18    14.10
    2020-03-19    14.66
    2020-03-20    17.39
    2020-03-21    13.67
    2020-03-22    10.34
    dtype: float64




```python
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
```


```python
percentage_increases = calculate_percentage_increases(all_results.counts)
percentage_increases_numbers = percentage_increases.copy()
percentage_increases = percentage_increases.replace(100,'First Case Confirmed')
percentage_increases
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th>Daily % increase</th>
      <th>2020-03-07</th>
      <th>2020-03-08</th>
      <th>2020-03-09</th>
      <th>2020-03-10</th>
      <th>2020-03-11</th>
      <th>2020-03-12</th>
      <th>2020-03-13</th>
      <th>2020-03-14</th>
      <th>2020-03-15</th>
      <th>2020-03-16</th>
      <th>2020-03-17</th>
      <th>2020-03-18</th>
      <th>2020-03-19</th>
      <th>2020-03-20</th>
      <th>2020-03-21</th>
    </tr>
    <tr>
      <th>Health Board</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>Ayrshire and Arran</th>
      <td>0</td>
      <td>0.00</td>
      <td>0</td>
      <td>0.00</td>
      <td>66.67</td>
      <td>25.00</td>
      <td>0.00</td>
      <td>33.33</td>
      <td>0.00</td>
      <td>14.29</td>
      <td>-16.67</td>
      <td>33.33</td>
      <td>25.00</td>
      <td>25.00</td>
      <td>23.81</td>
    </tr>
    <tr>
      <th>Borders</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>First Case Confirmed</td>
      <td>0.00</td>
      <td>33.33</td>
      <td>40</td>
      <td>28.57</td>
      <td>0</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>12.50</td>
      <td>11.11</td>
      <td>10.00</td>
    </tr>
    <tr>
      <th>Dumfries and Galloway</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>First Case Confirmed</td>
      <td>0.00</td>
      <td>75.00</td>
      <td>33.33</td>
      <td>40.00</td>
      <td>23.08</td>
    </tr>
    <tr>
      <th>Fife</th>
      <td>0</td>
      <td>0.00</td>
      <td>0</td>
      <td>0.00</td>
      <td>0</td>
      <td>33.33</td>
      <td>25.00</td>
      <td>33.33</td>
      <td>14.29</td>
      <td>0</td>
      <td>0.00</td>
      <td>12.50</td>
      <td>11.11</td>
      <td>25.00</td>
      <td>7.69</td>
    </tr>
    <tr>
      <th>Forth Valley</th>
      <td>0</td>
      <td>0.00</td>
      <td>0</td>
      <td>0.00</td>
      <td>0</td>
      <td>66.67</td>
      <td>0.00</td>
      <td>0</td>
      <td>40.00</td>
      <td>0</td>
      <td>16.67</td>
      <td>20.00</td>
      <td>11.76</td>
      <td>26.09</td>
      <td>14.81</td>
    </tr>
    <tr>
      <th>Grampian</th>
      <td>25</td>
      <td>0.00</td>
      <td>0</td>
      <td>33.33</td>
      <td>0</td>
      <td>14.29</td>
      <td>36.36</td>
      <td>-22.22</td>
      <td>25.00</td>
      <td>0</td>
      <td>45.45</td>
      <td>8.33</td>
      <td>-33.33</td>
      <td>5.26</td>
      <td>5.00</td>
    </tr>
    <tr>
      <th>Greater Glasgow and Clyde</th>
      <td>50</td>
      <td>33.33</td>
      <td>0</td>
      <td>0.00</td>
      <td>40</td>
      <td>50.00</td>
      <td>52.38</td>
      <td>32.26</td>
      <td>20.51</td>
      <td>11.36</td>
      <td>10.20</td>
      <td>14.04</td>
      <td>19.72</td>
      <td>21.98</td>
      <td>17.27</td>
    </tr>
    <tr>
      <th>Highland</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>First Case Confirmed</td>
      <td>50.00</td>
      <td>0</td>
      <td>60.00</td>
      <td>0.00</td>
      <td>16.67</td>
      <td>0.00</td>
      <td>25.00</td>
    </tr>
    <tr>
      <th>Lanarkshire</th>
      <td>First Case Confirmed</td>
      <td>0.00</td>
      <td>33.33</td>
      <td>0.00</td>
      <td>25</td>
      <td>42.86</td>
      <td>0.00</td>
      <td>30</td>
      <td>37.50</td>
      <td>20</td>
      <td>4.76</td>
      <td>16.00</td>
      <td>24.24</td>
      <td>19.51</td>
      <td>16.33</td>
    </tr>
    <tr>
      <th>Lothian</th>
      <td>50</td>
      <td>33.33</td>
      <td>40</td>
      <td>28.57</td>
      <td>12.5</td>
      <td>27.27</td>
      <td>45.00</td>
      <td>20</td>
      <td>10.71</td>
      <td>3.45</td>
      <td>3.33</td>
      <td>9.09</td>
      <td>5.71</td>
      <td>12.50</td>
      <td>9.09</td>
    </tr>
    <tr>
      <th>Shetland</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>First Case Confirmed</td>
      <td>0.00</td>
      <td>0</td>
      <td>66.67</td>
      <td>0.00</td>
      <td>45.45</td>
      <td>0.00</td>
      <td>26.67</td>
      <td>0.00</td>
      <td>6.25</td>
      <td>33.33</td>
      <td>0.00</td>
      <td>0.00</td>
    </tr>
    <tr>
      <th>Tayside</th>
      <td>0</td>
      <td>0.00</td>
      <td>0</td>
      <td>0.00</td>
      <td>50</td>
      <td>50.00</td>
      <td>-33.33</td>
      <td>72.73</td>
      <td>26.67</td>
      <td>11.76</td>
      <td>15.00</td>
      <td>16.67</td>
      <td>11.11</td>
      <td>12.90</td>
      <td>8.82</td>
    </tr>
  </tbody>
</table>
</div>




```python
min_days_present = (~percentage_increases.isna()).sum(axis=1)
min_days_present.name = 'Minimum number of days of confirmed cases'
min_days_present
```




    Health Board
    Ayrshire and Arran           15
    Borders                      11
    Dumfries and Galloway         6
    Fife                         15
    Forth Valley                 15
    Grampian                     15
    Greater Glasgow and Clyde    15
    Highland                      8
    Lanarkshire                  15
    Lothian                      15
    Shetland                     13
    Tayside                      15
    Name: Minimum number of days of confirmed cases, dtype: int64




```python
mean_daily_percent_increase_by_board = round(
    percentage_increases.mean(axis=1), 2)
mean_daily_percent_increase_by_board.name = "Mean % increase 6-21th March"
mean_daily_percent_increase_by_board.sort_values()
```




    Health Board
    Tayside                      10.78
    Ayrshire and Arran           11.55
    Shetland                     11.81
    Borders                      11.94
    Fife                         12.89
    Grampian                     13.97
    Lanarkshire                  16.12
    Lothian                      18.46
    Forth Valley                 19.60
    Greater Glasgow and Clyde    23.94
    Highland                     25.28
    Dumfries and Galloway        34.28
    Name: Mean % increase 6-21th March, dtype: float64




```python
all_results
```




                Deaths of confirmed cases  Tests concluded
    date                                                  
    2020-03-06                          0             1525
    2020-03-07                          0             1680
    2020-03-08                          0             1957
    2020-03-09                          0             2101
    2020-03-10                          0             2234
    2020-03-11                          0             2316
    2020-03-12                          0             2892
    2020-03-13                          1             3314
    2020-03-14                          1             3715
    2020-03-15                          1             4240
    2020-03-16                          1             4895
    2020-03-17                          2             5246
    2020-03-18                          3             6091
    2020-03-19                          6             6772
    2020-03-20                          6             7550
    2020-03-21                          7             8259



## Linking Council areas to Health Boards for population statistics purposes


```python
tables = pd.read_html('https://en.wikipedia.org/wiki/Subdivisions_of_Scotland')
```


```python
council_areas = tables[2].copy()
health_board_to_council = tables[4].copy()
```


```python
percentage_population_tested = all_results.tests/scottish_population_mid_2018_estimate*100
```


```python
percentage_population_tested
```




    date
    2020-03-06    0.028043
    2020-03-07    0.030893
    2020-03-08    0.035987
    2020-03-09    0.038635
    2020-03-10    0.041081
    2020-03-11    0.042588
    2020-03-12    0.053180
    2020-03-13    0.060940
    2020-03-14    0.068314
    2020-03-15    0.077968
    2020-03-16    0.090013
    2020-03-17    0.096468
    2020-03-18    0.112006
    2020-03-19    0.124529
    2020-03-20    0.138835
    2020-03-21    0.151873
    Name: Tests concluded, dtype: float64




```python
council_areas.set_index('Council area', inplace=True)
```


```python
council_areas
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Rank</th>
      <th>Population[3]</th>
      <th>Electors[4]</th>
      <th>Area (km²)[5]</th>
      <th>Density(per km²)</th>
    </tr>
    <tr>
      <th>Council area</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>Aberdeenshire</th>
      <td>6</td>
      <td>261470</td>
      <td>202194</td>
      <td>6313.00</td>
      <td>41</td>
    </tr>
    <tr>
      <th>Angus</th>
      <td>17</td>
      <td>116040</td>
      <td>89652</td>
      <td>2182.00</td>
      <td>53</td>
    </tr>
    <tr>
      <th>Argyll and Bute</th>
      <td>27</td>
      <td>86260</td>
      <td>68271</td>
      <td>6909.00</td>
      <td>12</td>
    </tr>
    <tr>
      <th>City of Aberdeen</th>
      <td>8</td>
      <td>227560</td>
      <td>163196</td>
      <td>185.70</td>
      <td>1225</td>
    </tr>
    <tr>
      <th>City of Dundee</th>
      <td>14</td>
      <td>148750</td>
      <td>112513</td>
      <td>59.83</td>
      <td>2486</td>
    </tr>
    <tr>
      <th>City of Edinburgh</th>
      <td>2</td>
      <td>518500</td>
      <td>367762</td>
      <td>263.40</td>
      <td>1969</td>
    </tr>
    <tr>
      <th>City of Glasgow</th>
      <td>1</td>
      <td>626410</td>
      <td>462286</td>
      <td>174.70</td>
      <td>3586</td>
    </tr>
    <tr>
      <th>Clackmannanshire</th>
      <td>29</td>
      <td>51400</td>
      <td>39186</td>
      <td>159.00</td>
      <td>323</td>
    </tr>
    <tr>
      <th>Dumfries and Galloway</th>
      <td>13</td>
      <td>148790</td>
      <td>116593</td>
      <td>6427.00</td>
      <td>23</td>
    </tr>
    <tr>
      <th>East Ayrshire</th>
      <td>16</td>
      <td>121840</td>
      <td>94657</td>
      <td>1262.00</td>
      <td>97</td>
    </tr>
    <tr>
      <th>East Dunbartonshire</th>
      <td>20</td>
      <td>108330</td>
      <td>85410</td>
      <td>174.50</td>
      <td>621</td>
    </tr>
    <tr>
      <th>East Lothian</th>
      <td>21</td>
      <td>105790</td>
      <td>80874</td>
      <td>679.20</td>
      <td>156</td>
    </tr>
    <tr>
      <th>East Renfrewshire</th>
      <td>23</td>
      <td>95170</td>
      <td>71618</td>
      <td>174.20</td>
      <td>546</td>
    </tr>
    <tr>
      <th>Falkirk</th>
      <td>11</td>
      <td>160340</td>
      <td>122471</td>
      <td>297.40</td>
      <td>539</td>
    </tr>
    <tr>
      <th>Fife</th>
      <td>3</td>
      <td>371910</td>
      <td>280622</td>
      <td>1325.00</td>
      <td>281</td>
    </tr>
    <tr>
      <th>Highland</th>
      <td>7</td>
      <td>235540</td>
      <td>184697</td>
      <td>25657.00</td>
      <td>9</td>
    </tr>
    <tr>
      <th>Inverclyde</th>
      <td>28</td>
      <td>78150</td>
      <td>59941</td>
      <td>160.50</td>
      <td>487</td>
    </tr>
    <tr>
      <th>Midlothian</th>
      <td>25</td>
      <td>91340</td>
      <td>70587</td>
      <td>353.70</td>
      <td>258</td>
    </tr>
    <tr>
      <th>Moray</th>
      <td>22</td>
      <td>95520</td>
      <td>73284</td>
      <td>2238.00</td>
      <td>43</td>
    </tr>
    <tr>
      <th>Na h-Eileanan Siar (Western Isles)</th>
      <td>30</td>
      <td>26830</td>
      <td>21874</td>
      <td>3059.00</td>
      <td>9</td>
    </tr>
    <tr>
      <th>North Ayrshire</th>
      <td>15</td>
      <td>135280</td>
      <td>107763</td>
      <td>885.40</td>
      <td>153</td>
    </tr>
    <tr>
      <th>North Lanarkshire</th>
      <td>4</td>
      <td>340180</td>
      <td>256174</td>
      <td>469.90</td>
      <td>724</td>
    </tr>
    <tr>
      <th>Orkney Islands</th>
      <td>32</td>
      <td>22190</td>
      <td>17135</td>
      <td>988.80</td>
      <td>22</td>
    </tr>
    <tr>
      <th>Perth and Kinross</th>
      <td>12</td>
      <td>151290</td>
      <td>114936</td>
      <td>5286.00</td>
      <td>29</td>
    </tr>
    <tr>
      <th>Renfrewshire</th>
      <td>10</td>
      <td>177790</td>
      <td>133295</td>
      <td>261.50</td>
      <td>680</td>
    </tr>
    <tr>
      <th>Scottish Borders</th>
      <td>18</td>
      <td>115270</td>
      <td>91919</td>
      <td>4732.00</td>
      <td>24</td>
    </tr>
    <tr>
      <th>Shetland Islands</th>
      <td>31</td>
      <td>22990</td>
      <td>17837</td>
      <td>1468.00</td>
      <td>16</td>
    </tr>
    <tr>
      <th>South Ayrshire</th>
      <td>19</td>
      <td>112550</td>
      <td>90300</td>
      <td>1222.00</td>
      <td>92</td>
    </tr>
    <tr>
      <th>South Lanarkshire</th>
      <td>5</td>
      <td>319020</td>
      <td>248875</td>
      <td>1772.00</td>
      <td>180</td>
    </tr>
    <tr>
      <th>Stirling</th>
      <td>24</td>
      <td>94330</td>
      <td>68778</td>
      <td>2187.00</td>
      <td>43</td>
    </tr>
    <tr>
      <th>West Dunbartonshire</th>
      <td>26</td>
      <td>89130</td>
      <td>68826</td>
      <td>158.80</td>
      <td>561</td>
    </tr>
    <tr>
      <th>West Lothian</th>
      <td>9</td>
      <td>182140</td>
      <td>137614</td>
      <td>427.70</td>
      <td>426</td>
    </tr>
  </tbody>
</table>
</div>




```python
scottish_polulation = council_areas.sum()[1]
```


```python
council_areas = council_areas.loc[council_areas.index.sort_values()]
```


```python
health_board_to_council.set_index(health_board_to_council.columns[0], inplace=True)
```


```python
health_board_to_council.rename({'Council areas':'council_areas'}, axis=1, inplace=True)
```


```python
health_board_to_council = health_board_to_council.council_areas.str.rsplit(' and ',1, expand=True)
```


```python
# Forth Valley
health_board_to_council.iloc[4].name = health_board_to_council.iloc[4].name.rsplit(' ', 5)[0]
```

Splitting on final ' and ' above create issues for the following areas


```python
# Handle Perth and Kinross
health_board_to_council.loc['Tayside'].iloc[1] = health_board_to_council.loc['Tayside'].iloc[1].split(' and ')[0]
health_board_to_council.loc['Tayside'].iloc[-1] = 'Perth and Kinross'
```


```python
# Handle Dumfries and Galloway
health_board_to_council.iloc[2][0] = health_board_to_council.iloc[2].name
```


```python
health_board_to_council = health_board_to_council.iloc[:,0].str.split(', ', expand=True).join(health_board_to_council.iloc[:,1], rsuffix='_1')
```


```python
health_board_to_council.loc['Dumfries and Galloway'][-1] = None
```


```python
# Use council area naming
health_board_to_council.iloc[-1][0] = 'Na h-Eileanan Siar (Western Isles)'
```


```python
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
```


```python
scottish_population_mid_2018_estimate = get_population_for_health_board(health_board_to_council, council_areas).sum()
```


```python
health_board_populations = get_population_for_health_board()
```


```python
health_board_populations
```




    Ayrshire and Arran                                  369670
    Borders                                             115270
    Dumfries and Galloway                               148790
    Fife                                                371910
    Forth Valley Central Scotland in other contexts     306070
    Grampian                                            584550
    Greater Glasgow and Clyde                          1174980
    Highland                                            321800
    Lanarkshire                                         659200
    Lothian                                             897770
    Orkney                                               22190
    Shetland                                             22990
    Tayside                                             416080
    Western Isles (Eileanan Siar)                        26830
    Name: Population mid 2018 ONS estimate, dtype: int64




```python
# Drop the Forth Valley suffix
health_board_to_council.rename({health_board_to_council.index[4]: health_board_to_council.index[4].rsplit(' ', 5)[0]}, inplace=True)
```


```python
last_counts = all_results.counts.iloc[-1]
```


```python
# Drop the Forth Valley suffix
health_board_populations.rename({health_board_populations.index[4]: health_board_populations.index[4].rsplit(' ', 5)[0]}, inplace=True)
```


```python
percentage_confirmed_cases_by_health_board_population = (last_counts / health_board_populations * 100).sort_values(ascending=False)
```


```python
percentage_confirmed_cases_by_health_board_population
```




    Shetland                         0.104393
    Greater Glasgow and Clyde        0.009362
    Forth Valley                     0.008822
    Dumfries and Galloway            0.008737
    Borders                          0.008675
    Tayside                          0.008172
    Lanarkshire                      0.007433
    Ayrshire and Arran               0.005681
    Lothian                          0.004901
    Fife                             0.003495
    Grampian                         0.003421
    Highland                         0.002486
    Orkney                                NaN
    Western Isles (Eileanan Siar)         NaN
    dtype: float64




```python
health_board_to_council.to_csv('health_board_to_council.csv')
```


```python
council_areas.to_csv('council_areas.csv')
```

## Reading Scottish Health Board polygons created from Ordnance Survey Boundary line Open Data


```python
health_boards = gpd.read_file('scottish_health_boards.gpkg', layer='scottish_health_boards')
```


```python
health_boards.set_index('Name', inplace=True)
```


```python
health_boards.index.sort_values()
```




    Index(['Aberdeen City', 'Clackmannanshire', 'Dumfries and Galloway',
           'Dundee City', 'East Ayrshire', 'East Dunbartonshire', 'Fife',
           'Highland', 'Midlothian', 'Na h-Eileanan an Iar', 'Orkney Islands',
           'Scottish Borders', 'Shetland Islands', 'South Lanarkshire'],
          dtype='object', name='Name')




```python
# Ensure naming is consistent with names used on Scottish Government COVID-19 page
health_boards.rename({
    'Aberdeen City': 'Grampian',
    'Clackmannanshire': 'Forth Valley',
    'Dundee City': 'Tayside',
    'East Ayrshire': 'Ayrshire and Arran',
    'East Dunbartonshire': 'Greater Glasgow and Clyde',
    'Midlothian': 'Lothian',
    'Na h-Eileanan an Iar': 'Western Isles (Eileanan Siar)',
    'Orkney Islands': 'Orkney',
    'Scottish Borders': 'Borders',
    'Shetland Islands': 'Shetland',
    'South Lanarkshire': 'Lanarkshire'
}, inplace=True)
```


```python
health_board_geo_series = health_boards.geometry
```


```python
health_board_geo_series = health_board_geo_series[health_board_geo_series.index.sort_values()]
```


```python
health_board_geo_df = gpd.GeoDataFrame(all_results.counts.iloc[-1], geometry=health_board_geo_series.geometry)
```


```python
health_board_geo_df.rename({health_board_geo_df.columns[0]: str(health_board_geo_df.columns[0])}, axis=1, inplace=True)
```


```python
health_board_geo_df.to_file('scottish_health_board_counts_20200322.gpkg', driver='GPKG')
```
