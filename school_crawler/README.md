# School Crawler

Crawls K12 school sites. To get the crawler running, follow the instructions below:

## Get the initial input data from NCES

To get started, first download the Common Core of Data from NCES:

https://nces.ed.gov/ccd/files.asp#Fiscal:2,LevelId:7,SchoolYearId:37,Page:1

For example, at the time of writing, the latest data is 2022-2023. Click on the "Flat and SAS Files (12.0 MB)" link under the "Directory" section, and you'll download this zip file:

https://nces.ed.gov/ccd/data/zip/ccd_sch_029_2223_w_1a_083023.zip

Extract all files, but we only need the CSV file. We have provided with one such sample file in the `input_data` directory. For the purpose of the crawler, we only care about the following columns in the CSV file:

    * `SCH_NAME`: School name
    * `WEBSITE`: School website

## Prepare the crawl queue

The websites from CSV data above may be outdated or may redirect to the a different site. In the case of outdated sites, we'll just exclude the URL from the crawl queue. We care about URL redirection because we need to figure out what is a third party or first party. For example, the CSV data may show a school having a website `http://www.schoolname.org`, but the site may redirect to `https://www.schoolname.statename.us`. In this case, `schoolname.statename.us` will be the hostname of the school (or in some cases the district). If a URL seen linked from the site does not contain the hostname above, then it is not the first party.

To prepare the crawl queue, run the following command from the root of the project:

```python

python -m venv env
source env/bin/activate
pip install -r requirements.txt
cd school_cralwer
python prepare_crawl_queue.py input_data/ccd_sch_029_2223_w_1a_083023.csv 2024-02-28.sqlite
```

This will create a SQLite database called `2024-02-28.sqlite` in the `school_crawler` directory. This database will contain the URLs to be crawled, as well as the contents of the URLs. We recommend naming the SQLite database with the date of the first crawl. This is one way to keep track of the crawl versions.


## Run the crawler

To run the crawler, run the following command from the root of the project:

```python
python -m venv env
source env/bin/activate
cd school_cralwer
python crawl.py
```

Feel free to stop or resume the crawler at any time. The crawler will pick up where it left off.

