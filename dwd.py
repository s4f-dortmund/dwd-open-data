import requests
import pandas as pd
import io
import zipfile
import os
from bs4 import BeautifulSoup
import re
from functools import lru_cache
from collections import defaultdict
from multiprocessing.pool import ThreadPool
from tqdm import tqdm


BASE_URL = 'https://opendata.dwd.de/climate_environment/CDC/'


def get_dwd_stations():
    '''Download and parse the dwd weather station list'''
    return pd.read_fwf(
        BASE_URL + 'help/KL_Tageswerte_Beschreibung_Stationen.txt',
        encoding='latin-1',
        names='id von_datum bis_datum Stationshoehe geoBreite geoLaenge Stationsname Bundesland'.split(),
        skiprows=2,
        parse_dates=['von_datum', 'bis_datum'],
    )


@lru_cache(maxsize=1)
def _build_kl_file_index():
    '''
    Create a mapping from station id to url
    for the subdirectories `historical` and `recent`
    '''
    url = BASE_URL + 'observations_germany/climate/daily/kl/'
    soup = requests.get(BASE_URL + 'recent/')

    hist_re = re.compile(r'tageswerte_KL_(\d{5})_(\d{8})_(\d{8})_hist.zip')
    akt_re = re.compile(r'tageswerte_KL_(\d{5})_akt.zip')

    index = defaultdict(dict)

    for d, r in zip(['historical', 'recent'], [hist_re, akt_re]):

        url = BASE_URL + 'observations_germany/climate/daily/kl/historical/'
        soup = BeautifulSoup(requests.get(url).text, 'lxml')
        links = soup.find_all('a', href=r)

        for l in links:
            href = l.attrs['href']
            m = r.match(href)
            station_id, von_date, bis_date = m.groups()
            index[int(station_id)][d] = url + href

    return index


def download_station_kl(station_id, outdir=os.path.join('data', 'kl')):
    '''
    Download data of dwd weather stations.

    Data is split in two files, the `historical` directory contains
    data until 2018-12-31, the `recent` directory contains
    more current data.

    Returns the written files.
    '''
    outdir = os.path.abspath(outdir)
    os.makedirs(outdir, exist_ok=True)
    index = _build_kl_file_index()

    if station_id not in index:
        raise KeyError('No data for station {}'.format(station_id))

    files = []
    for d, url in index[station_id].items():
        outfile = os.path.join(outdir, os.path.basename(url))
        download_file(url, outfile)
        files.append(outfile)

    return files


def download_file(url, outfile):
    ret = requests.get(url)
    ret.raise_for_status()

    with open(outfile, 'wb') as f:
        f.write(ret.content)


def download_all_kl_files(outdir=os.path.join('data', 'kl'), n_jobs=20):
    outdir = os.path.abspath(outdir)
    os.makedirs(outdir, exist_ok=True)
    index = _build_kl_file_index()

    urls = [
        url
        for station in index.values()
        for url in station.values()
    ]

    bar = tqdm(total=len(urls))

    def dl(u):
        download_file(u, os.path.join(outdir, os.path.basename(u)))
        bar.update(1)

    with ThreadPool(n_jobs) as pool:
        pool.map(dl, urls)


def read_dwd_kl_file(kl_zip_file):
    '''
    files from
    https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl
    '''
    f = zipfile.ZipFile(kl_zip_file)

    data_file = next(filter(lambda n: n.startswith('produkt_'), f.namelist()))

    return pd.read_csv(
        io.TextIOWrapper(f.open(data_file)),
        sep=r';\s*',
        parse_dates=['MESS_DATUM'],
        na_values=[-999],
        engine='python',
        encoding='ascii',
    ).drop('eor', axis=1)
