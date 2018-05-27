import os
import requests
import gzip
import shutil

def extract(input_path, output_path):
    if os.path.exists(output_path):
        return False
    with gzip.open(input_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return True

def download(url, output_path):
    if os.path.exists(output_path):
        return False
    r = requests.get(url, stream=True)
    with open(output_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            f.write(chunk)
    r.close()
    return True

def get_paths(base_path):
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    cache_path = os.path.join(base_path, 'downloads')
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
    data_path = os.path.join(base_path, 'ticks')
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    return (cache_path + '/{0}.csv.gz', data_path + '/{0}.csv')

def download_list(base_path, base_url, targets):
    cache_format, data_format = get_paths(base_path)
    for target in targets:
        url = base_url.format(target)
        cache_file = cache_format.format(target)
        data_file = data_format.format(target)
        print('Downloading {0}...'.format(target), end='', flush=True)
        if download(url, cache_file):
            print('done.')
        else:
            print('skipped.')
        print('Extracting {0}...'.format(target), end='', flush=True)
        if extract(cache_file, data_file):
            print('done.')
        else:
            print('skipped.')

if __name__ == "__main__":
    targets = ['bitfinexUSD', 'bitflyerJPY', 'bitstampUSD', 'coinbaseUSD', 'korbitKRW', 'zaifJPY']
    base_url = 'https://api.bitcoincharts.com/v1/csv/{0}.csv.gz'
    base_path = 'e:/datasets/'
    download_list(base_path, base_url, targets)