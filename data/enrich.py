import os
import pandas as pd
import ta

from utils import get_file_list

def load_sampleset(path):
    df = pd.read_csv(path)
    return df.set_index('timestamp')

def enrich_sampleset(df):
    df['OBV'] = ta.on_balance_volume(df['close'], df['volume'])
    df['ADX'] = ta.adx(df['high'], df['low'], df['close'], n=14)
    return df

def get_paths(base_path, sample_rate):
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    samples_path = os.path.join(base_path, 'samples', sample_rate)
    if not os.path.exists(samples_path):
        os.makedirs(samples_path)
    rich_path = os.path.join(base_path, 'rich')
    if not os.path.exists(rich_path):
        os.makedirs(rich_path)
    return (samples_path, rich_path)

sample_path, rich_path = get_paths('E:\\Datasets\\', '15min')

for filepath, filename in get_file_list(sample_path):
    print('Reading:', filename)
    df = load_sampleset(filepath)
    print('Enriching:', filename)
    df = enrich_sampleset(df)
    print('Saving:', filename)
    df.to_csv(os.path.join(rich_path, filename))
