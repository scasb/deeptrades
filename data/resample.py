import pandas as pd
import os

from utils import get_file_list

def load_dataset(path):
    df = pd.read_csv(path, names=['timestamp', 'price', 'amount'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    return df.set_index('timestamp')

def resample_dataset(df, sample_rate):
    result = df['price'].resample(sample_rate).ohlc().ffill()
    result['volume'] = df['amount'].resample(sample_rate).sum()
    return result

def resample_directory(input_path, output_path, sample_rate):
    for filepath, filename in get_file_list(input_path):
        print('Reading: ', filename)
        df = load_dataset(filepath)
        print('Resampling:', filename)
        df = resample_dataset(df, sample_rate)
        df.to_csv(os.path.join(output_path, filename))

def get_paths(base_path, sample_rate):
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    ticks_path = os.path.join(base_path, 'ticks')
    if not os.path.exists(ticks_path):
        os.makedirs(ticks_path)
    samples_path = os.path.join(base_path, 'samples', sample_rate)
    if not os.path.exists(samples_path):
        os.makedirs(samples_path)
    return (ticks_path, samples_path)

if __name__ == "__main__":
    base_path = 'e:/datasets'
    sample_rate = '15min'
    ticks_path, samples_path = get_paths(base_path, sample_rate)
    resample_directory(ticks_path, samples_path, '15min')