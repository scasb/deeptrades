import pandas as pd
import os

def load_dataset(path):
    print('loading: ', path)
    df = pd.read_csv(path, names=['timestamp', 'price', 'amount'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    return df.set_index('timestamp')

def load_datasets(path):
    for filename in os.listdir(path):
        filepath = os.path.join(path, filename)
        if not os.path.isfile(filepath):
            continue
        yield (filename, load_dataset(filepath))

def resample_dataset(df, sample_rate):
    result = df['price'].resample(sample_rate).ohlc().ffill()
    result['volume'] = df['amount'].resample(sample_rate).sum()
    return result

def resample_directory(path, sample_rate):
    output_directory = os.path.join(path, sample_rate)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    for filename, df in load_datasets(path):
        print('saving: ', filename)
        df = resample_dataset(df, sample_rate)
        df.to_csv(os.path.join(output_directory, filename))

def load(filename, sample_rate):
    df = pd.read_csv(filename)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp')
    return resample_dataset(df, sample_rate)

if __name__ == "__main__":
    resample_directory('e:/btc/datasets', '15min')