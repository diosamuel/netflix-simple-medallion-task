import pandas as pd
def pull_dataset():
    df = pd.read_csv("../dataset/netflix_titles.csv")
    return df