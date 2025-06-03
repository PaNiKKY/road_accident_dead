import os
import sys
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def load_to_dataframe(download_link):

    try:
        df = pd.read_csv(download_link, encoding="TIS-620")
    except:
        df = pd.read_csv(download_link, encoding="utf-8")
        df.rename(columns={"acc_district_name":"Acc Dist"}, inplace=True)
    finally:
        csv = df.to_csv(index=False)

    return csv

