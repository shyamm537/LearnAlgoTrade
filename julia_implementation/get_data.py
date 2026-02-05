import pandas as pd
import zipfile
import requests

from io import StringIO, BytesIO

def main():
    sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {'User-Agent': 'Mozilla/5.0'}

    print("Fetching S&P 500 data from wikipedia.")
    sp500_response = requests.get(sp500_url , headers=headers)
    print("S&P500 data fetched.") 
    sp500 = pd.read_html(StringIO(sp500_response.text), flavor='html5lib')[0]
    
    print("Fetching FF factors data from wikipedia.")
    ff_url = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip'
    ff_response = requests.get(ff_url, headers=headers)
    print("FF factors data fetched.") 

    with zipfile.ZipFile(BytesIO(ff_response.content)) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            data_text = f.read().decode('utf-8')
            monthly_data = data_text.split('\r\n\r\n')[1]

            ff_factors = pd.read_csv(StringIO(monthly_data))
            f.close()

    # now we have the two pandas dataframes: sp500 and ff_factors
    # we can just save this data and load it later

    # save sp500
    sp500.to_csv('../data/sp500_latest.csv')
    ff_factors.to_csv('../data/ff_factors.csv')
    print("S&P 500 and FF factors data saved in the data directory.")
    return

if __name__=="__main__":
    main()
