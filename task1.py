from datetime import datetime, timedelta

import pandas as pd
import requests

# API to fetch corona cases daily
CORONA_INDIA_API = 'https://api.covid19india.org/states_daily.json'

# Dictionary to match the abbreviated state name to the state's full name
abbreviation_dict = {
    "an": "Andaman and Nicobar Islands",
    "ap": "Andhra Pradesh",
    "ar": "Arunachal Pradesh",
    "as": "Assam",
    "br": "Bihar",
    "ch": "Chandigarh",
    "ct": "Chhattisgarh",
    "dd": "0",
    "dl": "Delhi",
    "dn": "Dadra and Nagar Haveli and Daman and Diu",
    "ga": "Goa",
    "gj": "Gujarat",
    "hp": "Himachal Pradesh",
    "hr": "Haryana",
    "jh": "Jharkhand",
    "jk": "Jammu and Kashmir",
    "ka": "Karnataka",
    "kl": "Kerala",
    "la": "Ladakh",
    "ld": "Lakshadweep",
    "mh": "Maharashtra",
    "ml": "Meghalaya",
    "mn": "Manipur",
    "mp": "Madhya Pradesh",
    "mz": "Mizoram",
    "nl": "Nagaland",
    "or": "Odisha",
    "pb": "Punjab",
    "py": "Puducherry",
    "rj": "Rajasthan",
    "sk": "Sikkim",
    "tg": "Telangana",
    "tn": "Tamil Nadu",
    "tr": "Tripura",
    "tt": "Total",
    "un": "State Unassigned",
    "up": "Uttar Pradesh",
    "ut": "Uttarakhand",
    "wb": "West Bengal"
}


def fetch_data_day_wise():
    """
    Function to fetch the number of cases in India Day-wise
    Stores the result in a CSV
    :return:
    """
    response = requests.get(CORONA_INDIA_API)
    response_json = response.json()
    for resp in response_json['states_daily']:
        if resp['date'] == yesterday_date and resp['status'] == 'Confirmed':
            resp_df = pd.DataFrame(list(resp.items()), columns=['abbreviated_state', 'count'])
            resp_df_final = resp_df.drop(
                resp_df[(resp_df.abbreviated_state == 'date') | (resp_df.abbreviated_state == 'status')].index)
            abbreviation_df = pd.DataFrame(list(abbreviation_dict.items()),
                                           columns=['abbreviated_state', 'state_name'])
            final_df = pd.merge(resp_df_final, abbreviation_df)[['state_name', 'count']]
            final_df.to_csv('/home/nineleaps/PycharmProjects/airflow/csvs/{}.csv'.format(yesterday_date), index=False)
            break
        else:
            continue


# Driver Program
if __name__ == '__main__':
    yesterday_date = (datetime.today() - timedelta(days=1)).strftime('%d-%b-%y')
    fetch_data_day_wise()
