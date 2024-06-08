import requests
import json
import pandas as pd
from bs4 import BeautifulSoup


url = 'https://www.speedtest.net/global-index#mobile'

response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')

    script_tags = soup.find_all('script')

    for script_tag in script_tags:
        if 'results' in script_tag.text:
            json_data = script_tag.text.split('results = ')[1]
            json_data = json_data.strip().rstrip(';')

            json_dict = json.loads(json_data)

            mobile_data = json_dict['mobileMean']
            fixed_data = json_dict['fixedMean']

            aggregate_date_mobile = []
            platform_mobile = []
            country_id_mobile = []
            download_mbps_mobile = []
            upload_mbps_mobile = []
            latency_ms_mobile = []
            jitter_mobile = []
            rank_mobile = []
            month_mobile = []
            prevMonth_mobile = []
            country_name_mobile = []

            for data in mobile_data:
                aggregate_date_mobile.append(data['aggregate_date'])
                platform_mobile.append(data['platform'])
                country_id_mobile.append(data['country_id'])
                download_mbps_mobile.append(data['download_mbps'])
                upload_mbps_mobile.append(data['upload_mbps'])
                latency_ms_mobile.append(data['latency_ms'])
                jitter_mobile.append(data['jitter'])
                rank_mobile.append(data['rank'])
                month_mobile.append(data['month'])
                prevMonth_mobile.append(data['prevMonth'])
                country_name_mobile.append(data['country']['country_name'])

            aggregate_date_fixed = []
            platform_fixed = []
            country_id_fixed = []
            download_mbps_fixed = []
            upload_mbps_fixed = []
            latency_ms_fixed = []
            jitter_fixed = []
            rank_fixed = []
            month_fixed = []
            prevMonth_fixed = []
            country_name_fixed = []

            for data in fixed_data:
                aggregate_date_fixed.append(data['aggregate_date'])
                platform_fixed.append('Fixed Broadband' if data['platform'] == 'Fixed' else data['platform'])
                country_id_fixed.append(data['country_id'])
                download_mbps_fixed.append(data['download_mbps'])
                upload_mbps_fixed.append(data['upload_mbps'])
                latency_ms_fixed.append(data['latency_ms'])
                jitter_fixed.append(data['jitter'])
                rank_fixed.append(data['rank'])
                month_fixed.append(data['month'])
                prevMonth_fixed.append(data['prevMonth'])
                country_name_fixed.append(data['country']['country_name'])

            df_mobile = pd.DataFrame({
                'Aggregate_Date': aggregate_date_mobile,
                'Platform': platform_mobile,
                'Country_ID': country_id_mobile,
                'Download_MBPS': download_mbps_mobile,
                'Upload_MBPS': upload_mbps_mobile,
                'Latency_MS': latency_ms_mobile,
                'Jitter': jitter_mobile,
                'Rank': rank_mobile,
                'Month': month_mobile,
                'PrevMonth': prevMonth_mobile,
                'Country_Name': country_name_mobile
            })

            df_fixed = pd.DataFrame({
                'Aggregate_Date': aggregate_date_fixed,
                'Platform': platform_fixed,
                'Country_ID': country_id_fixed,
                'Download_MBPS': download_mbps_fixed,
                'Upload_MBPS': upload_mbps_fixed,
                'Latency_MS': latency_ms_fixed,
                'Jitter': jitter_fixed,
                'Rank': rank_fixed,
                'Month': month_fixed,
                'PrevMonth': prevMonth_fixed,
                'Country_Name': country_name_fixed
            })

            print("Mobile Data:")
            print(df_mobile)
            df_mobile.to_csv('mobile_data.csv', index=False)
            print("Mobile Data is saved in CSV")
            print("\n\nFixed Broadband Data:")
            print(df_fixed)
            df_fixed.to_csv('fixed_broadband_data.csv', index=False)
            print("Fixed Broadband Data is saved in CSV")


            break
    else:
        print("JSON data not found in script tags")
else:
    print("Failed to retrieve the webpage")
