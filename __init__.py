from airflow import DAG
from airflow.operators.python_operators import PythonOperator 
from datetime import timedelta, datetime
from scraping import get_player_info

import gspread
import requests
from bs4 import BeautifulSoup as bs4


creds = {
  "type": "service_account",
  "project_id": "gspread-330006",
  "private_key_id": "e5294426192d31a9b4a214deb9f8da2f5d3320fa",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDCBg3BUzkJgDY1\n83I1vgl8yl1jfWvzPFp8V1gRX4dFeT1jh4g+7qrI37QZBRIxOacZH77Dif3vFG5X\nfJcIUF7tsk67AIzRCM1khuq78jHGhq99FTt+oMWFqmSUdifLBYul7gHXnc0TWeZd\nQrt0lHXAanfKshahTVvFHZGc3Jhj9hmZ1/TOlVZnfEiU68nmCY8Mssi+k3AzKTJY\n6c3HHv2AHXj6GKS8gpq8uZloEsKoQnZ8MrwaPXUlzq4r3lMjtGO/IV1+g0WnWUIj\ngq0Kkh1m6wUAnvBvn/w1qMP9QE6w4J8SnrhCJ1P0bkOwjWHvvX6IWyjmo+HimE1m\n8zcUuB3PAgMBAAECggEACo9nykDsVs8KpE8WxUQNoiQlhxe5e2iqiB/85B0t8KMT\nq0oiKUK2Ou6qD76N6e34YU6wBFaLYqrbApn+Ym51bDyLOnRiNbJj83fAaPBaszJ7\ndLRmI5M+jPLkPIxdyBgKLMLSiNjG46m99w99wm+eJRXmZTriLm9WclkUptP+21ne\nWrgVWCnrdTeoY6kSK2XEA/GHVL1B1XsdFTtmUCMJCyxjXx+eOAjgFg8KNXdy6JVI\nmJxIKuVzxKdCfZ6SiDacY7tKiAdQtzE3Ybpx8HUMYIWmOGUsGSuy5p/mNuBOH/r2\nn6ZzfYmNfDsQvQ79jTOyJB4NhIBupeWosG4Gjn2WEQKBgQDzFZHhlAC+03vnQP8x\nf947yD2/5kmFJSeDEWokFtXR+2mpWFnx2JRJ7iF7Uau3pV2dXOXzY4CzBAhFGd+N\n/RNi8J5zDboRU/LUwzi8IpSbuj9V2YfVC4aEJh7Zb5cefctz908rYyRMlICqi6or\nQOjguiuEfhKqtfxVN4HMcgGb9wKBgQDMVSlvSmwkPEqeF+RtBFpN59YL6WY5zdlP\nXwEYUWHYd3dFS9aSw0TuobrDQ20dz/71jOZit6A35bPP2ieGJzFb7cD6IpGaDss4\ns6AEE1bxTl1CWQNoKMLWs2bVLqxYlcI7Wq1p/gNQc3tFvVgnPSF01rZzWbb0jIrY\nrCBRjRmm6QKBgQC3PNk8zA5LyqupxOsZayQguG8jyvo8rac9O9Fg5v3DTdt2Vi1n\nCzWtDR6cERJ6WuNYjgChGa6YQz/gpwJHzC/i8zLd08kg6sEv9QE9gGu5gAniMxob\nVJbP4tR6X43w5C6Lei9pq1BfkcnzxVg/RqzFSlEnjMmReaF+s1ravE3OPQKBgDmL\nrmyqYARgen9FqU5OkIQqS6z1IZYb9ByXW/6WROEq3AtHDXotxkcSgz+kyKFBx8Mt\n2GBq6JN1fIuIG+N7PDpwng2UGL9zdSZV6DXdoHm7ISajXQNDn4PJ6KcXSvBz1MzU\nf8w+/n301/3PEnmSjM6T/bREyvLadewJpdxxyYypAoGBAMk9uoIjyb8kqkT3PKgQ\np1Fx0+y7lsrXrxLVDSnKUgIZiCQE8jtxPtr9gC3OldSmBpTO2ULF/HtgHWVyQXpD\nh1LBwZRsyYfYMMBueayrX69+hpgpJiXTBVWrycZwhhKqJCvhet6AN504w1PBapXn\nwfPB2dwQMZkUolkoon/ehciv\n-----END PRIVATE KEY-----\n",
  "client_email": "gspread@gspread-330006.iam.gserviceaccount.com",
  "client_id": "103670363662717283605",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gspread%40gspread-330006.iam.gserviceaccount.com"
}



def add_data():
    player_data =[]
    gc = gspread.service_account(filename = "creds.json")
    sh = gc.open("scraped_data").sheet1

    # for dic in player_data:
    #     sh.append_row([dic["picture"],
    #                   dic["ID"], 
    #                   dic["flag"],
    #                   dic["Name"],
    #                   dic["Age"],
    #                   dic["Position"],
    #                   dic["Overall"],
    #                   dic["Potential"],
    #                   dic["Team_image"],
    #                   dic["Team"],
    #                   dic["Value"],
    #                   dic["Wage"],
    #                   dic["Total_Point"] ])
add_data()



def get_player_info(**kwargs):
    offset = kwargs.get("offset", 60)
    player_data = []
    url = f"https://sofifa.com/players?offset={offset}"
    p_html = requests.get(url)
    p_soup = p_html.text
    data = bs4(p_soup,'html.parser')
    table = data.find('tbody')
    for i in table.findAll('tr'):
        data_dic = {}
        td = i.findAll('td')
        try:
            data_dic["picture"] = td[0].find('img').get('data-src')
        except:
            pass
        try:        
            data_dic["ID"] = td[0].find('img').get('id')
        except:
            pass
        try:

            data_dic["flag"] = td[1].find('img').get('data-src')
        except:
            pass
        try:        

            data_dic["Name"] = td[1].find("a").text
        except:
            pass
        try:        
            
            data_dic["Age"] = td[2].text.split()
        except:
            pass
        try:
            pos = td[1].find_all("span")
            totl_pos = ""
            for i in pos:
                totl_pos += f", {i.text}"

            data_dic["Position"] = totl_pos.strip(", ")
        except:
            pass
        try:

            data_dic["Overall"] = td[3].find('span').text
        except:
            pass
        try:        
            data_dic["Potential"] = td[4].find('span').text
        except:
            pass
        try:        

            data_dic["Team_image"] = td[5].find('img').get('data-src')
        except:
            pass
        try:        
            data_dic["Team"] = td[5].find('a').text
        except:
            pass
        try:        
            data_dic["Value"] = td[6].text.strip()
        except:
            pass
        try:        
            data_dic["Wage"] = td[7].text.strip()
        except:
            pass
        try:        

            data_dic["Total_Point"] = td[8].text.strip()
        except:
            pass
        player_data.append(data_dic)

        
    return player_data



default_args ={
 'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)

}



with DAG(
    dag_id = "scraper_dag",
    schedule_interval = "5 * * * *",
    default_args = default_args,
    start_date=datetime(2021, 1, 1),
    catchup = False
) as fp:
    run_scraper = PythonOperator(
        task_id = "run_scraper",
        python_callable = get_player_info,
        op_kwargs={"offset":60}
    )
    
    push_to_gsheet = PythonOperator(
        task_id = "push_to_gsheet",
        python_callable = insert_gsheet,
    )

    run_scraper >> push_to_gsheet