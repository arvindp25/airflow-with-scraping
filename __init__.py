from airflow import DAG
from airflow.operators.python_operators import PythonOperator 
from datetime import timedelta, datetime
from scraping import get_player_info

import gspread
import requests
from bs4 import BeautifulSoup as bs4


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
