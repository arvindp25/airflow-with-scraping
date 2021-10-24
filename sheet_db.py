import gspread
import json
from scraping import get_player_info

def add_data():
    player_data = get_player_info()
    gc = gspread.service_account(filename = "creds.json")
    sh = gc.open("scraped_data").sheet1
    # sh.append_row(["picture","ID", "flag","Name","Age", "Position", "Overall", "Potential", "Team_image", "Team", "Value","Wage","Total_Point" ])

    for dic in player_data:
        sh.append_row([dic["picture"],
                      dic["ID"], 
                      dic["flag"],
                      dic["Name"],
                      dic["Age"],
                      dic["Position"],
                      dic["Overall"],
                      dic["Potential"],
                      dic["Team_image"],
                      dic["Team"],
                      dic["Value"],
                      dic["Wage"],
                      dic["Total_Point"] ])
add_data()