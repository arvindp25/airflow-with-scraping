import gspread


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
