import requests
from bs4 import BeautifulSoup as bs4

class PlayerInfo:
    def __init__(self):
        self.url = url = "https://sofifa.com/players?offset="



    def get_player_info(self, offset:str):
        player_data = []
        self.url = self.url + str(offset)
        

        p_html = requests.get(self.url)
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



