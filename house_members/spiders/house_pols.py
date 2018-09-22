'''
Created on May 29, 2018

@author: cmatt
'''

import scrapy
# import psycopg2

class HousePolsSpider(scrapy.Spider):
    name = "house_pols"
    
    def start_requests(self):
        start_url = "https://www.house.gov/representatives"
        yield scrapy.Request(url = start_url, callback = self.parse)
    
    def parse(self, response):
#         hostname = 'localhost'
#         username = 'postgres'
#         password = 'postgres'
#         database = 'politics'
#         self.conn = psycopg2.connect(
#             host = hostname,
#             user = username,
#             password = password,
#             dbname = database)
#         self.cur = self.conn.cursor()
#         map_query = """select state, initials from state_map"""
#         self.cur.execute(map_query)
#         mapping_list = list(self.cur)
#         self.cur.close()
#         self.conn.close()
#         state_list = [i[0] for i in mapping_list]
#         initials_list = [i[1] for i in mapping_list]
        
        # finds the path to the table that contains a single state's representatives
        for region in response.xpath("//table[@class='table']"):
            # filters out any non-state tables
            if "state" not in region.xpath(".//caption/@id").extract_first():
                continue
            else: 
                pass
            # pulls the state of all representatives in this group; will be pulled into dictionary
            # also maps from full state to state initial
            state = region.xpath(".//caption/text()").extract_first()
            state = state.strip(' \t\n\r')
#             state = initials_list[state_list.index(state_processed)]
            
            # establishes the path to each representative of the one state
            for rep in region.xpath(".//tbody/tr"):
                full_name = rep.xpath(".//td/a/text()").extract_first()
                
                # pulls the first name of the representative
                if "," in full_name:
                    first_name = full_name.split(',')[1]
                else:
                    full_name = full_name.replace(" ", ",", 1)
                    first_name = full_name.split(',')[1]
                first_name = first_name.strip(' \t\n\r')
                
                # pulls the last name of the representative
                last_name = full_name.split(',')[0]
                last_name = last_name.strip(' \t\n\r')
                
                # pulls the party of the given representative
                party = rep.xpath(".//td/text()").extract()[2]
                party = party.strip(' \t\n\r')
                
                # pulls the district of the given representative
                district = rep.xpath(".//td/text()").extract()[0]
                district = district.strip(' \t\n\r')
                
                # combines all data pieces of a representative into a dictionary
                rep_dict = {
                    'first_name': first_name,
                    'last_name': last_name,
                    'party': party,
                    'state': state,
                    'district': district}
                pol_item = PoliticiansItem(rep_dict)
                print(pol_item)
                # yields the dictionary, which will then get uploaded to database in pipeline
                yield pol_item;
