# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

# import necessary modules
# import psycopg2
import datetime
from datetime import date
import time
from google.cloud import pubsub
from google.oauth2 import service_account
import subprocess
import scrapy
import scrapy.crawler
from scrapy.utils.project import get_project_settings
import json
import os
import tempfile

# set today's date
date_today = date.today()

class PoliticiansPipeline(object):
     def open_spider(self, spider):
          pass

#          hostname = 'localhost'
#          username = 'postgres'
#          password = 'postgres'
#          database = 'politics'
#          self.conn = psycopg2.connect(
#              host = hostname,
#              user = username,
#              password = password,
#              dbname = database)
#          self.cur = self.conn.cursor()
            
#      def close_spider(self, spider):
#          self.cur.close()
#          self.conn.close()
            
     def process_item(self, item, spider):
#          select_query = """select first_name, last_name, party, state from politicians"""
#          self.cur.execute(select_query)
#          politicians_list = list(self.cur)
#          pol_check_tuple = (item['first_name'], item['last_name'], item['party'], item['state'])
            
#          insert_query = """insert into politicians (first_name, last_name, party, state)
#              values (%s, %s, %s, %s)"""
#          pol_packet = (item['first_name'], item['last_name'], item['party'], item['state'])
                    
#          if pol_check_tuple in politicians_list:
#              return item
#          else:
#              self.cur.execute(insert_query, vars = pol_packet)
#              self.conn.commit()
#              return item
#           pass
            def doubleQString(x):
                 return "{0}".format(x);
            cred_dict = {
                             "auth_provider_x509_cert_url": doubleQString(spider.settings.get('auth_provider_x509_cert_url')),
                             "auth_uri": doubleQString(spider.settings.get('auth_uri')),
                             "client_email": doubleQString(spider.settings.get('client_email')),
                             "client_id": doubleQString(spider.settings.get('client_id')),
                             "client_x509_cert_url": doubleQString(spider.settings.get('client_x509_cert_url')),
                             "private_key": doubleQString(spider.settings.get('private_key')),
                             "private_key_id": doubleQString(spider.settings.get('private_key_id')),
                             "project_id": doubleQString(spider.settings.get('project_id')),
                             "token_uri": doubleQString(spider.settings.get('token_uri')),
                             "type": doubleQString(spider.settings.get('account_type'))
                 }
            print(cred_dict)
            cred_json = json.dumps(cred_dict)
               
               
            # Create a temporary file here
            fd, path = tempfile.mkstemp()
            print(path)

            # Then use a 'with open' statement as shown in the stackoverflow comments
            with os.fdopen(fd, 'w') as tmp:
                json.dump(cred_dict, tmp)
                tmp.close()
            # # Add in the json dump phrase with the right file location
            # # figure out how to properly add the file to either the application credentials or explicit in the call
            # # make sure to delete the temporary file

            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path

            publisher = pubsub.PublisherClient()
          
            topic = 'projects/{project_id}/topics/{topic}'.format(
                 project_id='politics-data-tracker-1',
                 topic='house_members')
            publisher.publish(topic, b'This is a representative in the House.', 
                              first_name = item['first_name'],
                              last_name = item['last_name'],
                              party = item['party'],
                              state = item['state'])
            os.remove(path)

# class HouseMembersPipeline(object):
# #     def open_spider(self, spider):
# #         hostname = 'localhost'
# #         username = 'postgres'
# #         password = 'postgres'
# #         database = 'politics'
# #         self.conn = psycopg2.connect(
# #             host = hostname,
# #             user = username,
# #             password = password,
# #             dbname = database)
# #         self.cur = self.conn.cursor()
    
# #     def close_spider(self, spider):
# #         self.cur.close()
# #         self.conn.close()
   
#     def process_item(self, item, spider):
# #         house_member = ()
        
# #         select_query = """select first_name, last_name, party, state from house"""
# #         self.cur.execute(select_query)
# #         rep_tuple_list = list(self.cur)
         
# #         select_query = """select first_name, last_name, party, state from politicians"""
# #         self.cur.execute(select_query)
# #         pol_tuple_list = list(self.cur)
            
# #         select_query = """select * from house"""
# #         self.cur.execute(select_query)
# #         all_reps_list = list(self.cur) # eventually change this to Pandas data frame
            
# #         select_query = """select * from politicians"""
# #         self.cur.execute(select_query)
# #         all_pols_list = list(self.cur) # eventually change this to Pandas data frame
            
# #         select_all_query = """select id from house"""
# #         self.cur.execute(select_query)
# #         house_reps_list = list(self.cur) # eventually change this to Pandas data frame
          

#         pol_tuple = (item['first_name'], item['last_name'], item['party'], item['state'])
        
# #         insert_query = """insert into house (id, first_name, last_name, party, 
# #             state, district, start_date, end_date, tenure_num)
# #             values (%s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
# #         update_query = """update house
# #                      set end_date = %s
# #                      where id = %s"""
      
#         pol_id = all_pols_list[pol_tuple_list.index(pol_tuple)][0]
#         house_pol_packet = (pol_id, 
#                             item['first_name'], 
#                             item['last_name'], 
#                             item['party'],
#                             item['state'],
#                             item['district'],
#                             date_today,
#                             date_today,
#                             house_reps_list.count(pol_id)+1)
              
#         if (pol_tuple in rep_tuple_list
#             ###fix this in the morning
#             and date_today - all_reps_list[rep_tuple_list.index(pol_tuple)][7] 
#             # the previous line needs to change as it will not be functional past two iterations
#             <= datetime.timedelta(days = 30)):
# #             self.cur.execute(update_query, vars = [date_today, pol_id])
# #             self.conn.commit()
#             return item
#         elif (pol_tuple in rep_tuple_list 
#             and date_today - all_reps_list[rep_tuple_list.index(pol_tuple)][7] 
#             # the previous line needs to change as it will not be functional past two iterations
#             > datetime.timedelta(days = 30)):
# #             self.cur.execute(insert_query, vars = house_pol_packet)
# #             self.conn.commit()
#             return item
#         else:
# #             self.cur.execute(insert_query, vars = house_pol_packet)
# #             self.conn.commit()
#             return item
        
#         # This is the trial code from the separate pipeline

# #         select_query = """select first_name, last_name, party, state from politicians"""
# #         self.cur.execute(select_query)
# #         politicians_list = list(self.cur)
# #         pol_check_tuple = (item['first_name'], item['last_name'], item['party'], item['state'])
# #            
# #         insert_query = """insert into politicians (first_name, last_name, party, state)
# #             values (%s, %s, %s, %s)"""
# #         pol_packet = (item['first_name'], item['last_name'], item['party'], item['state'])
# #   
# #         if pol_check_tuple in politicians_list:
# #              pass
# #         else:
# #              self.cur.execute(insert_query, vars = pol_packet)
# #              self.conn.commit()
# #              return item
       
