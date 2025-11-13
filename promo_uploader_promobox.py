import pandas as pd 
from datetime import datetime, timedelta
from sql_data import snowflake_queries
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import logging
import numpy as np 
from constants import GSHEET_CLIENT, SQL_CLIENT,  SF
from string import ascii_uppercase
from picnic.tools import config_loader

letters = ascii_uppercase * 4

import sys, os
origin=os.getcwd()
# origin += '/03_Preparation/Upload'

pd.options.mode.chained_assignment = None



class promo_to_salesforce:

    def __init__(self, salesforce_connector, sql_client,campaign_id,start_date,end_date):
        
        config = config_loader.load_config()

        self.environment = config['environment']
        
        self.saleforce_connector = salesforce_connector
        self.snowflake_instance = snowflake_queries(sql_client)



        self.worksheet = GSHEET_CLIENT.open_by_key('1pcrI_UBBduVqQhXDnsRpF06oHiNlQJCL9uWQCx_4KCk').worksheet('Promo_Upload')
        self.validator = "'false' && tags.contains('')"
        self.campaign_based = True
        self.promo_start_date = start_date
        self.promo_end_date = end_date
        self.campaign_id = campaign_id



    def read_data(self):    

        promo_dataframe = get_as_dataframe(worksheet=self.worksheet, evaluate_formulas=True)
        promo_dataframe = promo_dataframe[promo_dataframe['Promo Group'].notna()]


        promo_dataframe['X'] = promo_dataframe['X'].astype(str)
        promo_dataframe['Y'] = promo_dataframe['Y'].astype(str)
        promo_dataframe['article_id'] = promo_dataframe['article_id'].apply(lambda x: str(int(float(x))))
        print(promo_dataframe)
        return promo_dataframe
            
    
    def retrieve_su_info(self):

        su_data = self.snowflake_instance.collect_data(origin+'/selling_unit.sql')
        su_data['article_id'] = su_data['article_id'].astype(str)
        su_dict = su_data.set_index('article_id').to_dict()
        su_dict = su_dict['id']
        return su_dict



    def query_salesforce(self, sf_fields, sf_object, sf_filter):
        sf_select_from_template = 'SELECT {} FROM {} WHERE {}'
        query = sf_select_from_template.format(',\n'.join(sf_fields), sf_object, sf_filter)
        products = self.saleforce_connector.query_all(query)
        records = products['records'] 
        return pd.DataFrame(records)

    
    def return_product_ids(self, promo_dataframe : pd.DataFrame):

        article_ids_list = promo_dataframe['article_id'].unique()

        article_ids = [article_id.split('.')[0] for article_id in article_ids_list]
        

        sf_field = ['PN_Article_Id__c', 'Id']
        sf_object = 'Product2'

        sf_filter = f'PN_Article_Id__c IN {tuple(article_ids)}' if len(article_ids) != 1 else f"PN_Article_Id__c = '{article_ids[0]}'"

        found_id_df = self.query_salesforce(sf_field, sf_object, sf_filter)

        found_id_df['PN_Article_Id__c'] = found_id_df['PN_Article_Id__c'].astype(str)

        return found_id_df



    def create_salesforce_object(self, variable_data : list | dict,
        salesforce_object, depth : str):


        try:

            if self.environment != "prod":
                print(f"[SKIPPED - NON PROD ENV] Would create {depth}: {variable_data}")
                return 

            response = salesforce_object.create(variable_data)
            return response['id']

        except Exception as e:
            logging.error(f'Failed to create promotion object {depth}.')
            logging.error(f'With the following variable_data {variable_data}.')
            logging.error(f'{e}')


    def obtain_promotion_info(self, promo_data : pd.DataFrame, x_var : str, 
    y_var : str, mech_var : str, strike_var : str, name: str):

        row = promo_data.iloc[0]
        x = '' if row[x_var] == 'nan' else row[x_var]
        y = None if row[y_var] == 'nan' else int(float(row[y_var])) 
        mechanism = row[mech_var]
        strikethrough = True if row[strike_var] == 1 else False
        promo_name = row[name]
        
        return x, y, mechanism, strikethrough, promo_name
    
    def obtain_pg_info(self, group_data=pd.DataFrame, name=str):
        row=group_data.iloc[0]
        group_name=row[name]
        return group_name


    def article_id_to_sf(self, promo_dataframe : pd.DataFrame, 
    sf_dataframe : pd.DataFrame):

        merged = pd.merge(left = promo_dataframe, right = sf_dataframe,
        how = 'inner', left_on = 'article_id', right_on = 'PN_Article_Id__c')

        return merged




    def create_promotions(self, campaign_id : str,
        promo_dataframe : pd.DataFrame, promo_start_date : str,
        promo_end_date : str):

        su_dict = self.retrieve_su_info()

        promo_dataframe['Promo group combination'] = promo_dataframe['Promo Group'].astype(str) + promo_dataframe['Promo Name'].astype(str)

        unique_promo_groups = promo_dataframe['Promo group combination'].unique()

        promo_dataframe['Promo combination'] = promo_dataframe['Promo Name'].astype(str)  + promo_dataframe['X'].astype(str)  + promo_dataframe['Y'].astype(str)\
        +  promo_dataframe['Mechanism'].astype(str) +  promo_dataframe['Hide strikethrough'].astype(str)

        for promo_group in unique_promo_groups:
            print(f'Creating promotion group {promo_group}')
            

            promo_group_data = promo_dataframe[promo_dataframe['Promo group combination'] == promo_group]
            group_name = self.obtain_pg_info(group_data=promo_group_data, name='Promo Group')



            variable_dict = {'Campaign__c' : campaign_id,
                             'Name' : group_name,
                             'Start_Date__c' : self.promo_start_date,
                             'End_Date__c' : self.promo_end_date,
                             'Type__c' : 'Article',
                             'Sub_Type__c' : 'Discounts', 
                            #  'Rank_No__c' : promo_group_data['PG_Rank'].max(),
                             'Is_Promo_Box__c': True,
                             'Hidden_on_Promo_Page__c': False}
            
            promo_group_id = self.create_salesforce_object(salesforce_object=self.saleforce_connector.Promotion_Group__c,
            variable_data=variable_dict, depth = promo_group)
            
            unique_promotions = promo_group_data['Promo combination'].unique()
            
            for promotion in unique_promotions:

                promo_article_data = promo_group_data[promo_group_data['Promo combination'] == promotion]
                
                x, y, mechanism, strikethrough, promo_name = self.obtain_promotion_info(promo_data=promo_article_data, x_var='X', y_var='Y',
                mech_var='Mechanism', strike_var='Hide strikethrough', name='Promo Name')

                promo_var_dict = {'Name' : promo_name,
                                  'Promotion_Group__c' : promo_group_id,
                                  'Data_Flow_Via__c' : 'PIM Cache',
                                #    'Validator__c' : self.validator,
                                   'Campaign_Based__c': self.campaign_based,
                                   'Promotion_Mechanism__c' : mechanism,
                                   'Promotion_Mechanism_Variable_X__c' : x,
                                   'Promotion_Mechanism_Variable_Y__c' : y,
                                   'Hide_Strikethrough_Price__c' : strikethrough,
                                   'Hidden_On_Promo_Page__c': False,
                                   'Maximum_Applications_Per_Order__c': 5}
                
        

                print(f'Creating promotion {promo_group} with mechanism {x} {mechanism} {y}')
        

                promotion_id = self.create_salesforce_object(salesforce_object=self.saleforce_connector.Promotion__c,
                variable_data=promo_var_dict, depth = f'{promo_group}-{promotion}' )
                
                
                for _, row in promo_article_data.iterrows():

                        

                    sf_id = str(row['Id'])
                    art_id = row['article_id']


                    ppi_dict = {'Picnic_Article__c' : sf_id, 'Promotion__c' : promotion_id, 
                                'Purchase_Discount__c' : 0, 'Selling_Unit__c' : su_dict[art_id],
                                # 'Rank_No__c': row['PPI Rank'],
                                'Hidden_On_Promo_Page__c': False
                                }

        
                    
                    ppi = self.create_salesforce_object(salesforce_object=self.saleforce_connector.Picnic_Promotion_Item__c,
                    variable_data=ppi_dict, depth = f'{promo_group}-{art_id}')



    def upload_pipeline(self):

        promo_dataframe = self.read_data()

        sf_df = self.return_product_ids(promo_dataframe=promo_dataframe)

        merged_df = self.article_id_to_sf(promo_dataframe=promo_dataframe, sf_dataframe = sf_df)


        self.create_promotions(campaign_id=self.campaign_id, promo_dataframe=merged_df, 
        promo_start_date=self.promo_start_date, promo_end_date=self.promo_end_date)




test = promo_to_salesforce(salesforce_connector=SF,
sql_client=SQL_CLIENT,campaign_id='701P900000YXXUnIAP',start_date='2025-11-05',end_date='2026-01-31')

lift = test.upload_pipeline()
