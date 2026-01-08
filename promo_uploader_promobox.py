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

# To run locally 
# pipenv run python promo_uploader_promobox.py

# spreadsheet: https://docs.google.com/spreadsheets/d/1pcrI_UBBduVqQhXDnsRpF06oHiNlQJCL9uWQCx_4KCk/edit?gid=795318163#gid=795318163


class promo_to_salesforce:

    def __init__(self, salesforce_connector, sql_client,start_date,end_date):
        
        config = config_loader.load_config()
        print("Loaded configuration:")
        print(config)
        self.config = config

        self.environment = config['environment']
        print(f"Operating in environment: {self.environment}")
        print(type(self.environment))
        print(self.environment.extra)
        
        self.saleforce_connector = salesforce_connector
        self.snowflake_instance = snowflake_queries(sql_client)



        self.worksheet = GSHEET_CLIENT.open_by_key('1pcrI_UBBduVqQhXDnsRpF06oHiNlQJCL9uWQCx_4KCk').worksheet('Promo_Upload')
        self.validator = "'false' && tags.contains('')"
        self.campaign_based = True
        self.promo_start_date = start_date
        self.promo_end_date = end_date



    def read_data(self):    

        promo_dataframe = get_as_dataframe(worksheet=self.worksheet, evaluate_formulas=True)
        promo_dataframe = promo_dataframe[promo_dataframe['Promo Group'].notna()]

        promo_dataframe["Promo Group"] = promo_dataframe["Promo Group"].astype(str).str.strip()
        promo_dataframe["Promo Group"] = promo_dataframe["Promo Group"].replace({"nan": np.nan, "": np.nan})

        if promo_dataframe["Promo Group"].isna().any():
            raise ValueError("Found empty Promo Group values in the sheet. Fill them in for all rows.")

        if "Purchase Discount" not in promo_dataframe.columns:
            promo_dataframe["Purchase Discount"] = np.nan
        if "Purchase Discount Comment" not in promo_dataframe.columns:
            promo_dataframe["Purchase Discount Comment"] = np.nan


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

            if "prod" not in self.environment.extra:
                print(f"[SKIPPED - NON PROD ENV] Would create {depth}: {variable_data}")
                return 

            response = salesforce_object.create(variable_data)
            return response['id']

        except Exception as e:
            logging.error(f'Failed to create promotion object {depth}.')
            logging.error(f'With the following variable_data {variable_data}.')
            logging.error(f'{e}')
            raise e


    def obtain_promotion_info(self, promo_data : pd.DataFrame, x_var : str, 
    y_var : str, mech_var : str, name: str):

        row = promo_data.iloc[0]
        x = '' if row[x_var] == 'nan' else row[x_var]
        y = None if row[y_var] == 'nan' else int(float(row[y_var])) 
        mechanism = row[mech_var]
        promo_name = row[name]
        
        return x, y, mechanism, promo_name
    
    def obtain_pg_info(self, group_data=pd.DataFrame, name=str):
        row=group_data.iloc[0]
        group_name=row[name]
        return group_name


    def article_id_to_sf(self, promo_dataframe : pd.DataFrame, 
    sf_dataframe : pd.DataFrame):

        merged = pd.merge(left = promo_dataframe, right = sf_dataframe,
        how = 'inner', left_on = 'article_id', right_on = 'PN_Article_Id__c')

        return merged



    def create_promotions(
        self,
        promo_dataframe: pd.DataFrame,
        promo_start_date: str,
        promo_end_date: str,
    ) -> None:
        su_dict = self.retrieve_su_info()

        # --- Normalize + validate Campaign per row ---
        promo_dataframe["Campaign"] = promo_dataframe["Campaign"].astype(str).str.strip()
        promo_dataframe["Campaign"] = promo_dataframe["Campaign"].replace({"nan": np.nan, "": np.nan})

        if promo_dataframe["Campaign"].isna().any():
            raise ValueError("Found empty Campaign values in the sheet. Fill them in for all rows.")

        # Resolve campaign id per row
        promo_dataframe["campaign_id"] = promo_dataframe["Campaign"].apply(self.resolve_campaign_id)


        # --- Normalize fields we rely on ---
        for col in ["Promo Group", "Promo Name", "Mechanism", "X", "Y"]:
            promo_dataframe[col] = promo_dataframe[col].astype(str).str.strip()

        grouped = promo_dataframe.groupby(["campaign_id", "Campaign", "Promo Group"], dropna=False)
        if grouped.ngroups == 0:
            raise ValueError("No (Campaign, Promo Group) groups found in the sheet.")

        def _single_value(group_df: pd.DataFrame, col: str, group_name: str) -> str | None:
            vals = group_df[col].replace({"nan": np.nan, "": np.nan}).dropna().unique()
            if len(vals) == 0:
                return None
            if len(vals) > 1:
                raise ValueError(f"Promo Group '{group_name}' has multiple '{col}' values: {list(vals)}")
            return str(vals[0])


        # Define which mechanisms require Y
        MECH_REQUIRES_Y: set[str] = {"X_FOR_PRICE_Y", "ABSOLUTE_PRICE_Y", "X_PLUS_Y_FREE"}
        MECH_OPTIONAL_Y: set[str] = {"X_HALF_PRICE"}  # extend if needed

        for (campaign_id, campaign_name, group_name), group_df in grouped:
            group_df = group_df.copy()

            promo_name = _single_value(group_df, "Promo Name", group_name)
            mechanism = _single_value(group_df, "Mechanism", group_name)
            x_raw = _single_value(group_df, "X", group_name)
            y_raw = _single_value(group_df, "Y", group_name)

            if promo_name is None:
                raise ValueError(f"Promo Group '{group_name}' is missing required column 'Promo Name'.")
            if mechanism is None:
                raise ValueError(f"Promo Group '{group_name}' is missing required column 'Mechanism'.")
            if x_raw is None:
                raise ValueError(f"Promo Group '{group_name}' is missing required column 'X'.")

            # Enforce / allow Y depending on mechanism
            if mechanism in MECH_REQUIRES_Y:
                if y_raw is None:
                    raise ValueError(
                        f"Promo Group '{group_name}' uses mechanism '{mechanism}' but is missing required 'Y'."
                    )
            elif mechanism in MECH_OPTIONAL_Y:
                # Y can be missing; treat as None
                pass
            else:
                # Unknown mechanism: be strict so you don't create broken promos silently
                raise ValueError(
                    f"Promo Group '{group_name}' has unsupported/unknown mechanism '{mechanism}'. "
                    f"Add it to MECH_REQUIRES_Y or MECH_OPTIONAL_Y."
                )

            # Convert X/Y into the formats your existing logic expects
            x = "" if x_raw == "nan" else x_raw
            y = None if (y_raw is None or y_raw == "nan") else int(float(y_raw))

            # --- Create SF Promotion Group ---
            promo_group_dict = {
                "Campaign__c": campaign_id,
                "Name": group_name,
                "Start_Date__c": self.promo_start_date,
                "End_Date__c": self.promo_end_date,
                "Type__c": "Article",
                "Sub_Type__c": "Discounts",
                "Is_Promo_Box__c": True,
                "Hidden_on_Promo_Page__c": False,
            }

            promo_group_id = self.create_salesforce_object(
                salesforce_object=self.saleforce_connector.Promotion_Group__c,
                variable_data=promo_group_dict,
                depth=f"{campaign_name}-{group_name}",
            )

            # --- Create exactly 1 SF Promotion under that group ---
            promo_dict = {
                "Name": promo_name,
                "Promotion_Group__c": promo_group_id,
                "Data_Flow_Via__c": "PIM Cache",
                "Campaign_Based__c": self.campaign_based,
                "Promotion_Mechanism__c": mechanism,
                "Promotion_Mechanism_Variable_X__c": x,
                "Promotion_Mechanism_Variable_Y__c": y,
                "Hide_Strikethrough_Price__c": False,
                "Hidden_On_Promo_Page__c": False,
                "Maximum_Applications_Per_Order__c": 5,
            }

            print(f"Creating SF Promotion Group '{group_name}' and Promotion '{promo_name}' ({x} {mechanism} {y})")

            promotion_id = self.create_salesforce_object(
                salesforce_object=self.saleforce_connector.Promotion__c,
                variable_data=promo_dict,
                depth=f"{group_name}-{promo_name}",
            )

            # --- Attach all articles in this group to that one promotion ---
            for _, row in group_df.iterrows():
                sf_id = str(row["Id"])
                art_id = str(row["article_id"])

                if art_id not in su_dict:
                    raise KeyError(f"article_id {art_id} missing in selling_unit.sql mapping (su_dict)")

                ppi_dict = {
                    "Picnic_Article__c": sf_id,
                    "Promotion__c": promotion_id,
                    "Selling_Unit__c": su_dict[art_id],
                    "Hidden_On_Promo_Page__c": False,
                }

                # --- Optional fields from sheet ---
                # Purchase Discount (float)
                purchase_discount_raw = row.get("Purchase Discount", np.nan)
                if pd.notna(purchase_discount_raw) and str(purchase_discount_raw).strip() != "":
                    try:
                        ppi_dict["Purchase_Discount__c"] = float(purchase_discount_raw)
                    except ValueError as e:
                        raise ValueError(
                            f"Invalid 'Purchase Discount' value '{purchase_discount_raw}' for article_id {art_id} "
                            f"in promo group '{group_name}'. Expected a number."
                        ) from e

                # Purchase Discount Comment (string)
                purchase_discount_comment_raw = row.get("Purchase Discount Comment", np.nan)
                if pd.notna(purchase_discount_comment_raw) and str(purchase_discount_comment_raw).strip() != "":
                    ppi_dict["Purchase_Discount_Comment__c"] = str(purchase_discount_comment_raw).strip()

                self.create_salesforce_object(
                    salesforce_object=self.saleforce_connector.Picnic_Promotion_Item__c,
                    variable_data=ppi_dict,
                    depth=f"{group_name}-{art_id}",
                )


    def clear_sheet(self): 
        if "prod" not in self.environment.extra:
            print("[SKIPPED - NON PROD ENV] Would clear the sheet's content")
            return
        
        worksheet = self.worksheet
        
        num_rows = worksheet.row_count
        num_cols = worksheet.col_count

        # Build empty rows, except header row
        empty_rows = [["" for _ in range(num_cols)] for _ in range(num_rows - 1)]

        worksheet.update(
            f"A2:{letters[num_cols - 1]}{num_rows}",
            empty_rows
        )

        print("Cleared the sheet's content.")

    def upload_pipeline(self):

        promo_dataframe = self.read_data()

        sf_df = self.return_product_ids(promo_dataframe=promo_dataframe)

        merged_df = self.article_id_to_sf(promo_dataframe=promo_dataframe, sf_dataframe = sf_df)


        self.create_promotions(promo_dataframe=merged_df, promo_start_date=self.promo_start_date, promo_end_date=self.promo_end_date)

        self.clear_sheet()

    def resolve_campaign_id(self, campaign_name: str) -> str:
        # Normalize sheet input
        name = (campaign_name or "").strip()
        if not name:
            raise ValueError("Sheet column 'Campaign' is empty for at least one row.")

        # Read mapping from config
        campaign_id_mapping = self.config.get("campaign_ids", {})

        if not isinstance(campaign_id_mapping, dict) or not campaign_id_mapping:
            raise KeyError(
            "No campaign mapping found in config. Expected config key: campaign_ids"
            )

        if name not in campaign_id_mapping:
            known = ", ".join(sorted(campaign_id_mapping.keys()))
            raise KeyError(
                f"Campaign '{name}' not found in config mapping campaign_ids. "
                f"Known campaigns: {known}"
            )

        return campaign_id_mapping[name]



def main() -> None:
    # Check dates before running
    run = promo_to_salesforce(
        salesforce_connector=SF,
        sql_client=SQL_CLIENT,
        start_date="2026-01-04",
        end_date="2027-01-10",
    )
    run.upload_pipeline()


if __name__ == "__main__":
    main()
