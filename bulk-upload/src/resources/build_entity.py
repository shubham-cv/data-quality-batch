import pandas as pd
import random
import string
from util import *
from resources.map_templates import MapTemplates
from constants import *


class BuildEntity:
    def __init__(self, df):
        self.df = df
        
    def build(self):
        new_ids = get_new_ids(self.df)
        self.df['entity_id'] = new_ids

        entity_df = self.df[[x for x in self.df.columns if not(x.startswith('property') or x.startswith('value'))]]
        
        entity_template_df = read_file('data_files/Bulk_upload_template_HSBC.xlsx', 'entity_template')
        map_templates = MapTemplates()
        entity_df = map_templates.map_entity_template(entity_template_df, entity_df)

        entity_props_dict = {}

        entity_props_df = self.df[[x for x in self.df.columns if x.startswith('property') or x.startswith('value') or x == 'entity_id']]
        entity_props_df1 = pd.DataFrame(columns=entity_props_columns)

        for index, row in entity_props_df.iterrows():
                for i in range(1,11):
                    try:
                        if isNaN(row['property_'+str(i)]):
                            entity_props_df1.loc[len(entity_props_df1)] = [get_unique_id(), row['entity_id'],
                                                                        row['property_'+str(i)], row['value_'+str(i)]
                                                                        ]
                    except KeyError:
                        break

        
        entity_mapper = id_mapper(entity_df, 'entity_physical_name', 'entity_id')
        entity_df = entity_df[entity_columns]
        
        entity_df = add_who_columns(entity_df)
        entity_props_df1 = add_who_columns(entity_props_df1)
        
        insert_into_db('entity', entity_db_columns, entity_df, entity_df_columns)
        insert_into_db('entity_properties', entity_props_db_columns, entity_props_df1, entity_props_df_columns)

        entity_df.to_csv('output/entity.csv', index= False)
        entity_props_df1.to_csv('output/entity_properties.csv', index= False)
        return entity_mapper, entity_df