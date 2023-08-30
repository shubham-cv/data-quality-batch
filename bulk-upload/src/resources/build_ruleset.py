import pandas as pd
import random
import string
from util import *
from constants import *


class BuildRuleset:
    def __init__(self, df):
        self.df = df
        
    def build(self):
        new_ids = get_new_ids(self.df)
        self.df['ruleset_id'] = new_ids
        ruleset_df = add_who_columns(self.df)
        ruleset_mapper = id_mapper(ruleset_df, 'ruleset_name', 'ruleset_id')
        insert_into_db('ruleset', ruleset_db_columns, ruleset_df, ruleset_df_columns)
        ruleset_df.to_csv('output/ruleset.csv', index= False)
        return ruleset_mapper