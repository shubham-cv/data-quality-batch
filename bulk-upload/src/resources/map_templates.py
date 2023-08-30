import pandas as pd
import random
import string
from util import *
from constants import *

class MapTemplates():
    def __init__(self):
        pass

    def map_entity_template(self, entity_template_df, entity_df):
        entity_df = entity_df.merge(entity_template_df,
                                    how= 'left',
                                    left_on= ['entity_type', 'entity_subtype'],
                                    right_on= ['entity_type', 'entity_subtype'])
        return entity_df

    def map_rule_template(self, rule_template_df, rule_df):
        # print(rule_template_df, rule_df)
        rule_df = rule_df.merge(rule_template_df,
                                    how= 'left',
                                    left_on= ['rule_template_name', 'dq_metric'],
                                    right_on= ['rule_template_name', 'dq_metric'])
        return rule_df
