import pandas as pd
import numpy as np
from tqdm import tqdm

def load_moral_dict(dict_path):
    with open(dict_path, 'r') as file:
        moral_dict_ct = file.read()
    temp_dict = moral_dict_ct.splitlines()
    
    for line_i in range(len(temp_dict)):
        temp_dict[line_i] = temp_dict[line_i].replace('\t', ' ')

    sub_dict_keys = temp_dict[1:12]
    for line_i in range(len(sub_dict_keys)):
        sub_dict_keys[line_i] = sub_dict_keys[line_i].split()
    
    temp_sub_dicts = temp_dict[14:]
    sub_dicts = {}
    sub_dict_name = {}
    for item in sub_dict_keys:
        sub_dicts[item[0]] = []
        sub_dict_name[item[0]] = item[1]
        
    for line in temp_sub_dicts:
        if len(line.replace(' ', '')) == 0:
            continue
        temp_item = line.split()
        if len(temp_item) > 2:
            for key in temp_item[1:]:
                sub_dicts[key].append(temp_item[0])
        else:
            sub_dicts[temp_item[1]].append(temp_item[0])
    
    return sub_dict_name, sub_dicts

class panel2cross:
    def __init__(self,
                panel_df_path: str,
                lookup_df_path: str):
        
        panel_df = pd.read_csv(panel_df_path)
        panel_df["ceo_name"] = panel_df["ceo_name"].apply(lambda x: x.split(",")[0] if not pd.isna(x) else np.nan)
        panel_df["cfo_name"] = panel_df["cfo_name"].apply(lambda x: x.split(",")[0] if not pd.isna(x) else np.nan)

        panel_df["ceo_factset_person_id"] = panel_df["ceo_factset_person_id"].apply(
            lambda x: x.split(",")[0] if not pd.isna(x) else np.nan)
        panel_df["cfo_factset_person_id"] = panel_df["cfo_factset_person_id"].apply(
            lambda x: x.split(",")[0] if not pd.isna(x) else np.nan)
        
        self.panel_df = panel_df
        self.lookup_df = pd.read_csv(lookup_df_path)

        moral_dict_path = "C:/Users/niccolo/Desktop/QLFtask/moral word freq project/dictionary/moral foundations dictionary.txt"
        sub_dict_names, sub_dicts = load_moral_dict(moral_dict_path)

        sub_dict_names = list(sub_dict_names.values())
        indicator_list = ['90_FocusPast', '91_FocusPresent', '92_FocusFuture', 'a_agency', 'a_communion']
        indicator_list1 = ['QA_' + i for i in indicator_list]
        indicator_list2 = ['exclude_QA_' + i for i in indicator_list]
        indicator_list3 = ['QA_male_' + i for i in indicator_list if "a_" in i]
        indicator_list4 = ['QA_female_' + i for i in indicator_list if "a_" in i]
        indicator_list5 = ['QA_both_' + i for i in indicator_list if "a_" in i]

        indicator_list = indicator_list + indicator_list1 + indicator_list2 + indicator_list3 + indicator_list4 + indicator_list5

        full_wf_indicator_list = sub_dict_names
        exQA_wf_indicator_list = ['exclude_QA_' + sub_dict for sub_dict in sub_dict_names]
        QA_wf_indicator_list = ['QA_' + sub_dict for sub_dict in sub_dict_names]

        full_sf_indicator_list = [sub_dict + '_sentence_number' for sub_dict in sub_dict_names]
        exQA_sf_indicator_list = ['exclude_QA_' + sub_dict + '_sentence_number' for sub_dict in sub_dict_names]
        QA_sf_indicator_list = ['QA_' + sub_dict + '_sentence_number' for sub_dict in sub_dict_names]

        self.indicator_list = indicator_list + full_wf_indicator_list + exQA_wf_indicator_list + QA_wf_indicator_list + full_sf_indicator_list + exQA_sf_indicator_list + QA_sf_indicator_list

        self.ceo_indicator_list = [indicator + '_ceo' for indicator in indicator_list]
        self.cfo_indicator_list = [indicator + '_cfo' for indicator in indicator_list]


    def exe(self):
        ceo_list = sorted(set(self.panel_df["ceo_factset_person_id"].dropna()))
        cfo_list = sorted(set(self.panel_df["cfo_factset_person_id"].dropna()))

        manager_df = pd.DataFrame(columns = ["Name", "Role", "person_factset_id", "company_factset_id",
                                            'Company', 'cusip',
                                            "talk_words_num", "QA_talk_words_num", "exclude_QA_talk_words_num",
                                            "QA_male_talk_words_num","QA_female_talk_words_num", "QA_both_talk_words_num",
                                            'sentence_num'] + self.indicator_list)

        for p_id in tqdm(cfo_list + ceo_list):
            manager_idx = len(manager_df)
            if p_id in self.ceo_list:
                position = "ceo"
                talk_words_list = ["ceo_talk_words_num", "QA_ceo_talk_words_num", "exclude_QA_ceo_talk_words_num",
                                "QA_male_ceo_talk_words_num", "QA_female_ceo_talk_words_num", "QA_both_ceo_talk_words_num"]
                p_indicator_list = self.ceo_indicator_list
            else:
                position = "cfo"
                talk_words_list = ["cfo_talk_words_num", "QA_cfo_talk_words_num", "exclude_QA_cfo_talk_words_num",
                                "QA_male_cfo_talk_words_num", "QA_female_cfo_talk_words_num", "QA_both_cfo_talk_words_num"]
                p_indicator_list = self.cfo_indicator_list
            # id_df
            id_df = self.panel_df[self.panel_df[position + "_factset_person_id"] == p_id].reset_index(drop=True)
            # basic info
            manager_df.loc[manager_idx, ["Role", "person_factset_id"]] = position, p_id
            manager_df.loc[manager_idx, ["Name", "company_factset_id"]] = np.array(
                id_df.loc[0, [position + "_name","factset_entity_id"]])
            
            entity_id = id_df.loc[0, "factset_entity_id"].split(',')[0]
            
            if entity_id not in self.lookup_df['factset_entity_id'].values:
                manager_df.loc[manager_idx, ['Company','cusip']] = ['', '']
            else:
                manager_df.loc[manager_idx, ['Company','cusip']] = np.array(self.lookup_df.loc[
                    self.lookup_df['factset_entity_id'] == entity_id, ["proper_name", "cusip"]].iloc[0,:]) 

            # talk num
            manager_df.loc[
                manager_idx, ["talk_words_num", "QA_talk_words_num", "exclude_QA_talk_words_num",
                            "QA_male_talk_words_num","QA_female_talk_words_num", "QA_both_talk_words_num"]] = np.array(id_df[talk_words_list].sum())
            manager_df.loc[
                manager_idx, ['sentence_num']] = np.array(id_df[position + '_sentence_number']).sum()
            
            # indicator
            manager_df.loc[manager_idx, self.indicator_list] = np.array(id_df[p_indicator_list].sum())


    
