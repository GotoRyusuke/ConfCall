# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from joblib import Parallel, delayed

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

class Raw2Panel:
    def __init__(self, 
                panel_df_path: str,
                lookup_df_path: str,
                moral_dict_path: str):
        
        panel_df = pd.read_csv(panel_df_path)
        panel_df["ceo_name"] = panel_df["ceo_name"].apply(lambda x: x.split(",")[0] if not pd.isna(x) else np.nan)
        panel_df["cfo_name"] = panel_df["cfo_name"].apply(lambda x: x.split(",")[0] if not pd.isna(x) else np.nan)

        panel_df["ceo_factset_person_id"] = panel_df["ceo_factset_person_id"].apply(
            lambda x: x.split(",")[0] if not pd.isna(x) else np.nan)
        panel_df["cfo_factset_person_id"] = panel_df["cfo_factset_person_id"].apply(
            lambda x: x.split(",")[0] if not pd.isna(x) else np.nan)

        self.panel_df = panel_df
        self.lookup_df = pd.read_csv(lookup_df_path)

        indicator_list = ['90_FocusPast', '91_FocusPresent', '92_FocusFuture', 'a_agency', 'a_communion']
        indicator_list1 = ['QA_' + i for i in indicator_list]
        indicator_list2 = ['exclude_QA_' + i for i in indicator_list]
        indicator_list3 = ['QA_male_' + i for i in indicator_list if "a_" in i]
        indicator_list4 = ['QA_female_' + i for i in indicator_list if "a_" in i]
        indicator_list5 = ['QA_both_' + i for i in indicator_list if "a_" in i]

        indicator_list = indicator_list + indicator_list1 + indicator_list2 + indicator_list3 + indicator_list4 + indicator_list5

        sub_dict_names, sub_dicts = load_moral_dict(moral_dict_path)
        sub_dict_names = list(sub_dict_names.values())

        full_wf_indicator_list = sub_dict_names
        exQA_wf_indicator_list = ['exclude_QA_' + sub_dict for sub_dict in sub_dict_names]
        QA_wf_indicator_list = ['QA_' + sub_dict for sub_dict in sub_dict_names]

        full_sf_indicator_list = [sub_dict + '_sentence_number' for sub_dict in sub_dict_names]
        exQA_sf_indicator_list = ['exclude_QA_' + sub_dict + '_sentence_number' for sub_dict in sub_dict_names]
        QA_sf_indicator_list = ['QA_' + sub_dict + '_sentence_number' for sub_dict in sub_dict_names]

        indicator_list = indicator_list + full_wf_indicator_list + exQA_wf_indicator_list + QA_wf_indicator_list + full_sf_indicator_list + exQA_sf_indicator_list + QA_sf_indicator_list
        
        self.ceo_list = sorted(set(panel_df["ceo_factset_person_id"].dropna()))
        self.cfo_list = sorted(set(panel_df["cfo_factset_person_id"].dropna()))

        self.ceo_indicator_list = [indicator + '_ceo' for indicator in indicator_list]
        self.cfo_indicator_list = [indicator + '_cfo' for indicator in indicator_list]
    
    def process_single_manager(self, p_id: str):
        if p_id in self.ceo_list:
            position = "ceo"
            talk_words_list = ["ceo_talk_words_num", "QA_ceo_talk_words_num", "exclude_QA_ceo_talk_words_num",
                            "QA_male_ceo_talk_words_num", "QA_female_ceo_talk_words_num", "QA_both_ceo_talk_words_num",
                            'total_sentence_number', 'RD_sentence_number']
            p_indicator_list = self.ceo_indicator_list
        else:
            position = "cfo"
            talk_words_list = ["cfo_talk_words_num", "QA_cfo_talk_words_num", "exclude_QA_cfo_talk_words_num",
                            "QA_male_cfo_talk_words_num", "QA_female_cfo_talk_words_num", "QA_both_cfo_talk_words_num",
                            'total_sentence_number', 'RD_sentence_number']
            p_indicator_list = self.cfo_indicator_list

        id_df = self.panel_df[self.panel_df[position + "_factset_person_id"] == p_id].reset_index(drop=True)
        
        manager_df = pd.DataFrame(columns = ["Name","Role", "person_factset_id",               # personal info
                                            'transcript_ID', 'conf_date', 'conf_date_quarter',
                                            'fiscal_date', 'conf_type', 'conf_type_detail',    # conf call info
                                            'Company',"company_factset_id", 'cusip',           # company info
                                            "talk_words_num", "QA_talk_words_num", "exclude_QA_talk_words_num",
                                            "QA_male_talk_words_num","QA_female_talk_words_num", "QA_both_talk_words_num", # talk words
                                            'sents_num', 'RD_sents_num'] + self.indicator_list)
        
        # personal info
        manager_df.loc[0, ["Role", "person_factset_id"]] = position, p_id
        manager_df.loc[0, 'Name'] = id_df.loc[0,[position + '_name']].values[0]
        
        # company info                                                              
        manager_df.loc[0,"company_factset_id"] = np.array(id_df.loc[0, 'factset_entity_id'])
        entity_id = id_df.loc[0, "factset_entity_id"].split(',')
        
        for sub_id in entity_id:
            manager_df.loc[0,['Company','cusip']] = ['', '']
            if sub_id in self.lookup_df['factset_entity_id'].values:
                manager_df.loc[0, ['Company','cusip']] = np.array(self.lookup_df.loc[
                    self.lookup_df['factset_entity_id'] == sub_id, ["proper_name", "cusip"]].iloc[0,:]) 
                break

        # replicate the personal, conf call, and company info to have the save num of rows as id_df
        manager_df_rep = pd.concat([manager_df]*len(id_df), ignore_index=True)
        conf_call_info = ['transcript_ID', 'conf_date', 'conf_date_quarter',
                            'fiscal_date', 'conf_type', 'conf_type_detail']
        
        for sub_idx in id_df.index:
            # conf call info
            manager_df_rep.loc[sub_idx, conf_call_info] = np.array(id_df.loc[sub_idx,conf_call_info])
            # talk num
            manager_df_rep.loc[
                sub_idx, ["talk_words_num", "QA_talk_words_num", "exclude_QA_talk_words_num",
                            "QA_male_talk_words_num","QA_female_talk_words_num", "QA_both_talk_words_num",
                            'sents_num', 'RD_sents_num']] = np.array(id_df.loc[sub_idx, talk_words_list])
            # indicators
            manager_df_rep.loc[sub_idx, self.indicator_list] = np.array(id_df.loc[sub_idx, p_indicator_list])
        
        return manager_df_rep

    def threading(self, num_job: int):
        total_list = self.ceo_list + self.cfo_list
        
        # calculate ids per job
        files_per_job = int(len(total_list) / num_job)
        cut_list = []
        
        for i in range(num_job):
            if i != num_job - 1:
                cut_list.append(total_list[i * files_per_job: (i + 1) * files_per_job])
            else:
                cut_list.append(total_list[i * files_per_job:])
        
        # define a function to process a list of trans, return a dataframe
        def run(id_list):
            temp_df = pd.DataFrame(columns = ["Name","Role", "person_factset_id",              # personal info
                                            'transcript_ID', 'conf_date', 'conf_date_quarter',
                                            'fiscal_date', 'conf_type', 'conf_type_detail',    # conf call info
                                            'Company',"company_factset_id", 'cusip', 'gvkey',  # company info
                                            "talk_words_num", "QA_talk_words_num", "exclude_QA_talk_words_num",
                                            "QA_male_talk_words_num","QA_female_talk_words_num", "QA_both_talk_words_num", # talk words
                                            'sents_num', 'RD_sents_num'] + self.indicator_list)
            for person_id in id_list:
                person_df = self.raw2panel(person_id)
                temp_df = pd.concat([temp_df, person_df], axis = 0, ignore_index = True)
            return temp_df
        
        # deploy the treading
        final_dfs = Parallel(n_jobs=num_job, verbose=1)(delayed(run)(sub_list) for sub_list in cut_list)
        
        final_df = pd.DataFrame()
        for df in final_dfs:
            final_df = pd.concat([final_df, df], axis = 0, ignore_index = True)

        return final_df

if __name__ == '__main__':
    panel_df_path = './ConfCall/task1/raw2panel/factset_conf_call_panel_data_v5.csv'
    lookup_df_path = './ConfCall/task1/raw2panel/lookup.csv'
    moral_dict_path = './ConfCall/task1/moral foundations dictionary.txt'
    store_path = './ConfCall/task1/raw2panel'

    obj = Raw2Panel(panel_df_path, lookup_df_path, moral_dict_path)
    output = obj.threading(16)
    output.to_csv(store_path + '/panel.csv', index = False)