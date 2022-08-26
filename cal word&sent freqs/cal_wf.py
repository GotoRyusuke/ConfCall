# -*- coding: utf-8 -*-
'''
DESCRIPTION
-----------
Use threading to calculate word freq.

CONTENT
-------
- <FUNC> file_list
- <FUNC> load_moral_dict
- <CLASS> WordFreq

VERSION
-------
Last update: R4/8/26(Kin)

'''
import pandas as pd
import os
from collections import Counter
from joblib import Parallel, delayed

def file_list(path):
    f_list = os.listdir(path)
    if ".DS_Store" in f_list:
        f_list.remove(".DS_Store")
    return sorted(f_list)

# load the dictionary, return a dict of sub dictionaries
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
            
    # sub_dicts = pd.DataFrame(sub_dicts.values()).transpose()
    # sub_dicts.to_csv('Sub-dictionary.csv',encoding='utf-8')
    
    return sub_dict_name, sub_dicts

class WordFreq:
    def __init__(self, 
                panel_data_path:str, 
                processed_all_year_path:str, 
                moral_dict_path:str, 
                store_path: str):

        self.panel_data_path = panel_data_path
        self.processed_all_year_path = processed_all_year_path
        self.output_store_path = store_path
        self.panel_df = pd.read_csv(panel_data_path).set_index("transcript_ID", drop=False)
        self.trans_id_list = list(self.panel_df["transcript_ID"])
        
        # (1) generate a year-file list
        self.full_path_list = []
        year_trans_list = [self.processed_all_year_path + '/' + year for year in file_list(self.processed_all_year_path) if 'processed' in year]
        for yl in year_trans_list:
            sub_path_list = [yl + '/' + trans_id for trans_id in file_list(yl)]                            
            self.full_path_list += sub_path_list
            
        # load the moral foudnations dictionary
        self.word_dicts = {}
        self.sub_dict_name, sub_dicts = load_moral_dict(moral_dict_path)
        for key,value in sub_dicts.items():
            # every item in the new dict is a sub-dict from moral foundations dict
            self.word_dicts[key] = value
    
    def word_count_by_dict(self, talk_words: list):
        talk_count_result = {}
        talk_words_counter = dict(Counter(talk_words))
        counter_keys = list(talk_words_counter.keys())
        for word_dict_name, keyword_list in self.word_dicts.items():
            dict_count = 0
            for kw in keyword_list:
                if "*" not in kw:
                    if kw in counter_keys:
                        dict_count += talk_words_counter[kw]
                else:
                    kw = kw.split("*")[0]
                    for key in counter_keys:
                        if key[:len(kw)] == kw:
                            dict_count += talk_words_counter[key]
            talk_count_result[word_dict_name] = dict_count
        return talk_count_result

    def count_single_transcript(self, trans_path:str):
        # read the csv file
        ceo_df = pd.read_csv(trans_path + '/' + 'ceo_talk.csv')
        ceo_talk_all = " ".join([str(content) for content in list(ceo_df["talk_content"].dropna())])
        ceo_talk_exQA = " ".join([str(content) for content in list(ceo_df["talk_content"][pd.isna(ceo_df["question"])].dropna())])
        ceo_talk_QA = " ".join([str(content) for content in list(ceo_df["talk_content"][~pd.isna(ceo_df["question"])].dropna())])
         
        cfo_df = pd.read_csv(trans_path + '/' + 'cfo_talk.csv')
        cfo_talk_all = " ".join([str(content) for content in list(cfo_df["talk_content"].dropna())])
        cfo_talk_exQA = " ".join([str(content) for content in list(cfo_df["talk_content"][pd.isna(cfo_df["question"])].dropna())])
        cfo_talk_QA = " ".join([str(content) for content in list(cfo_df["talk_content"][~pd.isna(cfo_df["question"])].dropna())])
         
        others_df = pd.read_csv(trans_path + '/' + 'others_talk.csv')
        others_talk_all = " ".join([str(content) for content in list(others_df["talk_content"].dropna())])
        others_talk_exQA = " ".join([str(content) for content in list(others_df["talk_content"][pd.isna(others_df["question"])].dropna())])
        others_talk_QA = " ".join([str(content) for content in list(others_df["talk_content"][~pd.isna(others_df["question"])].dropna())])
         
        all_talk_all = ceo_talk_all + cfo_talk_all + others_talk_all
        all_talk_exQA = ceo_talk_exQA + cfo_talk_exQA + others_talk_exQA
        all_talk_QA = ceo_talk_QA + cfo_talk_QA + others_talk_QA
        
        # eliminate all punctuations
        symbols = [",", ".", "!", "?"]
        for symbol in symbols:
            ceo_talk_all = ceo_talk_all.replace(symbol, "").lower()
            cfo_talk_all = cfo_talk_all.replace(symbol, "").lower()
            all_talk_all = all_talk_all.replace(symbol, "").lower()
            
            ceo_talk_exQA = ceo_talk_exQA.replace(symbol, "").lower()
            cfo_talk_exQA = cfo_talk_exQA.replace(symbol, "").lower()
            all_talk_exQA = all_talk_exQA.replace(symbol, "").lower()
            
            ceo_talk_QA = ceo_talk_QA.replace(symbol, "").lower()
            cfo_talk_QA = cfo_talk_QA.replace(symbol, "").lower()
            all_talk_QA = all_talk_QA.replace(symbol, "").lower()
        
        # crush a single text into words
        ceo_talk_exQA_words = ceo_talk_exQA.split()
        cfo_talk_exQA_words = cfo_talk_exQA.split()
        all_talk_exQA_words = all_talk_exQA.split()
        
        ceo_talk_QA_words = ceo_talk_QA.split()
        cfo_talk_QA_words = cfo_talk_QA.split()
        all_talk_QA_words = all_talk_QA.split()
        
        ceo_talk_all_words = ceo_talk_all.split()
        cfo_talk_all_words = cfo_talk_all.split()
        all_talk_all_words = all_talk_all.split()
        
        # get the word count
        ceo_talk_exQA_result = self.word_count_by_dict(ceo_talk_exQA_words)
        cfo_talk_exQA_result = self.word_count_by_dict(cfo_talk_exQA_words)
        all_talk_exQA_result = self.word_count_by_dict(all_talk_exQA_words)
        
        ceo_talk_QA_result = self.word_count_by_dict(ceo_talk_QA_words)
        cfo_talk_QA_result = self.word_count_by_dict(cfo_talk_QA_words)
        all_talk_QA_result = self.word_count_by_dict(all_talk_QA_words)
        
        ceo_talk_all_result = self.word_count_by_dict(ceo_talk_all_words)
        cfo_talk_all_result = self.word_count_by_dict(cfo_talk_all_words)
        all_talk_all_result = self.word_count_by_dict(all_talk_all_words)
        
        ## combine the result as a dataframe
        transcript_id = trans_path.split('-')[-2]
        single_trans = pd.DataFrame()
        
        # all
        for word_dict_name, word_count in ceo_talk_all_result.items():
            single_trans.loc[transcript_id, self.sub_dict_name[word_dict_name] + "_ceo"] = word_count
        for word_dict_name, word_count in cfo_talk_all_result.items():
            single_trans.loc[transcript_id, self.sub_dict_name[word_dict_name] + "_cfo"] = word_count
        for word_dict_name, word_count in all_talk_all_result.items():
            single_trans.loc[transcript_id, self.sub_dict_name[word_dict_name] + "_all"] = word_count
        
        # exclude QA
        for word_dict_name, word_count in ceo_talk_exQA_result.items():
            single_trans.loc[transcript_id, 'exclude_QA_' + self.sub_dict_name[word_dict_name] + "_ceo"] = word_count
        for word_dict_name, word_count in cfo_talk_exQA_result.items():
            single_trans.loc[transcript_id, 'exclude_QA_' + self.sub_dict_name[word_dict_name] + "_cfo"] = word_count
        for word_dict_name, word_count in all_talk_exQA_result.items():
            single_trans.loc[transcript_id, 'exclude_QA_' + self.sub_dict_name[word_dict_name] + "_all"] = word_count
        
        # QA
        for word_dict_name, word_count in ceo_talk_QA_result.items():
            single_trans.loc[transcript_id, 'QA_' + self.sub_dict_name[word_dict_name] + "_ceo"] = word_count
        for word_dict_name, word_count in cfo_talk_QA_result.items():
            single_trans.loc[transcript_id, 'QA_' + self.sub_dict_name[word_dict_name] + "_cfo"] = word_count
        for word_dict_name, word_count in all_talk_QA_result.items():
            single_trans.loc[transcript_id, 'QA_' + self.sub_dict_name[word_dict_name] + "_all"] = word_count
        
        return single_trans
    
    def threading(self, num_job: int):
        path_list = self.full_path_list
        
        # calculate files per job
        files_per_job = int(len(path_list) / num_job)
        cut_list = []
        for i in range(num_job):
            if i != num_job - 1:
                cut_list.append(path_list[i * files_per_job: (i + 1) * files_per_job])
            else:
                cut_list.append(path_list[i * files_per_job:])

        # define a function to process a list of trans, return a dataframe
        def run(trans_list):
            out_df = pd.DataFrame()
            for path in trans_list:
                temp_id = int(path.split('-')[-2])
                temp_df = pd.DataFrame()
                if temp_id not in self.trans_id_list: continue
                
                temp_df = self.count_single_transcript(path)
                out_df = pd.concat([out_df, temp_df], axis = 0)
            
            out_df.reset_index(drop = True, inplace = True)
            return out_df
    
        # deploy the treading
        sub_dfs = Parallel(n_jobs = num_job, verbose=1)(delayed(run)(sub_list) for sub_list in cut_list)

        # combine all count results in  the treading processing return value together as a dataframe
        final_df = pd.DataFrame()
        for df in sub_dfs:
            final_df = pd.concat([final_df, df], axis = 0)

        final_df.reset_index(drop = True, inplace = True)
        return final_df

if __name__ == '__main__':
    panel_data_path = "./ConfCall/task1threading/factset_conf_call_panel_data_v4.csv"
    moral_dict_path = "./ConfCall/task1threading/moral foundations dictionary.txt" 
    processed_all_year_path = "./ConfCall/all_year_processed_data" 
    store_path = "./ConfCall/task1threading"     

    wf = WordFreq(panel_data_path,
                      processed_all_year_path,
                      moral_dict_path)

    wf_results = wf.threading(16)



