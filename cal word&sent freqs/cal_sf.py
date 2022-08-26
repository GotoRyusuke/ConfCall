# -*- coding: utf-8 -*-
'''
Description
-----------
Use threading to calculate word freq.

CONTENT
-------
- <FUNC> file_list
- <CLASS> SentFreq

VERSION
-------
Last update: R4/8/26(Kin)

'''
import pandas as pd
import os
from moral_sent_classifier import SentenceMoralClassifier, cut_sentence
from joblib import Parallel, delayed

def file_list(path):
    f_list = os.listdir(path)
    if ".DS_Store" in f_list:
        f_list.remove(".DS_Store")
    return sorted(f_list)

class SentFreq:
    def __init__(self, 
                panel_data_path: str, 
                processed_all_year_path: str,
                moral_dict_path: str):
        
        self.panel_data_path = panel_data_path
        self.processed_all_year_path = processed_all_year_path
        
        self.panel_df = pd.read_csv(panel_data_path).set_index("transcript_ID", drop=False)
        self.trans_id_list = list(self.panel_df["transcript_ID"])
        
        # generate a year-file list
        self.full_path_list = []
        year_trans_list = [self.processed_all_year_path + '/' + year for year in file_list(self.processed_all_year_path) if 'processed' in year]
        for yl in year_trans_list:
            sub_path_list = [yl + '/' + trans_id for trans_id in file_list(yl)]                            
            self.full_path_list += sub_path_list
                
        # load the classifier
        self.sent_moral_classifier = SentenceMoralClassifier(dict_path = moral_dict_path)
    
    def sent_count_by_dict(self, talk_df):
        out_sent_count = dict([(key, 0) for key in self.sent_moral_classifier.word_dicts.keys()])
        out_num_sent = 0
        if len(talk_df) == 0: return out_sent_count, out_num_sent
        for talk_content in talk_df:
            if isinstance(talk_content, str):
                talk_sentences = cut_sentence(talk_content)
                for talk_sentence in talk_sentences:
                    out_num_sent += 1
                    temp_word_count = self.sent_moral_classifier.word_count_by_dict(talk_sentence)
                    for key, value in temp_word_count.items():
                        out_sent_count[key] += value
                        
        return out_sent_count, out_num_sent
        
    def count_single_transcript(self, trans_path: str):
        ceo_df = pd.read_csv(trans_path + "/ceo_talk.csv")
        cfo_df = pd.read_csv(trans_path + "/cfo_talk.csv")
        others_df = pd.read_csv(trans_path + "/others_talk.csv")
        
        ## load the contents of talk
        # ceo
        ceo_talk_all = ceo_df['talk_content'].dropna()
        ceo_talk_exQA = ceo_df["talk_content"][pd.isna(ceo_df["question"])].dropna()
        ceo_talk_QA = ceo_df["talk_content"][~pd.isna(ceo_df["question"])].dropna()
        
        # cfo
        cfo_talk_all = cfo_df['talk_content'].dropna()
        cfo_talk_exQA = cfo_df["talk_content"][pd.isna(cfo_df["question"])].dropna()
        cfo_talk_QA = cfo_df["talk_content"][~pd.isna(cfo_df["question"])].dropna()
        
        # others
        others_talk_all = others_df['talk_content'].dropna()
        others_talk_exQA = others_df["talk_content"][pd.isna(others_df["question"])].dropna()
        others_talk_QA = others_df["talk_content"][~pd.isna(others_df["question"])].dropna()
        
        # all
        all_talk_exQA = pd.concat([ceo_talk_exQA, cfo_talk_exQA, others_talk_exQA], axis = 0)
        all_talk_QA = pd.concat([ceo_talk_QA, cfo_talk_QA, others_talk_QA], axis = 0)
        all_talk_all = pd.concat([ceo_talk_all, cfo_talk_all, others_talk_all], axis = 0)
        
        ## sentence count
        # ceo
        ceo_sent_count_all,ceo_num_sent_all = self.sent_count_by_dict(ceo_talk_all)
        ceo_sent_count_exQA,ceo_num_sent_exQA = self.sent_count_by_dict(ceo_talk_exQA)
        ceo_sent_count_QA,ceo_num_sent_QA = self.sent_count_by_dict(ceo_talk_QA)
        
        # cfo
        cfo_sent_count_all,cfo_num_sent_all = self.sent_count_by_dict(cfo_talk_all)
        cfo_sent_count_exQA,cfo_num_sent_exQA = self.sent_count_by_dict(cfo_talk_exQA)
        cfo_sent_count_QA,cfo_num_sent_QA = self.sent_count_by_dict(cfo_talk_QA)
        
        # all
        all_sent_count_all,all_num_sent_all = self.sent_count_by_dict(all_talk_all)
        all_sent_count_exQA,all_num_sent_exQA = self.sent_count_by_dict(all_talk_exQA)
        all_sent_count_QA,all_num_sent_QA = self.sent_count_by_dict(all_talk_QA)          
        
        ## combine the result as a dataframe
        transcript_id = trans_path.split('-')[-2]
        single_trans = pd.DataFrame()
        
        # sentence number 
        single_trans.loc[transcript_id, "all_sentence_number"] = all_num_sent_all
        single_trans.loc[transcript_id, "ceo_sentence_number"] = ceo_num_sent_all
        single_trans.loc[transcript_id, "cfo_sentence_number"] = cfo_num_sent_all

        # sent count
        for key, value in all_sent_count_all.items():                    
            single_trans.loc[transcript_id, self.sent_moral_classifier.sub_dict_name[key] + "_sentence_number" + '_all'] = value
        # ceo-all
        for key, value in ceo_sent_count_all.items():                    
            single_trans.loc[transcript_id, self.sent_moral_classifier.sub_dict_name[key] + "_sentence_number" + '_ceo'] = value
        # cfo-all
        for key, value in cfo_sent_count_all.items():                    
            single_trans.loc[transcript_id, self.sent_moral_classifier.sub_dict_name[key] + "_sentence_number" + '_cfo'] = value
        
        # all-excludeQA
        for key, value in all_sent_count_exQA.items():
            single_trans.loc[transcript_id, 'exclude_QA_' + self.sent_moral_classifier.sub_dict_name[key] + '_sentence_number_all'] = value
        # ceo-excludeQA
        for key, value in ceo_sent_count_exQA.items():
            single_trans.loc[transcript_id, 'exclude_QA_' + self.sent_moral_classifier.sub_dict_name[key] + '_sentence_number_ceo'] = value
        # cfo-excludeQA
        for key, value in cfo_sent_count_exQA.items():
            single_trans.loc[transcript_id, 'exclude_QA_' + self.sent_moral_classifier.sub_dict_name[key] + '_sentence_number_cfo'] = value

        # all-QA
        for key, value in all_sent_count_QA.items():
            single_trans.loc[transcript_id, 'QA_' + self.sent_moral_classifier.sub_dict_name[key] + '_sentence_number_all'] = value
        # ceo-QA
        for key, value in ceo_sent_count_QA.items():
            single_trans.loc[transcript_id, 'QA_' + self.sent_moral_classifier.sub_dict_name[key] + '_sentence_number_ceo'] = value
        # cfo-QA
        for key, value in cfo_sent_count_QA.items():
            single_trans.loc[transcript_id, 'QA_' + self.sent_moral_classifier.sub_dict_name[key] + '_sentence_number_cfo'] = value
        
        return single_trans

    def threading(self, num_job: int):
        path_list = self.full_path_list
        files_per_job = int(len(path_list) / num_job)
        cut_list = []
        for i in range(num_job):
            if i != num_job - 1:
                cut_list.append(path_list[i * files_per_job: (i + 1) * files_per_job])
            else:
                cut_list.append(path_list[i * files_per_job:])

        def run(trans_list):
            out_df = pd.DataFrame()
            for path in trans_list:
                temp_id = int(path.split('-')[-2])
                temp_df = pd.DataFrame()
                if temp_id not in self.trans_id_list: continue
            
                temp_df = self.count_single_transcript(path)
                out_df = pd.concat([out_df, temp_df], axis = 0)
            
            out_df.reset_index(drop = True, replace = True)
            return out_df
        
        # deploy the treading
        final_dfs = Parallel(n_jobs=num_job, verbose=1)(delayed(run)(sub_list) for sub_list in cut_list)

        # combine all count results in  the treading processing return value together as a dataframe
        final_df = pd.DataFrame()
        for df in final_dfs:
            final_df = pd.concat([final_df, df], axis = 0)
        
        final_df.reset_index(drop = True, inplace = True)
        return final_df

if __name__ == '__main__':
    moral_dict_path = "./ConfCall/moral foundations dictionary.txt"        
    panel_data_path = "./ConfCall/task1threading/factset_conf_call_panel_data_v4.csv"
    processed_all_year_path = "./ConfCall/all_year_processed_data"
    store_path = "./ConfCall/task1threading"

    sf = SentFreq(panel_data_path,
                processed_all_year_path,
                moral_dict_path)

    sf_results = sf.threading(16)


