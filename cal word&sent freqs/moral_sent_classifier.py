# -*- coding: utf-8 -*-
'''
DESCRIPTION
-----------

A module to classify a sent into a certain polarity, based on the words appear in it

CONTENTS
--------
- <FUNC> cut_sentence
- <FUNC> load_moral_dict
- <FUNC> word_in_sentence
- <CLASS> SentenceMoralClassifier

VERSION
-------
Last update: R4/8/26(Kin)

'''
def cut_sentence(talk_content: str):
    '''
    A func to cut a string of text into a list of sentences

    Parameters
    ----------
    talk_content: str
        A string
    
    Returns
    -------
    talk_sentences: list
        A list of sentences in the original content

    '''
    talk_sentences = []
    talk_words = talk_content.split()
    last_sentence_idx = 0
    exception_rule = ["Mr", "Mrs", "Miss", "Ms", "Sir", "Madam", "Dr", "Cllr", "Lady", "Lord", "Professor", "Prof",
                      "Chancellor", "Principal", "President", "Master", "Governer", "Gov", "Attorney", "Atty"]
    for w_i in range(len(talk_words)):
        talk_word = talk_words[w_i]
        if w_i == len(talk_words) - 1:
            talk_sentences.append(" ".join(talk_words[last_sentence_idx: w_i + 1]))
        else:
            if talk_word[-1] in [".", "?", "!"]:
                if talk_word[:-1] not in exception_rule:
                    if talk_words[w_i + 1][0].isupper():

                        talk_sentences.append(" ".join(talk_words[last_sentence_idx: w_i + 1]))
                        last_sentence_idx = w_i + 1
    return talk_sentences

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
  
def word_in_sentence(dict_word: str, sentence: list):
    '''
    A func to check whether a word(or lemma of word) is in the given sent

    Parameters
    ----------
    dict_word: str
        A word or lemma
    sentence: list
        A list of words in a given sentence, each element of the list a word
    
    Returns
    -------
    flag: bool
        = True if the word/lemma is in the sentence

    '''
    flag = False
    if '*' in dict_word:
        dict_word = dict_word.split("*")[0]
        for sent_word in sentence:
            if len(dict_word) > len(sent_word):
                continue
            if sent_word[:len(dict_word)] == dict_word:
                flag = True
    else:
        for sent_word in sentence:
            if len(dict_word) > len(sent_word):
                continue
            if sent_word == dict_word:
                flag = True
    return flag

class SentenceMoralClassifier:
    def __init__(self, dict_path):
        self.word_dicts = {}
        self.sub_dict_name, sub_dicts = load_moral_dict(dict_path = dict_path)
        for key,value in sub_dicts.items():
            self.word_dicts[key] = value

    def word_count_by_dict(self, sentence: str):
        '''
        A method to calculate the polarities of a sent

        Parameters
        ----------
        sentence: str
            A string of sent
        
        Returns
        -------
        moral_word_count: dict
            A dict that saves the polarity of the sent
            
        '''
        moral_word_count = {}
        sentence = [w.lower() if not w.isupper() else w for w in sentence.split()]

        replace_list = ["!", "?", ".", ",", ";"]
        for replace_w in replace_list:
            for word_i in range(len(sentence)):
                sentence[word_i] = sentence[word_i].replace(replace_w, "")
        
        for key, words in self.word_dicts.items():
            moral_word_count[key] = 0
            for word in words:
                if word_in_sentence(word, sentence):
                    moral_word_count[key] += 1
        return moral_word_count
    
