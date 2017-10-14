import pickle
import re
import string
from slots.app import RuleBaseSlot
from timeit import default_timer as timer

from nltk import ngrams
from nltk import word_tokenize
from pycorenlp import StanfordCoreNLP

from whatsapp_record import db_manager

nlp = StanfordCoreNLP('http://192.168.0.100:9000')

# REG_DATETIME = r"((?:0?[1-9]|1[0-2])\/(?:0?[1-9]|[1-2][0-9]|3[0-1])\/(?:[0-9]\d{1}), (?:[1-9]|1[0-2]):(?:0[1-9]|[0-5][0-9]) (?:PM|AM))"
# YYYY added
REG_DATETIME = r"((?:0?[1-9]|1[0-2])\/(?:0?[1-9]|[1-2][0-9]|3[0-1])\/((?:[0-9]\d{1})|(?:[0-9]\d{3})), (?:[1-9]|1[0-2]):(?:0[1-9]|[0-5][0-9]) (?:PM|AM))"
REG_NON_ENGLISH = r"[\u2E80-\u9FFF]+"
REG_MULTIMEDIA = r"IMG-[0-9]+|<Media omitted>"
EMOJI_PATTERN = re.compile(
    u"(\ud83d[\ude00-\ude4f])|"  # emoticons
    u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
    u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
    u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
    u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
    "+", flags=re.UNICODE)

# URL_PATTERN = re.compile(
#         r'(?:http|ftp)s?://' # http:// or https://
#         r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
#         r'localhost|' #localhost...
#         r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
#         r'(?::\d+)?' # optional port
#         r'(?:/?|[/?]\S+)', re.IGNORECASE)

# URL_PATTERN = r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b'

URL_PATTERN = r'(((https|http):\/\/)|(www\.))(\w|\.|\/|\?|=|&|\^|%|#|\$|\!|-|_|~|\+|\:|\,|\(|\)|\{|\})*|www\.\b'

table = {ord(f): ord(t) for f, t in zip(
    u'“”～‘’，。！？【】（）％＃＠＆１２３４５６７８９０',
    u'""~\'\',.!?[]()%#@&1234567890')}
exclude = set(string.punctuation)

db_conn = db_manager.DBConn()
file_meta_data = open('whatsapp_record/output/meta_data.txt', 'w')
start = timer()


rulebase = RuleBaseSlot('hku')


class RecordExtraction(object):

    def __init(self):
        self.URL_PATTERN = r'(((https|http):\/\/)|(www\.))(\w|\.|\/|\?|=|&|\^|%|#|\$|\!|-|_|~|\+|\:|\,|\(|\)|\{|\})*|www\.\b'

        self.table = {ord(f): ord(t) for f, t in zip(
            u'“”～‘’，。！？【】（）％＃＠＆１２３４５６７８９０',
            u'""~\'\',.!?[]()%#@&1234567890')}
        self.exclude = set(string.punctuation)

        self.db_conn = db_manager.DBConn()
        self.file_meta_data = open('whatsapp_record/output/meta_data.txt', 'w')
        self.start = timer()


def generate_dic():
    cnt_excep = 0
    sql = "SELECT id, user_input, new_conver, continuous FROM `whatsapp_record` WHERE chinese = 0 AND lib_response = ' ' AND user_input != ' ' AND multimedia = 0 ORDER BY `id` ASC"
    ls_result = db_conn.fetch_data(sql)
    # sql = "SELECT id, user_input, new_conver, continuous FROM `whatsapp_record` WHERE chinese = 0 AND user_input LIKE %s ORDER BY `id` ASC"
    # ls_result = db_conn.fetch_data(sql, ['%http%'])
    # print len(ls_result)
    # print ls_result
    print("total messages:", len(ls_result))
    ls_dic = []
    excep_file = open('whatsapp_record/output/excep.txt', 'w')
    i = 0
    seg = [50, 100, 200, 500, 700, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000]
    ls_msg_word_corpus = []
    for record in ls_result:
        id = record['id']
        message_origin = record['user_input']
        message = remove_punc(remove_non_ascii(rulebase.process(message_origin)))

        try:
            n_gram_result = gen_n_gram(message)
            # print(n_gram_result)
            # quit()
            n_gram = n_gram_result[0]
            if n_gram_result[1]:
                for word in n_gram_result[1]:
                    if word is not None:
                        ls_msg_word_corpus.append(word)
        except Exception as data:
            cnt_excep += 1
            str_excep = "id: " + str(id) + ' msg: ' + message
            print(str_excep)
            excep_file.write(str_excep)
            continue
        initial = record['new_conver']
        continuous = record['continuous']
        dic = {'id': id, 'message': message, 'msg_initial': initial, 'continuous': continuous, 'ngram': n_gram}
        ls_dic.append(dic)
        end = timer()
        if i in seg:
            print(str(i), " handled. ", str(len(ls_result)-i), " remaining.")
            print(str(round(end - start, 2)) + " seconds used")
        i += 1
    # print ls_dic
    with open('whatsapp_record/output/message_object.txt', 'wb') as result_file:
        pickle.dump(ls_dic, result_file)

        # for item in ls_dic:
        # print>> result_file, item
    # gen_matrix(ls_dic, ls_msg_word_corpus)
    print(cnt_excep, " unhandled sentences.")


def remove_non_ascii(text):
    return ''.join(ch for ch in text if ord(ch) < 128)


def remove_punc(text):
    return ''.join(ch for ch in text if ch not in exclude)


def stanford_tree(line, annotators='tokenize,pos,lemma'):
    output = nlp.annotate(line, properties={
        'annotators': annotators,
        'outputFormat': 'json'
    })
    try:
        return output
    except IndexError:
        pass


def gen_n_gram(input_sentence):
    ls_target_pos_tag = ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ', 'NN', 'NNS', 'NNP', 'NNPS']
    ls_lemma_pos_tag = ['NN', 'NNS', 'NNP', 'NNPS']
    ls_target_word = []
    ls_target_index = []

    result_tag = stanford_tree(input_sentence)
    # print result_tag
    # quit()
    ls_cnt_words = []
    ls_pos_sentence = []
    ls_sentence_seg = []
    for sentence in result_tag['sentences']:
        # print sentence
        cnt_words = 0
        for token in sentence['tokens']:
            pos = token['pos']
            if pos in ls_lemma_pos_tag:
                word = token['lemma']
                word_pos = pos[0] + '_' + word
            else:
                word = token['word']
                word_pos = pos[0] + '_' + word
            ls_pos_sentence.append(word_pos)
            if pos in ls_target_pos_tag:
                word_index = token['index'] - 1
                if ls_cnt_words:
                    for cnt_previous_word in ls_cnt_words:
                        word_index += cnt_previous_word
                dic_word = {'index': word_index, 'word': word_pos}
                ls_target_word.append(dic_word)
                ls_target_index.append(word_index)
            cnt_words += 1
        ls_cnt_words.append(cnt_words)

    pos_sentence = ' '.join(ls_pos_sentence)

    # word with pos
    ls_word = []
    for dic in ls_target_word:
        ls_word.append(dic['word'])


    # uncomment following line to generate n-gram without POS
    # words = word_tokenize(input_sentence)

    # comment following line to generate n-gram with POS
    words = word_tokenize(pos_sentence.lower())

    n_grams = ngrams(words, 5, pad_left=True, pad_right=True)
    ls_ngram = list(n_grams)

    ls_n_grams = []
    for index in ls_target_index:
        ls_n_grams.append(ls_ngram[index+2])
    # print ls_result
    return [ls_n_grams, ls_pos_sentence]


def get_dic_from_file():
    with open('whatsapp_record/output/message_object.txt', 'rb') as result_file:
        ls_dic = pickle.loads(result_file.read())
    return ls_dic
    # while True:
    #     line = file_dic.readline()


# extract n gram and all n gram words from message dic
def extract_n_gram_data(ls_dic, word_extraction = 1):
    ls_n_gram = []
    ls_n_gram_pos_word = []
    # quit()
    for dic in ls_dic:
        # print dic
        for n_gram in dic['ngram']:
            ls_n_gram.append(n_gram)
            for pos_word in n_gram:
                if pos_word is not None:
                    ls_n_gram_pos_word.append(pos_word)
    if word_extraction:
        return [ls_n_gram, ls_n_gram_pos_word]
    else:
        return ls_n_gram


# generate matrix for central word in n grams
def gen_central_word_matrix():
    ls_dic = get_dic_from_file()
    n_gram_data = extract_n_gram_data(ls_dic)
    ls_n_gram = n_gram_data[0]
    ls_n_gram_word = n_gram_data[1]
    # print(ls_n_gram[0])
    ls_central_word_all = []
    ls_central_word_unique = []
    file_meta_data.write("number of n gram:" + str(len(ls_n_gram)))

    for n_gram in ls_n_gram:
        word = n_gram[2]
        if word is not None:
            if word not in ls_central_word_unique:
                ls_central_word_unique.append(word)
            ls_central_word_all.append(word)
    print("all central words: ", len(ls_central_word_all))
    print("all unique central words:", len(ls_central_word_unique))
    file_meta_data.write("number of unique central words:" + str(len(ls_central_word_unique)))
    gen_adj_matrix(ls_central_word_unique, ls_n_gram_word, ls_n_gram)


# generate matrix
def gen_adj_matrix(n_gram_selected_words, n_gram_uni_word_corpus, n_gram_corpus):
    dic_corpus = {}
    # Build empty dictionary
    seg = [200, 500, 700, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000]
    i = 0

    # generate rows
    for word in n_gram_selected_words:
        # for each row, calculate its context
        dic_corpus[word] = {w: 0 for w in n_gram_uni_word_corpus}

        dic_corpus[word] = count_frequency(word, dic_corpus[word], n_gram_corpus)
        ls_cnt = []


        for k, v in dic_corpus[word].items():
            ls_cnt.append(v)

        i += 1
        if i in seg:
            end = timer()
            print(str(i), " finished, ", str(len(n_gram_selected_words) - i), " remaining.")
            print(str(round(end - start, 2)), " seconds used")

    with open('whatsapp_record/output/output_matrix.txt', 'wb') as file_matrix:
        pickle.dump(dic_corpus, file_matrix)


def count_frequency(word, dic, n_gram_corpus):
    # print "Dic for:", word, dic
    for n_gram in n_gram_corpus:
        ls_word_in_n_gram = list(n_gram)
        if n_gram[2] == word:
            for word in ls_word_in_n_gram:
                if word is not None:
                    dic[word] += 1

    return dic


# generate_dic()
gen_central_word_matrix()
