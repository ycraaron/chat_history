# coding: utf-8
from nltk import ngrams
from nltk import word_tokenize
from datetime import datetime
from collections import deque
from collections import OrderedDict
from pycorenlp import StanfordCoreNLP
from timeit import default_timer as timer
import xlrd
import re
import db_manager
import string
import time

nlp = StanfordCoreNLP('http://192.168.0.100:9000')

# REG_DATETIME = r"((?:0?[1-9]|1[0-2])\/(?:0?[1-9]|[1-2][0-9]|3[0-1])\/(?:[0-9]\d{1}), (?:[1-9]|1[0-2]):(?:0[1-9]|[0-5][0-9]) (?:PM|AM))"
# YYYY added
REG_DATETIME = r"((?:0?[1-9]|1[0-2])\/(?:0?[1-9]|[1-2][0-9]|3[0-1])\/((?:[0-9]\d{1})|(?:[0-9]\d{3})), (?:[1-9]|1[0-2]):(?:0[1-9]|[0-5][0-9]) (?:PM|AM))"
REG_NON_ENGLISH = ur"[\u2E80-\u9FFF]+"
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

URL_PATTERN = r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b'

table = {ord(f): ord(t) for f, t in zip(
    u'“”～‘’，。！？【】（）％＃＠＆１２３４５６７８９０',
    u'""~\'\',.!?[]()%#@&1234567890')}
exclude = set(string.punctuation)

db_conn = db_manager.DBConn()
file_meta_data = open('output/meta_data.txt','w')
start = timer()


def entry():

    src = "/Users/Aaron/Intralogue/chat_history/chat3.xlsx"

    # src = "/Users/Aaron/Intralogue/chat_history/chat_debug.xlsx"
    cnt_new_conver = 0
    # output_workbook = "/Users/Aaron/Intralogue/output.xls"
    sheet_num = 0
    work_book = xlrd.open_workbook(src)
    total_rows = 0
    # work_sheet = work_book.sheet_by_index(sheet_num)
    # cnt_row = work_sheet.nrows
    cnt_sheets = work_book.nsheets
    print "number of sheets"
    # content = work_sheet.cell_value(0, 0)
    # print(content.splitlines())
    # db_conn.clear_data("TRUNCATE TABLE `whatsapp_record`")
    ls_data = []
    # iterate through each single conversation

    i = 0
    sql = "INSERT INTO `whatsapp_record` (`user_input`,`lib_response`, `new_conver`,`multi_intent_conver`, `multimedia`, `chinese`, `time`) VALUES(%s, %s, %s, %s, %s, %s, %s)"

    while sheet_num < cnt_sheets:
        i = 0
        work_sheet = work_book.sheet_by_index(sheet_num)
        if work_sheet.cell_value(0, 0) == "ENDOFALL":
            print("ALL END")
            break
        cnt_row = work_sheet.nrows
        total_rows += cnt_row
        print("New worksheet")
        print("Row:", cnt_row)
        for i in range(0, cnt_row):
            dialogue = work_sheet.cell_value(i, 0)
            messages = dialogue.strip().splitlines()
            # print len(messages)
            # print messages
            ls_merge_index = []
            ls_remove_index = []
            j = 0
            new_conver_tag = 1
            for msg in messages:
                timestamp = re.search(REG_DATETIME, msg)
                # print timestamp
                # timestamp found
                if timestamp is not None:
                    timestamp = timestamp.group()
                    # if msg is not None:
                    #     print timestamp, " msg content: ", msg.replace(timestamp, "replaced&&div")
                # timestamp is not found
                # merge this msg with former one
                else:
                    # print "time stamp is none in message: ", msg
                    ls_merge_index.append(j)
                if "Messages you send to this chat and calls are now secured with end-to-end encryption. Tap for more info." in msg:
                    ls_remove_index.append(j)
                if "Messages to this chat and calls are now secured with end-to-end encryption. Tap for more info." in msg:
                    ls_remove_index.append(j)
                j += 1

            # print "remove index before merge: ", ls_remove_index
            # merge message
            if ls_merge_index:
                ls_merge_index.sort(reverse=True)
                # print(len(messages))
                for index in ls_merge_index:
                    # print "merged message: ", messages[index]
                    messages[index-1] += ' ' + messages[index]
                    # del messages[index]
                    if index not in ls_remove_index:
                        ls_remove_index.append(index)

            # print "merge index", ls_merge_index
            # delete message
            if ls_remove_index:
                ls_remove_index.sort(reverse=True)
                # print "remove index after merge:", ls_remove_index
                for index in ls_remove_index:
                    # print "DELETED MESSAGE: ", messages[index]
                    del messages[index]
            # print "Total number of messages: ", len(messages)
            # print "First message: ", messages[0]
            # print "Last message: ", messages[len(messages)-1]
            # print messages

            # quit()
            m = 0

            # print "msg length", len(messages)
            for m in xrange(0, len(messages)):
                msg_dic = [0, 0, 0, 0, 0, 0, 0]
                messages[m] = EMOJI_PATTERN.sub(r'', messages[m])
                # tmp_tag = 0
                timestamp = re.search(REG_DATETIME, messages[m])
                if timestamp is not None:
                    # print messages[m]
                    timestamp = timestamp.group()
                    # messages[m] = messages[m].replace(timestamp.strip(), "datetime_replacements")

                    # print timestamp
                    try:
                        timestamp = datetime.strptime(timestamp, "%m/%d/%y, %I:%M %p")
                    except Exception,data:
                        timestamp = datetime.strptime(timestamp, "%m/%d/%Y, %I:%M %p")
                    msg_dic[6] = timestamp
                # else:
                    # print "Timestamp not found: "
                    # print messages[m]

                    # print timestamp
                    # msg.replace(timestamp.strip(), "divider_replaced")
                    # print timestamp in msg
                    # print msg

                if m == 0:
                    # print messages[m]
                    msg_dic[2] = 1
                    cnt_new_conver += 1
                else:
                    msg_dic[2] = 0
                msg_dic[3] = 0

                if re.findall(REG_MULTIMEDIA, messages[m]):
                    msg_dic[4] = 1
                else:
                    msg_dic[4] = 0
                if re.findall(REG_NON_ENGLISH, messages[m]):
                    # print messages[m]
                    msg_dic[5] = 1
                else:
                    msg_dic[5] = 0

                # print messages[m]
                # print m
                if "WhatsApp-a-Librarian:" in messages[m]:
                    # print "new librarian response found"
                    response = messages[m].split("WhatsApp-a-Librarian:")[1]
                    msg_dic[1] = response
                    msg_dic[0] = ' '
                elif u"\u202c" not in messages[m] and u"\u202a" in messages[m]:
                    ls_message = unicode(messages[m]).split(u"\u202a")
                    question = ls_message[1]
                    # print ls_message
                    if len(question) < 4:
                        m += 1
                        continue
                    else:
                        msg_dic[1] = ' '
                        msg_dic[0] = question.strip(': ')
                elif u"\u202c" in messages[m]:
                    ls_message = unicode(messages[m]).split(u"\u202c")
                    question = ls_message[1]
                    if len(question) < 4:
                        m += 1
                        continue
                    else:
                        msg_dic[1] = ' '
                        msg_dic[0] = question.strip(': ')
                # elif u"- \u202c:" in messages[m]:
                #     question = unicode(messages[m]).split(u"- \u202c")[1]
                #     msg_dic[1] = ' '
                #     msg_dic[0] = question.strip(' ').strip(':').strip(' ')
                # elif u"\u202c-" in messages[m]:
                #     question = unicode(messages[m]).split(u"\u202c-")[1]
                #     msg_dic[1] = ' '
                #     msg_dic[0] = question.strip(' ').strip(':').strip(' ')
                # elif u"-\u202c" in messages[m]:
                #     question = unicode(messages[m]).split(u"-\u202c")[1]
                #     msg_dic[1] = ' '
                #     msg_dic[0] = question.strip(' ').strip(':').strip(' ')
                # elif u"-"
                elif " - -:" in messages[m]:
                    question = messages[m].split(" - -:")[1]
                    msg_dic[1] = ' '
                    msg_dic[0] = question.strip(' ')
                # elif " - ‬:" in messages[m]:
                #     question = messages[m].split(" - ‬:")[1]
                #     msg_dic[1] = ' '
                #     msg_dic[0] = question.strip(' ')
                elif " --:" in messages[m]:
                    question = messages[m].split(" --:")[1]
                    msg_dic[1] = ' '
                    msg_dic[0] = question.strip(' ')
                elif " - :" in messages[m]:
                    question = messages[m].split(" - :")[1]
                    msg_dic[1] = ' '
                    msg_dic[0] = question.strip(' ')
                elif " : " in messages[m]:
                    question = messages[m].split(" : ")[1]
                    msg_dic[1] = ' '
                    msg_dic[0] = question.strip(' ')
                elif " -:" in messages[m]:
                    question = messages[m].split(" -:")[1]
                    msg_dic[1] = ' '
                    msg_dic[0] = question.strip(' ')
                elif " -- " in messages[m]:
                    question = messages[m].split(" -- ")[1]
                    msg_dic[1] = ' '
                    msg_dic[0] = question.strip(' ')
                elif " - ":
                    if len(messages[m].split(" - ")) > 1:
                        question = messages[m].split(" - ")[1]
                    else:
                        m += 1
                        continue
                    msg_dic[1] = ' '
                    msg_dic[0] = question.strip(' ')

                # elif " -- " in messages[m]:
                #     question = messages[m].split(" --:")[1]
                #     msg_dic[1] = ' '
                #     msg_dic[0] = question.strip(' ')

                # print "message dictonary: ", msg_dic
                # if msg_dic[0] == 0:
                #     print messages[m]
                if msg_dic[0] == 0 and msg_dic[1] == 0:
                    print messages[m]
                db_conn.insert_data(sql, [msg_dic])
                m += 1
                # ls_data.append(msg_dic)

            # for msg in messages:
                # print msg

            # msg_to_db = [0, 0, 0, 0, 0, 0, 0, 0]
            i += 1
        sheet_num += 1

        # db_conn.insert_data(sql, ls_data)
        # print ls_data
        ls_data = []
        # break
        print "Total rows", total_rows

    # print ls_data
    # db_conn.clear_data("TRUNCATE TABLE `whatsapp_record`")
    # print db_conn
    print cnt_new_conver


def datetime_test():
    str = "9/30/2016, 8:57 AM"
    d = datetime.strptime(str, "%m/%d/%y, %I:%M %p")
    print d


def chinese_detect():
    sample = u'I am from 美國 We should be friends. 朋友風較大考慮防放大鏡愛瘋。'
    for n in re.findall(ur'[\u2E80-\u9FFF]+', sample):
        print n


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
    # print ls_target_word
    # print ls_target_word
    # print ls_target_index
    ls_word = []
    for dic in ls_target_word:
        ls_word.append(dic['word'])


    # uncomment following line to generate n-gram without POS
    # words = word_tokenize(input_sentence)

    # comment following line to generate n-gram with POS
    words = word_tokenize(pos_sentence.lower())

    n_grams = ngrams(words, 5, pad_left=True, pad_right=True)
    ls_ngram = list(n_grams)

    ls_result = []
    for index in ls_target_index:
        ls_result.append(ls_ngram[index+2])
    # print ls_result
    return ls_result


def remove_non_ascii(text):
    return ''.join(ch for ch in text if ord(ch) < 128)


def remove_punc(text):
    return ''.join(ch for ch in text if ch not in exclude)


def generate_dic():
    cnt_excep = 0
    sql = "SELECT id, user_input, new_conver, continuous FROM `whatsapp_record` WHERE chinese = 0 AND lib_response = ' ' AND user_input != ' ' AND multimedia = 0 ORDER BY `id` ASC"
    sql = "SELECT id, user_input, new_conver, continuous FROM `whatsapp_record` WHERE user_input LIKE %s ORDER BY `id` ASC"
    ls_result = db_conn.fetch_data(sql, ['%http%'])
    # print len(ls_result)
    # print ls_result
    print "total messages:", len(ls_result)
    ls_dic = []
    excep_file = open('output/excep.txt', 'w')
    result_file = open('output/result.txt', 'w')
    i = 0
    seg = [50, 100, 200, 500, 700, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000]
    for record in ls_result:
        # print record
        # print record
        id = record['id']
        message_origin = record['user_input']
        # message_origin = "How can I borrow some books or music from http://sunzi.lib.hku.hk/hkgro/view/b1880/51880025.pdf and library?"
        # message_origin = "How to cancel my study table booking please？"
        # print type(message_origin), message_origin
        # message = message_origin.translate(table)
        # message = message_origin.encode()
        message = re.sub(URL_PATTERN, 'replaced_url', message_origin, flags=re.IGNORECASE)
        # print message
        message = remove_punc(remove_non_ascii(message))
        # print message
        # print "AFTER translate", message
        try:
            n_gram = gen_n_gram(message.encode("utf-8"))
        except Exception, data:
            # print data.__str__()
            cnt_excep += 1
            n_gram = "Unhandled"
            str_excep = "id: " + str(id) + ' msg: ' + message
            print str_excep
            print >> excep_file, str_excep
            continue
            # quit()
        initial = record['new_conver']
        continuous = record['continuous']
        dic = {'id': id, 'message': message, 'msg_initial': initial, 'continuous': continuous, 'ngram': n_gram}
        print>> result_file, dic
        ls_dic.append(dic)
        end = timer()
        if i in seg:
            print str(i), " handled. ", str(len(ls_result)-i), " remaining."
            print (str(round(end - start, 2)) + " seconds used")

        i += 1
    # for item in ls_dic:
        # print>> result_file, item
    gen_matrix(ls_dic)

    print cnt_excep, " unhandled sentences."


def cal_term_frequency(words, corpus):
    dic_term_frequency = {k: 0 for k in words}

    # print corpus
    for word in words:
        for word_cmp in corpus:
            if word == word_cmp:
                dic_term_frequency[word] += 1

    dic_term_frequency['c_but'] = 40
    highest = max(dic_term_frequency.values())
    if highest > 0:
        ls_highest_pos = ([k for k, v in dic_term_frequency.items() if v == highest])

    if ls_highest_pos:
        print "highest frequency terms: ", ls_highest_pos
        print >> file_meta_data, "highest frequency terms: ", ls_highest_pos
        for key in ls_highest_pos:
            print "highest term frequency:", dic_term_frequency[key]
            print >> file_meta_data, "highest term frequency: ", dic_term_frequency[key]
            break

    # key_max = max(dic_term_frequency.iterkeys(), key=lambda k: dic_term_frequency[k])
    # print key_max
    #
    # print dic_term_frequency[key_max]

    print >> file_meta_data, "term frequency:", dic_term_frequency

    # quit()
    # for word in words:
    #     for word_cmp in corpus:
    #         if word == word_cmp:
    #             dicter


def gen_matrix(ls_dic):
    ls_n_gram = []
    ls_n_gram_pos_word = []
    for ls_dic in ls_dic:
        for n_gram in ls_dic['ngram']:
            ls_n_gram.append(n_gram)
            for pos_word in n_gram:
                if pos_word is not None:
                    ls_n_gram_pos_word.append(pos_word)

    # print ls_n_gram
    # print ls_n_gram_pos_word
    # print "before removing dup length: ", len(ls_n_gram_pos_word)
    set_n_gram_pos_word = set(ls_n_gram_pos_word)
    # print "duplicate removed length: ", len(set_n_gram_pos_word)

    ls_unique_n_gram_pos_word = list(set_n_gram_pos_word)

    ls_unique_n_gram_pos_word.sort()

    file_order_words = open('output/words_order.txt', 'w')
    print "total unique words in corpus: ", len(ls_unique_n_gram_pos_word)
    print "total n-grams in corpus: ", len(ls_n_gram)

    print >> file_meta_data, "total unique words in corpus: ", len(ls_unique_n_gram_pos_word)
    print >> file_meta_data, "total n-grams in corpus:", len(ls_n_gram)

    cal_term_frequency(ls_unique_n_gram_pos_word, ls_n_gram_pos_word)

    print >> file_order_words, ls_unique_n_gram_pos_word
    # quit()
    # print ls_unique_n_gram_pos_word
    gen_adj_matrix(ls_unique_n_gram_pos_word, ls_n_gram)


def gen_adj_matrix(uni_ngram_words, n_gram_corpus):
    dic_corpus = {}
    # print uni_ngram_words
    # Build empty dictionary
    seg = [200, 500, 700, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000]
    i = 0
    file_n_gram_matrix = open('output/n_gram_matrix_num.txt', 'w')

    for word in uni_ngram_words:
        dic_corpus[word] = {w: 0 for w in uni_ngram_words}
        dic_corpus[word] = count_frequency(word, dic_corpus[word], n_gram_corpus)
        ls_cnt = []
        # print dic_corpus[word]
        for k, v in dic_corpus[word].iteritems():
            ls_cnt.append(v)
        print >> file_n_gram_matrix, ls_cnt
        i += 1
        if i in seg:
            end = timer()
            print str(i), " finished, ", str(len(uni_ngram_words) - i), " remaining."
            print (str(round(end - start, 2)) + " seconds used")

    # print dic_corpus
    # print >>file_n_gram_matrix, dic_corpus
    # for dic in dic_corpus:
    #     print >>n_gram_matrix, list(dic)


def count_frequency(word, dic, n_gram_corpus):
    # print "Dic for:", word, dic
    for n_gram in n_gram_corpus:
        # print n_gram_corpus
        n_gram = list(n_gram)
        # print n_gram
        # print u"w_how" in n_gram
        indices = [i for i, x in enumerate(n_gram) if x == word]
        indices.sort()
        if indices:
            # print indices
            for index in indices:
                if index < len(n_gram) - 1: # 4
                    # print index
                    following_word = n_gram[index + 1]
                    if following_word is not None:
                        dic[following_word] += 1
        # if word in n_gram:
        #     index_word = n_gram.index(word)
        #     if index_word == len(n_gram) - 1:
        #         continue
        #     index_word_next = n_gram
    return dic


def check_conti():
    ls_sequence = deque([])
    result = db_conn.fetch_data("SELECT user_input, lib_response, id FROM whatsapp_record WHERE (user_input,lib_response)!= (' ',' ') ORDER BY id")
    for record in result:
        # Librarian response found
        if record['user_input'] == ' ':
            ls_sequence.append({'response': 1, 'id': record['id']})
        # User input found
        else:
            ls_sequence.append({'response': 0, 'id': record['id']})
    ls_msg_conti = []
    ls_msg_db = []
    while True:
        if not ls_sequence:
            break
        msg = ls_sequence.popleft()
        if msg['response'] == 0:
            ls_msg_conti.append(msg)
        elif msg['response'] == 1:
            if len(ls_msg_conti) > 1:
                for msg in ls_msg_conti:
                    print db_conn.update_data("UPDATE `whatsapp_record` SET `continuous` = 1 WHERE id = %s", [str(msg['id'])])
                    # ls_msg_db.append(msg)
                ls_msg_conti = []
            else:
                ls_msg_conti = []

    print len(ls_msg_db)
    ls_id = []
    for item in ls_msg_db:
        ls_id.append(str(item['id']))


def test():
    # msg = "How to cancel my study table booking please？"
    # str = unicode(msg, encoding="utf-8")
    # print str
    # str2 = str.translate(table)
    # print unicode(str2)
    # print str2
    msg = "a http://sunzi.lib.hku.hk/hkgro/view/b1880/51880025.pdf"
    print re.findall(URL_PATTERN, msg)
    pattern = r'https?:\/\/.*[\r\n]* ?'
    pattern = r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b'
    print re.sub(pattern, 'replaced_url', msg, flags=re.IGNORECASE)


# test()
# check_conti()
# print(content)
generate_dic()
# datetime_test()
# chinese_detect()
# sentence = "Good morning. If I request a book from another university through HKALL, and borrow it with my HKU studentcard, can I borrow and return the book from main library @ HKU?"
# gen_n_gram("How can I borrow some books or music from http://sunzi.lib.hku.hk/hkgro/view/b1880/51880025.pdf and library?")
