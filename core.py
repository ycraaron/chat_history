# coding: utf-8
from datetime import datetime
import xlrd
import re
import db_manager

REG_DATETIME = r"((?:0?[1-9]|1[0-2])\/(?:0?[1-9]|[1-2][0-9]|3[0-1])\/(?:[0-9]\d{1}), (?:[1-9]|1[0-2]):(?:0[1-9]|[0-5][0-9]) (?:PM|AM))"
REG_NON_ENGLISH = ur"[\u2E80-\u9FFF]+"
REG_MULTIMEDIA = r"IMG-[0-9]+|<Media omitted>"
EMOJI_PATTERN = re.compile(
    u"(\ud83d[\ude00-\ude4f])|"  # emoticons
    u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
    u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
    u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
    u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
    "+", flags=re.UNICODE)


def entry():

    src = "/Users/Aaron/Intralogue/chat_history/chat1.xlsx"

    # src = "/Users/Aaron/Intralogue/chat_history/chat_debug.xlsx"

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
    db_conn = db_manager.DBConn()
    db_conn.clear_data("TRUNCATE TABLE `whatsapp_record`")
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
        dialogue = work_sheet.cell_value(0, i)
        print("New worksheet")
        print("Row:", cnt_row)
        for i in range(0, cnt_row):

            dialogue = work_sheet.cell_value(i, 0)
            messages = dialogue.strip().splitlines()
            ls_merge_index = []
            ls_remove_index = []
            j = 0
            new_conver_tag = 1
            for msg in messages:
                timestamp = re.search(REG_DATETIME, msg)
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
                    messages[index-1] += messages[index]
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

            # quit()
            m = 0
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
                    timestamp = datetime.strptime(timestamp, "%m/%d/%y, %I:%M %p")
                    msg_dic[6] = timestamp
                # else:
                    # print "Timestamp not found: "
                    # print messages[m]

                    # print timestamp
                    # msg.replace(timestamp.strip(), "divider_replaced")
                    # print timestamp in msg
                    # print msg
                # print messages[m]
                # print m
                if "WhatsApp-a-Librarian:" in messages[m]:
                    # print "new librarian response found"
                    response = messages[m].split("WhatsApp-a-Librarian:")[1]
                    msg_dic[1] = response
                    msg_dic[0] = ' '
                elif u"\u202c" in messages[m]:
                    ls_message = unicode(messages[m]).split(u"\u202c")
                    question = ls_message[1]
                    if len(question) < 4:
                        m += 1
                        continue
                    else:
                        msg_dic[1] = ' '
                        msg_dic[0] = question.strip(': ')
                        print ls_message
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
                # elif " -- " in messages[m]:
                #     question = messages[m].split(" --:")[1]
                #     msg_dic[1] = ' '
                #     msg_dic[0] = question.strip(' ')
                if new_conver_tag == 1:
                    msg_dic[2] = 1
                    new_conver_tag = 0
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
                # print "message dictonary: ", msg_dic
                # if msg_dic[0] == 0:
                #     print messages[m]
                if msg_dic[0] == '0' and msg_dic[1] == '0':
                    print messages[m]
                m += 1

                ls_data.append(msg_dic)

            # for msg in messages:
                # print msg

            # msg_to_db = [0, 0, 0, 0, 0, 0, 0, 0]
            i += 1
        sheet_num += 1

        db_conn.insert_data(sql, ls_data)
        print ls_data
        ls_data = []
        print "Total rows", total_rows
        # break
    # print ls_data
    # db_conn.clear_data("TRUNCATE TABLE `whatsapp_record`")
    # print db_conn

entry()


def datetime_test():
    str = "9/30/16, 8:57 AM"
    d = datetime.strptime(str, "%m/%d/%y, %I:%M %p")
    print d


def chinese_detect():
    sample = u'I am from 美國 We should be friends. 朋友風較大考慮防放大鏡愛瘋。'
    for n in re.findall(ur'[\u2E80-\u9FFF]+', sample):
        print n

# print(content)

# datetime_test()

# chinese_detect()
# - 9
# 9-
# -9
# - 9
#
# - ‪+852 6997 3675‬: