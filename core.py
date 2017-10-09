# coding: utf-8
import xlrd
import re
import db_manager


def entry():
    REG_DATETIME = r"((?:0?[1-9]|1[0-2])\/(?:0?[1-9]|[1-2][0-9]|3[0-1])\/(?:[1-9]\d{1}), (?:[1-9]|1[0-2]):(?:0[1-9]|[1-5][0-9]) (?:PM|AM))"

    src = "/Users/Aaron/Intralogue/chat_history/chat1.xlsx"
    # output_workbook = "/Users/Aaron/Intralogue/output.xls"

    sheet_num = 0
    work_book = xlrd.open_workbook(src)
    work_sheet = work_book.sheet_by_index(sheet_num)

    cnt_row = work_sheet.nrows

    content = work_sheet.cell_value(0, 0)

    # print(content.splitlines())

    db_conn = db_manager.DBConn()
    ls_data = []

    # iterate through each single conversation
    for i in range(0, cnt_row):

        dialogue = work_sheet.cell_value(0, i)
        messages = dialogue.strip().splitlines()
        ls_merge_index = []
        ls_remove_index = []
        j = 0

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
                ls_merge_index.append(j)
            if "Messages you send to this chat and calls are now secured with end-to-end encryption. Tap for more info." in msg:
                ls_remove_index.append(j)
            j += 1

        # merge message
        if ls_merge_index:
            ls_merge_index.sort(reverse=True)
            # print(len(messages))
            for index in ls_merge_index:
                messages[index-1] += messages[index]
                # del messages[index]
                if index not in ls_remove_index:
                    ls_remove_index.append(index)

        # delete message
        if ls_remove_index:
            ls_remove_index.sort(reverse=True)
            for index in ls_remove_index:
                print "DELETED MESSAGE: ", messages[index]
                del messages[index]

        # print "Total number of messages: ", len(messages)
        # print "First message: ", messages[0]
        # print "Last message: ", messages[len(messages)-1]

        # quit()
        m = 0
        for m in xrange(0, len(messages)):
            msg_dic = [0, 0, 0, 0, 0, 0, 0]
            timestamp = re.search(REG_DATETIME, messages[m])
            if timestamp is not None:
                timestamp = timestamp.group()
                msg_dic[6] = timestamp

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
            else:
                # print messages[m]
                if u"-\u202c:" in messages[m]:
                    question = unicode(messages[m]).split(u"-\u202c")[1]
                    # print "Question", question
                    msg_dic[1] = ' '
                    msg_dic[0] = question.strip(': ')

            if m == 0:
                msg_dic[2] = 1
            else:
                msg_dic[2] = 0
            msg_dic[3] = 0
            msg_dic[4] = 0
            msg_dic[5] = 0
            m += 1
            # print "message dictonary: ", msg_dic
            ls_data.append(msg_dic)

        # for msg in messages:
            # print msg

        # msg_to_db = [0, 0, 0, 0, 0, 0, 0, 0]
        i += 1
        # break

    print ls_data
    db_conn.clear_data("TRUNCATE TABLE `whatsapp_record`")
    sql = "INSERT INTO `whatsapp_record` (`user_input`,`lib_response`, `new_conver`,`multi_intent_conver`, `multimedia`, `chinese`, `time`) VALUES(%s, %s, %s, %s, %s, %s, %s)"
    # print db_conn
    print db_conn.insert_data(sql, ls_data)

entry()

#print(content)
