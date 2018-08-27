# Usage: python miniSQL.py <query>

import sys, csv, re
from collections import OrderedDict

file_name = sys.argv[0]
# print("file_name:", file_name)
metadata_file = "./metadata.txt"
metadata = {}
table_to_alias = {}
alias_to_table = {}
try:
    # opens metadata.txt for the syntax of tables
    file = open(metadata_file, 'r')
except:
    print("ERROR in opening metadata file")
    print("exiting mini SQL engine...")
    sys.exit()
else:
    table_name, table_name_flag, count = "", False, 0
    for line in file:
        if line.strip() == "<begin_table>":
            table_name_flag = True
        elif table_name_flag:
            table_name = line.strip().lower()
            metadata[table_name] = ([], {});
            table_name_flag = False
            count = 0
        elif line.strip() != "<end_table>":
            metadata[table_name][0].append(line.strip().lower())
            metadata[table_name][1][line.strip().lower()] = count
            count = count + 1
    # print("metadata", metadata)

queries = sys.argv[1:]
# print("queries:")
# print("\n".join(queries))
not_present = -1
ambiguous = -1
verbose_mode = False

class MyError(Exception):
    '''
    Class to handle custom errors
    '''
    def __init__(self, value="Class MyError"):
        self.value = value
    def __str__(self):
        return repr(self.value)

def myAssert(condition, action):
    if not condition:
        # try:
        raise action
        # except:
        #     pass
        #     # print(action)

def check_ordering_ignoring(tokens, ignore_value):
    '''
    checks
    '''
    curr = tokens[0]
    for i in range(1, len(tokens)):
        if tokens[i] != ignore_value:
            if tokens[i] <= curr:
                return False
            else:
                curr = tokens[i]
    return True

def print_details(variable_name="", variable_value=""):
    if verbose_mode:
        print(repr(variable_name) + ": " + repr(variable_value))

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    return False

def combine_tables(table1, table1_metadata, table2, table2_metadata, combining_condition="True", combining_error="ERROR: combining error", table_name="", where_clause="True", select_clause=""):
    print_details("in combine_tables function")
    if not table2_metadata:
        return table1, table1_metadata
    if not table1_metadata:
        return table2, table2_metadata
    myAssert(eval(combining_condition), MyError(combining_error))
    if table_name == "":
        # smart naming of tables
        table1_name = list(map(str.strip, table1_metadata["table_name"].split("_")))
        table2_name = list(map(str.strip, table2_metadata["table_name"].split("_")))
        table_name_splitted = list(OrderedDict.fromkeys(table1_name + table2_name))
        print_details("table_name_splitted", table_name_splitted)
        table_name = "_".join(table_name_splitted)
        # table_name = table1_metadata["table_name"] + "_" + table2_metadata["table_name"]
    database, database_metadata = [], {"table_name": table_name, table_name:([],{}), "secondary_data":{}}
    count = 0
    for key, value in table1_metadata["secondary_data"].items():
        if key in database_metadata["secondary_data"] and len(set(value[0]) & set(database_metadata["secondary_data"][key][0])):
            raise MyError("ERROR: Not Unique Combining Table/Alias 1")
        database_metadata["secondary_data"][key] = (value[0], {})
        for col in table1_metadata[table1_metadata["table_name"]][0]:
            database_metadata[database_metadata["table_name"]][1][col] = ambiguous if col in database_metadata[database_metadata["table_name"]][0] else count
            database_metadata["secondary_data"][key][1][col] = count
            database_metadata[database_metadata["table_name"]][0].append(col)
            count = count + 1
    # print("database_metadata:", database_metadata)
    for key, value in table2_metadata["secondary_data"].items():
        if key in database_metadata["secondary_data"] and len(set(value[0]) & set(database_metadata["secondary_data"][key][0])):
            raise MyError("ERROR: Not Unique Combining Table/Alias 2")
        database_metadata["secondary_data"][key] = (value[0], {})
        for col in table2_metadata[table2_metadata["table_name"]][0]:
            database_metadata[database_metadata["table_name"]][1][col] = ambiguous if col in database_metadata[database_metadata["table_name"]][0] else count
            database_metadata["secondary_data"][key][1][col] = count
            database_metadata[database_metadata["table_name"]][0].append(col)
            count = count + 1
    print_details("(combine_tables)database_metadata", database_metadata)
    for index1, row1 in enumerate(table1):
        for index2, row2 in enumerate(table2):
            if eval(where_clause):
                database.append(row1 + row2)
    print_details("(combine_tables)database", database)
    return database, database_metadata

def evaluate_from(database, database_metadata, from_clause, where_clause="True", select_clause=""):
    print_details("in from subcommand", from_clause + " , " + where_clause)
    # cross rows of the given tables
    # pass
    this_where_clause = where_clause
    from_clause = from_clause.strip()
    from_clause_tokenize = from_clause.split(',')
    print_details("from_clause_tokenize", from_clause_tokenize)
    for tables in from_clause_tokenize:
        tables = tables.split()
        print_details("tables", tables)
        table = tables[0]
        alias_table = tables[-1]
        myAssert(len(tables) == 1 or (len(tables) == 3 and tables[1].lower() == 'as'), MyError('ERROR: You have an error in aliasing your SQL syntax'))
        table_to_alias[table] = alias_table
        alias_to_table[alias_table] = table
        try:
            csvfile = open(table+".csv", 'r')
            # rows = list(map(lambda x: list(map(int, x)), list(csv.reader(csvfile))))
            rows = list(csv.reader(csvfile))
        except:
            raise MyError('ERROR: Table ' + table + ' doesn\'t exist')
        else:
            cols = {"table_name":alias_table, alias_table:metadata[table], "secondary_data":{alias_table:metadata[table]}}
            print_details("cols", cols)
            database, database_metadata = combine_tables(database, database_metadata, rows, cols, combining_condition="table1_metadata[\"table_name\"] != table2_metadata[\"table_name\"]", combining_error="ERROR Not Unique Table/Alias", where_clause=this_where_clause)
    print_details("database_metadata", database_metadata)
    # print("database:")
    # print('\n'.join(','.join(row) for row in database))
    print_details("table_to_alias", table_to_alias)
    print_details("alias_to_table", alias_to_table)
    return database, database_metadata

def get_columns(database, database_metadata, column_list, aggregate_fuction):
    print_details("in get_columns function")
    # return column from database
    new_database, new_database_metadata = [], {"table_name": database_metadata["table_name"], database_metadata["table_name"]:([], {}), "secondary_data":{}}
    table_alias = column_list.split('.')[0]
    column_name = column_list.split('.')[-1]
    if column_name == "*":
        return aggregate_fuction[1](database), database_metadata
    myAssert(column_name in database_metadata[database_metadata["table_name"]][0], MyError("ERROR: Unknown column " + column_name + " in field list"))
    myAssert(not(database_metadata[database_metadata["table_name"]][1][column_name] == ambiguous and table_alias not in database_metadata["secondary_data"]), MyError("ERROR: Column " + column_name + " in field list is ambiguous"))
    if '.' not in column_list:
        for key, value in database_metadata["secondary_data"].items():
            if column_name in value[0]:
                table_alias = key
                break
    myAssert(column_name in database_metadata["secondary_data"][table_alias][0], MyError("ERROR: Unknown column " + table_alias + "." + column_name + " in field list"))
    new_database_metadata[new_database_metadata["table_name"]][0].append(aggregate_fuction[0](column_name))
    new_database_metadata[new_database_metadata["table_name"]][1][aggregate_fuction[0](column_name)] = 0
    new_database_metadata["secondary_data"][table_alias] = ([column_name], {column_name: 0})
    index = database_metadata[database_metadata["table_name"]][1][column_name] if database_metadata[database_metadata["table_name"]][1][column_name] != ambiguous else database_metadata["secondary_data"][table_alias][1][column_name]
    # new_database = [[]*len(database)]
    for row in database:
        new_database.append([row[index]])
    if not new_database:
        return [[]], new_database_metadata
    return aggregate_fuction[1](new_database), new_database_metadata

def break_list(l):
    print_details("l", l)
    if not l:
        return [[]]
    new_l = []
    for item in l:
        new_l.append([item])
    return new_l

def evaluate_select(database, database_metadata, select_clause):
    print_details("in select subcommand", select_clause)
    # select cols from cross rows of the given tables
    # pass
    print_details("(evaluate_select)database", database)
    select_clause = select_clause.strip()
    select_clause_tokenize = list(map(str.strip, select_clause.split(',')))
    new_database, new_database_metadata = [], {}
    aggregate_fuctions = {  "max": (lambda name: "max(" + name + ")", lambda data: [[str(max(list(map(int, x)))) for x in zip(*data)]] if len(data[0]) == 1 else myAssert(False, MyError("ERROR: Invalid use of aggregate fuction max"))),
                            "min": (lambda name: "min(" + name + ")", lambda data: [[str(min(list(map(int, x)))) for x in zip(*data)]] if len(data[0]) == 1 else myAssert(False, MyError("ERROR: Invalid use of aggregate fuction min"))),
                            "distinct": (lambda name: "distinct(" + name + ")", lambda data: list(map(lambda x: list(map(str, x)), break_list(list(OrderedDict.fromkeys([x[0] for x in data]))))) if len(data[0]) == 1 else myAssert(False, MyError("ERROR: Invalid use of aggregate fuction distinct"))),
                            "sum": (lambda name: "sum(" + name + ")", lambda data: [[str(sum(list(map(int, x)))) for x in zip(*data)]] if len(data[0]) == 1 else myAssert(False, MyError("ERROR: Invalid use of aggregate fuction sum"))),
                            "avg": (lambda name: "average(" + name + ")", lambda data: [[str((sum(list(map(int, x))))/(len(list(map(int, x))))) for x in zip(*data)]] if len(data[0]) == 1 else myAssert(False, MyError("ERROR: Invalid use of aggregate fuction avg"))),
                            "count": (lambda name: "count(" + name + ")", lambda data: [[str(len(data))]])}
    print_details("select_clause_tokenize", select_clause_tokenize)
    for cols in select_clause_tokenize:
        cols = cols.split('(')
        print_details("cols", cols)
        col = cols[-1].replace(')', '')
        new_column, new_column_metadata = get_columns(database, database_metadata, col, aggregate_fuctions.get(cols[0], (lambda name: name, lambda data: data)))
        print_details("new_column", new_column)
        print_details("new_column_metadata", new_column_metadata)
        myAssert(not len(new_database) or (len(new_database) and len(new_database) == len(new_column)), MyError('ERROR: AGGREGATE functions incompatible'))
        if not len(new_database):
            new_database = [[]*len(new_column)]
        new_database, new_database_metadata = combine_tables(new_database, new_database_metadata, new_column, new_column_metadata, combining_condition="len(table1) == len(table2)", combining_error="ERROR: combination of two different length database", where_clause="index1 == index2")
    print_details("database_metadata", database_metadata)
    if not new_database:
        new_database = [[]]
    print_details("new_database", '\n' + '\n'.join(','.join(row) for row in new_database))
    return new_database, database_metadata

def parse_where(where_clause):
    print_details("in parse_where function", where_clause)
    # parse the where query to proper executable clause
    # pass
    where_clause_parsed = where_clause.strip().lower()
    change_clause = {'=':'=='}
    conditions = ['and', 'or', '==', '>=', '<=', '>', '<']
    for key, value in change_clause.items():
        where_clause_parsed = where_clause_parsed.replace(key, value)
    where_clause_tokenized = where_clause_parsed.split()
    for clause in where_clause_tokenized:
        if clause not in conditions and not is_number(clause):
            # its attribute comparison to attribute or value
            table_alias = clause.split('.')[0]
            column_name = clause.split('.')[-1]
            # print("metadata.items():", list(metadata.items()))
            # column_name_count = len(list(filter(lambda x: column_name in x[1][0], list(metadata.items()))))
            # print("column_name_count:", column_name_count == 1)
            myAssert(column_name in database_metadata[database_metadata["table_name"]][0], MyError("ERROR: Unknown column " + column_name + " in field list"))
            myAssert(not(database_metadata[database_metadata["table_name"]][1][column_name] == ambiguous and table_name not in database_metadata["secondary_data"]), MyError("ERROR: Column " + column_name + " in field list is ambiguous"))

    return ' '.join(where_clause_tokenized)

def evaluate_where(database, database_metadata, where_clause):
    print_details("in where subcommand", where_clause)
    # parse the where query to proper executable clause
    # pass
    if where_clause == "":
        return database, database_metadata
    where_clause_parsed = where_clause.strip().lower()
    change_clause = {'(?<!\!)=':'==', "<>":"!="}
    conditions = ['and', 'or', 'not', '==', '>=', '<=', '>', '<', '!=', '(', ')']
    for key, value in change_clause.items():
        # where_clause_parsed = where_clause_parsed.replace(key, value)
        where_clause_parsed = re.sub(key, value, where_clause_parsed)
    print_details("where_clause_parsed", where_clause_parsed)
    where_clause_tokenized = where_clause_parsed.split()
    new_database, new_database_metadata = [], {}
    for i, clause in enumerate(where_clause_tokenized):
        clause = clause.strip()
        if clause not in conditions and not is_number(clause):
            # its attribute comparison to attribute or value
            table_alias = clause.split('.')[0]
            column_name = clause.split('.')[-1]
            # print_details("metadata.items()", list(metadata.items()))
            # column_name_count = len(list(filter(lambda x: column_name in x[1][0], list(metadata.items()))))
            # print("column_name_count:", column_name_count == 1)
            myAssert(column_name in database_metadata[database_metadata["table_name"]][0], MyError("ERROR: Unknown column " + column_name + " in field list"))
            myAssert(not(database_metadata[database_metadata["table_name"]][1][column_name] == ambiguous and table_alias not in database_metadata["secondary_data"]), MyError("ERROR: Column " + column_name + " in field list is ambiguous"))
            if '.' not in clause:
                for key, value in database_metadata["secondary_data"].items():
                    if column_name in value[0]:
                        table_alias = key
                        break
            myAssert(column_name in database_metadata["secondary_data"][table_alias][0], MyError("ERROR: Unknown column " + table_alias + "." + column_name + " in field list"))
            clause = "int(row[" + str(database_metadata[database_metadata["table_name"]][1][column_name] if database_metadata[database_metadata["table_name"]][1][column_name] != ambiguous else database_metadata["secondary_data"][table_alias][1][column_name]) + "])"
        where_clause_tokenized[i] = clause
    where_clause_parsed = ' '.join(where_clause_tokenized)
    print_details("where_clause_parsed", where_clause_parsed)
    for row in database:
        if eval(where_clause_parsed):
            new_database.append(row)
    print_details("(evaluate_where)new_database", new_database)
    return new_database, database_metadata

def parse_nowhere(clause):
    # parse the query to proper executable clause
    # pass
    return clause

def nowhere(database, database_metadata, *argv):
    return database, database_metadata

def select(query):
    # syntax: select <attributes> from <table> [where <condition>] [order by <condition>]
    print_details("in select command", query)
    print_details("len(query)", len(query))
    query_elements = query.split()
    print_details("query_elements", query_elements)
    tokens_ordering = ['select', 'from', 'where', 'group by', 'having', 'order by'] # ordering
    tokens_map = {'from': [evaluate_from, parse_nowhere, ''], 'where': [evaluate_where, parse_nowhere, ''], 'group by': [nowhere, parse_nowhere, ''], 'having': [nowhere, parse_nowhere, ''], 'order by': [nowhere, parse_nowhere, ''], 'select': [evaluate_select, parse_nowhere, '']}
    tokens_index = []
    tokens_execution = ['from', 'where', 'group by', 'having', 'order by', 'select'] # execution ordering
    required_tokens = [('select', 'ERROR: Unknown Command'), ('from', 'ERROR: No Tables used')]
    for req_tok in required_tokens:
        if req_tok[0] not in query_elements:
            raise MyError(req_tok[1])
    for tok in tokens_ordering:
        try:
            tokens_index.append(query.index(tok))
        except:
            tokens_index.append(not_present)
    print_details("tokens_ordering", tokens_ordering)
    print_details("tokens_index", tokens_index)
    if not check_ordering_ignoring(tokens_index, not_present):
        raise MyError('ERROR: You have an error in ordering SQL clause')
    database, database_metadata = [], {}
    for i in range(len(tokens_index)):
        if tokens_index[i] != not_present:
            j = i + 1
            tokens_index_end = len(query)
            while j < len(tokens_index):
                if tokens_index[j] != not_present:
                    tokens_index_end = tokens_index[j]
                    break
                j = j + 1
            # print_details("tokens_index_end", tokens_index_end)
            # print_details("len(tokens[i][0])", len(tokens[i][0]))
            # print_details("(tokens_index[i] + len(tokens[i][0]))", (tokens_index[i] + len(tokens[i][0])))
            arguments = query[(tokens_index[i] + len(tokens_ordering[i])):tokens_index_end].strip()
            print_details("arguments", arguments)
            myAssert(arguments, MyError('ERROR: You have an error in SQL syntax'))
            tokens_map[tokens_ordering[i]][2] = tokens_map[tokens_ordering[i]][1](arguments)
    for command in tokens_execution:
        print_details("executing", command)
        database, database_metadata = tokens_map[command][0](database, database_metadata, tokens_map[command][2])
    if not database:
        database = [[]]
    # print("====================================================================")
    # print("Final Output")
    # print("====================================================================")
    # print_details("database")
    column_header = []
    for key, value in database_metadata["secondary_data"].items():
        for column_name in value[0]:
            column_header.append(key+'.'+column_name)
            # print(key+'.'+column_name, end=',')
    print(','.join(column_header))
    print('\n'.join(','.join(row) for row in database))
    # print(len(database), " rows")
    # print("====================================================================")

commands_map = [('select', select)]
commands_dict = {}
for index, command in enumerate(commands_map):
    commands_dict[command[0]] = index
print_details("commands_map", commands_map)
print_details("commands_dict", commands_dict)

if __name__ == "__main__":
    # raise MyError()
    for query in queries:
        verbose_mode = False
        table_to_alias.clear()
        alias_to_table.clear()
        query = query.lower()
        if '-v' in query:
            verbose_mode = True
            query = query.replace('-v', '')
        query = query.strip()
        print_details("query", query)
        command = query.split(" ")[0]
        print_details("command", command)
        if command not in commands_dict:
            try:
                raise MyError('ERROR: Unknown Command')
            except MyError as me:
                print(me.value)
        else:
            index = commands_dict[command]
            try:
                commands_map[index][1](query)
            except MyError as me:
                print(me.value)
            # pass
