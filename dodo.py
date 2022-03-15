import constants as K
import time
import os


def task_project1_setup():
    def install_packages():
        os.system("pip3 install psycopg2")
        os.system("pip3 install sql-metadata")

    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            install_packages,
        ],
        'params': [
            {
                'name': 'timeout',
                'long': 'timeout',
                'short': 't',
                'default': '1m'
            },
        ],
        # Always rerun this task.
        "uptodate": [False],
        'verbosity': 2,
    }


def task_project1():
    import collections
    import pandas as pd
    import psycopg2
    from sql_metadata import Parser
    import re

    def establish_connection(database="project1db", user="project1user", password="project1pass"):
        """
        Establish connection to the DB using psycopg2
        @param database: DB name
        @param user: username
        @param password: password
        @return:
            conn: connection
            cur: cursor
        """
        conn = psycopg2.connect(f'dbname={database} user={user} password={password}')
        cur = conn.cursor()
        return conn, cur

    def close_connection(conn, cur):
        """
        Close connection to DB
        @param conn: connection to DB
        @param cur: cursor from psycopg2
        @return: nothing
        """
        cur.close()
        conn.close()

    def get_unique_index(cur):
        """
        Retrieve unique indices from the DB
        @param cur: cursor from psycopg2
        @return: a list of current indices
        @note: a primary key is considered unique in postgres, therefore, primary keys are also ignored
        """
        existing_indices = []
        cur.execute(
            "SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname='public' AND NOT indexdef LIKE '%UNIQUE%'")
        for table, index, indexdef in cur:
            m = re.match(r'CREATE INDEX .+? ON .+? USING .+? \((?P<column>.+?)\)', indexdef)
            if m is None:
                print("\t\t\nERROR: PATTERN MATCH FAILED, EXISITING INDEX {} NOT PARSED!".format(index))
                continue
            """
            Assume the pattern match 'column' group contains 
                
                    column1, column2, column3
                    
            if the table has multi-column index.
            
            This also works if the table only has single-column index.
            """
            columns = m.group('column').split(",")
            table_dot_column_list = list(map(lambda x: ".".join((table, x.strip())), columns))
            table_dot_column_list.sort()
            existing_indices.append(("+".join(table_dot_column_list), index))
        return existing_indices

    def filter_csv(workload_csv, col=13):
        """
        Read query logs from the csv file and get clean sql queries as strings
        @param workload_csv: workload (csv) path
        @param col: query log column (tries every column if fails to retrieve queries from default column
        @return: pandas dataframe of clean sql queries as strings
        """

        # read workload csv using the default column only
        df = pd.read_csv(workload_csv, header=None, usecols=[col])
        all_queries = df[col]

        # default column does not contain queries, check all other columns
        if not all_queries.str.contains(K.STATEMENT).any():
            print("Default col {} does not contain queries, checking other columns".format(col))
            df = pd.read_csv(workload_csv, header=None)
            for c in range(len(df.colums)):
                content = df[c]
                if content.str.contains(K.STATEMENT).any():
                    all_queries = content
                    break
                # no column contains queries, raise KeyError
                raise KeyError

        all_queries = all_queries[~all_queries.str.contains(K.BEGIN, case=False)]
        all_queries = all_queries[~all_queries.str.contains(K.COMMIT, case=False)]

        # obtain pure sql queries
        clean_queries = all_queries.apply(lambda x: x.split(":")[1])

        return clean_queries

    def filter_queries(all_queries, keyword):
        """
        Filter queries by keyword
        @param all_queries: pandas dataframe of clean sql queries as strings
        @param keyword: keyword to be filtered against
        @return:
            number of target queries
            pandas dataframe of target queries
        """
        target_queries = all_queries[all_queries.str.contains(keyword, case=False)]
        return len(target_queries), target_queries

    def find_frequent_cols(queries):
        """
        Get the columns that are referenced in the WHERE predicate
        @param queries: pandas dataframe of sql queries as strings
        @return:
            counter dictionary of simple column reference (sorted descendingly)
            counter dictionary of composite column reference (sorted descendingly)
            number of failed queries
        """
        # keeps columns reference together as a separate entry
        counter_composite_columns = collections.Counter()
        # entries are always single column
        counter_simple_columns = collections.Counter()
        # record number of queries that could not be processed
        num_failed_queries = 0
        for q in queries:
            parsed_q = Parser(q)
            # sql_meta data sometimes fail to process strings containing quotation marks
            try:
                columns = parsed_q.columns_dict[K.WHERE]
                table = parsed_q.tables[0]
                columns = list(map(lambda x: x if "." in x else ".".join((table, x)), columns))
                columns.sort()
                counter_composite_columns["+".join(columns)] += 1
                for col in columns:
                    counter_simple_columns[col] += 1
            except Exception:
                num_failed_queries += 1
        return counter_simple_columns.most_common(), counter_composite_columns.most_common(), num_failed_queries

    def find_update_target(queries):
        """
        Get the columns where the updates take place
        @param queries: pandas dataframe of sql queries as strings
        @return:
            counter dictionary of the columns where updates take place
            number of failed queries
        """
        counter = collections.Counter()
        num_failed_queries = 0
        for q in queries:
            parsed_q = Parser(q)
            try:
                column = parsed_q.columns_dict[K.UPDATE][0]
                table = parsed_q.tables[0]
                column = table + "." + column
                counter[column] += 1
            except Exception:
                num_failed_queries += 1
        return counter.most_common(), num_failed_queries

    def dump_workload_info(all_queries):
        """
        Helper method to print formated workload information
        @param all_queries: pandas dataframe of all sql queries in a workload
        @return: nothing
        """
        num_queries = len(all_queries)
        num_queries_with_predicates, queries_with_predicate = filter_queries(all_queries, K.WHERE)
        num_delete, _ = filter_queries(all_queries, K.DELETE)
        num_update, _ = filter_queries(all_queries, K.UPDATE)
        num_insert, _ = filter_queries(all_queries, K.INSERT)
        num_select, _ = filter_queries(all_queries, K.SELECT)

        print("=" * 120)
        print("\n")
        print("{: >50}".format("Dump workload information..."))
        print("\n")
        print("-" * 120)
        print("\t{:<80}{:<10}".format("Description", "metric"))
        print("-" * 120)
        print("\t{:<80}{:<10}".format("num queries", str(num_queries)))
        print("\t{:<80}{:<10.3f}%".format("queries with predicate", num_queries_with_predicates / num_queries * 100))
        print("\t{:<80}{:<10.3f}%".format("delete", num_delete / num_queries * 100))
        print("\t{:<80}{:<10.3f}%".format("update", num_update / num_queries * 100))
        print("\t{:<80}{:<10.3f}%".format("insert", num_insert / num_queries * 100))
        print("\t{:<80}{:<10.3f}%".format("select", num_select / num_queries * 100))
        print("-" * 120)

    def dump_predicate_info(counter, description):
        """
        Helper method to print formated counter information
        @param counter: counter dictionary (k: column name, v: number of occurrence)
        @param description: description of the counter dictionary
        @return: nothing
        """
        print("=" * 120)
        print("\n")
        print("{: >50}".format("Dump column information..."))
        print("{: >50}".format(description))
        print("\n")
        print("-" * 120)
        print("\t{:<80}{:<10}".format("Column", "Count"))
        print("-" * 120)
        for index, count in counter:
            print("\t{:<80}{:<10}".format(index, str(count)))
        print("-" * 120)

    def print_statements(statements, description):
        """
        Helper method to print out build index build_statements
        @param statements: a list of (type, sql statement) tuples, both are of string type
        @param description: description of the build_statements
        @return: nothing
        """
        print("=" * 120)
        print("\n")
        print("{: >50}".format("Dump statements..."))
        print("{: >50}".format(description))
        print("\n")
        print("-" * 120)
        print("\t{:<30}{:<100}".format("Type", "Statement"))
        print("-" * 120)
        for type, statement in statements:
            print("\t{:<30}{:<100}".format(type, statement))
        print("-" * 120)

    def generate_drop_index_statements(candidate_indices):
        """
        Generate drop index build_statements from a list of candidate indices.
        Candidate indices should be the index name from the current database.
        @param candidate_indices: a list of current indices
        @return: a list of drop build_statements
        """
        statements = []
        for index in candidate_indices:
            statements.append("DROP INDEX IF EXISTS {}".format(index))
        return statements

    def generate_build_index_statements(candidate_indices):
        """
        Generate build index build_statements from a list of candidate indices.
        Candidate indices should be in the following format:

            Single-column index:
                table_name.column_name

            Multi-column index (note that table_name should be the same):
                table_name.column_name1+table_name.column_name2

        @param candidate_indices: a list of candidate indices
        @return: a list of sql build_statements as strings
        """
        statements = []
        for candidate_index in candidate_indices:
            simple_index_list = candidate_index.split("+")
            table_referenced = None
            columns_referenced = []
            # iterate over each column inside of a multi-column index
            for simple_index in simple_index_list:
                table, column = simple_index.split(".")

                # cannot build a multi-column index that spans across different tables
                if table_referenced != table and table_referenced is not None:
                    print("\t\t\nERROR: TRYING TO BUILD MULTI-COLUMN INDEX ACROSS DIFFERENT TABLES!")
                    print("\t\tSELECTING ARBITRARILY!\n")
                    break
                table_referenced = table
                columns_referenced.append(column)

            # table has to be specified
            if table_referenced is None:
                print("\t\t\nERROR: TABLE IS NOT SPECIFIED, INDEX IS NOT BUILT!\n")
            else:
                index_name = "_".join(columns_referenced)
                index_name = "_".join(("idx", table_referenced, index_name))
                command = "CREATE INDEX IF NOT EXISTS {} ON {} USING btree ({})".format(index_name, table_referenced,
                                                                                        ", ".join(columns_referenced))
                statements.append(("Simple" if len(columns_referenced) == 1 else "Composite", command))
        return statements

    def generate_actions(workload_csv, verbose):
        if verbose:
            start_time = time.time()

        all_queries = filter_csv(workload_csv)
        num_queries = len(all_queries)

        _, queries_with_predicate = filter_queries(all_queries, K.WHERE)

        counter_simple, counter_composite, _ = find_frequent_cols(queries_with_predicate)
        candidate_indices_to_percent_usage = {}
        simple_to_composite_index = collections.defaultdict(set)

        """
        Iterate over all referenced composite columns (can be simple) in the predicates.
        Add the multi-column index to the candidate_indices_to_percent_usage along with the percent usage 
        if the composite columns are referenced more than the threshold (K.REFERENCE_CUTOFF_LOW).
        Add the record to simple_to_composite_index for all simple column index in the multi-column index.
        """
        for composite_index, occurance in counter_composite:
            percent_usage = occurance / num_queries
            if percent_usage >= K.REFERENCE_CUTOFF_LOW:
                candidate_indices_to_percent_usage[composite_index] = percent_usage
                for simple_index in composite_index.split("+"):
                    simple_to_composite_index[simple_index].add(composite_index)
            else:
                break

        """
        Iterate over all referenced simple columns in the predicates.
        Add the simple index to the candidate_indices_to_percent_usage along with the percent usage 
        if the simple columns are referenced more than the threshold (K.SIMPLE_REFERENCE_CUT_OFF_HIGH) 
        and if the simple column has not been added as part of the multi-column index.
        Add the record to simple_to_composite_index.
        """
        for simple_index, occurance in counter_simple:
            percent_usage = occurance / num_queries
            if percent_usage >= K.SIMPLE_REFERENCE_CUT_OFF_HIGH:
                if simple_index not in candidate_indices_to_percent_usage:
                    candidate_indices_to_percent_usage[simple_index] = percent_usage
                    simple_to_composite_index[simple_index].add(simple_index)
            else:
                break

        if verbose:
            print("=" * 120)
            print("\n")
            print("{:<50}".format("Following indices are recommended before analyzing update queries"))
            print("\n")
            print("-" * 120)
            for candidate_indices in candidate_indices_to_percent_usage:
                print("{:<50}".format(candidate_indices))
            print("-" * 120)

        _, update_queries = filter_queries(all_queries, K.UPDATE)
        update_target, _ = find_update_target(update_queries)

        """
        Iterate over all update columns.
        Remove all multi-column (single column included) indexes 
        if one of the single column update happens more than the threshold (K.UPDATE_CUTOFF)
        """
        for update_column, occurance in update_target:
            percent_usage = occurance / num_queries
            if percent_usage >= K.UPDATE_CUTOFF:
                if update_column in simple_to_composite_index:
                    for composite_index in simple_to_composite_index[update_column]:
                        if candidate_indices_to_percent_usage[composite_index] <= K.COMPOSITE_REFERENCE_CUTOFF_HIGH:
                            del candidate_indices_to_percent_usage[composite_index]
                            simple_to_composite_index[update_column].remove(composite_index)

        conn, cur = establish_connection()
        current_indices = get_unique_index(cur)
        indices_to_remove = []

        """
        Iterate over all current indices.
        If current index is not part of the multi-column index, remove it.
        Note that if the current index is not one of the recommended indices but made up of one of the 
        recommended indices, we keep it. 
        """
        for table_dot_column, index in current_indices:
            # if the index is multi-column, check using candidate_indices_to_percent_usage
            if "+" in table_dot_column:
                if table_dot_column not in candidate_indices_to_percent_usage:
                    indices_to_remove.append(index)
            # if the index is single-column, check using simple_to_composite_index
            else:
                if table_dot_column not in simple_to_composite_index or len(
                        simple_to_composite_index[table_dot_column]) == 0:
                    indices_to_remove.append(index)

        drop_statements = generate_drop_index_statements(indices_to_remove)

        if verbose:
            print("=" * 120)
            print("\n")
            print("{:<50}".format("Following indices are recommended after analyzing update queries"))
            print("\n")
            print("-" * 120)
            for candidate_indices in candidate_indices_to_percent_usage:
                print("{:<50}".format(candidate_indices))
            print("-" * 120)

        build_statements = generate_build_index_statements(list(candidate_indices_to_percent_usage.keys()))

        close_connection(conn, cur)

        if verbose:
            dump_workload_info(all_queries)
            print("\n")
            dump_predicate_info(counter_simple, "Select/Update simple candidate indices")
            print("\n")
            dump_predicate_info(counter_composite, "Select/Update composite candidate indices")
            print("\n")
            dump_predicate_info(update_target, "Update target")
            print("\n")
            print_statements(build_statements, "build index statements")
            print("\n")
            print_statements(list(map(lambda x: ("DROP", x), drop_statements)), "drop index statements")
            print("\n")
            print("\t\t--- Program exists properly, total time spent: %s seconds ---" % (time.time() - start_time))

        with open("actions.sql", "w+") as f:
            for _, s in build_statements:
                f.write(s)
                f.write(";")
                f.write("\n")

            for s in drop_statements:
                f.write(s)
                f.write(";")
                f.write("\n")

    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            # test_step_one,
            generate_actions,
            'echo \'{"VACUUM": false}\' > config.json',
        ],
        'params': [
            {
                'name': 'workload_csv',
                'long': 'workload_csv',
                'short': 'w',
                'default': 'epinions.csv'
            },

            {
                'name': 'timeout',
                'long': 'timeout',
                'short': 't',
                'default': '1m'
            },

            {
                'name': 'verbose',
                'long': 'verbose',
                'short': 'v',
                'default': False
            }

        ],
        # Always rerun this task.
        "uptodate": [False],
        'verbosity': 2,
    }
