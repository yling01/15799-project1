import constants as K


def task_project1_setup():
    import os

    def install_packages():
        os.system("pip3 install psycopg2")
        os.system("pip3 install pandas")
        os.system("pip3 install sql-metadata")

    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Faking action generation."',
            install_packages,

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

    def establish_connection(database, user, password):
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

    def drop_tpcc_indexes(cur):
        """
        Drop two TPCC indices
        @param cur: cursor from psycopg2
        @return: nothing
        """
        cur.execute("DROP INDEX IF EXISTS idx_customer_name")
        cur.execute("DROP INDEX IF EXISTS idx_order")

    def add_tpcc_indexes(cur):
        """
        Add the two indices back
        @param cur: cursor from psycopg2
        @return: nothing
        """
        cur.execute("CREATE INDEX idx_customer_name ON public.customer USING btree (c_w_id, c_d_id, c_last, c_first)")
        cur.execute("CREATE INDEX idx_order ON public.oorder USING btree (o_w_id, o_d_id, o_c_id, o_id)")

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
        """
        cur.execute("SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname='public' AND NOT indexdef LIKE '%UNIQUE%'")
        return list(cur)

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
        print("\t{:<80}{:>10}".format("Description", "metric"))
        print("-" * 120)
        print("\t{:<80}{:>10}".format("num queries", str(num_queries)))
        print("\t{:<80}{:10.3f}%".format("queries with predicate", num_queries_with_predicates / num_queries * 100))
        print("\t{:<80}{:10.3f}%".format("delete", num_delete / num_queries * 100))
        print("\t{:<80}{:10.3f}%".format("update", num_update / num_queries * 100))
        print("\t{:<80}{:10.3f}%".format("insert", num_insert / num_queries * 100))
        print("\t{:<80}{:10.3f}%".format("select", num_select / num_queries * 100))

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
        print("\t{: <80}{: <10}".format("Column", "Count"))
        print("-" * 120)
        for index, count in counter:
            print("\t{: <80}{: <10}".format(index, str(count)))

    def test_step_two(workload_csv, verbose=True):
        all_queries = filter_csv(workload_csv)
        _, queries_with_predicate = filter_queries(all_queries, K.WHERE)
        _, select_queries_with_predicate = filter_queries(queries_with_predicate, K.SELECT)
        _, update_queries_with_predicate = filter_queries(queries_with_predicate, K.UPDATE)

        select_counter_table_level, select_counter_query_level, _ = find_frequent_cols(select_queries_with_predicate)
        update_counter_table_level, update_counter_query_level, _ = find_frequent_cols(update_queries_with_predicate)
        _, update_queries = filter_queries(all_queries, K.UPDATE)
        update_target, _ = find_update_target(update_queries)

        if verbose:
            dump_workload_info(all_queries)
            print("\n")
            dump_predicate_info(select_counter_table_level, "Select queries simple index")
            print("\n")
            dump_predicate_info(select_counter_query_level, "Select queries composite index")
            print("\n")
            dump_predicate_info(update_counter_table_level, "Update queries simple index")
            print("\n")
            dump_predicate_info(update_counter_query_level, "Update queries composite index")
            print("\n")
            dump_predicate_info(update_target, "Update target")
            print("\n")

    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            # test_step_one,
            test_step_two,
            'echo "Faking action generation."',
            # 'echo "SELECT 1;" > actions.sql',
            # 'echo "SELECT 2;" >> actions.sql',
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
            }

        ],
        # Always rerun this task.
        "uptodate": [False],
        'verbosity': 2,
    }
