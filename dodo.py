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

    """
    Install psycopg2, required to establish connection to the database
    :return: connection, cursor
    """

    def establish_connection(user, password):
        conn = psycopg2.connect(f'dbname=benchbase user={user} password={password}')
        cur = conn.cursor()
        return conn, cur

    """
    Drop two non-unique indexes
    """

    def drop_tpcc_indexes(cur):
        cur.execute("DROP INDEX IF EXISTS idx_customer_name")
        cur.execute("DROP INDEX IF EXISTS idx_order")

    """
    Add the indexes back
    """

    def add_tpcc_indexes(cur):
        cur.execute("CREATE INDEX idx_customer_name ON public.customer USING btree (c_w_id, c_d_id, c_last, c_first)")
        cur.execute("CREATE INDEX idx_order ON public.oorder USING btree (o_w_id, o_d_id, o_c_id, o_id)")

    """
    Close connection
    """

    def close_connection(conn, cur):
        cur.close()
        conn.close()

    """
    Get the number indexes
    """

    def get_index(cur):
        cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
        print(f'Number of indexes: {len(list(cur))}')

    """
    Meta method to test adding/dropping indexes
    """

    def test_step_one():
        conn, cur = establish_connection("TimLing", "TimLing")
        get_index(cur)
        drop_tpcc_indexes(cur)
        get_index(cur)
        add_tpcc_indexes(cur)
        get_index(cur)
        close_connection(conn, cur)
        return True

    def filter_csv(workload_csv, col=13):
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
        target_queries = all_queries[all_queries.str.contains(keyword, case=False)]

        return len(target_queries), target_queries

    def find_frequent_cols(queries):
        # query level counter keeps columns used in a single query as a separate entry
        counter_query_level = collections.Counter()
        # table level counter only cares about the columns used
        counter_table_level = collections.Counter()
        # record number of queries that could not be processed
        num_failed_queries = 0
        for q in queries:
            parsed_q = Parser(q)
            try:
                columns = parsed_q.columns_dict[K.WHERE]
                first_column = parsed_q.tables[0]
                columns = list(map(lambda x: x if "." in x else ".".join((first_column, x)), columns))
                columns.sort()
                counter_query_level["+".join(columns)] += 1
                for col in columns:
                    counter_table_level[col] += 1
            except Exception as e:
                num_failed_queries += 1
        return counter_table_level.most_common(), counter_query_level.most_common(), num_failed_queries

    def dump_workload_info(all_queries):
        num_queries = len(all_queries)
        num_queries_with_predicates, queries_with_predicate = filter_queries(all_queries, K.WHERE)
        num_delete, _ = filter_queries(all_queries, K.DELETE)
        num_update, _ = filter_queries(all_queries, K.UPDATE)
        num_insert, _ = filter_queries(all_queries, K.INSERT)
        num_select, _ = filter_queries(all_queries, K.SELECT)

        print("=" * 120)
        print("\tDumping workload information...\n")
        print("-" * 120)
        print("\t{:<80}{:>10}".format("Description", "metric"))
        print("-" * 120)
        print("\t{:<80}{:>10}".format("num queries", str(num_queries)))
        print("\t{:<80}{:10.3f}%".format("queries with predicate", num_queries_with_predicates / num_queries * 100))
        print("\t{:<80}{:10.3f}%".format("delete", num_delete / num_queries * 100))
        print("\t{:<80}{:10.3f}%".format("update", num_update / num_queries * 100))
        print("\t{:<80}{:10.3f}%".format("insert", num_insert / num_queries * 100))
        print("\t{:<80}{:10.3f}%".format("select", num_select / num_queries * 100))
        print("\n\n")

    def dump_predicate_info(counter_table_level, counter_query_level, num_failed_queries):
        print("=" * 120)
        print("\tDump predicate information...")
        if num_failed_queries == 0:
            print("\tAll queries processed successfully...")
        else:
            print("\t{} queries cannot be processed...".format(num_failed_queries))
        print("-" * 120)
        print("\ttable level count:\n\t{: <80}{: <10}".format("Index", "Count"))
        print("-" * 120)
        for index, count in counter_table_level:
            print("\t{: <80}{: <10}".format(index, str(count)))
        print("\n\n")
        print("-" * 120)
        print("\n\tquery level count:\n\t{0: <80}{1: <10}".format("Index", "Count"))
        print("-" * 120)
        for index, count in counter_query_level:
            print("\t{: <80}{: <10}".format(index, str(count)))
        print("\n\n")

    def test_step_two(workload_csv, verbose=True):
        all_queries = filter_csv(workload_csv)
        _, queries_with_predicate = filter_queries(all_queries, K.WHERE)
        counter_table_level, counter_query_level, num_failed_queries = find_frequent_cols(queries_with_predicate)
        if verbose:
            dump_workload_info(all_queries)
            dump_predicate_info(counter_table_level, counter_query_level, num_failed_queries)

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
