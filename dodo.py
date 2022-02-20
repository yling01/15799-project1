import collections
import os
import pandas as pd

def task_project1():
    """
    Install psycopg2, required to establish connection to the database
    :return: connection, cursor
    """
    def install_psycopg2(user, password):
        os.system("pip install psycopg2")
        import psycopg2
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
        conn, cur = install_psycopg2("TimLing", "TimLing")
        get_index(cur)
        drop_tpcc_indexes(cur)
        get_index(cur)
        add_tpcc_indexes(cur)
        get_index(cur)
        close_connection(conn, cur)
        return True

    def filter_csv(workload_csv, query_col_title="ending log output to stderr"):
        df = pd.read_csv(workload_csv)
        all_queries = df[query_col_title]
        clean_queries = all_queries[all_queries.str.contains("where", case=False)]
        clean_queries = clean_queries[~clean_queries.str.contains("when", case=False)]
        clean_queries = clean_queries[~clean_queries.str.contains("pg_", case=False)]
        clean_queries = clean_queries.apply(lambda x: x.split(":")[1])
        return clean_queries.values.tolist()

    def find_frequent_cols(queries):
        os.system("pip install sql-metadata")
        from sql_metadata import Parser
        counter = collections.Counter()
        for q in queries:
            parsed_q = Parser(q)
            for col in parsed_q.columns_dict["where"]:
                if "." not in col:
                    col = parsed_q.tables[0] + "." + col
                counter[col] += 1
        return counter

    def test_step_two():
        queries = filter_csv("postgresql-2022-02-15_172350.csv")
        counter = find_frequent_cols(queries)
        print(counter)

    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            # test_step_one,
            test_step_two,
            'echo "Faking action generation."',
            'echo "SELECT 1;" > actions.sql',
            'echo "SELECT 2;" >> actions.sql',
            'echo \'{"VACUUM": true}\' > config.json',
        ],
        # Always rerun this task.
        "uptodate": [False],
        'verbosity': 2,
    }