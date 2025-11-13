class snowflake_queries:

    def __init__(self, client):

        self.client = client

    def collect_data(self, path):
        sql_path = open(path,encoding='utf-8')
        query = sql_path.read()
        result = self.client.select(query).as_dataframe()
        if result.empty:
            print('Return is empty, please check query')
            print(query)
            return None
        result.columns = map(str.lower, result.columns)
        return result
    


class snowflake_queries_string:

    def __init__(self, client):

        self.client = client

    def collect_data(self, query):
        
        query = ' '.join(query.replace('\n', ' ').split())
        result = self.client.select(query).as_dataframe()
        if result.empty:
            print('Return is empty, please check query')
            print(query)
            return None
        result.columns = map(str.lower, result.columns)
        return result