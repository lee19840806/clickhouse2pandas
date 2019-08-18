## clickhouse2pandas
Select **ClickHouse** data, convert **to pandas** dataframes and various other formats, by using the [ClickHouse HTTP interface](https://clickhouse.yandex/docs/en/interfaces/http/).

### Features
- The transmitting data is [compressed](https://clickhouse.yandex/docs/en/operations/settings/settings/#settings-enable_http_compression) by default, which reduces network traffic and thus reduces the time for downloading data.
- Converts the ClickHouse query result into proper pandas data types, e.g., [ClickHouse DateTime](https://clickhouse.yandex/docs/en/data_types/datetime/) -> pandas datetime64.
- Minimum dependencies, 3 standard python libraries (urllib, gzip, json) and 1 external library ([pandas](https://pandas.pydata.org/)).

### Installation
```
pip install clickhouse2pandas
```

### Usage
```python
import clickhouse2pandas as ch2pd

connection_url = 'http://user:password@clickhouse_host:8123'

query = """
select *
from system.numbers
limit 100
"""

df = ch2pd.select_data(connection_url, query)
# df is a pandas dataframe converted from ClickHouse query result
```

### API Reference
```python
clickhouse2pandas.select_data(connection_url, query = None, convert_to = 'DataFrame', settings = None)
```
Return a formatted query result specified by "convert_to" parameter.

**Parameters:**
- **connection_url**: the connection url to the ClickHouse HTTP interface, e.g., `http://user:password@clickhouse_host:8123`
- **query**: the SQL query
- **convert_to**: convert the query result into specific format, could be one of the following: 'DataFrame', 'TabSeparated', 'TabSeparatedRaw', 'TabSeparatedWithNames', 'TabSeparatedWithNamesAndTypes', 'CSV', 'CSVWithNames', 'Values', 'Vertical', 'JSON', 'JSONCompact', 'JSONEachRow', 'TSKV', 'Pretty', 'PrettyCompact', 'PrettyCompactMonoBlock', 'PrettyNoEscapes',  'PrettySpace', 'XML'. Refer to ClickHouse [Input and Output Formats](https://clickhouse.yandex/docs/en/interfaces/formats/)
- **settings**: a dict containing the setting key-values, default settings are {'enable_http_compression': 1, 'send_progress_in_http_headers': 0,'log_queries': 1, 'connect_timeout': 10, 'receive_timeout': 300, 'send_timeout': 300, 'output_format_json_quote_64bit_integers': 0, 'wait_end_of_query': 0}. Refer to ClickHouse [Settings](https://clickhouse.yandex/docs/en/operations/settings/settings/)
