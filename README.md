# clickhouse2pandas
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
