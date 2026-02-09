# data-sdk

## Integration w/ Jupyter

In your first block, you are required to install this repo via pip.
```
!pip install git+https://github.com/lawrence910426/data-sdk.git
```

Then, you may now import the classes and use the data.
```
from data_sdk import FinMindWrapper, ShioajiWrapper

shioaji_wrapper = ShioajiWrapper()
finmind_wrapper = FinMindWrapper()

# ...
```

## Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/lawrence910426/data-sdk.git
    cd data-sdk
    ```

2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3.  Install the package:
    ```bash
    pip install .
    ```

## Configuration

Set the following environment variables to configure the SDK:

### Parquet Storage Path
These variables govern where the parquet files will be stored and read from.
If not set, they default to the current working directory.

```bash
export DATA_SDK_FINMIND_BROKER_PATH="/mnt/nfs/backup/finmind_broker"
export DATA_SDK_SHIOAJI_TICKS_PATH="/mnt/nfs/backup/shioaji_ticks"
```

### API Keys
Required for fetching data from FinMind and Shioaji.

```bash
export FINMIND_API_TOKEN=your_token
export SHIOAJI_API_KEY=your_api_key
export SHIOAJI_SECRET_KEY=your_secret_key
```

## Usage

### Wrappers

```python
from data_sdk import FinMindWrapper, ShioajiWrapper

# Get FinMind broker data (downloads if missing)
finmind = FinMindWrapper()
df = finmind.get_broker("2024-01-02", "2330")

# Get Shioaji order book data (downloads if missing)
shioaji = ShioajiWrapper()
df_ticks = shioaji.get_order_book("2024-01-02", "2330")
```

### Crawlers

```python
from data_sdk.crawlers import get_intraday_lending_info

df_combined = get_intraday_lending_info("2024-01-02")
```

See `examples/example_crawler.py` for a complete example.
