# Group 03
- Pham Vo Quynh Nhu - 21125052
- Ho Viet Bao Long - 21125071
- Nguyen Phuc Bao Uyen - 21125146
## Abstract
This study introduces a rule-based trading strategy for VN30F1M, using technical indicators to define precise entry and exit points for long and short positions. Long entries rely on short-term EMAs crossing above long-term EMAs, high RSI, and strong volume, while short entries occur under opposite conditions. Exits are governed by strict rules, including stop-loss limits, EMA cross reversals, RSI shifts, and an ATR-based trailing stop. The strategy aims to enhance trade consistency and risk management in a Vietnam derivatives market.
## Introduction
- Briefly introduce the project.
- Problem statement, research question or the hypothesis.
- Method(s) to solve the problem
- What are the results?

## Background

### EMA (Exponential Moving Average)
The Exponential Moving Average (EMA) is a weighted moving average that gives more weight to recent prices. It is calculated as:

$$
EMA_t = EMA_{t-1} + \alpha \times (P_t - EMA_{t-1})
$$

Where:
- $ EMA_t $ is the EMA at time $ t $,
- $ EMA_{t-1} $ is the previous EMA value,
- $ P_t $ is the current price at time $ t $,
- $ \alpha $ is the smoothing factor, defined as $ \alpha = \frac{2}{N+1} $,
- $ N $ is the number of periods.


### RSI (Relative Strength Index)
RSI measures momentum by comparing recent gains to recent losses, with values between 0 and 100. It is calculated as:

$$
RSI = 100 - \left( \frac{100}{1 + RS} \right)
$$

Where:
- $RS$ is the average gain of up periods divided by the average loss of down periods.

### ATR (Average True Range)
The Average True Range (ATR) measures market volatility. The formula is:

$$
ATR = \frac{1}{N} \sum_{i=1}^{N} \text{True Range}_i
$$

Where:
- $N$ is the number of periods,
- $\text{True Range}_i$ is the maximum of:
  - Current high - current low,
  - Absolute value of current high - previous close,
  - Absolute value of current low - previous close.



## Trading (Algorithm) Hypotheses
The trading strategy focuses on trend-following and momentum signals to determine entry points. A position is only opened when sufficient historical data is available and all specified conditions are met. For a LONG position: a short-term EMA above the long-term EMA, price above the short EMA, RSI above a set upper threshold, and strong volume. A SHORT position is triggered under the opposite conditions, indicating a bearish trend with high trading activity. Only one position is held at a time, ensuring clear directional exposure.

### Preconditions
- The starting balance is 40,000,000 VND

- The portfolio holds only one position at a time

- Only one action is allowed per trade: either Long or Short

### Hypotheses
Generally, the decision-making process of the algorithm is driven by the following parameters:

| Parameter            | Description                                                                 |
|----------------------|-----------------------------------------------------------------------------|
| `EMA_SHORT`          | Period for calculating the short-term Exponential Moving Average (EMA).     |
| `EMA_LONG`           | Period for calculating the long-term Exponential Moving Average (EMA).      |
| `RSI_PERIOD`         | Lookback period used to compute the Relative Strength Index (RSI).          |
| `RSI_LOWER_THRESHOLD`| RSI value below which a long position may be considered (oversold zone).    |
| `RSI_UPPER_THRESHOLD`| RSI value above which a short position may be considered (overbought zone). |
| `ATR_PERIOD`         | Lookback period used to calculate the Average True Range (ATR).             |
| `ATR_MULTIPLIER`     | Factor used to scale the ATR for setting trailing stop-loss levels.         |
| `MIN_PROFIT`         | Minimum required profit to trigger a profit-taking condition.               |
| `MAX_LOSS`           | Maximum allowed loss before triggering a stop-loss.                         |
| `VOLUME_THRESHOLD`   | Multiplier applied to average volume to detect significant volume spikes.   |
| `VOLUME_WINDOW`      | Number of bars used to compute rolling average volume.                      |
| `RSI_EXIT_THRESHOLD` | RSI value used to help decide when to exit an active trade.                 |

**There are two main components to the hypothesis: Criteria for opening a position and Criteria for closing a position.**

### Opening a Position
- **No position is opened if either:**
    - There is insufficient historical data (e.g., not enough bars to calculate RSI or EMA), or
    - None of the entry conditions are satisfied.
- **Open a** `LONG` **position when all of the following conditions are met:** 
    - EMA_SHORT is greater than EMA_LONG (indicating an uptrend),
    - CURRENT_PRICE is above EMA_SHORT,
    - RSI exceeds the RSI_UPPER_THRESHOLD,
    - CURRENT_VOLUME is greater than VOLUME_THRESHOLD $\times$ AVERAGE_VOLUME (indicating strong market interest).
- **Open a** `SHORT` **position when all of the following conditions are met:**
    - EMA_SHORT is less than EMA_LONG (indicating a downtrend).
    - CURRENT_PRICE is below EMA_SHORT.
    - RSI falls below the RSI_LOWER_THRESHOLD.
    - CURRENT_VOLUME is greater than VOLUME_THRESHOLD $\times$ AVERAGE_VOLUME.

### Close position:

- **No position is closed if none of the exit conditions are triggered.**
- **Close a** `LONG` **position when any of the following conditions apply**
    - The current loss exceeds the MAX_LOSS threshold.
    - CURRENT_PRICE drops below EMA_LONG.
    - RSI falls below the RSI_EXIT_THRESHOLD,
    - Trailing Stop Condition: If profit exceeds MIN_PROFIT, and CURRENT_PRICE < max(ENTRY_PRICE + MIN_PROFIT,PREVIOUS_CLOSE - ATR × ATR_MULTIPLIER).
- **Close** `SHORT` **position when any of the following conditions apply:**
    - The current loss exceeds the MAX_LOSS threshold.
    - CURRENT_PRICE rises above EMA_LONG,
    - Trailing Stop Condition: If profit exceeds MIN_PROFIT, and CURRENT_PRICE > min(ENTRY_PRICE - MIN_PROFIT, PREVIOUS_CLOSE + ATR × ATR_MULTIPLIER)

## Data
- Data source: Algotrade internship database
- Data type: VN30F1M
- Data period: 2023- present
- Both input and output data will be stored in a specified data path. The default data path is **data folder** in the **src** directory of the project. You can change the data path by changing the DATA_PATH variable in the .env file.
### Data collection
- The data is collected from Algotrade internship database
### Data Processing
- The queried data is extracted and saved as CSV files, with the results stored in the <DATA_PATH> directory.

- The data is then processed into tick data based on a preferred interval (more infomation in the [Implementation Data Collection and Preprocessing](#data-collection-and-preprocessing)). In this project, we focus on a 5-minute interval.

The resulting file will be saved under the name: `<start_date>_to_<end_date>_by_<tick_interval>.csv`
For example:
```
src/data/2023-01-01_to_2023-12-31_by_5T.csv
```
The CSV file will include the following columns:
- ``Date``: The date of the interval
- ``Time``: The time of the interval in format (YY-MM-DD)
- ``tickersymbol``: That month ticketsymbol
- ``Open``: Opening price
- ``Close``: Closing price
- ``High``: Highest price during the interval
- ``Low``: Lowest price during the interval
- ``Volume``: Trading volume

## Implementation
### Environment Setup
1. Set up Python Virtual Environment
```
python3 -m venv venv
python -m venv venv
source venv/bin/activate # for Linux/MacOS
.\venv\Scripts\activate.bat # for Windows command line
.\venv\Scripts\Activate.ps1 # for Windows PowerShell
```
2. Install the required packages
```
pip install -r requirements.txt
```
3. (OPTIONAL) Create `.env` file in the directory `src` of the project and fill in the required information. The `.env` file is used to store environment variables that are used in the project. The following is an example of a `.env` file:
```
DB_NAME=<database name>
DB_USER=<database user name>
DB_PASSWORD=<database password>
DB_HOST=<host name or IP address>
DB_PORT=<database port>
DATA_PATH=<path to the data folder of your choice, if not specified, the default is `data`>
```
The DATA_PATH variable is used to specify the path to the data folder where the input data is stored. The other variables are used to connect to the database.

### Data Collection and Preprocessing
**Option 1. Using Available Data Files**
All available data files are located in the `src/data/` directory.

**Option 2. Run code to collect data**
This command runs the `src/preprocess.py` script to extract and preprocess market data between specified dates with a defined resampling interval.
- `--start_date`: The beginning date of the data range (format: YYYY-MM-DD).

- `--end_date`: The ending date of the data range (format: YYYY-MM-DD).

- `--interval`: The time interval for resampling the data using pandas (default: 5T, which means 5-minute intervals).
```
python3 src/preprocess.py --start_date 2023-01-01 --end_date 2023-12-31 --interval 5T
```
This example processes data from January 1, 2023, to December 31, 2023, and resamples it into 5-minute intervals. The process may take approximately 2-3 minutes to complete.

The resulting file will be saved in the `<DATA_PATH>` directory (default `src/data/`) under the name: `<start_date>_to_<end_date>_by_<tick_interval>.csv`
The CSV file will include the following columns:
```
,Date,Time,tickersymbol,Open,Close,High,Low,Volume
1,2023-01-03,09:00:00,VN30F2301,1000.7,1002.0,1002.6,1000.7,186
```

## In-sample Backtesting
In the `src/parameters.csv` file, key hyperparameters for our hypothesis are set to default values for in-sample backtesting. These include settings for indicators such as EMA, RSI, ATR, and volume thresholds, as well as risk management parameters like stop-loss and take-profit levels.

| EMA_Short | EMA_Long | RSI_Period | RSI_lower | RSI_upper | ATR_period | Max_Loss | Min_Profit | ATR_Mult | Volume_Threshold | Volume_window | RSI_exit_threshold |
|-----------|----------|------------|-----------|-----------|-------------|----------|-------------|-----------|-------------------|----------------|---------------------|
| 10        | 30       | 14         | 45        | 55        | 14          | 3.0      | 0.5         | 1.5       | 1.2               | 10             | 50                  |
 
For backtesting, we use data from the period 2023-01-01 to 2023-12-01 with a 5T interval as the in-sample period. By default, the data file is saved in `src/data` if you do not define DATAPATH in `src/.env`.
- `--dataset`: specify the dataset file to be used
- `--result_dir`: specify the directory for saving charts, trade logs, and portfolio values
- `--parameters`: specify the directory for parameters.
```
python src/backtesting.py --dataset 2023-01-01_to_2023-12-31_by_5T.csv --result_dir result_in_sample --parameters src/parameters_in_sample.csv
python src/evaluation.py --input_csv result_in_sample/portfolio_values_10.0_30.0_14.csv --output_file result_in_sample/plot_10_30_14.png 

```
### In-sample Backtesting Result
After running the command below, we obtained the Sharpe Ratio and Maximum Drawdown:
```
SHARPE RATIO: 1.0846
MDD: -16.49%
```
Here is the in-sample backtesting result:
![Diagram](/result_in_sample/plot_10_30_14.png)

![Diagram](/figures/all_backtests_in_sample.png)

## Optimization

To identify a well-performing set of parameters for our hypothesis, we applied a random sampling approach. Initially, we generated the full set of possible parameter combinations using Python's `itertools.product`, based on predefined ranges. From this comprehensive list, we employed `NumPy` to randomly sample a subset for evaluation. Each sampled configuration was tested through backtesting, and its performance was assessed using the **Sharpe Ratio** and **Maximum Drawdown (MDD)** metrics. The results were then visualized using Python's `Matplotlib` library to facilitate analysis.

In total, twelve parameters were considered in the optimization process: ema_short, ema_long, rsi_range, rsi_lower_threshold, rsi_upper_threshold, atr_period, max_loss, min_profit, atr_multiplier, volume_threshold, volume_window, and rsi_exit_threshold. The range of possible values for each parameter is specified in the `optimization.json` file, with the following defaults:

```json
{
  "ema_short_range": [5, 8, 10, 12, 15],
  "ema_long_range": [20, 25, 30, 35, 40],
  "rsi_range": [10, 14, 18],
  "rsi_lower_threshold_range": [25, 30, 35, 40, 45],
  "rsi_upper_threshold_range": [50, 55, 60, 65, 70],
  "atr_period_range": [10, 14, 21],
  "max_loss_range": [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0],
  "min_profit_range": [0.5, 1.0, 1.5, 2.0],
  "atr_mult_range": [1.0, 1.5, 2.0],
  "vol_thresh_range": [0.8, 1.0, 1.2, 1.4],
  "volume_window_range": [5, 10, 15, 20],
  "rsi_exit_threshold_range": [45, 50, 55]
}
```

To run optimization, run the following command:
- `-d`: Specify the dataset file to be used
- `-o`: Location to save the optimization results
```
python3 src/optimization.py -d src/data/2023-01-01_to_2023-12-31_by_5T.csv -o src/OPTIMIZATION_RANDOM
```

To draw the graphs to illustrate the results of optimization process, run the following script:
- `-v`: Path to summary.csv file created by the optimization process.
```
python3 src/optimization.py -v src/OPTIMIZATION_RANDOM/summary.csv
```

The performance of each model was evaluated primarily using the Sharpe Ratio, followed by Maximum Drawdown (MDD). We first filtered out any model with a Sharpe Ratio below 0.7 or an MDD lower than -30. After this filtering step, we sorted the remaining models by their Sharpe Ratio and selected the top five with the highest values.

### Optimization Result
The optimization process took approximately 4.5 hours in total, during which 2,500 parameter combinations were evaluated.

The figure below illustrates the relationship between the Sharpe Ratio and MDD for the 2,500 parameter combinations considered. The highest Sharpe Ratio observed was 2.782, while the lowest was -0.4861. In contrast, the MDD ranged from a maximum of -0.0556 to a minimum of -0.3143.

![Diagram](/figures/SharpeVSMDD.png)

## Out-of-sample Backtesting
After optimization, we identified the top 5 parameter sets with the highest Sharpe ratios. The results are saved in `/OPTIMIZATION_RANDOM/parameters.csv`, and the top 5 are listed below.
| SampleIndex | EMA_Short | EMA_Long | RSI_Period | RSI_lower | RSI_upper | ATR_period | Max_Loss | Min_Profit | ATR_Mult | Volume_Threshold | Volume_window | RSI_exit_threshold | Sharpe        | mdd             | net_profit |
|-------------|-----------|----------|------------|-----------|-----------|-------------|----------|-------------|-----------|-------------------|----------------|---------------------|----------------|------------------|-------------|
| 271         | 10        | 25       | 18         | 25        | 70        | 14          | 2.5      | 1           | 1         | 1.2               | 20             | 45                  | 2.782016973    | -0.07720320466   | 29702000    |
| 1634        | 8         | 40       | 14         | 35        | 70        | 21          | 5        | 1           | 1         | 0.8               | 10             | 45                  | 2.717278496    | -0.07548636353   | 41320000    |
| 974         | 5         | 20       | 18         | 35        | 70        | 14          | 4.5      | 1           | 1         | 0.8               | 15             | 50                  | 2.674389187    | -0.06388517122   | 36240000    |
| 2500        | 15        | 25       | 18         | 25        | 70        | 14          | 1.5      | 1           | 1         | 0.8               | 20             | 50                  | 2.594883786    | -0.07613530181   | 29810000    |
| 2358        | 10        | 30       | 18         | 25        | 70        | 21          | 2        | 0.5         | 1         | 0.8               | 15             | 55                  | 2.573981968    | -0.07130522918   | 30525000    |
 
For out-of-sample testing, we use data from the period 2024-01-01 to 2025-01-01 with a 5T interval as the in-sample period. By default, the data file is saved in `src/data` if you do not define DATAPATH in `src/.env`.

```
python src/backtesting.py --dataset 2024-01-01_to_2025-01-01_by_5T.csv --result_dir result_out_sample --parameters src/parameters_out_sample.csv
```

After running the command below, we obtained the Sharpe Ratio and Maximum Drawdown:

| Sample Index | Sharpe Ratio | Maximum Drawdown (MDD) |
|------------|--------------|-------------------------|
| 271    | 0.5741       | -11.77%                 |
| 1634    | -0.0494      | -25.98%                 |
| 974    | 0.3945       | -17.39%                 |
| 2500    | 1.6497       | -8.00%                  |
| 2358   | **1.4416**       | -9.66%                  |

![Diagram](/figures/all_backtests_out_sample.png)

The highest parameter for the outsample is 
| SampleIndex | EMA_Short | EMA_Long | RSI_Period | RSI_lower | RSI_upper | ATR_period | Max_Loss | Min_Profit | ATR_Mult | Volume_Threshold | Volume_window | RSI_exit_threshold | Sharpe        | mdd             | net_profit |
|-------------|-----------|----------|------------|-----------|-----------|-------------|----------|-------------|-----------|-------------------|----------------|---------------------|----------------|------------------|-------------|
| 2500        | 15        | 25       | 18         | 25        | 70        | 14          | 1.5      | 1           | 1         | 0.8               | 20             | 50                  | 2.594883786    | -0.07613530181   | 29810000    |

## Paper Trading
All the code in paper trading is in the paper_trading folder

 ```
 cd src/paper_trading
 python paper_trading.py
 ```
We have configured the paper trading environment using incoming messages from Kafka in combination with historical data retrieved from the database. However, the current backtesting implementation does not support dynamic parameter adjustment and is operating with a fixed set of parameters.

| EMA_FAST | EMA_SLOW | RSI_PERIOD | ATR_PERIOD | VOL_WINDOW | VOL_THRES | RSI_UPPER | RSI_LOWER | MAX_LOSS | MIN_PROFIT | ATR_MULT | RSI_EXIT |
|----------|----------|------------|------------|-------------|------------|------------|------------|-----------|-------------|-----------|-----------|
| 10       | 30       | 18         | 21         | 15          | 0.8        | 70         | 25         | 2.0       | 0.5         | 1.0       | 55        |

The message received from Kafka is preprocessed to align with the format of the above data.


## Conclusion
This trading strategy utilizes a combination of trend and momentum indicators—specifically EMA, RSI, and volume—to identify high-probability entry points in the market. The project offers a basic view of the algorithmic trading workflow. However, the final results can vary significantly depending on the dataset, so no definitive conclusions can be drawn at this stage. In the future, we aim to further optimize the process to uncover more concrete evidence that supports our hypotheses.

## Reference
- [Investopedia: Exponential Moving Average (EMA)](https://www.investopedia.com/terms/e/ema.asp)  
- [AlgoTrade Hub: Thực tiễn giao dịch thuật toán tại thị trường chứng khoán Việt Nam](https://hub.algotrade.vn/knowledge-hub/thuc-tien-giao-dich-thuat-toan-tai-thi-truong-chung-khoan-viet-nam/)

