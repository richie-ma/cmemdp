# cmemdp: Clean and Analyze Chicago Mercantile Exchange Market Data in Python
Author: Richie R. Ma

The Python package `cmemdp` is inspired by the author's R package `cme.mdp` published on April 2025. This package covers almost all features in that package and I also include other important functions into this package. The goal of this package is to make market data cleaning more easily and more user-friendly. Users can learn how modern financial markets work from the microstructure perspective.

## Introduction
Financial markets have become more transparent, and exchanges can provide high-frequency data for traders to better monitor markets, which creates more demand about the high-frequency data usage both in the academia and industry, either for real-time or historical. Most exchanges do not disseminate tabulated complete market data to non-member market participants, and almost all market data are specially coded to enhance the communication efficiency, such as various binary protocols (Simple Binary Enconding in the CME). Thus, financial economists need to know how to clean these non-tabular data at first, which is a substantially time-consuming task and might not be very user-friendly. This project will closely focus on how to parse and clean the market data of Chicago Mercantile Exchange (CME) under the FIX and binary (new feature!!) protocols and provide a faster limit order book reconstruction without explicit for loop statements for either outright, implied, or consolidated books.

## CME market data overview
So far, there have been Market by Price (MBP) data which aggregates all individual order information (e.g., size) at every price level, and Market by Order (MBO) data that can show all individual order details (e.g., order priority) at each price level. Both data formats are included in the raw PCAP data. The MBO data also provide more information about trade summaries than the MBP, so that traders are able to know which limit orders are matched in each trade and their corresponding matching quantities. The detailed trade summaries also assign the trade direction more precisely than the MBP and no quote merge is required for almost all trades. In general, CME will disseminate the MBP incremental updates followed by the order-level details (e.g., submission, cancellation) that describes the reason for MBP updates. This package considers the above characters and can process both the MBP and MBO data including quote messages and trade summaries.

## Installation
The package can be installed through the TestPyPI by typing the following code in your terminal.


```python
pip install -i https://test.pypi.org/simple/ cmemdp
```

Alternatively, this package can also be installed through the Github


```python
pip install git+https://github.com/richie-ma/cmemdp.git
```

## FIX data cleaning
This package almost mirrors the basic features that are published in the `cme.mdp` package in R. Users can refer to the R package documents.

## Binary PCAP data processing
CME Packet capture data is the raw dataset that captures all public data messages. CME stipulates a complete message template schema that uses different numbers to represent all messages. The raw messages are stored in the binary format which is not human readable and byte-wise. The message structure in the real PCAP data, e.g., data including technical header, packet header, payload, etc., is as follows:


```python
# Sequence | Time | Message Size | Block Length | Template ID | Schemal ID | Version | FIX header | FIX Message Body |
#    (Packet Header) |   (2 bytes)  |........(Simplie Binary Header, 8 bytes)..........|.......(FIX message)...........|
#    (12 bytes)      |..........................(message header)........................|
#                    |.................................... MDP messages.................................................|
```

The PCAP data from the CME Datamine is not the real PCAP data while it contains the main parts of the real pcap data. The message structure of the CME Datamine PCAP data is as follows:


```python
#Channel | Length|Sequence | Time | Message Size | Block Length | Template ID | Schemal ID | Version | FIX header | FIX Message Body |
#    2 bytes  2 bytes| (Packet Header)|   (2 bytes)  |........(Simplie Binary Header, 8 bytes).......... |.......(FIX message)...........|
#                    | (12 bytes)     |..........................(message header)........................|
#                                     |.................................... MDP messages.................................................|

```

This package can tickle the two scenarios. For the real PCAP data, one should use `cme_parser_pcap` function, which parses the raw data based on the standard PCAP format. For PCAP data from the CME Datamine, one should use `cme_parser_datamine`. Both functions are expected to give the same outputs.

### Example
Sample PCAP data can be obtained from the [CME Datamine](https://datamine.cmegroup.com/#/datasets/cme.pcap) Sales Team. An example from April 20, 2025. One can know the number of messages for each message. This function will return a dictionary of pandas DataFrames eventually, which is expected to be convenient for users to manipulate them easily. To reduce the load of outputs, this function can let users to specify what type of messages they need, and just type `msgs_teamplate = [46, 47]`, for example. Users can also specify how many packets are need to be processed, while the default is reading all packets within the data. To acquire the message sequences and sending timestamps, one can set `cme_header = True`, Currently, this function supports saving files in pickle or csv formats. The message template script can trace back to 2017 when the PCAP data was first publicized by the CME.


```python
from cmemdp.cme_parser import cme_parser_datamine

example = cme_parser_datamine(
    path="R:/_RawData/PCAP/20250420-PCAP_318_0_0_0_e",
    max_read_packets=None, msgs_template=None, cme_header=True,
    save_files=False, save_file_path=None, disable_progress_bar=False,
    save_file_type=None)

example[17]  ## showing the msgs_MDIncrementalRefreshBook46 as an example

```

    maximum number of packets read does not provide. Read the whole file by default
    Read total bytes: 1298714
    

    Reading:   0%|                                                       | 0/1298714 [00:00<?, ?bytes/s]

    Reading: 1324226bytes [00:00, 6025241.73bytes/s]                                                    
    

    +----+-----------------------------------------------------+---------------+
    |    | msg_types                                           |   number_msgs |
    +====+=====================================================+===============+
    |  0 | msgs_ChannelReset4                                  |             0 |
    +----+-----------------------------------------------------+---------------+
    |  1 | msgs_AdminLogout16                                  |             0 |
    +----+-----------------------------------------------------+---------------+
    |  2 | msgs_MDInstrumentDefinitionFuture27                 |             0 |
    +----+-----------------------------------------------------+---------------+
    |  3 | msgs_MDInstrumentDefinitionSpread29                 |             0 |
    +----+-----------------------------------------------------+---------------+
    |  4 | msgs_SecurityStatus30                               |           318 |
    +----+-----------------------------------------------------+---------------+
    |  5 | msgs_MDIncrementalRefreshBook32                     |             0 |
    +----+-----------------------------------------------------+---------------+
    |  6 | msgs_MDIncrementalRefreshDailyStatistics33          |             0 |
    +----+-----------------------------------------------------+---------------+
    |  7 | msgs_MDIncrementalRefreshLimitsBanding34            |             0 |
    +----+-----------------------------------------------------+---------------+
    |  8 | msgs_MDIncrementalRefreshSessionStatistics35        |             0 |
    +----+-----------------------------------------------------+---------------+
    |  9 | msgs_MDIncrementalRefreshTrade36                    |             0 |
    +----+-----------------------------------------------------+---------------+
    | 10 | msgs_MDIncrementalRefreshVolume37                   |             1 |
    +----+-----------------------------------------------------+---------------+
    | 11 | msgs_SnapshotFullRefresh38                          |             0 |
    +----+-----------------------------------------------------+---------------+
    | 12 | msgs_QuoteRequest39                                 |             0 |
    +----+-----------------------------------------------------+---------------+
    | 13 | msgs_MDInstrumentDefinitionOption41                 |             0 |
    +----+-----------------------------------------------------+---------------+
    | 14 | msgs_MDIncrementalRefreshTradeSummary42             |             0 |
    +----+-----------------------------------------------------+---------------+
    | 15 | msgs_MDIncrementalRefreshOrderBook43                |             0 |
    +----+-----------------------------------------------------+---------------+
    | 16 | msgs_SnapshotFullRefreshOrderBook44                 |             0 |
    +----+-----------------------------------------------------+---------------+
    | 17 | msgs_MDIncrementalRefreshBook46                     |           913 |
    +----+-----------------------------------------------------+---------------+
    | 18 | msgs_MDIncrementalRefreshOrderBook47                |           658 |
    +----+-----------------------------------------------------+---------------+
    | 19 | msgs_MDIncrementalRefreshTradeSummary48             |             1 |
    +----+-----------------------------------------------------+---------------+
    | 20 | msgs_MDIncrementalRefreshDailyStatistics49          |           707 |
    +----+-----------------------------------------------------+---------------+
    | 21 | msgs_MDIncrementalRefreshLimitsBanding50            |           658 |
    +----+-----------------------------------------------------+---------------+
    | 22 | msgs_MDIncrementalRefreshSessionStatistics51        |             0 |
    +----+-----------------------------------------------------+---------------+
    | 23 | msgs_SnapshotFullRefresh52                          |             0 |
    +----+-----------------------------------------------------+---------------+
    | 24 | msgs_SnapshotFullRefreshOrderBook53                 |             0 |
    +----+-----------------------------------------------------+---------------+
    | 25 | msgs_MDInstrumentDefinitionFuture54                 |           835 |
    +----+-----------------------------------------------------+---------------+
    | 26 | msgs_MDInstrumentDefinitionOption55                 |             0 |
    +----+-----------------------------------------------------+---------------+
    | 27 | msgs_MDInstrumentDefinitionSpread56                 |           693 |
    +----+-----------------------------------------------------+---------------+
    | 28 | msgs_MDInstrumentDefinitionFixedIncome57            |             0 |
    +----+-----------------------------------------------------+---------------+
    | 29 | msgs_MDInstrumentDefinitionRepo58                   |             0 |
    +----+-----------------------------------------------------+---------------+
    | 30 | msgs_SnapshotRefreshTopOrders59                     |             0 |
    +----+-----------------------------------------------------+---------------+
    | 31 | msgs_SecurityStatusWorkup60                         |             0 |
    +----+-----------------------------------------------------+---------------+
    | 32 | msgs_SnapshotFullRefreshTCP61                       |             0 |
    +----+-----------------------------------------------------+---------------+
    | 33 | msgs_CollateralMarketValue62                        |             0 |
    +----+-----------------------------------------------------+---------------+
    | 34 | msgs_MDInstrumentDefinitionFX63                     |             0 |
    +----+-----------------------------------------------------+---------------+
    | 35 | msgs_MDIncrementalRefreshBookLongQty64              |             0 |
    +----+-----------------------------------------------------+---------------+
    | 36 | msgs_MDIncrementalRefreshTradeSummaryLongQty65      |             0 |
    +----+-----------------------------------------------------+---------------+
    | 37 | msgs_MDIncrementalRefreshVolumeLongQty66            |             0 |
    +----+-----------------------------------------------------+---------------+
    | 38 | msgs_MDIncrementalRefreshSessionStatisticsLongQty67 |             0 |
    +----+-----------------------------------------------------+---------------+
    | 39 | msgs_SnapshotFullRefreshTCPLongQty68                |             0 |
    +----+-----------------------------------------------------+---------------+
    | 40 | msgs_SnapshotFullRefreshLongQty69                   |             0 |
    +----+-----------------------------------------------------+---------------+
     --> Dataframe: Success!
    
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>MsgSeq</th>
      <th>SendingTime</th>
      <th>TransactTime</th>
      <th>MatchEventIndicator</th>
      <th>MDEntryPx</th>
      <th>MDEntrySize</th>
      <th>SecurityID</th>
      <th>RptSeq</th>
      <th>NumberOfOrders</th>
      <th>MDPriceLevel</th>
      <th>MDUpdateAction</th>
      <th>MDEntryType</th>
      <th>TradeableSize</th>
      <th>OrderID</th>
      <th>MDOrderPriority</th>
      <th>MDDisplayQty</th>
      <th>ReferenceID</th>
      <th>OrderUpdateAction</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2121</td>
      <td>1745150405301571140</td>
      <td>1745150405255395633</td>
      <td>0b100</td>
      <td>2900000000000</td>
      <td>10</td>
      <td>179467</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>NaN</td>
      <td>6.862780e+12</td>
      <td>8.438052e+10</td>
      <td>10.0</td>
      <td>1.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2123</td>
      <td>1745150405303015194</td>
      <td>1745150405255395633</td>
      <td>0b100</td>
      <td>1010000000000</td>
      <td>1</td>
      <td>42018002</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>NaN</td>
      <td>6.862907e+12</td>
      <td>8.454003e+10</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2123</td>
      <td>1745150405303015194</td>
      <td>1745150405255395633</td>
      <td>0b100</td>
      <td>1005000000000</td>
      <td>1</td>
      <td>42018002</td>
      <td>2</td>
      <td>1</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>NaN</td>
      <td>6.862907e+12</td>
      <td>8.454003e+10</td>
      <td>1.0</td>
      <td>2.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2123</td>
      <td>1745150405303015194</td>
      <td>1745150405255395633</td>
      <td>0b100</td>
      <td>9990000000000</td>
      <td>1</td>
      <td>42018002</td>
      <td>3</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>NaN</td>
      <td>6.862907e+12</td>
      <td>8.454003e+10</td>
      <td>1.0</td>
      <td>3.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2123</td>
      <td>1745150405303015194</td>
      <td>1745150405255395633</td>
      <td>0b100</td>
      <td>9995000000000</td>
      <td>1</td>
      <td>42018002</td>
      <td>4</td>
      <td>1</td>
      <td>2</td>
      <td>0</td>
      <td>1</td>
      <td>NaN</td>
      <td>6.862907e+12</td>
      <td>8.454003e+10</td>
      <td>1.0</td>
      <td>4.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>513</th>
      <td>6357</td>
      <td>1745185200709909483</td>
      <td>1745185200709751423</td>
      <td>0b100</td>
      <td>189100000000000</td>
      <td>1</td>
      <td>42000774</td>
      <td>35</td>
      <td>1</td>
      <td>1</td>
      <td>2</td>
      <td>0</td>
      <td>NaN</td>
      <td>6.862973e+12</td>
      <td>8.462263e+10</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>514</th>
      <td>6358</td>
      <td>1745185200729591738</td>
      <td>1745185200729417705</td>
      <td>0b100</td>
      <td>1845000000000000</td>
      <td>4</td>
      <td>42009475</td>
      <td>29</td>
      <td>1</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>NaN</td>
      <td>6.862973e+12</td>
      <td>8.462288e+10</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>515</th>
      <td>6359</td>
      <td>1745185201738872715</td>
      <td>1745185201738587541</td>
      <td>0b100</td>
      <td>1842775000000000</td>
      <td>1</td>
      <td>42005804</td>
      <td>27</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>NaN</td>
      <td>6.862973e+12</td>
      <td>8.462289e+10</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>516</th>
      <td>6370</td>
      <td>1745185411633781899</td>
      <td>1745185411633388863</td>
      <td>0b100</td>
      <td>34640000000000</td>
      <td>1</td>
      <td>42191461</td>
      <td>15</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>NaN</td>
      <td>6.862973e+12</td>
      <td>8.462289e+10</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>517</th>
      <td>6372</td>
      <td>1745185444837503145</td>
      <td>1745185444836843513</td>
      <td>0b100</td>
      <td>34650000000000</td>
      <td>1</td>
      <td>42191461</td>
      <td>17</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>NaN</td>
      <td>6.862973e+12</td>
      <td>8.462289e+10</td>
      <td>1.0</td>
      <td>2.0</td>
      <td>1.0</td>
    </tr>
  </tbody>
</table>
<p>518 rows × 18 columns</p>
</div>



Users need to deal with the timestamp conversions and are encouraged to use `timestamp_conversion` function. Users still need to deal with the display format of the price in the `MDEntryPX` column. Basically, the numbers shown in the `MDEntryPX` column needs to times $10^{-9}$ and then times the price display format stipulated by the CME, which can be found in the security definition messages.

## Limit order book reconstruction
CME Market by Price messages incrementally show the depths in the limit order book thus this package reconstructs the limit order book based on MBP. To avoid the explicit for loop, the `order_book` function first groups the MBP messages based in their sides (i.e., bid or or offer) and then aggregate the quantity for each incremental update to get the snapshot in tick-by-tick level. Then, a re-assignment of depths is conducted to know for each price, what the depth it should be. This function uses the `numpy` propagation to speed the process. This function can also supports the consolidated limit order book where both outright quotes that are generated by traders and implied quotes generated by the CME implied functionality, while the only difference is the re-assignment of depths is based on the aggregated quantity of the two quote sources (i.e., outright and implied). Now this function is under the `FIX_input` module, but can also be used for the output from the binary data cleaning as long as the columns are set as what this package shows in the FIX (future version might bridge the two).

## Acknowledgements
I acknowledge the financial support from the [Bielfeldt Office for Futures and Options Research](https://ofor.illinois.edu/) at the University of Illinois at Urbana-Champaign. I also acknowledge prior practice from former OFOR members, including but not limited to Anabelle Couleau and Siyu Bian. Some codes are heavily inspired by their work. The OFOR has signed non-disclosure agreement with the CME and only sample data are used here for illustration purposes.

## Help, Feature Requests and Bug Reports
Please post an issue on the [GitHub repository](https://github.com/richie-ma/cmemdp).
