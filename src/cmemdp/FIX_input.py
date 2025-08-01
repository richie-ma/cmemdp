# -*- coding: utf-8 -*-
"""
CME data processing for CME Makret by Price (MBP) and Market by Order (MBO) data
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pandas import isnull


def meta_data(sunday_input_path, date):
    """
    Getting the meta_data from the Sunday input file

    Parameters
    ----------
    sunday_input_path : str
        The path of the Sunday input.
    date : string
        date in "YYYY-MM-DD" format.

    Returns
    -------
    meta_data : pandas DataFrame
        DataFrame that contains necessary meta data.

    """

    if not isinstance(date, datetime):
        date = datetime.strptime(date, "%Y-%m-%d")

    def main(data, date, security=None):

        if date < datetime.strptime("2015-11-20", "%Y-%m-%d"):

            if not isnull(security):

                data = data[
                    data[data.str.contains(rf"\x01107={security}(?![\s-])")].index
                ]
                data = data[data[data.str.contains(r"\x0135=d")].index]
                data.replace(to_replace=r"\x01", value=",", regex=True, inplace=True)

            else:
                data = data[data[data.str.contains(r"\x0135=d")].index]
                data.replace(to_replace=r"\x01", value=",", regex=True, inplace=True)

            data.replace(
                to_replace=r"1128=([^,]*),9=([^,]*),35=([^,]*),49=([^,]*),34=([^,]*),52=([^,]*),",
                value="",
                regex=True,
                inplace=True,
            )
            data.replace(
                to_replace=r",22=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",461=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",731=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1150=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",864=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(to_replace=r"865=([^,]*)", value=",", regex=True, inplace=True)
            data.replace(to_replace=r"866=([^,]*)", value=",", regex=True, inplace=True)
            data.replace(
                to_replace=r"1145=([^,]*)", value=",", regex=True, inplace=True
            )
            data.replace(to_replace=r"870=([^,]*)", value=",", regex=True, inplace=True)
            data.replace(to_replace=r"871=([^,]*)", value=",", regex=True, inplace=True)
            data.replace(to_replace=r"872=([^,]*)", value=",", regex=True, inplace=True)
            data.replace(
                to_replace=r",1141=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1146=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1180=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1300=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",5796=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",9850=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",15=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",48=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",202=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",462=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",827=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",947=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1143=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1144=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1147=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1151=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",55=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1148=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1149=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",555=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r"600=([^,]*),602=([^,]*),603=([^,]*),623=([^,]*),624=([^,]*),",
                value=",",
                regex=True,
                inplace=True,
            )
            data.replace(
                to_replace=r",762=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(to_replace=r"10=([^,]*),", value=",", regex=True, inplace=True)
            data.replace(to_replace=r",+", value=",", regex=True, inplace=True)

            if len(data) != 0:

                if True in data.str.contains(r"1022=GBI,").unique():

                    data1 = data[data[data.str.contains(r"1022=GBI,")].index]

                    data1 = data1.str.extractall(
                        r"15=([^,]*),107=([^,]*),200=([^,]*),207=([^,]*),562=([^,]*),969=([^,]*),996=([^,]*),1140=([^,]*),1022=([^,]*),264=([^,]*),1022=([^,]*),264=([^,]*),1142=([^,]*),9787=([^,]*),"
                    )

                    data1.columns = [
                        "Currency",
                        "Symbol",
                        "MaturityMonthYear",
                        "SecurityExchange",
                        "MinTradeVol",
                        "MinPriceIncrement",
                        "UnitOfMeasure",
                        "MaxTradeVol",
                        "MDFeedType",
                        "MarketDepth",
                        "MDFeedTypeimplied",
                        "MarketDepthimplied",
                        "MatchAlgorithm",
                        "DisplayFactor",
                    ]

                else:

                    data1 = data[data[data.str.contains(r"1022=GBX,")].index]

                    data1 = data1.str.extractall(
                        r"15=([^,]*),107=([^,]*),200=([^,]*),207=([^,]*),562=([^,]*),969=([^,]*),996=([^,]*),1140=([^,]*),1022=([^,]*),264=([^,]*),1022=([^,]*),9787=([^,]*),"
                    )

                    data1.columns = [
                        "Currency",
                        "Symbol",
                        "MaturityMonthYear",
                        "SecurityExchange",
                        "MinTradeVol",
                        "MinPriceIncrement",
                        "UnitOfMeasure",
                        "MaxTradeVol",
                        "MDFeedType",
                        "MarketDepth",
                        "MatchAlgorithm",
                        "DisplayFactor",
                    ]

            meta_data = data1

        else:

            if not isnull(security):

                data = data[
                    data[data.str.contains(rf"\x0155={security}(?![\s-])")].index
                ]
                data = data[data[data.str.contains(r"\x0135=d")].index]
                data.replace(to_replace=r"\x01", value=",", regex=True, inplace=True)

            else:
                data = data[data[data.str.contains(r"\x0135=d")].index]
                data.replace(to_replace=r"\x01", value=",", regex=True, inplace=True)

            data.replace(
                to_replace=r"1128=([^,]*),9=([^,]*),35=([^,]*),49=([^,]*),",
                value="",
                regex=True,
                inplace=True,
            )
            data.replace(
                to_replace=r",34=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",201=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",52=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",5799=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",980=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",779=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1180=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1300=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",48=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",22=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1151=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",6937=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",461=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",9779=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",462=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",37702=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",9800=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1141=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",864=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r"865=([^,]*),1145=([^,]*)",
                value=",",
                regex=True,
                inplace=True,
            )
            data.replace(
                to_replace=r",870=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",871=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",872=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",731=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",10=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r"602=([^,]*),603=([^,]*),624=([^,]*),623=([^,]*)",
                value=",",
                regex=True,
                inplace=True,
            )
            data.replace(
                to_replace=r",555=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",947=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",9850=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",711=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",311=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",309=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1146=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",305=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1150=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1147=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1149=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1148=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1143=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",762=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",202=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1234=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(to_replace=r",+", value=",", regex=True, inplace=True)

            if len(data) != 0:

                if True in data.str.contains(r"1022=GBI,").unique():

                    data1 = data[data[data.str.contains(r"1022=GBI,")].index]

                    data1 = data1.str.extractall(
                        r"75=([^,]*),55=([^,]*),200=([^,]*),167=([^,]*),207=([^,]*),15=([^,]*),1142=([^,]*),562=([^,]*),1140=([^,]*),969=([^,]*),9787=([^,]*),1022=([^,]*),264=([^,]*),1022=([^,]*),264=([^,]*),996=([^,]*),"
                    )

                    data1.columns = [
                        "Date",
                        "Symbol",
                        "MaturityMonthYear",
                        "SecurityType",
                        "SecurityExchange",
                        "Currency",
                        "MatchAlgorithm",
                        "MinTradeVol",
                        "MaxTradeVol",
                        "MinPriceIncrement",
                        "DisplayFactor",
                        "MDFeedType",
                        "MarketDepth",
                        "MDFeedTypeImplied",
                        "MarketDepthImplied",
                        "UnitOfMeasure",
                    ]
                else:

                    data1 = data[data[data.str.contains(r"1022=GBX,")].index]

                    data1 = data1.str.extractall(
                        r"75=([^,]*),55=([^,]*),200=([^,]*),167=([^,]*),207=([^,]*),15=([^,]*),1142=([^,]*),562=([^,]*),1140=([^,]*),969=([^,]*),9787=([^,]*),1022=([^,]*),264=([^,]*),996=([^,]*)"
                    )

                    data1.columns = [
                        "Date",
                        "Symbol",
                        "MaturityMonthYear",
                        "SecurityType",
                        "SecurityExchange",
                        "Currency",
                        "MatchAlgorithm",
                        "MinTradeVol",
                        "MaxTradeVol",
                        "MinPriceIncrement",
                        "DisplayFactor",
                        "MDFeedType",
                        "MarketDepth",
                        "UnitOfMeasure",
                    ]

            meta_data = data1

        return meta_data

    data = pd.read_csv(sunday_input_path, header=None)[0]
    meta_data = main(data, date=date)

    print("CME MDP 3.0 Securitity Definition (Meta data)")

    return meta_data


class mbp_input_fix:
    def __init__(self, path):
        self.path = path

    def trade_summary(self, date, price_displayformat=None, sunday_input_path=None):
        """
        Extracting trade summary messages.

        Parameters
        ----------
        date : string
            date in "YYYY-MM-DD" format.
        price_displayformat : float, optional
            how the price should be displayed. The default is None.
        sunday_input_path : str, optional
            If the price_displayformat is not given, this function will extract
            it from the Sunday's input. The default is None.


        Returns
        -------
        Trades : pandas DataFrame
            DataFrame that includes the trade summary.

        """

        data = pd.read_csv(self.path, header=None)[0]

        if not isinstance(date, datetime):
            date = datetime.strptime(date, "%Y-%m-%d")

        if date < datetime.strptime("2015-11-20", "%Y-%m-%d"):

            Trades = data[data[data.str.contains(r"\x01269=2")].index]
            Trades.reset_index(drop=True, inplace=True)
            Trades.replace(
                to_replace=r"\x01", value=",", regex=True, inplace=True
            )  # delete \x01
            Trades.replace(to_replace=r"269=2,", value="", regex=True, inplace=True)

            # further delete some unnecessary tags
            Trades.replace(
                to_replace=r"1128=([^,]*),9=([^,]*),35=([^,]*),49=([^,]*),",
                value="",
                regex=True,
                inplace=True,
            )
            Trades.replace(
                to_replace=r",5799=([^,]*),", value=",", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r",268=([^,]*),", value=",", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r",279=([^,]*),", value=",", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r",48=([^,]*),", value=",", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r",273=([^,]*),", value=",", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r",274=([^,]*),", value=",", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r",451=([^,]*),", value=",", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r",1020=([^,]*),", value=",", regex=True, inplace=True
            )

            if len(Trades) != 0:

                trade = Trades.str.extractall(
                    r"83=([^,]*),107=([^,]*),270=([^,]*),271=([^,]*),1003=([^,]*),5797=([^,]*)"
                )
                # Should use str.extractall() instead of extract(),
                # mutiple matches in a row will be assigned to a new row
                # with one record at each row
                # could be similar as R stringr::str_match_all()

                trade.columns = ["Seq", "Code", "PX", "Size", "TrdID", "agg"]
                trade.index.names = ["record", "match"]

                trade_info = Trades.str.extractall(r"34=([^,]*),52=([^,]*),75=([^,]*)")

                trade_info.columns = ["MsgSeq", "SendingTime", "Date"]

                trade_info.index.names = ["record", "match"]

                # Though the number of rows is not the same, we could merge
                # based on the index

                Trades1 = trade_info.merge(trade, how="inner", on="record").reset_index(
                    drop=True
                )

                Trades1["Seq"] = Trades1["Seq"].astype(int)
                Trades1["MsgSeq"] = Trades1["MsgSeq"].astype(int)
                Trades1["PX"] = Trades1["PX"].astype(float)
                Trades1["Size"] = Trades1["Size"].astype(int)
                Trades1["TrdID"] = Trades1["TrdID"].astype(int)
                Trades1["agg"] = Trades1["agg"].astype(int)

                # second part
                trade2 = Trades.str.extractall(
                    r"83=([^,]*),107=([^,]*),270=([^,]*),271=([^,]*),1003=([^,]*),"
                )
                # Should use str.extractall() instead of extract(),
                # mutiple matches in a row will be assigned to a new row
                # with one record at each row
                # could be similar as R stringr::str_match_all()

                trade2.columns = ["Seq", "Code", "PX", "Size", "TrdID"]
                trade2.index.names = ["record", "match"]

                trade2_info = Trades.str.extractall(r"34=([^,]*),52=([^,]*),75=([^,]*)")

                trade2_info.columns = ["MsgSeq", "SendingTime", "Date"]

                trade2_info.index.names = ["record", "match"]

                # Though the number of rows is not the same, we could merge
                # based on the index

                Trades2 = trade2_info.merge(
                    trade2, how="inner", on="record"
                ).reset_index(drop=True)

                Trades2["Seq"] = Trades2["Seq"].astype(int)
                Trades2["MsgSeq"] = Trades2["MsgSeq"].astype(int)
                Trades2["PX"] = Trades2["PX"].astype(float)
                Trades2["Size"] = Trades2["Size"].astype(int)
                Trades2["TrdID"] = Trades2["TrdID"].astype(int)

                Trades1_1 = Trades1.drop(columns="agg")

                trades = pd.concat([Trades1_1, Trades2], axis=0, ignore_index=True)
                trades.drop_duplicates(inplace=True)

                trades = trades.merge(
                    Trades1[["Seq", "Code", "agg"]], on=["Seq", "Code"], how="outer"
                ).reset_index(drop=True)

                trades.sort_values(["Code", "Seq"], inplace=True)

                trade3 = Trades.str.extractall(
                    r"83=([^,]*),107=([^,]*),270=([^,]*),271=([^,]*),277=([^,]*),1003=([^,]*),5797=([^,]*)"
                )
                trade3.columns = ["Seq", "Code", "PX", "Size", "TrdCon", "TrdID", "agg"]
                trade3.index.names = ["record", "match"]

                trade3_info = Trades.str.extractall(r"34=([^,]*),52=([^,]*),75=([^,]*)")

                trade3_info.columns = ["MsgSeq", "SendingTime", "Date"]

                trade3_info.index.names = ["record", "match"]

                # Though the number of rows is not the same, we could merge
                # based on the index

                Trades3 = trade3_info.merge(
                    trade3, how="inner", on="record"
                ).reset_index(drop=True)

                Trades3["Seq"] = Trades3["Seq"].astype(int)
                Trades3["MsgSeq"] = Trades3["MsgSeq"].astype(int)
                Trades3["PX"] = Trades3["PX"].astype(float)
                Trades3["Size"] = Trades3["Size"].astype(int)
                Trades3["TrdID"] = Trades3["TrdID"].astype(int)
                Trades3["agg"] = Trades3["agg"].astype(int)

                # special trades

                trade4 = Trades.str.extractall(
                    r"83=([^,]*),107=([^,]*),270=([^,]*),271=([^,]*),277=([^,]*),1003=([^,]*),"
                )
                trade4.columns = ["Seq", "Code", "PX", "Size", "TrdCon", "TrdID"]
                trade4.index.names = ["record", "match"]

                trade4_info = Trades.str.extractall(r"34=([^,]*),52=([^,]*),75=([^,]*)")

                trade4_info.columns = ["MsgSeq", "SendingTime", "Date"]

                trade4_info.index.names = ["record", "match"]

                # Though the number of rows is not the same, we could merge
                # based on the index

                Trades4 = trade4_info.merge(
                    trade4, how="inner", on="record"
                ).reset_index(drop=True)

                Trades4["Seq"] = Trades4["Seq"].astype(int)
                Trades4["MsgSeq"] = Trades4["MsgSeq"].astype(int)
                Trades4["PX"] = Trades4["PX"].astype(float)
                Trades4["Size"] = Trades4["Size"].astype(int)
                Trades4["TrdID"] = Trades4["TrdID"].astype(int)

                Trades3_1 = Trades3.drop(columns="agg")
                Trades3_4_all = pd.concat(
                    [Trades3_1, Trades4], axis=0, ignore_index=True
                )
                Trades3_4_all.drop_duplicates(inplace=True)
                Trades3_4_all = Trades3_4_all.merge(
                    Trades3[["Code", "Seq", "agg"]], on=["Code", "Seq"], how="outer"
                )

                Trades3_4_all.sort_values(["Code", "Seq"], inplace=True)

                Trades = pd.concat([trades, Trades3_4_all], ignore_index=True)

            else:
                raise Exception("No trade summary detected")
        else:
            Trades = data[data[data.str.contains(r"\x01269=2")].index]
            Trades.reset_index(drop=True, inplace=True)
            Trades.replace(
                to_replace=r"\x01", value=",", regex=True, inplace=True
            )  # delete \x01
            Trades.replace(to_replace=r"269=2,", value="", regex=True, inplace=True)

            # further delete some unnecessary tags
            Trades.replace(
                to_replace=r"1128=([^,]*),9=([^,]*),35=([^,]*),49=([^,]*),",
                value="",
                regex=True,
                inplace=True,
            )
            Trades.replace(
                to_replace=r",5799=([^,]*),", value=",", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r",268=([^,]*),", value=",", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r",279=([^,]*),", value=",", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r",48=([^,]*),", value=",", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r",37705=([^,]*),", value=",", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r"37=([^,]*),", value="", regex=True, inplace=True
            )
            Trades.replace(
                to_replace=r"32=([^,]*),", value="", regex=True, inplace=True
            )

            if len(Trades) != 0:

                trade = Trades.str.extractall(
                    r"55=([^,]*),83=([^,]*),270=([^,]*),271=([^,]*),346=([^,]*),5797=([^,]*)"
                )
                # Should use str.extractall() instead of extract(),
                # mutiple matches in a row will be assigned to a new row
                # with one record at each row
                # could be similar as R stringr::str_match_all()

                trade.columns = ["Code", "Seq", "PX", "Size", "Ord", "agg"]
                trade.index.names = ["record", "match"]

                trade_info = Trades.str.extractall(
                    r"75=([^,]*),34=([^,]*),52=([^,]*),60=([^,]*)"
                )

                trade_info.columns = ["Date", "MsgSeq", "SendingTime", "TransactTime"]

                trade_info.index.names = ["record", "match"]

                # Though the number of rows is not the same, we could merge
                # based on the index

                Trades = trade_info.merge(trade, how="outer", on="record").reset_index(
                    drop=True
                )

                Trades["Seq"] = Trades["Seq"].astype(int)
                Trades["MsgSeq"] = Trades["MsgSeq"].astype(int)
                Trades["PX"] = Trades["PX"].astype(float)
                Trades["Size"] = Trades["Size"].astype(int)
                Trades["Ord"] = Trades["Ord"].astype(int)
                Trades["agg"] = Trades["agg"].astype(int)

            else:
                raise Exception("No trade summary detected")

        codes = Trades["Code"].unique()

        if isnull(price_displayformat):

            if isnull(sunday_input_path):

                raise Exception(
                    "Sunday's security definition at the same week must be provided to get the price display format"
                )

            definition = meta_data(sunday_input_path, date=date)
            definition.rename(columns={"Symbol": "Code"}, inplace=True)

            definition = definition.loc[definition["Code"].isin(codes)]
            definition["DisplayFactor"] = definition["DisplayFactor"].astype("float")

            Trades = (
                Trades.merge(
                    definition[["Code", "DisplayFactor"]], how="left", on="Code"
                )
                .assign(PX=lambda x: x["PX"] * x["DisplayFactor"])
                .drop(columns=["DisplayFactor"])
            )

        else:

            Trades["PX"] = Trades["PX"] * price_displayformat

        Trades = [
            Trades.loc[Trades["Code"] == code] for code in Trades["Code"].unique()
        ]

        print(f"CME MDP 3.0 Trade Summary \n contracts: {codes}")

        return Trades

    def quote_messages(self, date, price_displayformat=None, sunday_input_path=None):
        """
        Extracting the quote messages

        Parameters
        ----------
        date : string
            date in "YYYY-MM-DD" format.
        price_displayformat : float, optional
            how the price should be displayed. The default is None.
        sunday_input_path : string, optional
            If the price_displayformat is not given, this function will extract
            it from the Sunday's input. The default is None.


        Returns
        -------
        Quotes : pandas DataFrame
            DataFrame that includes the quote messages.

        """

        data = pd.read_csv(self.path, header=None)[0]

        if not isinstance(date, datetime):
            date = datetime.strptime(date, "%Y-%m-%d")

        if date < datetime.strptime("2015-11-20", "%Y-%m-%d"):

            Quotes = data[data[data.str.contains(r"\x01269=[01]|\x01276=[RK]")].index]
            # incremental updates
            Quotes = Quotes[Quotes[Quotes.str.contains(r"\x0135=X")].index]
            Quotes.reset_index(drop=True, inplace=True)
            Quotes.replace(
                to_replace=r"\x01", value=",", regex=True, inplace=True
            )  # delete \x01

            if len(Quotes) != 0:

                # further delete some unnecessary tags
                Quotes.replace(
                    to_replace=r"1128=([^,]*),9=([^,]*),35=([^,]*),49=([^,]*),",
                    value="",
                    regex=True,
                    inplace=True,
                )
                Quotes.replace(
                    to_replace=r",268=([^,]*),", value=",", regex=True, inplace=True
                )
                Quotes.replace(
                    to_replace=r",22=([^,]*),", value=",", regex=True, inplace=True
                )
                Quotes.replace(
                    to_replace=r",48=([^,]*),", value=",", regex=True, inplace=True
                )
                Quotes.replace(
                    to_replace=r",273=([^,]*),", value=",", regex=True, inplace=True
                )
                Quotes.replace(
                    to_replace=r",336=([^,]*),", value=",", regex=True, inplace=True
                )
                Quotes.replace(
                    to_replace=r",10=([^,]*),", value=",", regex=True, inplace=True
                )
                Quotes.replace(
                    to_replace=r",5797=([^,]*),", value=",", regex=True, inplace=True
                )

                # --- processing outright quotes

                if True in Quotes.str.contains(r"269=[01]").unique():

                    outright = Quotes.str.extractall(
                        r"279=([012]),83=([^,]*),107=([^,]*),269=([01]*),270=([^,]*),271=([^,]*),346=([^,]*),1023=([^,]*),"
                    )

                    outright.columns = [
                        "Update",
                        "Seq",
                        "Code",
                        "Side",
                        "PX",
                        "Qty",
                        "Ord",
                        "PX_depth",
                    ]
                    outright.index.names = ["record", "match"]

                    outright_info = Quotes.str.extractall(
                        r"34=([^,]*),52=([^,]*),75=([^,]*)"
                    )

                    outright_info.columns = ["MsgSeq", "SendingTime", "Date"]

                    outright_info.index.names = ["record", "match"]

                    Quotes_outright = outright_info.merge(
                        outright, how="inner", on="record"
                    ).reset_index(drop=True)

                    Quotes_outright["Update"] = Quotes_outright["Update"].astype(int)
                    Quotes_outright["Seq"] = Quotes_outright["Seq"].astype(int)
                    Quotes_outright["MsgSeq"] = Quotes_outright["MsgSeq"].astype(int)
                    Quotes_outright["PX"] = Quotes_outright["PX"].astype(float)
                    Quotes_outright["Qty"] = Quotes_outright["Qty"].astype(int)
                    Quotes_outright["Ord"] = Quotes_outright["Ord"].astype(int)
                    Quotes_outright["PX_depth"] = Quotes_outright["PX_depth"].astype(
                        int
                    )

                    Quotes_outright["Implied"] = "N"

                # -- processing implied quotes

                if True in Quotes.str.contains(r"276=K").unique():

                    # Implied_Quotes = Quotes[Quotes[Quotes.str.contains(
                    #   r'276=K')].index]
                    implied = Quotes.str.extractall(
                        r"279=([^,]*),83=([^,]*),107=([^,]*),269=([^,]*),270=([^,]*),271=([^,]*),276=([^,]*),1023=([^,]*),"
                    )

                    implied.columns = [
                        "Update",
                        "Seq",
                        "Code",
                        "Side",
                        "PX",
                        "Qty",
                        "Implied",
                        "PX_depth",
                    ]
                    implied.index.names = ["record", "match"]

                    implied_info = Quotes.str.extractall(
                        r"34=([^,]*),52=([^,]*),75=([^,]*)"
                    )

                    implied_info.columns = ["MsgSeq", "SendingTime", "Date"]

                    implied_info.index.names = ["record", "match"]

                    Quotes_implied = implied_info.merge(
                        implied, how="inner", on="record"
                    ).reset_index(drop=True)

                    Quotes_implied["Update"] = Quotes_implied["Update"].astype(int)
                    Quotes_implied["Seq"] = Quotes_implied["Seq"].astype(int)
                    Quotes_implied["MsgSeq"] = Quotes_implied["MsgSeq"].astype(int)
                    Quotes_implied["PX"] = Quotes_implied["PX"].astype(float)
                    Quotes_implied["Qty"] = Quotes_implied["Qty"].astype(int)
                    Quotes_implied["PX_depth"] = Quotes_implied["PX_depth"].astype(int)

                    Quotes_implied["Implied"] = "Y"

                    Quotes = pd.concat(
                        [Quotes_outright, Quotes_implied], axis=0, ignore_index=True
                    )

                    Quotes.sort_values(["Code", "Seq"], inplace=True)

                else:
                    Quotes = Quotes_outright

            else:
                raise Exception("No quote information detected")
        else:
            Quotes = data[data[data.str.contains(r"\x01269=[01EF]")].index]

            if len(Quotes) != 0:
                # incremental updates
                Quotes.replace(
                    to_replace=r"\x01", value=",", regex=True, inplace=True
                )  # delete \x01

                # further delete some unnecessary tags
                Quotes.replace(
                    to_replace=r"1128=([^,]*),9=([^,]*),35=([^,]*),49=([^,]*),",
                    value="",
                    regex=True,
                    inplace=True,
                )
                Quotes.replace(
                    to_replace=r",5799=([^,]*),", value=",", regex=True, inplace=True
                )
                Quotes.replace(
                    to_replace=r",268=([^,]*),", value=",", regex=True, inplace=True
                )
                Quotes.replace(
                    to_replace=r",48=([^,]*),", value=",", regex=True, inplace=True
                )

                # --- processing outright quotes

                if True in Quotes.str.contains(r"269=[01]").unique():

                    outright = Quotes.str.extractall(
                        r"279=([012]),269=([01]),55=([^,]*),83=([^,]*),270=([^,]*),271=([^,]*),346=([^,]*),1023=([^,]*),"
                    )

                    outright.columns = [
                        "Update",
                        "Side",
                        "Code",
                        "Seq",
                        "PX",
                        "Qty",
                        "Ord",
                        "PX_depth",
                    ]
                    outright.index.names = ["record", "match"]

                    outright_info = Quotes.str.extractall(
                        r"75=([^,]*),34=([^,]*),52=([^,]*),60=([^,]*)"
                    )

                    outright_info.columns = [
                        "Date",
                        "MsgSeq",
                        "SendingTime",
                        "TransactTime",
                    ]

                    outright_info.index.names = ["record", "match"]

                    Quotes_outright = outright_info.merge(
                        outright, how="inner", on="record"
                    ).reset_index(drop=True)

                    Quotes_outright["Update"] = Quotes_outright["Update"].astype(int)
                    Quotes_outright["Seq"] = Quotes_outright["Seq"].astype(int)
                    Quotes_outright["MsgSeq"] = Quotes_outright["MsgSeq"].astype(int)
                    Quotes_outright["PX"] = Quotes_outright["PX"].astype(float)
                    Quotes_outright["Qty"] = Quotes_outright["Qty"].astype(int)
                    Quotes_outright["Ord"] = Quotes_outright["Ord"].astype(int)
                    Quotes_outright["PX_depth"] = Quotes_outright["PX_depth"].astype(
                        int
                    )

                    Quotes_outright["Implied"] = "N"

                # -- processing implied quotes

                if True in Quotes.str.contains(r"269=[EF]").unique():

                    implied = Quotes.str.extractall(
                        r"279=([012]),269=([EF]),55=([^,]*),83=([^,]*),270=([^,]*),271=([^,]*),1023=([^,]*),"
                    )

                    implied.columns = [
                        "Update",
                        "Side",
                        "Code",
                        "Seq",
                        "PX",
                        "Qty",
                        "PX_depth",
                    ]
                    implied.index.names = ["record", "match"]

                    implied_info = Quotes.str.extractall(
                        r"75=([^,]*),34=([^,]*),52=([^,]*),60=([^,]*)"
                    )

                    implied_info.columns = [
                        "Date",
                        "MsgSeq",
                        "SendingTime",
                        "TransactTime",
                    ]

                    implied_info.index.names = ["record", "match"]

                    Quotes_implied = implied_info.merge(
                        implied, how="inner", on="record"
                    ).reset_index(drop=True)

                    Quotes_implied["Update"] = Quotes_implied["Update"].astype(int)
                    Quotes_implied["Seq"] = Quotes_implied["Seq"].astype(int)
                    Quotes_implied["MsgSeq"] = Quotes_implied["MsgSeq"].astype(int)
                    Quotes_implied["PX"] = Quotes_implied["PX"].astype(float)
                    Quotes_implied["Qty"] = Quotes_implied["Qty"].astype(int)
                    Quotes_implied["PX_depth"] = Quotes_implied["PX_depth"].astype(int)

                    Quotes_implied["Implied"] = "Y"

                    Quotes = pd.concat(
                        [Quotes_outright, Quotes_implied], axis=0, ignore_index=True
                    )

                    Quotes.sort_values(["Code", "Seq"], inplace=True)

                else:
                    Quotes = Quotes_outright
            else:
                raise Exception("No quote information detected")

        codes = Quotes["Code"].unique()

        if isnull(price_displayformat):

            if isnull(sunday_input_path):

                raise Exception(
                    "Sunday's security definition at the same week must be provided to get the price display format"
                )

            definition = meta_data(sunday_input_path, date=date)
            definition.rename(columns={"Symbol": "Code"}, inplace=True)

            definition = definition.loc[definition["Code"].isin(codes)]
            definition["DisplayFactor"] = definition["DisplayFactor"].astype("float")

            Quotes = (
                Quotes.merge(
                    definition[["Code", "DisplayFactor"]], how="left", on="Code"
                )
                .assign(PX=lambda x: x["PX"] * x["DisplayFactor"])
                .drop(columns=["DisplayFactor"])
            )

        else:

            Quotes["PX"] = Quotes["PX"] * price_displayformat

        Quotes = [
            Quotes.loc[Quotes["Code"] == code] for code in Quotes["Code"].unique()
        ]

        print(f"CME MDP 3.0 Quote Messages \n contracts: {codes}")

        return Quotes

    def statistics(self, date, security=None):
        """
        Extracting statistics.

        Parameters
        ----------
        date : data
            date in "YYYY-MM-DD" format.
        security : string, optional
            A specific security. The default is None.

        Returns
        -------
        Pandas DataFrame
            DataFrame that includes the quote messages.

        """

        date = datetime.strptime(date, "%Y-%m-%d")

        def stat_main(data, date, security=None):

            if date < datetime.strptime("2015-11-20", "%Y-%m-%d"):

                data.replace(to_replace=r"\x01", value=",", regex=True, inplace=True)

                data.replace(
                    to_replace=r"1128=([^,]*),9=([^,]*),",
                    value="",
                    regex=True,
                    inplace=True,
                )
                data.replace(
                    to_replace=r"49=([^,]*),34=([^,]*)",
                    value="",
                    regex=True,
                    inplace=True,
                )
                data.replace(
                    to_replace=r",5799=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",268=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",279=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",22=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",48=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",273=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",274=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",451=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",1003=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",1020=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",277=([^,]*),", value=",", regex=True, inplace=True
                )

                OPEN = data[data[data.str.contains(r"269=4")].index]
                SETTLE = data[data[data.str.contains(r"269=6")].index]
                HIGH_PX = data[data[data.str.contains(r"269=7")].index]
                LOW_PX = data[data[data.str.contains(r"269=8")].index]
                HIGH_BID = data[data[data.str.contains(r"269=N")].index]
                LOW_OFFER = data[data[data.str.contains(r"269=O")].index]
                VOLUME = data[data[data.str.contains(r"269=B")].index]
                OPEN_INT = data[data[data.str.contains(r"269=C")].index]
                SIMULATE_SELL = data[data[data.str.contains(r"269=E")].index]
                SIMULATE_BUY = data[data[data.str.contains(r"269=F")].index]
                # g =Threshold Limits and Price Band Variation
                LIMIT = data[data[data.str.contains(r"35=f")].index]

                if len(OPEN) != 0:

                    open_stat = OPEN.str.extractall(
                        r"83=([^,]*),107=([^,]*),269=4,270=([^,]*),286=([^,]*),"
                    )

                    open_stat.columns = ["Seq", "Code", "OPEN_PX", "Flag"]
                    open_stat["Flag"] = open_stat["Flag"].astype("str")

                    open_stat.loc[open_stat["Flag"] == "5", "Flag"] = "IndicativeOpen"
                    open_stat.loc[open_stat["Flag"] == "0", "Flag"] = "DailyOpen"
                    open_stat.index.names = ["record", "match"]

                    open_info = OPEN.str.extractall(r"52=([^,]*),75=([^,]*),")
                    open_info.columns = ["Time", "Date"]
                    open_info.index.names = ["record", "match"]

                    open_stat = open_info.merge(
                        open_stat, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(SETTLE) != 0:

                    settle = SETTLE.str.extractall(
                        r"83=([^,]*),107=([^,]*),269=6,270=([^,]*),"
                    )

                    settle.columns = ["Seq", "Code", "SETTLE_PX"]
                    settle.index.names = ["record", "match"]

                    settle_info = SETTLE.str.extractall(r"52=([^,]*),75=([^,]*),")
                    settle_info.columns = ["Time", "Date"]
                    settle_info.index.names = ["record", "match"]

                    settle = settle_info.merge(
                        settle, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(HIGH_PX) != 0:

                    high_px = HIGH_PX.str.extractall(
                        r"83=([^,]*),107=([^,]*),269=7,270=([^,]*),"
                    )

                    high_px.columns = ["Seq", "Code", "HIGH"]
                    high_px.index.names = ["record", "match"]

                    high_px_info = HIGH_PX.str.extractall(r"52=([^,]*),75=([^,]*),")
                    high_px_info.columns = ["Time", "Date"]
                    high_px_info.index.names = ["record", "match"]

                    high_px = high_px_info.merge(
                        high_px, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(LOW_PX) != 0:

                    low_px = LOW_PX.str.extractall(
                        r"83=([^,]*),107=([^,]*),269=8,270=([^,]*),"
                    )

                    low_px.columns = ["Seq", "Code", "LOW"]
                    low_px.index.names = ["record", "match"]

                    low_px_info = LOW_PX.str.extractall(r"52=([^,]*),75=([^,]*),")
                    low_px_info.columns = ["Time", "Date"]
                    low_px_info.index.names = ["record", "match"]

                    low_px = low_px_info.merge(
                        low_px, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(HIGH_BID) != 0:

                    high_bid = HIGH_BID.str.extractall(
                        r"83=([^,]*),107=([^,]*),269=N,270=([^,]*),"
                    )

                    high_bid.columns = ["Seq", "Code", "HIGH_BID"]
                    high_bid.index.names = ["record", "match"]

                    high_bid_info = HIGH_BID.str.extractall(r"52=([^,]*),75=([^,]*),")
                    high_bid_info.columns = ["Time", "Date"]
                    high_bid_info.index.names = ["record", "match"]

                    high_bid = high_bid_info.merge(
                        high_bid, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(LOW_OFFER) != 0:

                    low_offer = LOW_OFFER.str.extractall(
                        r"83=([^,]*),107=([^,]*),269=O,270=([^,]*),"
                    )

                    low_offer.columns = ["Seq", "Code", "LOW_OFFER"]
                    low_offer.index.names = ["record", "match"]

                    low_offer_info = LOW_OFFER.str.extractall(r"52=([^,]*),75=([^,]*),")
                    low_offer_info.columns = ["Time", "Date"]
                    low_offer_info.index.names = ["record", "match"]

                    low_offer = low_offer_info.merge(
                        low_offer, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(VOLUME) != 0:

                    volume = VOLUME.str.extractall(
                        r"83=([^,]*),107=([^,]*),269=B,271=([^,]*),"
                    )

                    volume.columns = ["Seq", "Code", "VOLUME"]
                    volume.index.names = ["record", "match"]

                    volume_info = VOLUME.str.extractall(r"52=([^,]*),75=([^,]*),")
                    volume_info.columns = ["Time", "Date"]
                    volume_info.index.names = ["record", "match"]

                    volume = volume_info.merge(
                        volume, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(VOLUME) != 0:

                    volume = VOLUME.str.extractall(
                        r"83=([^,]*),107=([^,]*),269=B,271=([^,]*),"
                    )

                    volume.columns = ["Seq", "Code", "VOLUME"]
                    volume.index.names = ["record", "match"]

                    volume_info = VOLUME.str.extractall(r"52=([^,]*),75=([^,]*),")
                    volume_info.columns = ["Time", "Date"]
                    volume_info.index.names = ["record", "match"]

                    volume = volume_info.merge(
                        volume, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(OPEN_INT) != 0:

                    open_int = OPEN_INT.str.extractall(
                        r"83=([^,]*),107=([^,]*),269=C,271=([^,]*),"
                    )

                    open_int.columns = ["Seq", "Code", "OPEN_INT"]
                    open_int.index.names = ["record", "match"]

                    open_int_info = OPEN_INT.str.extractall(r"52=([^,]*),75=([^,]*),")
                    open_int_info.columns = ["Time", "Date"]
                    open_int_info.index.names = ["record", "match"]

                    open_int = open_int_info.merge(
                        open_int, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(SIMULATE_SELL) != 0:

                    simulate_sell = SIMULATE_SELL.str.extractall(
                        r"83=([^,]*),107=([^,]*),269=E,270=([^,]*),271=([^,]*),"
                    )

                    simulate_sell.columns = ["Seq", "Code", "PX", "Qty"]
                    simulate_sell.index.names = ["record", "match"]

                    simulate_sell_info = SIMULATE_SELL.str.extractall(
                        r"52=([^,]*),75=([^,]*),"
                    )
                    simulate_sell_info.columns = ["Time", "Date"]
                    simulate_sell_info.index.names = ["record", "match"]

                    simulate_sell = simulate_sell_info.merge(
                        simulate_sell, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(SIMULATE_BUY) != 0:

                    simulate_buy = SIMULATE_BUY.str.extractall(
                        r"83=([^,]*),107=([^,]*),269=F,270=([^,]*),271=([^,]*),"
                    )

                    simulate_buy.columns = ["Seq", "Code", "PX", "Qty"]
                    simulate_buy.index.names = ["record", "match"]

                    simulate_buy_info = SIMULATE_BUY.str.extractall(
                        r"52=([^,]*),75=([^,]*),"
                    )
                    simulate_buy_info.columns = ["Time", "Date"]
                    simulate_buy_info.index.names = ["record", "match"]

                    simulate_buy = simulate_buy_info.merge(
                        simulate_buy, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                info_list = {
                    "open_stat": open_stat,
                    "settle": settle,
                    "high_px": high_px,
                    "low_px": low_px,
                    "high_bid": high_bid,
                    "low_offer": low_offer,
                    "volume": volume,
                    "open_int": open_int,
                    "simulate_buy": simulate_buy,
                    "simulated_sell": simulate_sell,
                }

            else:

                data.replace(to_replace=r"\x01", value=",", regex=True, inplace=True)

                data.replace(
                    to_replace=r"1128=([^,]*),9=([^,]*),35=([^,]*),49=([^,]*),",
                    value="",
                    regex=True,
                    inplace=True,
                )
                data.replace(
                    to_replace=r"34=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",5799=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",268=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",279=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",48=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",37705=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",37=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",32=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(
                    to_replace=r",10=([^,]*),", value=",", regex=True, inplace=True
                )
                data.replace(to_replace=r",+,", value=",", regex=True, inplace=True)

                OPEN = data[data[data.str.contains(r"269=4")].index]
                SETTLE = data[data[data.str.contains(r"269=6")].index]
                HIGH_PX = data[data[data.str.contains(r"269=7")].index]
                LOW_PX = data[data[data.str.contains(r"269=8")].index]
                HIGH_BID = data[data[data.str.contains(r"269=N")].index]
                LOW_OFFER = data[data[data.str.contains(r"269=O")].index]
                VOLUME = data[data[data.str.contains(r"269=B")].index]
                OPEN_INT = data[data[data.str.contains(r"269=C")].index]
                ELEC_VOLUME = data[data[data.str.contains(r"269=e")].index]
                LIMIT = data[data[data.str.contains(r"269=g")].index]

                if len(OPEN) != 0:

                    open_stat = OPEN.str.extractall(
                        r"269=4,55=([^,]*),83=([^,]*),270=([^,]*),286=([^,]*),"
                    )

                    open_stat.columns = ["Seq", "Code", "OPEN_PX", "Flag"]
                    open_stat["Flag"] = open_stat["Flag"].astype("str")

                    open_stat.loc[open_stat["Flag"] == "5", "Flag"] = "IndicativeOpen"
                    open_stat.loc[open_stat["Flag"] == "0", "Flag"] = "DailyOpen"
                    open_stat.index.names = ["record", "match"]

                    open_info = OPEN.str.extractall(
                        r"75=([^,]*),52=([^,]*),60=([^,]*),"
                    )
                    open_info.columns = ["Date", "SendingTime", "TransactTime"]
                    open_info.index.names = ["record", "match"]

                    open_stat = open_info.merge(
                        open_stat, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(SETTLE) != 0:

                    settle = SETTLE.str.extractall(
                        r"269=6,55=([^,]*),83=([^,]*),270=([^,]*),"
                    )

                    settle.columns = ["Seq", "Code", "SETTLE_PX"]
                    settle.index.names = ["record", "match"]

                    settle_info = SETTLE.str.extractall(
                        r"75=([^,]*),52=([^,]*),60=([^,]*),"
                    )
                    settle_info.columns = ["Date", "SendingTime", "TransactTime"]
                    settle_info.index.names = ["record", "match"]

                    settle = settle_info.merge(
                        settle, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(HIGH_PX) != 0:

                    high_px = HIGH_PX.str.extractall(
                        r"269=7,55=([^,]*),83=([^,]*),270=([^,]*),"
                    )

                    high_px.columns = ["Seq", "Code", "HIGH"]
                    high_px.index.names = ["record", "match"]

                    high_px_info = HIGH_PX.str.extractall(
                        r"75=([^,]*),52=([^,]*),60=([^,]*),"
                    )
                    high_px_info.columns = ["Date", "SendingTime", "TransactTime"]
                    high_px_info.index.names = ["record", "match"]

                    high_px = high_px_info.merge(
                        high_px, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(LOW_PX) != 0:

                    low_px = LOW_PX.str.extractall(
                        r"269=8,55=([^,]*),83=([^,]*),270=([^,]*),"
                    )

                    low_px.columns = ["Seq", "Code", "LOW"]
                    low_px.index.names = ["record", "match"]

                    low_px_info = LOW_PX.str.extractall(
                        r"75=([^,]*),52=([^,]*),60=([^,]*),"
                    )
                    low_px_info.columns = ["Date", "SendingTime", "TransactTime"]
                    low_px_info.index.names = ["record", "match"]

                    low_px = low_px_info.merge(
                        low_px, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(HIGH_BID):

                    high_bid = HIGH_BID.str.extractall(
                        r"269=N,55=([^,]*),83=([^,]*),270=([^,]*),"
                    )

                    high_bid.columns = ["Seq", "Code", "HIGH_BID"]
                    high_bid.index.names = ["record", "match"]

                    high_bid_info = HIGH_BID.str.extractall(
                        r"75=([^,]*),52=([^,]*),60=([^,]*),"
                    )
                    high_bid_info.columns = ["Date", "SendingTime", "TransactTime"]
                    high_bid_info.index.names = ["record", "match"]

                    high_bid = high_bid_info.merge(
                        high_bid, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(LOW_OFFER):

                    low_offer = LOW_OFFER.str.extractall(
                        r"269=O,55=([^,]*),83=([^,]*),270=([^,]*),"
                    )

                    low_offer.columns = ["Seq", "Code", "LOW_OFFER"]
                    low_offer.index.names = ["record", "match"]

                    low_offer_info = LOW_OFFER.str.extractall(
                        r"75=([^,]*),52=([^,]*),60=([^,]*),"
                    )
                    low_offer_info.columns = ["Date", "SendingTime", "TransactTime"]
                    low_offer_info.index.names = ["record", "match"]

                    low_offer = low_offer_info.merge(
                        low_offer, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(VOLUME) != 0:

                    volume = VOLUME.str.extractall(
                        r"269=B,55=([^,]*),83=([^,]*),271=([^,]*),"
                    )

                    volume.columns = ["Seq", "Code", "VOLUME"]
                    volume.index.names = ["record", "match"]

                    volume_info = VOLUME.str.extractall(
                        r"75=([^,]*),52=([^,]*),60=([^,]*),"
                    )
                    volume_info.columns = ["Date", "SendingTime", "TransactTime"]
                    volume_info.index.names = ["record", "match"]

                    volume = volume_info.merge(
                        volume, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(OPEN_INT) != 0:

                    open_int = OPEN_INT.str.extractall(
                        r"269=C,55=([^,]*),83=([^,]*),271=([^,]*),"
                    )

                    open_int.columns = ["Seq", "Code", "OPEN_INT"]
                    open_int.index.names = ["record", "match"]

                    open_int_info = OPEN_INT.str.extractall(
                        r"75=([^,]*),52=([^,]*),60=([^,]*),"
                    )
                    open_int_info.columns = ["Date", "SendingTime", "TransactTime"]
                    open_int_info.index.names = ["record", "match"]

                    open_int = open_int_info.merge(
                        open_int, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(ELEC_VOLUME) != 0:

                    elec_volume = ELEC_VOLUME.str.extractall(
                        r"269=e,55=([^,]*),83=([^,]*),271=([^,]*),"
                    )

                    elec_volume.columns = ["Seq", "Code", "ELEC_VOLUME"]
                    elec_volume.index.names = ["record", "match"]

                    elec_volume_info = ELEC_VOLUME.str.extractall(
                        r"75=([^,]*),52=([^,]*),60=([^,]*),"
                    )
                    elec_volume_info.columns = ["Date", "SendingTime", "TransactTime"]
                    elec_volume_info.index.names = ["record", "match"]

                    elec_volume = elec_volume_info.merge(
                        elec_volume, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                if len(LIMIT) != 0:

                    limit = LIMIT.str.extractall(
                        r"269=g,55=([^,]*),83=([^,]*),1149=([^,]*),1148=([^,]*),1143=([^,]*),"
                    )

                    limit.columns = [
                        "Code",
                        "Seq",
                        "high_limit",
                        "low_limit",
                        "variation",
                    ]
                    limit.index.names = ["record", "match"]

                    limit_info = LIMIT.str.extractall(
                        r"75=([^,]*),52=([^,]*),60=([^,]*),"
                    )
                    limit_info.columns = ["Date", "SendingTime", "TransactTime"]
                    limit_info.index.names = ["record", "match"]

                    limit = limit_info.merge(
                        limit, how="inner", left_on="record", right_on="record"
                    ).reset_index(drop=True)

                info_list = {
                    "open_stat": open_stat,
                    "settle": settle,
                    "high_px": high_px,
                    "low_px": low_px,
                    "high_bid": high_bid,
                    "low_offer": low_offer,
                    "volume": volume,
                    "open_int": open_int,
                    "limit": limit,
                }

            return info_list

        data = pd.read_csv(self.path, header=None)[0]
        stat = stat_main(data, date=date, security=security)

        return stat

    def status(self, date, security=None):
        """
        Extracting the status messages.

        Parameters
        ----------
        date : string
            date in "YYYY-MM-DD" format.
        security : string, optional
            A specific security. The default is None.

        Returns
        -------
        pandas DataFrame
            DataFrame that includes the quote messages.


        """

        data = pd.read_csv(self.path, header=None)[0]

        if date < datetime.strptime("2015-11-20", "%Y-%m-%d"):

            data = data[data[data.str.contains(r"\x01336=([^,]*)")].index]
            data.replace(to_replace=r"\x01", value=",", regex=True, inplace=True)

            data.replace(
                to_replace=r"1128=([^,]*),9=([^,]*),35=([^,]*),49=([^,]*),",
                value="",
                regex=True,
                inplace=True,
            )
            data.replace(
                to_replace=r",268=([^,]*),279=([^,]*),22=([^,]*),48=([^,]*),",
                value=",",
                regex=True,
                inplace=True,
            )
            data.replace(
                to_replace=r",269=([^,]*),270=([^,]*),271=([^,]*),273=([^,]*)",
                value=",",
                regex=True,
                inplace=True,
            )
            data.replace(
                to_replace=r",276=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",10=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(to_replace=r",+,", value=",", regex=True, inplace=True)

            data1 = data.str.extractall(r"83=([^,]*),107=([^,]*),336=([^,]*),")

            data1.columns = ["Seq", "Code", "SessionID"]
            data1.index.names = ["record", "match"]

            data1_info = data.str.extractall(r"34=([^,]*),52=([^,]*),75=([^,]*),")

            data1_info.columns = ["MsgSeq", "Time", "Date"]
            data1_info.index.names = ["record", "match"]

            data1 = data1_info.merge(data1, how="inner", on="record").reset_index(
                drop=True
            )
            data1["SessionID"] = data1["SessionID"].astype(int)

            data1 = data1.loc[(data1["SessionID"] == 0) | (data1["SessionID"] == 1)]
            data1["SessionID"] = np.where(data1["SessionID"] == 0, "preopen", "opening")
            status = data1.sort_values(["Code", "SessionID", "Seq"]).reset_index(
                drop=True
            )

        else:

            data = data[data[data.str.contains(r"\x0135=f")].index]
            data.replace(to_replace=r"\x01", value=",", regex=True, inplace=True)

            data.replace(
                to_replace=r"1128=([^,]*),9=([^,]*),35=([^,]*),49=([^,]*),",
                value="",
                regex=True,
                inplace=True,
            )
            data.replace(
                to_replace=r",5799=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",1151=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",6937=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",327=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(
                to_replace=r",10=([^,]*),", value=",", regex=True, inplace=True
            )
            data.replace(to_replace=r",+,", value=",", regex=True, inplace=True)

            data1 = data[data[data.str.contains(r"326=21")].index]
            data2 = data[data[data.str.contains(r"326=15")].index]
            data3 = data[data[data.str.contains(r"326=17")].index]

            data_all = [data1, data2, data3]

            def status_processing(data):
                if len(data) != 0:

                    data = data.str.extractall(
                        r"34=([^,]*),52=([^,]*),60=([^,]*),75=([^,]*),326=([^,]*),1174=([^,]*),"
                    )

                    data.columns = [
                        "MsgSeq",
                        "SendingTime",
                        "TransactTime",
                        "Date",
                        "TradingStatus",
                        "TradingEvent",
                    ]

                return data

            status = [status_processing(data) for data in data_all]
            status = pd.concat(status, axis=0).reset_index(drop=True)
            status["TradingStatus"], status["TradingEvent"] = status[
                "TradingStatus"
            ].astype(int), status["TradingEvent"].astype(int)

            if status.shape[0] != 0:

                status["session"] = np.where(
                    (status["TradingStatus"] == 21)
                    & ((status["TradingEvent"] == 0) | (status["TradingEvent"] == 4)),
                    "preopen",
                    np.where(
                        (status["TradingStatus"] == 21) & (status["TradingEvent"] == 1),
                        "preopen_nocancel",
                        np.where(
                            status["TradingStatus"] == 15,
                            "opening",
                            np.where(status["TradingStatus"] == 17, "open", "none"),
                        ),
                    ),
                )

        return status


class quotes:
    @staticmethod
    def order_book(data, security, level, consolidate=True):
        """
        Reconstructing the limit order book

        Parameters
        ----------
        data : pandas DataFrame
            Input MBP quote data.
        security : string
            A specific security.
        level : int
            The number of book depths within the limit order book.
        consolidate : bool, optional
            Whether to reconstruct the consolidated limit order book that
            integrates implied and outright quotes. The default is True.

        Returns
        -------
        pandas DataFrame
            A dataframe that contains the limit order book defined by
            the maximum number of book depths.

        """

        if isinstance(data, pd.DataFrame) == False:
            raise Exception(
                "Input should be a DataFrame that consists all MDP quote messages in a trading week"
            )

        if security != data["Code"].unique().astype("str")[0]:
            raise Exception("Cannot find the security in the data input")

        print("Processing", security, "...")

        mbp_quotes = data.drop_duplicates()
        mbp_quotes = data.sort_values(["MsgSeq", "Seq"])

        mbp_quotes = mbp_quotes.astype(
            {
                "Update": "int",
                "Qty": "int",
                "Seq": "float",
                "MsgSeq": "float",
                "Ord": "int",
                "PX": "float",
                "PX_depth": "int",
            }
        )

        # Subseting the outright messages and implied messages

        mbp_quotes["Side"] = np.where(
            mbp_quotes["Side"] == "E",
            0,
            np.where(mbp_quotes["Side"] == "F", 1, mbp_quotes["Side"]),
        )

        mbp_quotes["Side"] = mbp_quotes["Side"].astype("int")

        mbp_quotes = mbp_quotes.sort_values("Seq", ignore_index=True)

        message_outright = mbp_quotes.loc[mbp_quotes["Implied"] == "N"].reset_index(
            drop=True
        )

        message_outright = message_outright.sort_values("Seq", ignore_index=True)

        # calculating the quantity updated
        # Please note that if the previous message is deletion, one should not do
        # such a calculation as the submission message will only occur when
        # the previous message is a cancellation

        # Note that not every message might have the cancellation messages
        # Might be a direct modification message

        message_outright["qty_diff"] = np.where(
            message_outright.groupby(["Side", "PX"])["Update"].shift(1) != 2,
            message_outright["Qty"]
            - message_outright.groupby(["Side", "PX"])["Qty"].shift(1),
            message_outright["Qty"],
        )

        message_outright["ord_diff"] = np.where(
            message_outright.groupby(["Side", "PX"])["Update"].shift(1) != 2,
            message_outright["Ord"]
            - message_outright.groupby(["Side", "PX"])["Ord"].shift(1),
            message_outright["Ord"],
        )

        message_outright.loc[message_outright["Update"] == 2, "qty_diff"] = (
            message_outright.loc[message_outright["Update"] == 2, "Qty"] * -1
        )
        message_outright.loc[message_outright["Update"] == 2, "ord_diff"] = (
            message_outright.loc[message_outright["Update"] == 2, "Ord"] * -1
        )

        # NA values would be the initial quantity
        # similar logic, you should get all submission messages the same way
        message_outright["qty_diff"] = message_outright["qty_diff"].fillna(
            message_outright["Qty"]
        )
        message_outright["ord_diff"] = message_outright["ord_diff"].fillna(
            message_outright["Ord"]
        )

        # check whether all update actions with na values are submissions

        # calculating the cumulative sum

        message_outright["Qty"] = message_outright.groupby(["Side", "PX"])[
            "qty_diff"
        ].cumsum()
        message_outright["Ord"] = message_outright.groupby(["Side", "PX"])[
            "ord_diff"
        ].cumsum()
        message_outright = message_outright.sort_values("Seq", ignore_index=True)

        if "Y" in message_outright["Implied"].unique():

            message_implied = mbp_quotes.loc[mbp_quotes["Implied"] == "Y"].reset_index(
                drop=True
            )

            message_implied["qty_diff"] = np.where(
                message_implied.groupby(["Side", "PX"])["Update"].shift(1) != 2,
                message_implied["Qty"]
                - message_implied.groupby(["Side", "PX"])["Qty"].shift(1),
                message_implied["Qty"],
            )

            message_implied.loc[message_implied["Update"] == 2, "qty_diff"] = (
                message_implied.loc[message_implied["Update"] == 2, "Qty"] * -1
            )

            message_implied["qty_diff"] = message_implied["qty_diff"].fillna(
                message_implied["Qyt"]
            )

            message_implied["Qty"] = message_implied.groupby(["Side", "PX"])[
                "qty_diff"
            ].cumsum()

            message_implied = message_implied.sort_values("Seq", ignore_index=True)

        def book_reconstruction_main(quotes, level=None, conso_quotes=False):

            if not conso_quotes:

                quotes = quotes.loc[quotes["Qty"] > 0]
                quotes = quotes.sort_values("Seq", ignore_index=True)

            # bid side

            bid = quotes.loc[quotes["Side"] == 0].reset_index(drop=True)

            if bid.shape[0] != 0:
                if isnull(level):
                    bid_level = max(len(bid["PX_depth"].unique()))
                else:
                    if level > max(bid["PX_depth"]):
                        raise Exception(
                            "Price level given is greater than what the data suggest"
                        )
                    else:
                        bid_level = level

            bid_rank = pd.pivot_table(
                bid, index=["Seq"], columns=["PX_depth"], values=["PX"]
            )

            bid_rank = bid_rank.ffill()
            bid_rank = bid_rank.fillna(0)

            # using the numpy for the broadcasting
            # Elementwise Comparison

            bid_prices_array = bid_rank.iloc[:, :bid_level].to_numpy()
            bid_price = bid.sort_values("Seq")["PX"].to_numpy()
            bid_matches = bid_prices_array == bid_price[:, None]
            bid_price_depth = bid_matches.argsort(axis=1)[:, -1] + 1

            bid["PX_depth"] = bid_price_depth

            # offer side

            offer = quotes.loc[quotes["Side"] == 1].reset_index(drop=True)
            if offer.shape[0] != 0:
                if isnull(level):
                    offer_level = max(len(offer["PX_depth"].unique()))
                else:  # checking whether the price levels given are out of bound or not
                    if level > max(len(offer["PX_depth"].unique())):
                        raise Exception(
                            "Price level given is greater than what the data suggest"
                        )
                    else:
                        offer_level = level

            offer_rank = pd.pivot_table(
                offer, index=["Seq"], columns=["PX_depth"], values=["PX"]
            )

            offer_rank = offer_rank.ffill()
            offer_rank = offer_rank.fillna(0)

            offer_prices_array = offer_rank.iloc[:, :offer_level].to_numpy()
            offer_price = -offer.sort_values("Seq")["PX"].to_numpy()  # descending
            offer_matches = -offer_prices_array == offer_price[:, None]
            offer_price_depth = offer_matches.argsort(axis=1)[:, -1] + 1

            offer["PX_depth"] = offer_price_depth

            quotes = pd.concat([bid, offer]).sort_values("Seq")

            quotes["Side"] = np.where(quotes["Side"] == 0, "Bid", "Ask")

            order_book = pd.pivot_table(
                quotes,
                index=quotes.columns[0:5],
                columns=["Side", "PX_depth"],
                values=["PX", "Qty", "Ord"],
            )

            # we need to make last observation carry forward

            order_book = order_book.ffill()
            order_book = order_book.fillna(0)

            return order_book

        if message_outright.shape[0] == 0:
            message_outright = None

        if message_implied.shape[0] == 0:
            message_implied = None

        if message_outright is not None and message_implied is None:

            if consolidate:
                LOB_conso = book_reconstruction_main(message_outright)
            else:
                LOB_conso = None

            LOB_outright = book_reconstruction_main(message_outright)
            LOB_implied = None
            print(
                "No implied orders and the consolidated limit order book is the same as the outright limit order book"
            )

        elif message_outright is None and message_implied is not None:

            if consolidate:
                LOB_conso = book_reconstruction_main(message_implied)
            else:
                LOB_conso = None

            LOB_implied = book_reconstruction_main(LOB_implied)
            LOB_outright = None
            print(
                "No outright orders and the consolidated limit order book is the same as the implied limit order book\n"
            )

        else:

            LOB_implied = book_reconstruction_main(LOB_implied)
            LOB_outright = book_reconstruction_main(message_outright)

            if consolidate:
                print("Both outright order book and implied order book detected\n")

                conso_quotes = pd.concat(message_outright, message_implied)
                conso_quotes["ord_diff"].fillna(0)
                conso_quotes = conso_quotes.sort_values("Seq", ignore_index=True)

                conso_quotes["Qty"] = conso_quotes.groupby(["Side", "PX"])[
                    "qty_diff"
                ].cumsum()
                conso_quotes["Ord"] = conso_quotes.groupby(["Side", "PX"])[
                    "ord_diff"
                ].cumsum()

                LOB_conso = book_reconstruction_main(conso_quotes)

            else:
                LOB_conso = None

        results = {
            "LOB_conso": LOB_conso,
            "LOB_outright": LOB_outright,
            "LOB_implied": LOB_implied,
        }

        return results


class orderbook:

    @staticmethod
    def resample(
        book,
        trading_tz,
        resample_freq,
        resample_period,
        resample_start,
        resample_end,
        transact_time=True,
    ):
        """
        Resampling the limit order book

        Parameters
        ----------
        book : pandas DataFrame
            reconstructed order book in DataFrame format.
        trading_tz : string
            The timezone of the timestamps.
        resample_freq : string
            The resampling frequency, e.g., milliseconds, seconds, etc.
        resample_period : int
            The resampling period, e.g., 1, 10, etc.
        resample_start : string
            The timestamp where the resampled data starts in format "YYYY-MM-DD HH:MM:SS."
        resample_end : string
            The timestamp where the resampled data ends in format "YYYY-MM-DD HH:MM:SS.".
        transact_time : bool, optional
            Whether to use the TransactTime. The default is True.

        Returns
        -------
        book : pandas DataFrame
            Resampled limit order book.

        """

        book["TransactTime"] = book["TransactTime"].astype(str)
        book["SendingTime"] = book["SendingTime"].astype(str)
        book["TransactTime"] = pd.to_datetime(
            book["TransactTime"], format="%Y%m%d%H%M%S%f", utc=True
        )
        book["SendingTime"] = pd.to_datetime(
            book["SendingTime"], format="%Y%m%d%H%M%S%f", utc=True
        )

        # The default time zone of CME data is UTC, and we transfer to custom trading timezone
        book["TransactTime"] = book["TransactTime"].dt.tz_convert(trading_tz)
        book["SendingTime"] = book["SendingTime"].dt.tz_convert(trading_tz)

        if not isinstance(resample_start, datetime):
            resample_start = pd.to_datetime(resample_start, format="%Y-%m-%d %H:%M:%S")
            resample_start = resample_start.tz_localize(trading_tz)

        if not isinstance(resample_end, datetime):
            resample_end = pd.to_datetime(resample_end, format="%Y-%m-%d %H:%M:%S")
            resample_end = resample_end.tz_localize(trading_tz)

        # pandas resample needs unique index, so we need to choose the last
        # obersevation for each duplicated index

        if transact_time == True:
            book = book.loc[
                (book["TransactTime"] >= resample_start)
                & (book["TransactTime"] <= resample_end)
            ]
            book = book.sort_values("Seq")
            book.drop_duplicates("TransactTime", keep="last", inplace=True)
            book.set_index("TransactTime", inplace=True)

        else:
            book = book.loc[
                (book["SendingTime"] >= resample_start)
                & (book["SendingTime"] <= resample_end)
            ]
            book = book.sort_values("Seq")
            book.set_index("SendingTime", inplace=True)

        book = book.asfreq(f"{resample_period}{resample_freq}", method="ffill")

        return book

    def tbbo(book, trades, merge_method, assign_trades=False):
        """
        Generating the TBBO data from the book and trade summary.

        Parameters
        ----------
        book : pandas DataFrame
            Limit order book.
        trades : pandas DataFrame
            Trade summary.
        merge_method : str
            Either by 'Seq_number' or by 'TransactTime', which are squence number
             and transact timestamp, respectively.
        assign_trades : bool, optional
            Whether to assign trades by Lee and Ready (1991). The default is False.

        Returns
        -------
        tbbo : pandas DataFrame
            A dataframe that contains the TBBO.

        """

        if isinstance(trades, pd.DataFrame) and isinstance(book, pd.DataFrame):
            if any(
                date not in book["Date"].astype(str).unique()
                for date in trades["Date"].unique()
            ):
                raise Exception("Trading dates of book and trades need to be the same")

        else:
            raise Exception("Inputs have to be DataFrame")

        if merge_method not in ["Seq_number", "TransactTime", "SendingTime"]:

            raise Exception(
                "Merge should be based on either Seq_number, TransactTime, or SendingTime"
            )

        if merge_method == "Seq_number":

            # find the BBO immediately before the trade happens
            tbbo = book[
                [
                    "Seq",
                    "Bid_PX_1",
                    "Bid_Qty_1",
                    "Bid_Ord_1",
                    "Ask_PX_1",
                    "Ask_Qty_1",
                    "Ask_Ord_1",
                ]
            ]
            tbbo["Seq"] = tbbo["Seq"].astype(int)
            tbbo = pd.merge_asof(trades, tbbo, on="Seq")

        elif merge_method == "TransactTime":
            tbbo = book[
                [
                    "TransactTime",
                    "Bid_PX_1",
                    "Bid_Qty_1",
                    "Bid_Ord_1",
                    "Ask_PX_1",
                    "Ask_Qty_1",
                    "Ask_Ord_1",
                ]
            ]

            if not isinstance(trades["TransactTime"], datetime):

                trades["TransactTime"] = pd.to_datetime(
                    trades["TransactTime"], format="%Y%m%d%H%M%S%f", utc=True
                )
                trades["TransactTime"] = trades["TransactTime"].dt.tz_convert(
                    "America/Chicago"
                )

            tbbo = pd.merge_asof(trades, tbbo, on="TransactTime")

        elif merge_method == "SendingTime":
            tbbo = book[
                [
                    "SendingTime",
                    "Bid_PX_1",
                    "Bid_Qty_1",
                    "Bid_Ord_1",
                    "Ask_PX_1",
                    "Ask_Qty_1",
                    "Ask_Ord_1",
                ]
            ]

            if not isinstance(trades["SendingTime"], datetime):

                trades["SendingTime"] = pd.to_datetime(
                    trades["SendingTime"], format="%Y%m%d%H%M%S%f", utc=True
                )
                trades["SendingTime"] = trades["SendingTime"].dt.tz_convert(
                    "America/Chicago"
                )

            tbbo = pd.merge_asof(trades, tbbo, on="SendingTime")

        else:
            raise Exception(
                "Merge between trades and quotes should be either TransactTime, Seq, or SendingTime"
            )

        if assign_trades == True:

            if 0 not in tbbo["agg"].unique():
                raise Exception(
                    "All trades are defined by the CME. No need to be redefined."
                )

            tbbo.sort_values("Seq", inplace=True)
            tbbo = tbbo.dropna()

            tbbo.loc[tbbo["agg"] == 0, "agg"] = np.where(
                tbbo.loc[tbbo["agg"] == 0, "PX"]
                > (
                    tbbo.loc[tbbo["agg"] == 0, "Bid_PX_1"]
                    + tbbo.loc[tbbo["agg"] == 0, "Ask_PX_1"]
                )
                / 2,
                1,
                np.where(
                    tbbo.loc[tbbo["agg"] == 0, "PX"]
                    < (
                        tbbo.loc[tbbo["agg"] == 0, "Bid_PX_1"]
                        + tbbo.loc[tbbo["agg"] == 0, "Ask_PX_1"]
                    )
                    / 2,
                    2,
                    0,
                ),
            )

        while True:

            if 0 in tbbo.loc[tbbo["agg"] == 0, "agg"].index:
                break

            tbbo["agg"] = np.where(
                (tbbo["agg"] == 0) & 1 == (tbbo["agg"].shift(-1)),
                1,
                np.where((tbbo["agg"] == 0) & 2 == (tbbo["agg"].shift(-1)), 2, 0),
            )

            if tbbo.loc[tbbo["agg"] == 0, "agg"].count() == 0:
                break

        return tbbo
