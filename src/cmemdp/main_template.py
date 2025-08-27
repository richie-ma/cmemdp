# -*- coding: utf-8 -*-
"""
CME Packet Capture data templates
"""

# Note that the block length might include some padding, which ensures
# that every message type would have the fixed block length

# at this point, do not use byte_to_int function
# use byte_to_str instead

import struct
import pandas as pd
import numpy as np


def byte_to_str(block):

    # "All alpha fields are ASCII fields which are left justified and padded on the right with spaces."
    block = block.decode('ascii').rstrip('\x00')
    return block


def byte_to_int(block):

    block = int.from_bytes(block, byteorder='little')
    return block


def ChannelReset4(msgs_blocks, BlockLength, version, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<QB', msgs_blocks[0:BlockLength])

    info = {
        'TransactTime': TransactTime,
        'MatchEventIndicator': bin(MatchEventIndicator)
    }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if (len(msgs_blocks) > BlockLength) and version > 3:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            ApplID = struct.unpack(
                '<h',
                msgs_blocks[pos:(pos+group_length)]
            )[0]

            msgs = info | {'ApplID': ApplID}

            pos += group_length
            group_repeat += 1

            msgs_list.append(msgs)

    return msgs_list


def AdminLogout16(msgs_blocks, BlockLength, cme_packet):

    text = struct.unpack('180s', msgs_blocks[0:BlockLength])

    if not isinstance(cme_packet, bool):
        msgs = [cme_packet | {'text': byte_to_str(text)}]

    else:

        msgs = [{'text': byte_to_str(text)}]

    return msgs


def MDInstrumentDefinitionFuture27(msgs_blocks, BlockLength, version, cme_packet):

    msgs_list = []

    # 216 bytes in total for version 9

    (MatchEventIndicator, TotNumReports, SecurityUpdateAction, LastUpdateTime,
     MDSecurityTradingStatus, ApplID, MarketSegmentID, UnderlyingProduct,
     SecurityExchange, SecurityGroup, Asset, Symbol,
     SecurityID, SecurityType, CFICode,
     MaturityMonthYear, Currency, SettlCurrency, MatchAlgorithm,
     MinTradeVol, MaxTradeVol, MinPriceIncrement, DisplayFactor,
     MainFraction, SubFraction, PriceDisplayFormat, UnitOfMeasure,
     UnitOfMeasureQty, TradingReferencePrice, SettlPriceType, OpenInterestQty,
     ClearedVolume, HighLimitPrice, LowLimitPrice, MaxPriceVariation,
     DecayQuantity, DecayStartDate, OriginalContractSize, ContractMultiplier,
     ContractMultiplierUnit, FlowScheduleType, MinPriceIncrementAmount, UserDefinedInstrument) = struct.unpack(
         '<BIcQBhBB4s6s6s20si6s6s5s3s3scIIqqBBB30sqqBiiqqqiHiibbqc', msgs_blocks[0:214])

    if DecayStartDate == 65535:

        DecayStartDate = np.nan

    info = {
        "MatchEventIndicator": bin(MatchEventIndicator),
        "TotNumReports": TotNumReports,
        "SecurityUpdateAction": byte_to_str(SecurityUpdateAction),
        "LastUpdateTime": LastUpdateTime,
        "MDSecurityTradingStatus": MDSecurityTradingStatus,
        "ApplID": ApplID,
        "MarketSegmentID": MarketSegmentID,
        "UnderlyingProduct": UnderlyingProduct,
        "SecurityExchange": byte_to_str(SecurityExchange),
        "SecurityGroup": byte_to_str(SecurityGroup),
        "Asset": byte_to_str(Asset),
        "Symbol": byte_to_str(Symbol),
        "SecurityID": SecurityID,
        "SecurityType": byte_to_str(SecurityType),
        "CFICode": byte_to_str(CFICode),
        "MaturityMonthYear": byte_to_int(MaturityMonthYear),
        "Currency": byte_to_str(Currency),
        "SettlCurrency":  byte_to_str(SettlCurrency),
        "MatchAlgorithm": byte_to_str(MatchAlgorithm),
        "MinTradeVol": MinTradeVol,
        "MaxTradeVol": MaxTradeVol,
        "MinPriceIncrement": MinPriceIncrement,
        "MinPriceIncrementAmount": MinPriceIncrementAmount,
        "DisplayFactor": DisplayFactor,
        "MainFraction": MainFraction,
        "SubFraction": SubFraction,
        "PriceDisplayFormat": PriceDisplayFormat,
        "UnitOfMeasure": byte_to_str(UnitOfMeasure),
        "UnitOfMeasureQty": UnitOfMeasure,
        "TradingReferencePrice": TradingReferencePrice,
        "SettlPriceType": SettlPriceType,
        "OpenInterestQty": OpenInterestQty,
        "ClearedVolume": ClearedVolume,
        "HighLimitPrice": HighLimitPrice,
        "LowLimitPrice": LowLimitPrice,
        'MaxPriceVariation': MaxPriceVariation,
        'DecayQuantity': DecayQuantity,
        'DecayStartDate': DecayStartDate,
        'OriginalContractSize': OriginalContractSize,
        'ContractMultiplier': ContractMultiplier,
        'ContractMultiplierUnit': ContractMultiplierUnit,
        'FlowScheduleType': FlowScheduleType,
        'MinPriceIncrementAmount': MinPriceIncrementAmount,
        'UserDefinedInstrument': byte_to_str(UserDefinedInstrument)
    }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if version >= 6:

        TradingReferenceDate = struct.unpack(
            '<H', msgs_blocks[214:BlockLength])

        info = info | {'TradingReferenceDate': TradingReferenceDate}

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (EventType, EventTime) = struct.unpack(
                '<BQ',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'EventType': EventType,
                           'EventTime': EventTime}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDFeedType, MarketDepth) = struct.unpack(
                '<3sb',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDFeedType': byte_to_str(MDFeedType),
                           'MarketDepth': MarketDepth}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (InstAttribValue) = struct.unpack(
                '<I',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {
                'InstAttribValue': InstAttribValue[0]}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (LotType, MinLotSize) = struct.unpack(
                '<bi',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'LotType': LotType,
                           'MinLotSize': MinLotSize}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDInstrumentDefinitionSpread29(msgs_blocks, BlockLength, version, cme_packet):

    msgs_list = []

    # total 195 bytes

    (MatchEventIndicator, TotNumReports, SecurityUpdateAction, LastUpdateTime,
     MDSecurityTradingStatus, ApplID, MarketSegmentID, UnderlyingProduct,
     SecurityExchange, SecurityGroup, Asset, Symbol, SecurityID, SecurityType,
     CFICode, MaturityMonthYear, Currency,
     SecuritySubType, UserDefinedInstrument, MatchAlgorithm, MinTradeVol,
     MaxTradeVol, MinPriceIncrement, DisplayFactor, PriceDisplayFormat,
     PriceRatio, TickRule, UnitOfMeasure, TradingReferencePrice, SettlPriceType,
     OpenInterestQty, ClearedVolume, HighLimitPrice, LowLimitPrice,
     MaxPriceVariation, MainFraction, SubFraction) = struct.unpack(
        '<BIcQBhBB4s6s6s20si6s6s5s3s6scIIqqBqb30sqBiiqqqBB', msgs_blocks[0:193])

    info = {
        "MatchEventIndicator": bin(MatchEventIndicator),
        "TotNumReports": TotNumReports,
        "SecurityUpdateAction": byte_to_str(SecurityUpdateAction),
        "LastUpdateTime": LastUpdateTime,
        "MDSecurityTradingStatus": MDSecurityTradingStatus,
        "ApplID": ApplID,
        "MarketSegmentID": MarketSegmentID,
        "UnderlyingProduct": UnderlyingProduct,
        "SecurityExchange": byte_to_str(SecurityExchange),
        "SecurityGroup": byte_to_str(SecurityGroup),
        "Asset": byte_to_str(Asset),
        "Symbol": byte_to_str(Symbol),
        "SecurityID": SecurityID,
        "SecurityType": byte_to_str(SecurityType),
        "CFICode": byte_to_str(CFICode),
        "MaturityMonthYear": byte_to_int(MaturityMonthYear),
        "Currency": byte_to_str(Currency),
        "SecuritySubType": byte_to_str(SecuritySubType),
        "UserDefinedInstrument": byte_to_str(UserDefinedInstrument),
        "MatchAlgorithm": byte_to_str(MatchAlgorithm),
        "MinTradeVol": MinTradeVol,
        "MaxTradeVol": MaxTradeVol,
        "MinPriceIncrement": MinPriceIncrement,
        "DisplayFactor": DisplayFactor,
        "PriceDisplayFormat": PriceDisplayFormat,
        "PriceRatio": PriceRatio,
        "TickRule": TickRule,
        "UnitOfMeasure": byte_to_str(UnitOfMeasure),
        "TradingReferencePrice": TradingReferencePrice,
        "SettlPriceType": SettlPriceType,
        "OpenInterestQty": OpenInterestQty,
        "ClearedVolume": ClearedVolume,
        "HighLimitPrice": HighLimitPrice,
        "LowLimitPrice": LowLimitPrice,
        "MaxPriceVariation": MaxPriceVariation,
        "MainFraction": MainFraction,
        "SubFraction": SubFraction,
    }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if version >= 6:

        TradingReferenceDate = struct.unpack(
            '<H', msgs_blocks[193: BlockLength])

        info = info | {'TradingReferenceDate': TradingReferenceDate}

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (EventType, EventTime) = struct.unpack(
                '<BQ',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'EventType': EventType,
                           'EventTime': EventTime}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDFeedType, MarketDepth) = struct.unpack(
                '<3sb',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDFeedType': byte_to_str(MDFeedType),
                           'MarketDepth': MarketDepth}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (InstAttribValue) = struct.unpack(
                '<I',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {
                'InstAttribValue': InstAttribValue[0]}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (LotType, MinLotSize) = struct.unpack(
                '<bi',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'LotType': LotType,
                           'MinLotSize': MinLotSize}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (LegSecurityID, LegSide, LegRatioQty, LegPrice, LegOptionDelta) = struct.unpack(
                '<iBbqi', msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'LegSecurityID': LotType,
                           'LegSide': LegSide,
                           'LegRatioQty': LegRatioQty,
                           'LegPrice': LegPrice,
                           'LegOptionDelta': LegOptionDelta}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def SecurityStatus30(msgs_blocks, BlockLength, cme_packet):

    (TransactTime, SecurityGroup, Asset, SecurityID, TradeDate,
     MatchEventIndicator, SecurityTradingStatus, HaltReason,
     SecurityTradingEvent) = struct.unpack('<Q6s6siHBBBB', msgs_blocks[0:BlockLength])

    if TradeDate == 65535:
        TradeDate = np.nan

    if SecurityID == 2147483647:
        SecurityID = np.nan

    if not isinstance(cme_packet, bool):

        msgs_list = [cme_packet | {'TransactTime': TransactTime,
                                   'SecurityGroup': byte_to_str(SecurityGroup),
                                   'Asset': byte_to_str(Asset),
                                   'SecurityID': SecurityID,
                                   'TradeDate': TradeDate,
                                   'MatchEventIndicator': bin(MatchEventIndicator),
                                   'SecurityTradingStatus': SecurityTradingStatus,
                                   'HaltReason': HaltReason,
                                   'SecurityTradingEvent': SecurityTradingEvent
                                   }]

    else:

        msgs_list = [{'TransactTime': TransactTime,
                      'SecurityGroup': byte_to_str(SecurityGroup),
                      'Asset': byte_to_str(Asset),
                      'SecurityID': SecurityID,
                      'TradeDate': TradeDate,
                      'MatchEventIndicator': bin(MatchEventIndicator),
                      'SecurityTradingStatus': SecurityTradingStatus,
                      'HaltReason': HaltReason,
                      'SecurityTradingEvent': SecurityTradingEvent
                      }]

    return msgs_list


def MDIncrementalRefreshBook32(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, SecurityID,
             RptSeq, NumberOfOrders, MDPriceLevel,
             MDUpdateAction, MDEntryType) = struct.unpack(
                '<qiiIiBB6s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'MDEntrySize': MDEntrySize,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'NumberOfOrders': NumberOfOrders,
                           'MDPriceLevel': MDPriceLevel,
                           'MDUpdateAction': MDUpdateAction,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        return msgs_list


def MDIncrementalRefreshDailyStatistics33(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, SecurityID,
             RptSeq, TradingReferenceDate, SettlPriceType,
             MDUpdateAction, MDEntryType) = struct.unpack(
                '<qiiIHBB8s',
                msgs_blocks[pos:(pos+group_length)]
            )

            if TradingReferenceDate == 65535:
                TradingReferenceDate = np.nan

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'MDEntrySize': MDEntrySize,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'TradingReferenceDate': TradingReferenceDate,
                           'SettlPriceType': SettlPriceType,
                           'MDUpdateAction': MDUpdateAction,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshLimitsBanding34(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries--32 bytes

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (HighLimitPrice, LowLimitPrice, MaxPriceVariation,
             SecurityID, RptSeq, MDUpdateAction,
             MDEntryType) = struct.unpack(
                '<qqqiI4s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'HighLimitPrice': HighLimitPrice,
                           'LowLimitPrice': LowLimitPrice,
                           'MaxPriceVariation': MaxPriceVariation,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'MDUpdateAction': MDUpdateAction,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            msgs_list.append(msgs_list)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshSessionStatistics35(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries--24 bytes

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, SecurityID, RptSeq,
             OpenCloseSettlFlag, MDUpdateAction, MDEntryType) = struct.unpack(
                '<qiIBB6s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'OpenCloseSettlFlag': OpenCloseSettlFlag,
                           'MDUpdateAction': MDUpdateAction,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshTrade36(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, SecurityID,
             RptSeq, NumberOfOrders, TradeID, AggressorSide,
             MDUpdateAction) = struct.unpack(
                '<qiiIiiB3s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'MDEntrySize': MDEntrySize,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'NumberOfOrders': NumberOfOrders,
                           'AggressorSide': AggressorSide,
                           'MDUpdateAction': byte_to_str(MDUpdateAction)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshVolume37(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {
        'TransactTime': TransactTime,
        'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))
    }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntrySize, SecurityID, RptSeq,
             MDUpdateAction) = struct.unpack(
                '<iiI4s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDEntrySize': MDEntrySize,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'MDUpdateAction': byte_to_int(MDUpdateAction)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def SnapshotFullRefresh38(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (LastMsgSeqNumProcessed, TotNumReports,
     SecurityID, RptSeq, TransactTime, LastUpdateTime,
     TradeDate, MDSecurityTradingStatus, HighLimitPrice,
     LowLimitPrice, MaxPriceVariation) = struct.unpack('<IIiIQQHBqqq', msgs_blocks[0:BlockLength])

    if TradeDate == 65535:
        TradeDate = np.nan

    info = {'LastMsgSeqNumProcessed': LastMsgSeqNumProcessed,
            'TotNumReports': TotNumReports,
            'SecurityID': SecurityID,
            'RptSeq': RptSeq,
            'TransactTime': TransactTime,
            'LastUpdateTime': LastUpdateTime,
            'TradeDate': TradeDate,
            'MDSecurityTradingStatus': MDSecurityTradingStatus,
            'HighLimitPrice': HighLimitPrice,
            'LowLimitPrice': LowLimitPrice,
            'MaxPriceVariation': MaxPriceVariation}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries--22 bytes

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, NumberOfOrders,
             MDPriceLevel, TradingReferenceDate, OpenCloseSettlFlag,
             SettlPriceType, MDEntryType) = struct.unpack(
                '<qiibHBBc',
                msgs_blocks[pos:(pos+group_length)]
            )

            if TradingReferenceDate == 65535:

                TradingReferenceDate = np.nan

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'MDEntrySize': MDEntrySize,
                           'NumberOfOrders': NumberOfOrders,
                           'MDPriceLevel': MDPriceLevel,
                           'TradingReferenceDate': TradingReferenceDate,
                           'OpenCloseSettlFlag': OpenCloseSettlFlag,
                           'SettlPriceType': SettlPriceType,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def QuoteRequest39(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, QuoteReqID, MatchEventIndicator) = struct.unpack(
        '<Q23s4s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'QuoteReqID': byte_to_int(QuoteReqID),
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))
            }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (Symbol, SecurityID, OrderQty,
             QuoteType, Side) = struct.unpack(
                '<20siib3s',
                msgs_blocks[pos:(pos+group_length)]
            )

            if Side == 127:
                Side = np.nan

            if OrderQty == 2147483647:
                OrderQty = np.nan

            msgs = info | {'Symbol': byte_to_str(Symbol),
                           'SecurityID': SecurityID,
                           'OrderQty': OrderQty,
                           'QuoteType': QuoteType,
                           'Side': byte_to_int(Side)}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDInstrumentDefinitionOption41(msgs_blocks, BlockLength, version, cme_packet):

    msgs_list = []

    (MatchEventIndicator, TotNumReports, SecurityUpdateAction, LastUpdateTime,
     MDSecurityTradingStatus, ApplID, MarketSegmentID, UnderlyingProduct,
     SecurityExchange, SecurityGroup, Asset, Symbol, SecurityID,
     SecurityType, CFICode, PutOrCall, MaturityMonthYear,
     Currency, StrikePrice, StrikeCurrency, SettlCurrency, MinCabPrice,
     MatchAlgorithm, MinTradeVol, MaxTradeVol, MinPriceIncrement,
     MinPriceIncrementAmount, DisplayFactor, TickRule, MainFraction,
     SubFraction, PriceDisplayFormat, UnitOfMeasure, UnitOfMeasureQty,
     TradingReferencePrice, SettlPriceType, ClearedVolume, OpenInterestQty,
     LowLimitPrice, HighLimitPrice, UserDefinedInstrument) = struct.unpack(
        '<BIcQBhBB4s6s6s20si6s6sB5s3sq3s3sqcIIqqqbBBB30sqqBiiqqcH',
        msgs_blocks[0:BlockLength]
    )

    info = {
        "MatchEventIndicator": bin(MatchEventIndicator),
        "TotNumReports": TotNumReports,
        "SecurityUpdateAction": byte_to_str(SecurityUpdateAction),
        "LastUpdateTime": LastUpdateTime,
        "MDSecurityTradingStatus": MDSecurityTradingStatus,
        "ApplID": ApplID,
        "MarketSegmentID": MarketSegmentID,
        "UnderlyingProduct": UnderlyingProduct,
        "SecurityExchange": byte_to_str(SecurityExchange),
        "SecurityGroup": byte_to_str(SecurityGroup),
        "Asset": byte_to_str(Asset),
        "Symbol": byte_to_str(Symbol),
        "SecurityID": SecurityID,
        "SecurityType": byte_to_str(SecurityType),
        "CFICode": byte_to_str(CFICode),
        "PutOrCall": PutOrCall,
        "MaturityMonthYear": byte_to_int(MaturityMonthYear),
        "Currency": byte_to_str(Currency),
        "StrikePrice": StrikePrice,
        "StrikeCurrency": byte_to_str(StrikeCurrency),
        "SettlCurrency":  byte_to_str(SettlCurrency),
        "MinCabPrice": MinCabPrice,
        "MatchAlgorithm": byte_to_str(MatchAlgorithm),
        "MinTradeVol": MinTradeVol,
        "MaxTradeVol": MaxTradeVol,
        "MinPriceIncrement": MinPriceIncrement,
        "MinPriceIncrementAmount": MinPriceIncrementAmount,
        "DisplayFactor": DisplayFactor,
        "TickRule": TickRule,
        "MainFraction": MainFraction,
        "SubFraction": SubFraction,
        "PriceDisplayFormat": PriceDisplayFormat,
        "UnitOfMeasure": byte_to_str(UnitOfMeasure),
        "UnitOfMeasureQty": UnitOfMeasure,
        "TradingReferencePrice": TradingReferencePrice,
        "SettlPriceType": SettlPriceType,
        "ClearedVolume": ClearedVolume,
        "OpenInterestQty": OpenInterestQty,
        "LowLimitPrice": LowLimitPrice,
        "HighLimitPrice": HighLimitPrice,
        "UserDefinedInstrument": byte_to_str(UserDefinedInstrument),
    }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (EventType, EventTime) = struct.unpack(
                '<BQ',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'EventType': EventType,
                           'EventTime': EventTime}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDFeedType, MarketDepth) = struct.unpack(
                '<3sb',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDFeedType': byte_to_str(MDFeedType),
                           'MarketDepth': MarketDepth}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (InstAttribValue) = struct.unpack(
                '<I',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {
                'InstAttribValue': InstAttribValue[0]}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (LotType, MinLotSize) = struct.unpack(
                '<bi',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'LotType': LotType,
                           'MinLotSize': MinLotSize}
            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (UnderlyingSecurityID, UnderlyingSymbol) = struct.unpack(
                '<i20s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'UnderlyingSecurityID': UnderlyingSecurityID,
                           'UnderlyingSymbol': byte_to_str(UnderlyingSymbol)}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

    return msgs_list


def MDIncrementalRefreshTradeSummary42(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, SecurityID,
             RptSeq, NumberOfOrders, AggressorSide,
             MDUpdateAction) = struct.unpack(
                '<qiiIiB7s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'MDEntrySize': MDEntrySize,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'NumberOfOrders': NumberOfOrders,
                           'AggressorSide': AggressorSide,
                           'MDUpdateAction': byte_to_str(MDUpdateAction),
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        # NoOrderIDEntries

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 7

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (OrderID, LastQty) = struct.unpack(
                '<Q8s',
                msgs_blocks[pos:(pos+group_length)]
            )

            try:
                msgs

            except:

                msgs = info | {'OrderID': OrderID,
                               'LastQty': byte_to_str(LastQty)
                               }

            else:

                msgs = msgs | {'OrderID': OrderID,
                               'LastQty': byte_to_str(LastQty)
                               }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshOrderBook43(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (OrderID, MDOrderPriority, MDEntryPx,
             MDDisplayQty, SecurityID, MDUpdateAction,
             MDEntryType) = struct.unpack(
                '<QQqiiB7s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'OrderID': OrderID,
                           'MDOrderPriority': MDOrderPriority,
                           'MDEntryPx': MDEntryPx,
                           'MDDisplayQty': MDDisplayQty,
                           'SecurityID': SecurityID,
                           'MDUpdateAction': MDUpdateAction,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def SnapshotFullRefreshOrderBook44(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (LastMsgSeqNumProcessed, TotNumReports, SecurityID, NoChunks,
     CurrentChunk, TransactTime) = struct.unpack('<IIiIIQ', msgs_blocks[0:BlockLength])

    info = {'LastMsgSeqNumProcessed': LastMsgSeqNumProcessed,
            'TotNumReports': TotNumReports,
            'SecurityID': SecurityID,
            'NoChunks': NoChunks,
            'CurrentChunk': CurrentChunk,
            'TransactTime': TransactTime,
            }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries--22 bytes

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (OrderID, MDOrderPriority, MDEntryPx,
             MDDisplayQty, MDEntryType) = struct.unpack(
                '<QQqic',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'OrderID': OrderID,
                           'MDOrderPriority': MDOrderPriority,
                           'MDEntryPx': MDEntryPx,
                           'MDDisplayQty': MDDisplayQty,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshBook46(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        end_group = pos + group_length*NumInGroup

        group_repeat = 0

        # since there is reference ID so we need to make sure
        # the order and MBP parts are connected correctly

        MBP = []

        if struct.unpack(
                '<B', msgs_blocks[(end_group+7):(end_group+8)])[0] != 0:

            msgappend = False

        else:

            msgappend = True

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, SecurityID,
             RptSeq, NumberOfOrders, MDPriceLevel,
             MDUpdateAction, MDEntryType, TradeableSize) = struct.unpack(
                '<qiiIiBBc5s',
                msgs_blocks[pos:(pos+group_length)]
            )

            if MDEntrySize == 2147483647:
                MDEntrySize = np.nan

            if NumberOfOrders == 2147483647:
                NumberOfOrders = np.nan

            if byte_to_int(TradeableSize) == 2147483647:
                TradeableSize = np.nan

            else:
                TradeableSize = byte_to_int(TradeableSize)

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'MDEntrySize': MDEntrySize,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'NumberOfOrders': NumberOfOrders,
                           'MDPriceLevel': MDPriceLevel,
                           'MDUpdateAction': MDUpdateAction,
                           'MDEntryType': byte_to_str(MDEntryType),
                           'TradeableSize': TradeableSize
                           }

            MBP.append(msgs)

            # when there is MBO information, we do not need to append at this step

            if msgappend:

                msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        # NoOrderIDEntries

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 7

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (OrderID, MDOrderPriority, MDDisplayQty,
             ReferenceID, OrderUpdateAction) = struct.unpack(
                '<QQiB3s',
                msgs_blocks[pos:(pos+group_length)]
            )

            if MDDisplayQty == 2147483647:
                MDDisplayQty = np.nan

            if MDOrderPriority == 18446744073709551615:
                MDOrderPriority = np.nan

            if ReferenceID == 255:
                ReferenceID = np.nan

            try:
                msgs

            except:

                msgs = info | {'OrderID': OrderID,
                               'MDOrderPriority': MDOrderPriority,
                               'MDDisplayQty': MDDisplayQty,
                               'ReferenceID': ReferenceID,
                               'OrderUpdateAction': byte_to_int(OrderUpdateAction)
                               }

            else:

                msgs = MBP[(ReferenceID-1)] | {'OrderID': OrderID,
                                               'MDOrderPriority': MDOrderPriority,
                                               'MDDisplayQty': MDDisplayQty,
                                               'ReferenceID': ReferenceID,
                                               'OrderUpdateAction': byte_to_int(OrderUpdateAction)
                                               }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        return msgs_list


def MDIncrementalRefreshOrderBook47(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (OrderID, MDOrderPriority, MDEntryPx,
             MDDisplayQty, SecurityID, MDUpdateAction,
             MDEntryType) = struct.unpack(
                '<QQqiiB7s',
                msgs_blocks[pos:(pos+group_length)]
            )

            if MDDisplayQty == 2147483647:
                MDDisplayQty = np.nan

            if OrderID == 18446744073709551615:
                OrderID = np.nan

            if MDOrderPriority == 18446744073709551615:
                MDOrderPriority = np.nan

            msgs = info | {'OrderID': OrderID,
                           'MDOrderPriority': MDOrderPriority,
                           'MDEntryPx': MDEntryPx,
                           'MDDisplayQty': MDDisplayQty,
                           'SecurityID': SecurityID,
                           'MDUpdateAction': MDUpdateAction,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshTradeSummary48(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        end_group = pos + group_length*NumInGroup

        group_repeat = 0

        # since there is reference ID so we need to make sure
        # the order and MBP parts are connected correctly

        if struct.unpack(
                '<B', msgs_blocks[(end_group+7):(end_group+8)])[0] != 0:

            msgappend = False

        else:

            msgappend = True

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, SecurityID,
             RptSeq, NumberOfOrders, AggressorSide,
             MDUpdateAction, MDTradeEntryID) = struct.unpack(
                '<qiiIiBB6s',
                msgs_blocks[pos:(pos+group_length)]
            )

            if MDTradeEntryID == 4294967295:
                MDTradeEntryID = np.nan

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'MDEntrySize': MDEntrySize,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'NumberOfOrders': NumberOfOrders,
                           'AggressorSide': AggressorSide,
                           'MDUpdateAction': MDUpdateAction,
                           'MDTradeEntryID': byte_to_int(MDTradeEntryID)
                           }

            if msgappend:

                msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        # NoOrderIDEntries

        """
        <composite name="groupSize8Byte" description="8 Byte aligned repeating group dimensions" semanticType="NumInGroup">
            <type name="blockLength" description="Length" primitiveType="uint16"/>
            <type name="numInGroup" description="NumInGroup" offset="7" primitiveType="uint8"/>
        """

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 7

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[(pos):(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (OrderID, LastQty) = struct.unpack(
                '<Q8s',
                msgs_blocks[pos:(pos+group_length)]
            )

            try:
                msgs

            except:

                msgs = info | {'OrderID': OrderID,
                               'LastQty': byte_to_int(LastQty)
                               }

            else:

                msgs = msgs | {'OrderID': OrderID,
                               'LastQty': byte_to_int(LastQty)
                               }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshDailyStatistics49(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, SecurityID,
             RptSeq, TradingReferenceDate, SettlPriceType,
             MDUpdateAction, MDEntryType) = struct.unpack(
                '<qiiIHBB8s',
                msgs_blocks[pos:(pos+group_length)]
            )

            if MDEntrySize == 2147483647:
                MDEntrySize = np.nan

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'MDEntrySize': MDEntrySize,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'TradingReferenceDate': TradingReferenceDate,
                           'SettlPriceType': SettlPriceType,
                           'MDUpdateAction': MDUpdateAction,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshLimitsBanding50(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries--32 bytes

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (HighLimitPrice, LowLimitPrice, MaxPriceVariation,
             SecurityID, RptSeq) = struct.unpack(
                '<qqqiI',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'HighLimitPrice': HighLimitPrice,
                           'LowLimitPrice': LowLimitPrice,
                           'MaxPriceVariation': MaxPriceVariation,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshSessionStatistics51(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries--24 bytes

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, SecurityID, RptSeq,
             OpenCloseSettlFlag, MDUpdateAction, MDEntryType,
             MDEntrySize) = struct.unpack(
                '<qiIBBc5s',
                msgs_blocks[pos:(pos+group_length)]
            )

            if byte_to_int(MDEntrySize) == 2147483647:
                MDEntrySize = np.nan
            else:
                MDEntrySize = byte_to_int(MDEntrySize)

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'OpenCloseSettlFlag': OpenCloseSettlFlag,
                           'MDUpdateAction': MDUpdateAction,
                           'MDEntryType': byte_to_str(MDEntryType),
                           'MDEntrySize': MDEntrySize
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def SnapshotFullRefresh52(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (LastMsgSeqNumProcessed, TotNumReports,
     SecurityID, RptSeq, TransactTime, LastUpdateTime,
     TradeDate, MDSecurityTradingStatus, HighLimitPrice,
     LowLimitPrice, MaxPriceVariation) = struct.unpack('<IIiIQQHBqqq', msgs_blocks[0:BlockLength])

    if TradeDate == 65535:
        TradeDate = np.nan

    info = {'LastMsgSeqNumProcessed': LastMsgSeqNumProcessed,
            'TotNumReports': TotNumReports,
            'SecurityID': SecurityID,
            'RptSeq': RptSeq,
            'TransactTime': TransactTime,
            'LastUpdateTime': LastUpdateTime,
            'TradeDate': TradeDate,
            'MDSecurityTradingStatus': MDSecurityTradingStatus,
            'HighLimitPrice': HighLimitPrice,
            'LowLimitPrice': LowLimitPrice,
            'MaxPriceVariation': MaxPriceVariation}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries--22 bytes

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, NumberOfOrders,
             MDPriceLevel, TradingReferenceDate, OpenCloseSettlFlag,
             SettlPriceType, MDEntryType) = struct.unpack(
                '<qiibHBBc',
                msgs_blocks[pos:(pos+group_length)]
            )

            if MDEntrySize == 2147483647:
                MDEntrySize = np.nan

            if MDPriceLevel == 127:
                MDPriceLevel = np.nan

            if NumberOfOrders == 2147483647:
                NumberOfOrders = np.nan

            if TradingReferenceDate == 65535:
                TradingReferenceDate = np.nan

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'MDEntrySize': MDEntrySize,
                           'NumberOfOrders': NumberOfOrders,
                           'MDPriceLevel': MDPriceLevel,
                           'TradingReferenceDate': TradingReferenceDate,
                           'OpenCloseSettlFlag': OpenCloseSettlFlag,
                           'SettlPriceType': SettlPriceType,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def SnapshotFullRefreshOrderBook53(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (LastMsgSeqNumProcessed, TotNumReports, SecurityID, NoChunks,
     CurrentChunk, TransactTime) = struct.unpack('<IIiIIQ', msgs_blocks[0:BlockLength])

    info = {'LastMsgSeqNumProcessed': LastMsgSeqNumProcessed,
            'TotNumReports': TotNumReports,
            'SecurityID': SecurityID,
            'NoChunks': NoChunks,
            'CurrentChunk': CurrentChunk,
            'TransactTime': TransactTime,
            }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        # NoMDEntries--22 bytes

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (OrderID, MDOrderPriority, MDEntryPx,
             MDDisplayQty, MDEntryType) = struct.unpack(
                '<QQqic',
                msgs_blocks[pos:(pos+group_length)]
            )

            if MDOrderPriority == 18446744073709551615:
                MDOrderPriority = np.nan

            msgs = info | {'OrderID': OrderID,
                           'MDOrderPriority': MDOrderPriority,
                           'MDEntryPx': MDEntryPx,
                           'MDDisplayQty': MDDisplayQty,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDInstrumentDefinitionFuture54(msgs_blocks, BlockLength, version, cme_packet):

    msgs_list = []

    # 216 bytes in total for version 9

    (MatchEventIndicator, TotNumReports, SecurityUpdateAction, LastUpdateTime,
     MDSecurityTradingStatus, ApplID, MarketSegmentID, UnderlyingProduct,
     SecurityExchange, SecurityGroup, Asset, Symbol,
     SecurityID, SecurityType, CFICode,
     MaturityMonthYear, Currency, SettlCurrency, MatchAlgorithm,
     MinTradeVol, MaxTradeVol, MinPriceIncrement, DisplayFactor,
     MainFraction, SubFraction, PriceDisplayFormat, UnitOfMeasure,
     UnitOfMeasureQty, TradingReferencePrice, SettlPriceType, OpenInterestQty,
     ClearedVolume, HighLimitPrice, LowLimitPrice, MaxPriceVariation,
     DecayQuantity, DecayStartDate, OriginalContractSize, ContractMultiplier,
     ContractMultiplierUnit, FlowScheduleType, MinPriceIncrementAmount, UserDefinedInstrument,
     TradingReferenceDate) = struct.unpack(
         '<BIcQBhBB4s6s6s20si6s6s5s3s3scIIqqBBB30sqqBiiqqqiHiibbqcH', msgs_blocks[0:216])

    if ContractMultiplierUnit == 127:
        ContractMultiplierUnit = np.nan

    if FlowScheduleType == 127:
        FlowScheduleType = np.nan

    if TotNumReports == 4294967295:
        TotNumReports = np.nan

    if OpenInterestQty == 2147483647:
        OpenInterestQty = np.nan

    if ClearedVolume == 2147483647:
        ClearedVolume = np.nan

    if DecayQuantity == 2147483647:
        DecayQuantity = np.nan

    if OriginalContractSize == 2147483647:
        OriginalContractSize = np.nan

    if ContractMultiplier == 2147483647:
        ContractMultiplier = np.nan

    if DecayStartDate == 65535:
        DecayStartDate = np.nan

    if TradingReferenceDate == 65535:
        TradingReferenceDate = np.nan

    if MainFraction == 255:
        MainFraction = np.nan

    if SubFraction == 255:
        SubFraction = np.nan

    if PriceDisplayFormat == 255:
        PriceDisplayFormat = np.nan

    info = {
        "MatchEventIndicator": bin(MatchEventIndicator),
        "TotNumReports": TotNumReports,
        "SecurityUpdateAction": byte_to_str(SecurityUpdateAction),
        "LastUpdateTime": LastUpdateTime,
        "MDSecurityTradingStatus": MDSecurityTradingStatus,
        "ApplID": ApplID,
        "MarketSegmentID": MarketSegmentID,
        "UnderlyingProduct": UnderlyingProduct,
        "SecurityExchange": byte_to_str(SecurityExchange),
        "SecurityGroup": byte_to_str(SecurityGroup),
        "Asset": byte_to_str(Asset),
        "Symbol": byte_to_str(Symbol),
        "SecurityID": SecurityID,
        "SecurityType": byte_to_str(SecurityType),
        "CFICode": byte_to_str(CFICode),
        "MaturityMonthYear": byte_to_int(MaturityMonthYear),
        "Currency": byte_to_str(Currency),
        "SettlCurrency":  byte_to_str(SettlCurrency),
        "MatchAlgorithm": byte_to_str(MatchAlgorithm),
        "MinTradeVol": MinTradeVol,
        "MaxTradeVol": MaxTradeVol,
        "MinPriceIncrement": MinPriceIncrement,
        "DisplayFactor": DisplayFactor,
        "MainFraction": MainFraction,
        "SubFraction": SubFraction,
        "PriceDisplayFormat": PriceDisplayFormat,
        "UnitOfMeasure": byte_to_str(UnitOfMeasure),
        "UnitOfMeasureQty": UnitOfMeasure,
        "TradingReferencePrice": TradingReferencePrice,
        "SettlPriceType": SettlPriceType,
        "OpenInterestQty": OpenInterestQty,
        "ClearedVolume": ClearedVolume,
        "HighLimitPrice": HighLimitPrice,
        "LowLimitPrice": LowLimitPrice,
        "MaxPriceVariation": MaxPriceVariation,
        "DecayQuantity": DecayQuantity,
        "DecayStartDate": DecayStartDate,
        "OriginalContractSize": OriginalContractSize,
        "ContractMultiplier": ContractMultiplier,
        "ContractMultiplierUnit": ContractMultiplierUnit,
        "FlowScheduleType": FlowScheduleType,
        "MinPriceIncrementAmount": MinPriceIncrementAmount,
        "UserDefinedInstrument": byte_to_str(UserDefinedInstrument),
        "TradingReferenceDate": TradingReferenceDate
    }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if version > 9:

        InstrumentGUID = struct.unpack('<8s', msgs_blocks[216:BlockLength])[0]

        if InstrumentGUID == 18446744073709551615:
            InstrumentGUID = np.nan

        info = info | {'InstrumentGUID': byte_to_str(InstrumentGUID)}

    if len(msgs_blocks) > BlockLength:

        repeat_msgs = []
        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (EventType, EventTime) = struct.unpack(
                '<BQ',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'EventType': EventType,
                           'EventTime': EventTime}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDFeedType, MarketDepth) = struct.unpack(
                '<3sb',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDFeedType': byte_to_str(MDFeedType),
                           'MarketDepth': MarketDepth}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (InstAttribValue) = struct.unpack(
                '<I',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {
                'InstAttribValue': InstAttribValue[0]}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (LotType, MinLotSize) = struct.unpack(
                '<bi',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'LotType': LotType,
                           'MinLotSize': MinLotSize}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDInstrumentDefinitionOption55(msgs_blocks, BlockLength, version, cme_packet):

    msgs_list = []

    (MatchEventIndicator, TotNumReports, SecurityUpdateAction, LastUpdateTime,
     MDSecurityTradingStatus, ApplID, MarketSegmentID, UnderlyingProduct,
     SecurityExchange, SecurityGroup, Asset, Symbol, SecurityID,
     SecurityType, CFICode, PutOrCall, MaturityMonthYear,
     Currency, StrikePrice, StrikeCurrency, SettlCurrency, MinCabPrice,
     MatchAlgorithm, MinTradeVol, MaxTradeVol, MinPriceIncrement,
     MinPriceIncrementAmount, DisplayFactor, TickRule, MainFraction,
     SubFraction, PriceDisplayFormat, UnitOfMeasure, UnitOfMeasureQty,
     TradingReferencePrice, SettlPriceType, ClearedVolume, OpenInterestQty,
     LowLimitPrice, HighLimitPrice, UserDefinedInstrument, TradingReferenceDate) = struct.unpack(
        '<BIcQBhBB4s6s6s20si6s6sB5s3sq3s3sqcIIqqqbBBB30sqqBiiqqcH',
        msgs_blocks[0:213]
    )

    if TickRule == 127:
        TickRule = np.nan

    if TotNumReports == 4294967295:
        TotNumReports = np.nan

    if ClearedVolume == 2147483647:
        ClearedVolume = np.nan

    if OpenInterestQty == 2147483647:
        OpenInterestQty = np.nan

    if TradingReferenceDate == 65535:
        TradingReferenceDate = np.nan

    if MainFraction == 255:
        MainFraction = np.nan

    if SubFraction == 255:
        SubFraction = np.nan

    if PriceDisplayFormat == 255:
        PriceDisplayFormat = np.nan

    info = {
        "MatchEventIndicator": bin(MatchEventIndicator),
        "TotNumReports": TotNumReports,
        "SecurityUpdateAction": byte_to_str(SecurityUpdateAction),
        "LastUpdateTime": LastUpdateTime,
        "MDSecurityTradingStatus": MDSecurityTradingStatus,
        "ApplID": ApplID,
        "MarketSegmentID": MarketSegmentID,
        "UnderlyingProduct": UnderlyingProduct,
        "SecurityExchange": byte_to_str(SecurityExchange),
        "SecurityGroup": byte_to_str(SecurityGroup),
        "Asset": byte_to_str(Asset),
        "Symbol": byte_to_str(Symbol),
        "SecurityID": SecurityID,
        "SecurityType": byte_to_str(SecurityType),
        "CFICode": byte_to_str(CFICode),
        "PutOrCall": PutOrCall,
        "MaturityMonthYear": byte_to_int(MaturityMonthYear),
        "Currency": byte_to_str(Currency),
        "StrikePrice": StrikePrice,
        "StrikeCurrency": byte_to_str(StrikeCurrency),
        "SettlCurrency":  byte_to_str(SettlCurrency),
        "MinCabPrice": MinCabPrice,
        "MatchAlgorithm": byte_to_str(MatchAlgorithm),
        "MinTradeVol": MinTradeVol,
        "MaxTradeVol": MaxTradeVol,
        "MinPriceIncrement": MinPriceIncrement,
        "MinPriceIncrementAmount": MinPriceIncrementAmount,
        "DisplayFactor": DisplayFactor,
        "TickRule": TickRule,
        "MainFraction": MainFraction,
        "SubFraction": SubFraction,
        "PriceDisplayFormat": PriceDisplayFormat,
        "UnitOfMeasure": byte_to_str(UnitOfMeasure),
        "UnitOfMeasureQty": UnitOfMeasure,
        "TradingReferencePrice": TradingReferencePrice,
        "SettlPriceType": SettlPriceType,
        "ClearedVolume": ClearedVolume,
        "OpenInterestQty": OpenInterestQty,
        "LowLimitPrice": LowLimitPrice,
        "HighLimitPrice": HighLimitPrice,
        "UserDefinedInstrument": byte_to_str(UserDefinedInstrument),
        "TradingReferenceDate": TradingReferenceDate
    }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if version > 9:

        InstrumentGUID = struct.unpack('<8s', msgs_blocks[213:BlockLength])[0]

        if InstrumentGUID == 18446744073709551615:
            InstrumentGUID = np.nan

        info = info | {'InstrumentGUID': byte_to_str(InstrumentGUID)}

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (EventType, EventTime) = struct.unpack(
                '<BQ',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'EventType': EventType,
                           'EventTime': EventTime}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDFeedType, MarketDepth) = struct.unpack(
                '<3sb',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDFeedType': byte_to_str(MDFeedType),
                           'MarketDepth': MarketDepth}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (InstAttribValue) = struct.unpack(
                '<I',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {
                'InstAttribValue': InstAttribValue[0]}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (LotType, MinLotSize) = struct.unpack(
                '<bi',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'LotType': LotType,
                           'MinLotSize': MinLotSize}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (UnderlyingSecurityID, UnderlyingSymbol) = struct.unpack(
                '<i20s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'UnderlyingSecurityID': UnderlyingSecurityID,
                           'UnderlyingSymbol': byte_to_str(UnderlyingSymbol)}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (RelatedSecurityID, RelatedSymbol) = struct.unpack(
                '<i20s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'RelatedSecurityID': RelatedSecurityID,
                           'RelatedSymbol': RelatedSymbol}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDInstrumentDefinitionSpread56(msgs_blocks, BlockLength, version, cme_packet):

    msgs_list = []

    # total 195 bytes

    (MatchEventIndicator, TotNumReports, SecurityUpdateAction, LastUpdateTime,
     MDSecurityTradingStatus, ApplID, MarketSegmentID, UnderlyingProduct,
     SecurityExchange, SecurityGroup, Asset, Symbol, SecurityID, SecurityType,
     CFICode, MaturityMonthYear, Currency,
     SecuritySubType, UserDefinedInstrument, MatchAlgorithm, MinTradeVol,
     MaxTradeVol, MinPriceIncrement, DisplayFactor, PriceDisplayFormat,
     PriceRatio, TickRule, UnitOfMeasure, TradingReferencePrice, SettlPriceType,
     OpenInterestQty, ClearedVolume, HighLimitPrice, LowLimitPrice,
     MaxPriceVariation, MainFraction, SubFraction, TradingReferenceDate) = struct.unpack(
        '<BIcQBhBB4s6s6s20si6s6s5s3s5sccIIqqBqb30sqBiiqqqBBH', msgs_blocks[0:195])

    if TickRule == 127:
        TickRule = np.nan

    if TotNumReports == 4294967295:
        TotNumReports = np.nan

    if OpenInterestQty == 2147483647:
        OpenInterestQty = np.nan

    if ClearedVolume == 2147483647:
        ClearedVolume = np.nan

    if TradingReferenceDate == 65535:
        TradingReferenceDate = np.nan

    if UnderlyingProduct == 255:
        UnderlyingProduct = np.nan

    if MainFraction == 255:
        MainFraction = np.nan

    if SubFraction == 255:
        SubFraction = np.nan

    if PriceDisplayFormat == 255:
        PriceDisplayFormat = np.nan

    info = {
        "MatchEventIndicator": bin(MatchEventIndicator),
        "TotNumReports": TotNumReports,
        "SecurityUpdateAction": byte_to_str(SecurityUpdateAction),
        "LastUpdateTime": LastUpdateTime,
        "MDSecurityTradingStatus": MDSecurityTradingStatus,
        "ApplID": ApplID,
        "MarketSegmentID": MarketSegmentID,
        "UnderlyingProduct": UnderlyingProduct,
        "SecurityExchange": byte_to_str(SecurityExchange),
        "SecurityGroup": byte_to_str(SecurityGroup),
        "Asset": byte_to_str(Asset),
        "Symbol": byte_to_str(Symbol),
        "SecurityID": SecurityID,
        "SecurityType": byte_to_str(SecurityType),
        "CFICode": byte_to_str(CFICode),
        "MaturityMonthYear": byte_to_int(MaturityMonthYear),
        "Currency": byte_to_str(Currency),
        "SecuritySubType": byte_to_str(SecuritySubType),
        "UserDefinedInstrument": byte_to_str(UserDefinedInstrument),
        "MatchAlgorithm": byte_to_str(MatchAlgorithm),
        "MinTradeVol": MinTradeVol,
        "MaxTradeVol": MaxTradeVol,
        "MinPriceIncrement": MinPriceIncrement,
        "DisplayFactor": DisplayFactor,
        "PriceDisplayFormat": PriceDisplayFormat,
        "PriceRatio": PriceRatio,
        "TickRule": TickRule,
        "UnitOfMeasure": byte_to_str(UnitOfMeasure),
        "TradingReferencePrice": TradingReferencePrice,
        "SettlPriceType": SettlPriceType,
        "OpenInterestQty": OpenInterestQty,
        "ClearedVolume": ClearedVolume,
        "HighLimitPrice": HighLimitPrice,
        "LowLimitPrice": LowLimitPrice,
        "MaxPriceVariation": MaxPriceVariation,
        "MainFraction": MainFraction,
        "SubFraction": SubFraction,
        "TradingReferenceDate": TradingReferenceDate
    }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if version > 9:

        (PriceQuoteMethod, RiskSet,
         MarketSet, InstrumentGUID, FinancialInstrumentFullName) = struct.unpack(
             '<5s6s6sQ35s', msgs_blocks[195:BlockLength])

        if InstrumentGUID == 18446744073709551615:
            InstrumentGUID = np.nan

        info = info | {'PriceQuoteMethod': byte_to_str(PriceQuoteMethod),
                       'RiskSet': byte_to_str(RiskSet),
                       'MarketSet': byte_to_str(MarketSet),
                       'InstrumentGUID': InstrumentGUID,
                       'FinancialInstrumentFullName': byte_to_str(FinancialInstrumentFullName)
                       }

    if len(msgs_blocks) > BlockLength:

        repeat_msgs = []
        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (EventType, EventTime) = struct.unpack(
                '<BQ',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'EventType': EventType,
                           'EventTime': EventTime}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDFeedType, MarketDepth) = struct.unpack(
                '<3sb',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDFeedType': byte_to_str(MDFeedType),
                           'MarketDepth': MarketDepth}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (InstAttribValue) = struct.unpack(
                '<I',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {
                'InstAttribValue': InstAttribValue[0]}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (LotType, MinLotSize) = struct.unpack(
                '<bi',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'LotType': LotType,
                           'MinLotSize': MinLotSize}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (LegSecurityID, LegSide, LegRatioQty, LegPrice, LegOptionDelta) = struct.unpack(
                '<iBbqi', msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'LegSecurityID': LegSecurityID,
                           'LegSide': LegSide,
                           'LegRatioQty': LegRatioQty,
                           'LegPrice': LegPrice,
                           'LegOptionDelta': LegOptionDelta}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDInstrumentDefinitionFixedIncome57(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    # 338 bytes in total

    (MatchEventIndicator, TotNumReports, SecurityUpdateAction, LastUpdateTime,
     MDSecurityTradingStatus, ApplID, MarketSegmentID, UnderlyingProduct,
     SecurityExchange, SecurityGroup, Asset, Symbol,
     SecurityID, SecurityType, CFICode,
     Currency, SettlCurrency, MatchAlgorithm, MinTradeVol,
     MaxTradeVol, MinPriceIncrement, DisplayFactor, MainFraction,
     SubFraction, PriceDisplayFormat, UnitOfMeasure, UnitOfMeasureQty,
     TradingReferencePrice, TradingReferenceDate, HighLimitPrice, LowLimitPrice,
     MaxPriceVariation, MinPriceIncrementAmount, IssueDate, DatedDate,
     MaturityDate, CouponRate, ParValue, CouponFrequencyUnit,
     CouponFrequencyPeriod, CouponDayCount, CountryOfIssue, Issuer,
     FinancialInstrumentFullName, SecurityAltID, SecurityAltIDSource, PriceQuoteMethod,
     PartyRoleClearingOrg, UserDefinedInstrument, RiskSet, MarketSet,
     InstrumentGUID) = struct.unpack(
        '<BIcQBhBB4s6s6s20si6s6s3s3scIIqqBBB30sqqHqqqqHHHqq3sH20s2s25s35s12sB5s5sc6s6sQ',
        msgs_blocks[0:338])

    if TotNumReports == 4294967295:
        TotNumReports = np.nan

    if TradingReferenceDate == 65535:
        TradingReferenceDate = np.nan

    if IssueDate == 65535:
        IssueDate = np.nan

    if DatedDate == 65535:
        DatedDate = np.nan

    if MaturityDate == 65535:
        MaturityDate = np.nan

    if CouponFrequencyPeriod == 65535:
        CouponFrequencyPeriod = np.nan

    if InstrumentGUID == 18446744073709551615:
        InstrumentGUID = np.nan

    if MainFraction == 255:
        MainFraction = np.nan

    if SubFraction == 255:
        SubFraction = np.nan

    if PriceDisplayFormat == 255:
        PriceDisplayFormat = np.nan

    info = {
        'MatchEventIndicator': bin(MatchEventIndicator),
        'TotNumReports': TotNumReports,
        'SecurityUpdateAction': byte_to_str(SecurityUpdateAction),
        'LastUpdateTime': LastUpdateTime,
        'MDSecurityTradingStatus': MDSecurityTradingStatus,
        'ApplID': ApplID,
        'MarketSegmentID': MarketSegmentID,
        'UnderlyingProduct': UnderlyingProduct,
        'SecurityExchange': byte_to_str(SecurityExchange),
        'SecurityGroup': byte_to_str(SecurityGroup),
        'Asset': byte_to_str(Asset),
        'Symbol': byte_to_str(Symbol),
        'SecurityID': SecurityID,
        'SecurityType': byte_to_str(SecurityType),
        "CFICode": byte_to_str(CFICode),
        "Currency": byte_to_str(Currency),
        'SettlCurrency': byte_to_str(SettlCurrency),
        'MatchAlgorithm': byte_to_str(MatchAlgorithm),
        'MinTradeVol': MinTradeVol,
        'MaxTradeVol': MaxTradeVol,
        'MinPriceIncrement': MinPriceIncrement,
        'DisplayFactor': DisplayFactor,
        'MainFraction': MainFraction,
        'SubFraction': SubFraction,
        'PriceDisplayFormat': PriceDisplayFormat,
        'UnitOfMeasure': byte_to_str(UnitOfMeasure),
        'UnitOfMeasureQty': UnitOfMeasureQty,
        'TradingReferencePrice': TradingReferencePrice,
        'TradingReferenceDate': TradingReferenceDate,
        'HighLimitPrice': HighLimitPrice,
        'LowLimitPrice': LowLimitPrice,
        'MaxPriceVariation': MaxPriceVariation,
        'MinPriceIncrementAmount': MinPriceIncrementAmount,
        'IssueDate': IssueDate,
        'DatedDate': DatedDate,
        'MaturityDate': MaturityDate,
        'CouponRate': CouponRate,
        'ParValue': ParValue,
        'CouponFrequencyUnit': byte_to_str(CouponFrequencyUnit),
        'CouponFrequencyPeriod': CouponFrequencyPeriod,
        'CouponDayCount': byte_to_str(CouponDayCount),
        'CountryOfIssue':  byte_to_str(CountryOfIssue),
        'Issuer':  byte_to_str(Issuer),
        'FinancialInstrumentFullName':  byte_to_str(FinancialInstrumentFullName),
        'SecurityAltID':  byte_to_str(SecurityAltID),
        'SecurityAltIDSource': SecurityAltIDSource,
        'PriceQuoteMethod': byte_to_str(PriceQuoteMethod),
        'PartyRoleClearingOrg': byte_to_str(PartyRoleClearingOrg),
        'UserDefinedInstrument': byte_to_str(UserDefinedInstrument),
        'RiskSet': byte_to_str(RiskSet),
        'MarketSet': byte_to_str(MarketSet),
        'InstrumentGUID': InstrumentGUID
    }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (EventType, EventTime) = struct.unpack(
                '<BQ',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'EventType': EventType,
                           'EventTime': EventTime}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDFeedType, MarketDepth) = struct.unpack(
                '<3sb',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDFeedType': byte_to_str(MDFeedType),
                           'MarketDepth': MarketDepth}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (InstAttribValue) = struct.unpack(
                '<I',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {
                'InstAttribValue': InstAttribValue[0]}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (LotType, MinLotSize) = struct.unpack(
                '<bi',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'LotType': LotType,
                           'MinLotSize': MinLotSize}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDInstrumentDefinitionRepo58(msgs_blocks, BlockLength, version, cme_packet):

    msgs_list = []

    # 255 bytes in total

    (MatchEventIndicator, TotNumReports, SecurityUpdateAction, LastUpdateTime,
     MDSecurityTradingStatus, ApplID, MarketSegmentID, UnderlyingProduct,
     SecurityExchange, SecurityGroup, Asset, Symbol,
     SecurityID, SecurityType, CFICode,
     Currency, SettlCurrency, MatchAlgorithm, MinTradeVol,
     MaxTradeVol, MinPriceIncrement, DisplayFactor, UnitOfMeasure,
     UnitOfMeasureQty, TradingReferencePrice, TradingReferenceDate, HighLimitPrice,
     LowLimitPrice, MaxPriceVariation, FinancialInstrumentFullName, PartyRoleClearingOrg,
     StartDate, EndDate, TerminationType, SecuritySubType,
     MoneyOrPar, MaxNoOfSubstitutions, PriceQuoteMethod, UserDefinedInstrument,
     RiskSet, MarketSet, InstrumentGUID) = struct.unpack(
        '<BIcQBhBB4s6s6s20si6s6s3s3scIIqq30sqqHqqq35s5sHH8sBBB5sc6s6sQ', msgs_blocks[0:255])

    if TotNumReports == 4294967295:
        TotNumReports = np.nan

    if TradingReferenceDate == 65535:
        TradingReferenceDate = np.nan

    if StartDate == 65535:
        StartDate = np.nan

    if EndDate == 65535:
        EndDate = np.nan

    if InstrumentGUID == 18446744073709551615:
        InstrumentGUID = np.nan

    info = {
        'MatchEventIndicator': bin(MatchEventIndicator),
        'TotNumReports': TotNumReports,
        'SecurityUpdateAction': byte_to_str(SecurityUpdateAction),
        'LastUpdateTime': LastUpdateTime,
        'MDSecurityTradingStatus': MDSecurityTradingStatus,
        'ApplID': ApplID,
        'MarketSegmentID': MarketSegmentID,
        'UnderlyingProduct': UnderlyingProduct,
        'SecurityExchange': byte_to_str(SecurityExchange),
        'SecurityGroup': byte_to_str(SecurityGroup),
        'Asset': byte_to_str(Asset),
        'Symbol': byte_to_str(Symbol),
        'SecurityID': SecurityID,
        'SecurityType': byte_to_str(SecurityType),
        'CFICode': byte_to_str(CFICode),
        'Currency': byte_to_str(Currency),
        'SettlCurrency':  byte_to_str(SettlCurrency),
        'MatchAlgorithm': byte_to_str(MatchAlgorithm),
        'MinTradeVol': MinTradeVol,
        'MaxTradeVol': MaxTradeVol,
        'MinPriceIncrement': MinPriceIncrement,
        'DisplayFactor': DisplayFactor,
        'UnitOfMeasure': byte_to_str(UnitOfMeasure),
        'UnitOfMeasureQty': UnitOfMeasureQty,
        'TradingReferencePrice': TradingReferencePrice,
        'TradingReferenceDate': TradingReferenceDate,
        'HighLimitPrice': HighLimitPrice,
        'LowLimitPrice': LowLimitPrice,
        'MaxPriceVariation': MaxPriceVariation,
        'FinancialInstrumentFullName': byte_to_str(FinancialInstrumentFullName),
        'PartyRoleClearingOrg':  byte_to_str(PartyRoleClearingOrg),
        'StartDate': StartDate,
        'EndDate': EndDate,
        'TerminationType': byte_to_str(TerminationType),
        'SecuritySubType': SecuritySubType,
        'MoneyOrPar': MoneyOrPar,
        'MaxNoOfSubstitutions': MaxNoOfSubstitutions,
        'PriceQuoteMethod': byte_to_str(PriceQuoteMethod),
        'UserDefinedInstrument': byte_to_str(UserDefinedInstrument),
        'RiskSet': byte_to_str(RiskSet),
        'MarketSet': byte_to_str(MarketSet),
        'InstrumentGUID': InstrumentGUID
    }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if version >= 11:

        TermCode = struct.unpack('<20s', msgs_blocks[255:275])

        info = info | {'TermCode': byte_to_str(TermCode)}

    if version >= 13:

        BrokenDateTermType = struct.unpack('<B', msgs_blocks[275:276])

        if BrokenDateTermType == 255:
            BrokenDateTermType = np.nan

        info = info | {'BrokenDateTermType': BrokenDateTermType}

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (EventType, EventTime) = struct.unpack(
                '<BQ',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'EventType': EventType,
                           'EventTime': EventTime}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDFeedType, MarketDepth) = struct.unpack(
                '<3sb',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDFeedType': byte_to_str(MDFeedType),
                           'MarketDepth': MarketDepth}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (InstAttribValue) = struct.unpack(
                '<I',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {
                'InstAttribValue': InstAttribValue[0]}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (LotType, MinLotSize) = struct.unpack(
                '<bi',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'LotType': LotType,
                           'MinLotSize': MinLotSize}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            # total byte 118

            (UnderlyingSymbol, UnderlyingSecurityID,
             UnderlyingSecurityAltID, UnderlyingSecurityAltIDSource,
             UnderlyingFinancialInstrumentFullName, UnderlyingSecurityType,
             UnderlyingCountryOfIssue, UnderlyingIssuer, UnderlyingMaxLifeTime,
             UnderlyingMinDaysToMaturity) = struct.unpack(
                '<20si12sB35s6s2s25sBH',
                msgs_blocks[pos:(pos+108)]
            )

            if UnderlyingSecurityID == 2147483647:
                UnderlyingSecurityID = np.nan

            if UnderlyingMinDaysToMaturity == 65535:
                UnderlyingMinDaysToMaturity = np.nan

            if UnderlyingMaxLifeTime == 255:
                UnderlyingMaxLifeTime = np.nan

            msgs = info | {'UnderlyingSymbol': byte_to_str(UnderlyingSymbol),
                           'UnderlyingSecurityID': UnderlyingSecurityID,
                           'UnderlyingSecurityAltID': byte_to_str(UnderlyingSecurityAltID),
                           'UnderlyingSecurityAltIDSource': UnderlyingSecurityAltIDSource,
                           'UnderlyingFinancialInstrumentFullName': byte_to_str(UnderlyingFinancialInstrumentFullName),
                           'UnderlyingSecurityType': byte_to_str(UnderlyingSecurityType),
                           'UnderlyingCountryOfIssue': byte_to_str(UnderlyingCountryOfIssue),
                           'UnderlyingIssuer': byte_to_str(UnderlyingIssuer),
                           'UnderlyingMaxLifeTime': UnderlyingMaxLifeTime,
                           'UnderlyingMinDaysToMaturity': UnderlyingMinDaysToMaturity
                           }

            msgs_list.append(msgs)

            pos += 108

            if version >= 11:

                (UnderlyingInstrumentGUID, UnderlyingMaturityDate) = struct.unpack(
                    '<QH', msgs_blocks[pos:BlockLength])

                if UnderlyingMaturityDate == 65535:
                    UnderlyingMaturityDate = np.nan

                if UnderlyingInstrumentGUID == 18446744073709551615:
                    UnderlyingInstrumentGUID = np.nan

                msgs = msgs | {'UnderlyingInstrumentGUID': UnderlyingInstrumentGUID,
                               'UnderlyingMaturityDate': UnderlyingMaturityDate}

                msgs_list.append(msgs)

                pos += 10

            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (RelatedSecurityID, RelatedSymbol, RelatedInstrumentGUID) = struct.unpack(
                '<i20sQ', msgs_blocks[pos:(pos+BlockLength)])

            if RelatedInstrumentGUID == 18446744073709551615:
                RelatedInstrumentGUID = np.nan

            msgs = info | {'RelatedSecurityID': RelatedSecurityID,
                           'RelatedSymbol': byte_to_str(RelatedSymbol),
                           'RelatedInstrumentGUID': RelatedInstrumentGUID}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        if version >= 13:

            group_length = struct.unpack(
                '<H', msgs_blocks[pos:(pos+2)])[0]

            pos += 2

            NumInGroup = struct.unpack(
                '<B', msgs_blocks[pos:(pos+1)])[0]

            pos += 1
            group_repeat = 0

            while group_repeat < NumInGroup:

                (BrokenDateGUID, BrokenDateSecurityID, BrokenDateStart, BrokenDateEnd) = struct.unpack(
                    '<QiHH', msgs_blocks[pos:(pos+BlockLength)])

                if BrokenDateStart == 65535:
                    BrokenDateStart = np.nan

                if BrokenDateEnd == 65535:
                    BrokenDateEnd = np.nan

                msgs = msgs | {'BrokenDateGUID': BrokenDateGUID,
                               'BrokenDateSecurityID': BrokenDateSecurityID,
                               'BrokenDateStart': BrokenDateStart,
                               'BrokenDateEnd': BrokenDateEnd}

                msgs_list.append(msgs)

                pos += group_length
                group_repeat += 1

    return msgs_list


def SnapshotRefreshTopOrders59(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator, SecurityID) = struct.unpack(
        '<QBi', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(MatchEventIndicator),
            'SecurityID': SecurityID}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (OrderID, MDOrderPriority, MDEntryPx, MDDisplayQty, MDEntryType) = struct.unpack(
                '<QQqic',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'OrderID': OrderID,
                           'MDOrderPriority': MDOrderPriority,
                           'MDEntryPx': MDEntryPx,
                           'MDDisplayQty': MDDisplayQty,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def SecurityStatusWorkup60(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MDEntryPx, SecurityID, MatchEventIndicator, TradeDate,
     TradeLinkID, SecurityTradingStatus, HaltReason, SecurityTradingEvent) = struct.unpack(
         '<QqiBHIBBB', msgs_blocks[0:BlockLength])

    if TradeDate == 65535:
        TradeDate = np.nan

    info = {'TransactTime': TransactTime,
            'MDEntryPx': MDEntryPx,
            'SecurityID': SecurityID,
            'MatchEventIndicator': bin(MatchEventIndicator),
            'TradeDate': TradeDate,
            'TradeLinkID': TradeLinkID,
            'SecurityTradingStatus': SecurityTradingStatus,
            'HaltReason': HaltReason,
            'SecurityTradingEvent': SecurityTradingEvent
            }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (OrderID, Side, AggressorIndicator) = struct.unpack(
                '<QBB',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'OrderID': OrderID,
                           'Side': Side,
                           'AggressorIndicator': AggressorIndicator
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def SnapshotFullRefreshTCP61(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator, SecurityID, HighLimitPrice,
     LowLimitPrice, MaxPriceVariation) = struct.unpack('<QBiqqq', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(MatchEventIndicator),
            'SecurityID': SecurityID,
            'HighLimitPrice': HighLimitPrice,
            'LowLimitPrice': LowLimitPrice,
            'MaxPriceVariation': MaxPriceVariation}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, TradeableSize,
             NumberOfOrders, MDPriceLevel, OpenCloseSettlFlag,
             MDEntryType, TradingReferenceDate, SettlPriceType) = struct.unpack(
                '<qiiibBcHB',
                msgs_blocks[pos:(pos+group_length)]
            )

            if MDPriceLevel == 127:
                MDPriceLevel = np.nan

            if MDEntrySize == 2147483647:
                MDEntrySize = np.nan

            if TradeableSize == 2147483647:
                TradeableSize = np.nan

            if NumberOfOrders == 2147483647:
                NumberOfOrders = np.nan

            if TradingReferenceDate == 65535:
                TradingReferenceDate = np.nan

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'MDEntrySize': MDEntrySize,
                           'TradeableSize': TradeableSize,
                           'NumberOfOrders': NumberOfOrders,
                           'MDPriceLevel': OpenCloseSettlFlag,
                           'MDEntryType': byte_to_str(
                               MDEntryType),
                           'TradingReferenceDate': TradingReferenceDate,
                           'SettlPriceType': SettlPriceType
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def CollateralMarketValue62(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (UnderlyingSecurityAltID, UnderlyingSecurityAltIDSource,
             CollateralMarketPrice, DirtyPrice, UnderlyingInstrumentGUID,
             MDStreamID) = struct.unpack(
                '<12sBqqQ3s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'UnderlyingSecurityAltID': byte_to_str(UnderlyingSecurityAltID),
                           'UnderlyingSecurityAltIDSource': UnderlyingSecurityAltIDSource,
                           'CollateralMarketPrice': CollateralMarketPrice,
                           'DirtyPrice': DirtyPrice,
                           'UnderlyingInstrumentGUID': UnderlyingInstrumentGUID,
                           'MDStreamID{group_repeat}': byte_to_str(MDStreamID)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDInstrumentDefinitionFX63(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (MatchEventIndicator, TotNumReports, SecurityUpdateAction, LastUpdateTime,
        MDSecurityTradingStatus, ApplID, MarketSegmentID, UnderlyingProduct,
        SecurityExchange, SecurityGroup, Asset, Symbol,
        SecurityID, SecurityType, CFICode,
        Currency, SettlCurrency, PriceQuoteCurrency, MatchAlgorithm,
        MinTradeVol, MaxTradeVol, MinPriceIncrement, DisplayFactor,
        PricePrecision, UnitOfMeasure, UnitOfMeasureQty, HighLimitPrice,
        LowLimitPrice, MaxPriceVariation, UserDefinedInstrument, FinancialInstrumentFullName,
        FXCurrencySymbol, SettlType, InterveningDays, FXBenchmarkRateFix,
        RateSource, FixRateLocalTime, FixRateLocalTimeZone, MinQuoteLife,
        MaxPriceDiscretionOffset, InstrumentGUID, MaturityMonthYear, SettlementLocale,
        AltMinPriceIncrement, AltMinQuoteLife, AltPriceIncrementConstraint,
        MaxBidAskConstraint) = struct.unpack(
            '<BIcQBhBB4s6s6s20si6s6s3s3s3scIIqqB30sqqqqc35s7s3sH20s12s8s20sIqQ5s8sqIqq', msgs_blocks[0:BlockLength])

    if TotNumReports == 4294967295:
        TotNumReports = np.nan

    if AltMinQuoteLife == 4294967295:
        AltMinQuoteLife = np.nan

    if InstrumentGUID == 18446744073709551615:
        InstrumentGUID = np.nan

    info = {
        'MatchEventIndicator': bin(MatchEventIndicator),
        'TotNumReports': TotNumReports,
        'SecurityUpdateAction': byte_to_str(SecurityUpdateAction),
        'LastUpdateTime': LastUpdateTime,
        'MDSecurityTradingStatus': MDSecurityTradingStatus,
        'ApplID': ApplID,
        'MarketSegmentID': MarketSegmentID,
        'UnderlyingProduct': UnderlyingProduct,
        'SecurityExchange': byte_to_str(SecurityExchange),
        'SecurityGroup': byte_to_str(SecurityGroup),
        'Asset': byte_to_str(Asset),
        'Symbol': byte_to_str(Symbol),
        'SecurityID': SecurityID,
        'SecurityType': byte_to_str(SecurityType),
        "CFICode": byte_to_str(CFICode),
        "Currency": byte_to_str(Currency),
        'SettlCurrency': byte_to_str(SettlCurrency),
        'PriceQuoteCurrency': byte_to_str(PriceQuoteCurrency),
        'MatchAlgorithm': byte_to_str(MatchAlgorithm),
        'MinTradeVol': MinTradeVol,
        'MaxTradeVol': MaxTradeVol,
        'MinPriceIncrement': MinPriceIncrement,
        'DisplayFactor': DisplayFactor,
        'PricePrecision': PricePrecision,
        'UnitOfMeasure': byte_to_str(UnitOfMeasure),
        'UnitOfMeasureQty': UnitOfMeasureQty,
        'HighLimitPrice': HighLimitPrice,
        'LowLimitPrice': LowLimitPrice,
        'MaxPriceVariation': MaxPriceVariation,
        'UserDefinedInstrument': byte_to_str(UserDefinedInstrument),
        'FinancialInstrumentFullName':  byte_to_str(FinancialInstrumentFullName),
        'FXCurrencySymbol': byte_to_str(FXCurrencySymbol),
        'SettlType': byte_to_str(SettlType),
        'FXBenchmarkRateFix': byte_to_str(FXBenchmarkRateFix),
        'RateSource': byte_to_str(RateSource),
        'FixRateLocalTime': byte_to_str(FixRateLocalTime),
        'FixRateLocalTimeZone': byte_to_str(FixRateLocalTimeZone),
        'MinQuoteLife': MinQuoteLife,
        'MaxPriceDiscretionOffset': MaxPriceDiscretionOffset,
        'InstrumentGUID': InstrumentGUID,
        'MaturityMonthYear': byte_to_int(MaturityMonthYear),
        'SettlementLocale': byte_to_str(SettlementLocale),
        'AltMinPriceIncrement': AltMinPriceIncrement,
        'AltMinQuoteLife': AltMinQuoteLife,
        'AltPriceIncrementConstraint': AltPriceIncrementConstraint,
        'MaxBidAskConstraint': MaxBidAskConstraint
    }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (EventType, EventTime) = struct.unpack(
                '<BQ',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'EventType': EventType,
                           'EventTime': EventTime}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDFeedType, MarketDepth) = struct.unpack(
                '<3sb',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDFeedType': byte_to_str(MDFeedType),
                           'MarketDepth': MarketDepth}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (InstAttribValue) = struct.unpack(
                '<I',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {
                'InstAttribValue': InstAttribValue[0]}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            (LotType, MinLotSize) = struct.unpack(
                '<bQ',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'LotType': LotType,
                           'MinLotSize': MinLotSize}

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1
        group_repeat = 0

        while group_repeat < NumInGroup:

            # No trading sessions
            # total byte 118

            (TradeDate, SettlDate, MaturityDate, SecurityAltID) = struct.unpack(
                '<HHH12s',
                msgs_blocks[pos:(pos+group_length)]
            )

            if TradeDate == 65535:
                TradeDate = np.nan

            if SettlDate == 65535:
                SettlDate = np.nan

            if MaturityDate == 65535:
                MaturityDate = np.nan

            msgs = info | {'TradeDate': TradeDate,
                           'SettlDate': SettlDate,
                           'MaturityDate': MaturityDate,
                           'SecurityAltID': byte_to_str(SecurityAltID)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshBookLongQty64(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        'Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        end_group = pos + group_length*NumInGroup

        group_repeat = 0

        # since there is reference ID so we need to make sure
        # the order and MBP parts are connected correctly

        MBP = []

        if struct.unpack(
                '<B', msgs_blocks[(end_group+7):(end_group+8)])[0] != 0:

            msgappend = False

        else:

            msgappend = True

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, SecurityID, RptSeq, NumberOfOrders,
             MDPriceLevel, MDUpdateAction, MDEntryType) = struct.unpack(
                '<qQiIiBB2s',
                msgs_blocks[pos:(pos+group_length)]
            )

            if NumberOfOrders == 2147483647:
                NumberOfOrders = np.nan

            if MDEntrySize == 18446744073709551615:
                MDEntrySize = np.nan

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'MDEntrySize': MDEntrySize,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'NumberOfOrders': NumberOfOrders,
                           'MDPriceLevel': MDPriceLevel,
                           'MDUpdateAction': MDUpdateAction,
                           'MDEntryType': byte_to_str(MDEntryType)
                           }

            MBP.append(msgs)

            # when there is MBO information, we do not need to append at this step

            if msgappend:
                msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 7

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[(pos):(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (OrderID, MDOrderPriority, MDDisplayQty, ReferenceID, OrderUpdateAction) = struct.unpack(
                '<QQiB3s',
                msgs_blocks[pos:(pos+group_length)]
            )

            if MDDisplayQty == 2147483647:
                MDDisplayQty = np.nan

            if MDOrderPriority == 18446744073709551615:
                MDOrderPriority = np.nan

            if ReferenceID == 255:
                ReferenceID = np.nan

            try:
                msgs

            except:

                msgs = info | {'OrderID': OrderID,
                               'MDOrderPriority': MDOrderPriority,
                               'MDDisplayQty': MDDisplayQty,
                               'ReferenceID': ReferenceID,
                               'OrderUpdateAction': byte_to_str(OrderUpdateAction)
                               }

            else:

                msgs = MBP[ReferenceID-1] | {'OrderID': OrderID,
                                             'MDOrderPriority': MDOrderPriority,
                                             'MDDisplayQty': MDDisplayQty,
                                             'ReferenceID': ReferenceID,
                                             'OrderUpdateAction': byte_to_str(OrderUpdateAction)
                                             }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshTradeSummaryLongQty65(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, SecurityID, RptSeq, NumberOfOrders,
             MDTradeEntryID, AggressorSide, MDUpdateAction) = struct.unpack(
                '<qQiIiI8s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {'MDEntryPx': MDEntryPx,
                           'MDEntrySize': MDEntrySize,
                           'SecurityID': SecurityID,
                           'RptSeq': RptSeq,
                           'NumberOfOrders': NumberOfOrders,
                           'MDTradeEntryID': MDTradeEntryID,
                           'AggressorSide': AggressorSide,
                           'MDUpdateAction': byte_to_str(MDUpdateAction)
                           }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 7

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[(pos):(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (OrderID, LastQty) = struct.unpack(
                '<Q8s',
                msgs_blocks[pos:(pos+group_length)]
            )

            try:
                msgs

            except:

                msgs = info | {'OrderID': OrderID,
                               'LastQty': byte_to_str(LastQty)
                               }

            else:

                msgs = msgs | {'OrderID': OrderID,
                               'LastQty': byte_to_str(LastQty)
                               }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def MDIncrementalRefreshVolumeLongQty66(msgs_blocks, BlockLength, cme_packet):

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(byte_to_int(MatchEventIndicator))}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntrySize, SecurityID, RptSeq, MDUpdateAction) = struct.unpack(
                '<QiI8s',
                msgs_blocks[pos:(pos+group_length)]
            )

            msgs = info | {
                'MDEntrySize': MDEntrySize,
                'SecurityID': SecurityID,
                'RptSeq': RptSeq,
                'MDUpdateAction': byte_to_str(MDUpdateAction)
            }

            pos += group_length
            group_repeat += 1

    return msgs


def MDIncrementalRefreshSessionStatisticsLongQty67(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator) = struct.unpack(
        '<Q3s', msgs_blocks[0:BlockLength])

    info = {'TransactTime': TransactTime,
            'MatchEventIndicator': bin(MatchEventIndicator)}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, SecurityID, RptSeq, OpenCloseSettlFlag,
             MDUpdateAction, MDEntryType) = struct.unpack(
                '<qQiIBB6s',
                msgs_blocks[pos:(pos+group_length)]
            )

            if MDEntrySize == 18446744073709551615:
                MDEntrySize = np.nan

            msgs = info | {
                'MDEntryPx': MDEntryPx,
                'MDEntrySize': MDEntrySize,
                'SecurityID': RptSeq,
                'RptSeq': RptSeq,
                'OpenCloseSettlFlag': OpenCloseSettlFlag,
                'MDUpdateAction': MDUpdateAction,
                'MDEntryType': byte_to_str(MDEntryType),
            }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def SnapshotFullRefreshTCPLongQty68(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (TransactTime, MatchEventIndicator, SecurityID,
     HighLimitPrice, LowLimitPrice, MaxPriceVariation) = struct.unpack('<QBiqqq', msgs_blocks[0:BlockLength])

    info = {
        'TransactTime': TransactTime,
        'MatchEventIndicator': bin(MatchEventIndicator),
        'SecurityID': SecurityID,
        'HighLimitPrice': HighLimitPrice,
        'LowLimitPrice': LowLimitPrice,
        'MaxPriceVariation': MaxPriceVariation,
    }

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, NumberOfOrders, MDPriceLevel,
             OpenCloseSettlFlag, MDEntryType) = struct.unpack(
                '<qQiBBc',
                msgs_blocks[pos:(pos+group_length)]
            )

            if NumberOfOrders == 2147483647:
                NumberOfOrders = np.nan

            if MDEntrySize == 18446744073709551615:
                MDEntrySize = np.nan

            if MDPriceLevel == 255:
                MDPriceLevel = np.nan

            msgs = info | {
                'MDEntryPx': MDEntryPx,
                'MDEntrySize': MDEntrySize,
                'NumberOfOrders': NumberOfOrders,
                'MDPriceLevel': MDPriceLevel,
                'OpenCloseSettlFlag': OpenCloseSettlFlag,
                'MDEntryType': byte_to_str(MDEntryType),
            }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list


def SnapshotFullRefreshLongQty69(msgs_blocks, BlockLength, cme_packet):

    msgs_list = []

    (LastMsgSeqNumProcessed, TotNumReports, SecurityID, RptSeq,
     TransactTime, LastUpdateTime, TradeDate, MDSecurityTradingStatus,
     HighLimitPrice, LowLimitPrice, MaxPriceVariation) = struct.unpack('<IIiIQQHBqqq', msgs_blocks[0:BlockLength])

    if TradeDate == 65535:
        TradeDate = np.nan

    info = {'LastMsgSeqNumProcessed': LastMsgSeqNumProcessed,
            'TotNumReports': TotNumReports,
            'SecurityID': SecurityID,
            'RptSeq': RptSeq,
            'TransactTime': TransactTime,
            'LastUpdateTime': LastUpdateTime,
            'TradeDate': TradeDate,
            'MDSecurityTradingStatus': MDSecurityTradingStatus,
            'HighLimitPrice': HighLimitPrice,
            'LowLimitPrice': LowLimitPrice,
            'MaxPriceVariation': MaxPriceVariation}

    if not isinstance(cme_packet, bool):

        info = cme_packet | info

    if len(msgs_blocks) > BlockLength:

        pos = BlockLength

        group_length = struct.unpack(
            '<H', msgs_blocks[pos:(pos+2)])[0]

        pos += 2

        NumInGroup = struct.unpack(
            '<B', msgs_blocks[pos:(pos+1)])[0]

        pos += 1

        group_repeat = 0

        while group_repeat < NumInGroup:

            (MDEntryPx, MDEntrySize, NumberOfOrders, MDPriceLevel,
             OpenCloseSettlFlag, MDEntryType) = struct.unpack(
                '<qQiBBc',
                msgs_blocks[pos:(pos+group_length)]
            )

            if MDEntrySize == 18446744073709551615:
                MDEntrySize = np.nan

            if MDPriceLevel == 255:
                MDPriceLevel = np.nan

            msgs = info | {
                'MDEntryPx': MDEntryPx,
                'MDEntrySize': MDEntrySize,
                'NumberOfOrders': RptSeq,
                'MDPriceLevel': RptSeq,
                'OpenCloseSettlFlag': OpenCloseSettlFlag,
                'MDEntryType': byte_to_str(MDEntryType),
            }

            msgs_list.append(msgs)

            pos += group_length
            group_repeat += 1

    return msgs_list
