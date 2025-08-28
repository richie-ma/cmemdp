# -*- coding: utf-8 -*-
"""
CME Packet Capture Data Decoder
## This decoder is only used for Packet Capture data directly bought from
## CME Datamine. This dataset is not a standard PCAP 

## Information from the CME Website

CME uses the simple binary encoding
Little-endian byte ordering

Schema Versioning
The templates.xml file is versioned each time an update is made.
All elements in a message schema are of the same version.
The first version of a schema is version zero, and the version number is incremented each time a schema is changed.
The “sinceVersion” attribute indicates when a template is extended.

MDP Packet Structure
The encoded FIX transmission is sent in a packet structured as follows:
Packet header - contains packet sequence number, sending time
Message Size - field indicating size of message
Message header - contains message size, templateID, schemaID, and version
FIX Message - contains FIX header and FIX message
MDP Message - contains message header and FIX updates such as book updates and trades

Packet header - contains packet sequence number, sending time
Message Size - field indicating size of message
Message header - contains message size, templateID, schemaID, and version
FIX Message - contains FIX header and FIX message
MDP Message - contains message header and FIX updates such as book updates and trades

PCAP sepcifcatio refers to the XML templates
##
<ns2:messageSchema ... description="20230411" byteOrder="littleEndian"...">
description means the date when the format has been modified
"""


from . import main_template
from tabulate import tabulate
from pandas import isnull, notnull
import pandas as pd
import struct
import numpy as np
from tqdm import tqdm
import os
from itertools import chain, islice
import pickle


def cme_parser_datamine(path, max_read_packets=None, msgs_template=None, cme_header=True,
                        save_file_path=None, disable_progress_bar=False, chunk_size=5000):
    """
    `cme_parser_datamine` is a binary pacaket capture (PCAP) data parser for 
    market data obtained from the Chicago Mercantile Exchange (CME) Datamine.
    PCAP data from the CME Datamine does not follow standard PCAP data format,
    e.g., packet headers, UDP, payloads. It remains the main part of the actual
    PCAP data and an overall data structure can be found as follows:

    Channel | Length|Sequence | Time | Message Size | Block Length | Template ID | Schemal ID | Version | FIX header | FIX Message Body |
    2 bytes  2 bytes| (Packet Header)|   (2 bytes)  |........(Simplie Binary Header, 8 bytes).......... |.......(FIX message)...........|
                    | (12 bytes)     |..........................(message header)........................|
                                     |.................................... MDP messages.................................................|

    Parameters
    ----------
    path : str
        The path of the raw data file.
    max_read_packets : int, optional
        The maximum number of packaets need to be processed, if None, 
        all packets are read. The default is None.
    msgs_template : list, optional
        Types of messages need to be returned. CME provides 
        numbers to different message templates, e.g., the channel reset messages
        are marked as 4. Users need to give the template numbers into a list. If None,
        all messages are returned. The default is None.
    cme_header : bool, optional
        Whether to parser the packet header, which includes the
        message sequence number and sending timestamps. The default is True.
    save_file_path : str, optional
        The path for the saving file. The default is None.
    disable_progress_bar : bool, optional
        Whether to disable the progress bar. The default is False.
    chunk_size : int
        The chunk size that needs to be saved.

    """

    # you could only return the messages you want
    # users need to give the template IDs of that messages

    template_id = [4, 16, 27, 29, 30, 32, 33, 34, 35, 36, 37, 38, 39, 41, 42, 43, 44, 46, 47, 48,
                   49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69]

    msgs_ChannelReset4 = []
    msgs_AdminLogout16 = []
    msgs_MDInstrumentDefinitionFuture27 = []
    msgs_MDInstrumentDefinitionSpread29 = []
    msgs_SecurityStatus30 = []
    msgs_MDIncrementalRefreshBook32 = []
    msgs_MDIncrementalRefreshDailyStatistics33 = []
    msgs_MDIncrementalRefreshLimitsBanding34 = []
    msgs_MDIncrementalRefreshSessionStatistics35 = []
    msgs_MDIncrementalRefreshTrade36 = []
    msgs_MDIncrementalRefreshVolume37 = []
    msgs_SnapshotFullRefresh38 = []
    msgs_QuoteRequest39 = []
    msgs_MDInstrumentDefinitionOption41 = []
    msgs_MDIncrementalRefreshTradeSummary42 = []
    msgs_MDIncrementalRefreshOrderBook43 = []
    msgs_SnapshotFullRefreshOrderBook44 = []
    msgs_MDIncrementalRefreshBook46 = []
    msgs_MDIncrementalRefreshOrderBook47 = []
    msgs_MDIncrementalRefreshTradeSummary48 = []
    msgs_MDIncrementalRefreshDailyStatistics49 = []
    msgs_MDIncrementalRefreshLimitsBanding50 = []
    msgs_MDIncrementalRefreshSessionStatistics51 = []
    msgs_SnapshotFullRefresh52 = []
    msgs_SnapshotFullRefreshOrderBook53 = []
    msgs_MDInstrumentDefinitionFuture54 = []
    msgs_MDInstrumentDefinitionOption55 = []
    msgs_MDInstrumentDefinitionSpread56 = []
    msgs_MDInstrumentDefinitionFixedIncome57 = []
    msgs_MDInstrumentDefinitionRepo58 = []
    msgs_SnapshotRefreshTopOrders59 = []
    msgs_SecurityStatusWorkup60 = []
    msgs_SnapshotFullRefreshTCP61 = []
    msgs_CollateralMarketValue62 = []
    msgs_MDInstrumentDefinitionFX63 = []
    msgs_MDIncrementalRefreshBookLongQty64 = []
    msgs_MDIncrementalRefreshTradeSummaryLongQty65 = []
    msgs_MDIncrementalRefreshVolumeLongQty66 = []
    msgs_MDIncrementalRefreshSessionStatisticsLongQty67 = []
    msgs_SnapshotFullRefreshTCPLongQty68 = []
    msgs_SnapshotFullRefreshLongQty69 = []

    chunk_index4 = 1
    chunk_index16 = 1
    chunk_index27 = 1
    chunk_index29 = 1
    chunk_index30 = 1
    chunk_index32 = 1
    chunk_index33 = 1
    chunk_index34 = 1
    chunk_index35 = 1
    chunk_index36 = 1
    chunk_index37 = 1
    chunk_index38 = 1
    chunk_index39 = 1
    chunk_index41 = 1
    chunk_index42 = 1
    chunk_index43 = 1
    chunk_index44 = 1
    chunk_index46 = 1
    chunk_index47 = 1
    chunk_index48 = 1
    chunk_index49 = 1
    chunk_index50 = 1
    chunk_index51 = 1
    chunk_index52 = 1
    chunk_index53 = 1
    chunk_index54 = 1
    chunk_index55 = 1
    chunk_index56 = 1
    chunk_index57 = 1
    chunk_index58 = 1
    chunk_index59 = 1
    chunk_index60 = 1
    chunk_index61 = 1
    chunk_index62 = 1
    chunk_index63 = 1
    chunk_index64 = 1
    chunk_index65 = 1
    chunk_index66 = 1
    chunk_index67 = 1
    chunk_index68 = 1
    chunk_index69 = 1

    if isnull(max_read_packets):
        print('maximum number of packets read does not provide. Read the whole file by default')
        max_read = os.path.getsize(path)
        print(f'Read total bytes: {max_read}')

    else:
        max_read = max_read_packets  # excluding the global header
        print(f'Read maximum number of packets {max_read_packets}')

    if notnull(max_read_packets):
        unit = 'packets'
    else:
        unit = 'bytes'

    # check whether the file is gzip

    with open(path, 'rb') as f:
        magic = f.read(2)

        if magic == b'\x1f\x8b':

            raise Exception(
                'Compressed file is not supported. Please use the uncompressed file')

    with open(path, "rb") as f:

        with tqdm(total=max_read, desc="Reading", ncols=100, unit=unit,
                  disable=disable_progress_bar) as pbar:

            read = 0

            # total should be set correctly, otherwise the progress bar can't be shown correctly
            # Directly set the bytes maximum read would not be exactly eqaul to what you set
            # I define the maximum number of messages read

            # skip the packet header -- 16 bytes
            # find the packet length

            end_pos = 0

            while read < max_read:

                (Channel, message_length) = struct.unpack('<HH', f.read(4))

                end_pos += (message_length + 4)

                if isnull(max_read_packets):
                    read = 4
                    pbar.update(4)

                # binary packet header

                if cme_header:

                    (MsgSeq, SendingTime) = struct.unpack('<IQ', f.read(12))
                    cme_packet = {'MsgSeq': MsgSeq,
                                  'SendingTime': SendingTime}

                else:

                    f.read(12)
                    cme_packet = False

                # parse message header
                """
                #---------------------------------------------------------------------
                Name        |  Type  |Description
                ----------------------------------------------------------------------
                MsgSize     | uInt16 |Length of entire message, including binary header
                            |        |in number of bytes
                ----------------------------------------------------------------------
                BlockLength | uInt16 |Length of the root of the FIX message contained
                            |        |before repeating groups or ariable/conditions fields
                ----------------------------------------------------------------------
                TemplateID  | uInt16 |Template ID used to encode the message
                ----------------------------------------------------------------------
                SchemaID    | uInt16 |ID of the system publishing the message
                ----------------------------------------------------------------------
                Version     | uInt16 |Schema version
                ----------------------------------------------------------------------
                """

                (MsgSize, BlockLength, TemplateID, SchemaID, Version) = struct.unpack(
                    '<HHHHH', f.read(10))

                # print

                # MsgSize =12, which is heartbeat
                # No action needed

                # print(TemplateID, SchemaID, Version)

                # One needs to find the template ID, Schema ID in a XML file of the correct version

                if BlockLength > 0:

                    # should be 16
                    messages = f.read(MsgSize-10)

                    # guding to the signle message

                    if TemplateID == 4:

                        if (msgs_template is None) or (4 in msgs_template):
                            msgs_ChannelReset4.append(main_template.ChannelReset4(
                                messages, BlockLength, cme_packet))

                        if len(msgs_ChannelReset4) >= chunk_size:

                            msgs_ChannelReset4 = pd.DataFrame(
                                chain.from_iterable(msgs_ChannelReset4))
                            msgs_ChannelReset4.to_pickle(
                                f"{save_file_path}/{msgs_ChannelReset4}_{chunk_index4}.pkl")
                            chunk_index4 += 1
                            msgs_ChannelReset4 = []

                    elif TemplateID == 16:

                        if (msgs_template is None) or (16 in msgs_template):
                            msgs_AdminLogout16.append(main_template.AdminLogout16(
                                messages, BlockLength, cme_packet))

                        if len(msgs_AdminLogout16) >= chunk_size:

                            msgs_AdminLogout16 = pd.DataFrame(
                                chain.from_iterable(msgs_AdminLogout16))
                            msgs_AdminLogout16.to_pickle(
                                f"{save_file_path}/{msgs_AdminLogout16}_{chunk_index16}.pkl")
                            chunk_index16 += 1
                            msgs_AdminLogout16 = []

                    elif TemplateID == 27:

                        if (msgs_template is None) or (27 in msgs_template):
                            msgs_MDInstrumentDefinitionFuture27.append(
                                main_template.MDInstrumentDefinitionFuture27(messages, BlockLength, Version, cme_packet))

                        if len(msgs_MDInstrumentDefinitionFuture27) >= chunk_size:

                            msgs_MDInstrumentDefinitionFuture27 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionFuture27))
                            msgs_MDInstrumentDefinitionFuture27.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionFuture27}_{chunk_index27}.pkl")
                            chunk_index27 += 1
                            msgs_MDInstrumentDefinitionFuture27 = []

                    elif TemplateID == 29:

                        if (msgs_template is None) or (29 in msgs_template):
                            msgs_MDInstrumentDefinitionSpread29.append(
                                main_template.MDInstrumentDefinitionSpread29(messages, BlockLength, Version, cme_packet))

                        if len(msgs_MDInstrumentDefinitionSpread29) >= chunk_size:

                            msgs_MDInstrumentDefinitionSpread29 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionSpread29))
                            msgs_MDInstrumentDefinitionSpread29.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionSpread29}_{chunk_index29}.pkl")
                            chunk_index29 += 1
                            msgs_MDInstrumentDefinitionSpread29 = []

                    elif TemplateID == 30:

                        if (msgs_template is None) or (30 in msgs_template):
                            msgs_SecurityStatus30.append(
                                main_template.SecurityStatus30(messages, BlockLength, cme_packet))

                        if len(msgs_SecurityStatus30) >= chunk_size:

                            msgs_SecurityStatus30 = pd.DataFrame(
                                chain.from_iterable(msgs_SecurityStatus30))
                            msgs_SecurityStatus30.to_pickle(
                                f"{save_file_path}/{msgs_SecurityStatus30}_{chunk_index30}.pkl")
                            chunk_index30 += 1
                            msgs_SecurityStatus30 = []

                    elif TemplateID == 32:

                        if (msgs_template is None) or (32 in msgs_template):
                            msgs_MDIncrementalRefreshBook32.append(
                                main_template.MDIncrementalRefreshBook32(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshBook32) >= chunk_size:

                            msgs_MDIncrementalRefreshBook32 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshBook32))
                            msgs_MDIncrementalRefreshBook32.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshBook32}_{chunk_index32}.pkl")
                            chunk_index32 += 1
                            msgs_MDIncrementalRefreshBook32 = []

                    elif TemplateID == 33:

                        if (msgs_template is None) or (33 in msgs_template):
                            msgs_MDIncrementalRefreshDailyStatistics33.append(
                                main_template.MDIncrementalRefreshDailyStatistics33(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshDailyStatistics33) >= chunk_size:

                            msgs_MDIncrementalRefreshDailyStatistics33 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshDailyStatistics33))
                            msgs_MDIncrementalRefreshDailyStatistics33.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshDailyStatistics33}_{chunk_index33}.pkl")
                            chunk_index33 += 1
                            msgs_MDIncrementalRefreshDailyStatistics33 = []

                    elif TemplateID == 34:

                        if (msgs_template is None) or (34 in msgs_template):
                            msgs_MDIncrementalRefreshLimitsBanding34.append(
                                main_template.MDIncrementalRefreshLimitsBanding34(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshLimitsBanding34) >= chunk_size:

                            msgs_MDIncrementalRefreshLimitsBanding34 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshLimitsBanding34))
                            msgs_MDIncrementalRefreshLimitsBanding34.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshLimitsBanding34}_{chunk_index34}.pkl")
                            chunk_index34 += 1
                            msgs_MDIncrementalRefreshLimitsBanding34 = []

                    elif TemplateID == 35:

                        if (msgs_template is None) or (35 in msgs_template):
                            msgs_MDIncrementalRefreshSessionStatistics35.append(
                                main_template.MDIncrementalRefreshSessionStatistics35(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshSessionStatistics35) >= chunk_size:

                            msgs_MDIncrementalRefreshSessionStatistics35 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshSessionStatistics35))
                            msgs_MDIncrementalRefreshSessionStatistics35.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshSessionStatistics35}_{chunk_index35}.pkl")
                            chunk_index35 += 1
                            msgs_MDIncrementalRefreshSessionStatistics35 = []

                    elif TemplateID == 36:

                        if (msgs_template is None) or (36 in msgs_template):
                            msgs_MDIncrementalRefreshTrade36.append(
                                main_template.MDIncrementalRefreshTrade36(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshTrade36) >= chunk_size:

                            msgs_MDIncrementalRefreshTrade36 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshTrade36))
                            msgs_MDIncrementalRefreshTrade36.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshTrade36}_{chunk_index36}.pkl")
                            chunk_index36 += 1
                            msgs_MDIncrementalRefreshTrade36 = []

                    elif TemplateID == 37:

                        if (msgs_template is None) or (37 in msgs_template):
                            msgs_MDIncrementalRefreshVolume37.append(
                                main_template.MDIncrementalRefreshVolume37(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshVolume37) >= chunk_size:

                            msgs_MDIncrementalRefreshVolume37 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshVolume37))
                            msgs_MDIncrementalRefreshVolume37.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshVolume37}_{chunk_index37}.pkl")
                            chunk_index37 += 1
                            msgs_MDIncrementalRefreshVolume37 = []

                    elif TemplateID == 38:

                        if (msgs_template is None) or (38 in msgs_template):
                            msgs_SnapshotFullRefresh38.append(
                                main_template.SnapshotFullRefresh38(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefresh38) >= chunk_size:

                            msgs_SnapshotFullRefresh38 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefresh38))
                            msgs_SnapshotFullRefresh38.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefresh38}_{chunk_index38}.pkl")
                            chunk_index38 += 1
                            msgs_SnapshotFullRefresh38 = []

                    elif TemplateID == 39:

                        if (msgs_template is None) or (39 in msgs_template):
                            msgs_QuoteRequest39.append(
                                main_template.QuoteRequest39(messages, BlockLength, cme_packet))

                        if len(msgs_QuoteRequest39) >= chunk_size:

                            msgs_QuoteRequest39 = pd.DataFrame(
                                chain.from_iterable(msgs_QuoteRequest39))
                            msgs_QuoteRequest39.to_pickle(
                                f"{save_file_path}/{msgs_QuoteRequest39}_{chunk_index39}.pkl")
                            chunk_index39 += 1
                            msgs_QuoteRequest39 = []

                    elif TemplateID == 41:

                        if (msgs_template is None) or (41 in msgs_template):
                            msgs_MDInstrumentDefinitionOption41.append(
                                main_template.MDInstrumentDefinitionOption41(messages, BlockLength, cme_packet))

                        if len(msgs_MDInstrumentDefinitionOption41) >= chunk_size:

                            msgs_MDInstrumentDefinitionOption41 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionOption41))
                            msgs_MDInstrumentDefinitionOption41.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionOption41}_{chunk_index41}.pkl")
                            chunk_index41 += 1
                            msgs_MDInstrumentDefinitionOption41 = []

                    elif TemplateID == 42:

                        if (msgs_template is None) or (42 in msgs_template):
                            msgs_MDIncrementalRefreshTradeSummary42.append(
                                main_template.MDIncrementalRefreshTradeSummary42(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshTradeSummary42) >= chunk_size:

                            msgs_MDIncrementalRefreshTradeSummary42 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshTradeSummary42))
                            msgs_MDIncrementalRefreshTradeSummary42.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshTradeSummary42}_{chunk_index42}.pkl")
                            chunk_index42 += 1
                            msgs_MDIncrementalRefreshTradeSummary42 = []

                    elif TemplateID == 43:

                        if (msgs_template is None) or (43 in msgs_template):
                            msgs_MDIncrementalRefreshOrderBook43.append(
                                main_template.MDIncrementalRefreshOrderBook43(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshOrderBook43) >= chunk_size:

                            msgs_MDIncrementalRefreshOrderBook43 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshOrderBook43))
                            msgs_MDIncrementalRefreshOrderBook43.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshOrderBook43}_{chunk_index43}.pkl")
                            chunk_index43 += 1
                            msgs_MDIncrementalRefreshOrderBook43 = []

                    elif TemplateID == 44:

                        if (msgs_template is None) or (44 in msgs_template):
                            msgs_SnapshotFullRefreshOrderBook44.append(
                                main_template.SnapshotFullRefreshOrderBook44(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefreshOrderBook44) >= chunk_size:

                            msgs_SnapshotFullRefreshOrderBook44 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefreshOrderBook44))
                            msgs_SnapshotFullRefreshOrderBook44.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefreshOrderBook44}_{chunk_index44}.pkl")
                            chunk_index44 += 1
                            msgs_SnapshotFullRefreshOrderBook44 = []

                    elif TemplateID == 46:

                        if (msgs_template is None) or (46 in msgs_template):
                            msgs_MDIncrementalRefreshBook46.append(
                                main_template.MDIncrementalRefreshBook46(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshBook46) >= chunk_size:

                            msgs_MDIncrementalRefreshBook46 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshBook46))
                            msgs_MDIncrementalRefreshBook46.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshBook46}_{chunk_index46}.pkl")
                            chunk_index46 += 1
                            msgs_MDIncrementalRefreshBook46 = []

                    elif TemplateID == 47:

                        if (msgs_template is None) or (47 in msgs_template):
                            msgs_MDIncrementalRefreshOrderBook47.append(
                                main_template.MDIncrementalRefreshOrderBook47(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshOrderBook47) >= chunk_size:

                            msgs_MDIncrementalRefreshOrderBook47 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshOrderBook47))
                            msgs_MDIncrementalRefreshOrderBook47.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshOrderBook47}_{chunk_index47}.pkl")
                            chunk_index47 += 1
                            msgs_MDIncrementalRefreshOrderBook47 = []

                    elif TemplateID == 48:

                        if (msgs_template is None) or (48 in msgs_template):
                            msgs_MDIncrementalRefreshTradeSummary48.append(
                                main_template.MDIncrementalRefreshTradeSummary48(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshTradeSummary48) >= chunk_size:

                            msgs_MDIncrementalRefreshTradeSummary48 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshTradeSummary48))
                            msgs_MDIncrementalRefreshTradeSummary48.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshTradeSummary48}_{chunk_index48}.pkl")
                            chunk_index48 += 1
                            msgs_MDIncrementalRefreshTradeSummary48 = []

                    elif TemplateID == 49:

                        if (msgs_template is None) or (49 in msgs_template):
                            msgs_MDIncrementalRefreshDailyStatistics49.append(
                                main_template.MDIncrementalRefreshDailyStatistics49(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshDailyStatistics49) >= chunk_size:

                            msgs_MDIncrementalRefreshDailyStatistics49 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshDailyStatistics49))
                            msgs_MDIncrementalRefreshDailyStatistics49.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshDailyStatistics49}_{chunk_index49}.pkl")
                            chunk_index49 += 1
                            msgs_MDIncrementalRefreshDailyStatistics49 = []

                    elif TemplateID == 50:

                        if (msgs_template is None) or (50 in msgs_template):
                            msgs_MDIncrementalRefreshLimitsBanding50.append(
                                main_template.MDIncrementalRefreshLimitsBanding50(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshLimitsBanding50) >= chunk_size:

                            msgs_MDIncrementalRefreshLimitsBanding50 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshLimitsBanding50))
                            msgs_MDIncrementalRefreshLimitsBanding50.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshLimitsBanding50}_{chunk_index50}.pkl")
                            chunk_index50 += 1
                            msgs_MDIncrementalRefreshLimitsBanding50 = []

                    elif TemplateID == 51:

                        if (msgs_template is None) or (51 in msgs_template):
                            msgs_MDIncrementalRefreshSessionStatistics51.append(
                                main_template.MDIncrementalRefreshSessionStatistics51(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshSessionStatistics51) >= chunk_size:

                            msgs_MDIncrementalRefreshSessionStatistics51 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshSessionStatistics51))
                            msgs_MDIncrementalRefreshSessionStatistics51.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshSessionStatistics51}_{chunk_index51}.pkl")
                            chunk_index51 += 1
                            msgs_MDIncrementalRefreshSessionStatistics51 = []

                    elif TemplateID == 52:

                        if (msgs_template is None) or (52 in msgs_template):
                            msgs_SnapshotFullRefresh52.append(
                                main_template.SnapshotFullRefresh52(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefresh52) >= chunk_size:

                            msgs_SnapshotFullRefresh52 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefresh52))
                            msgs_SnapshotFullRefresh52.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefresh52}_{chunk_index52}.pkl")
                            chunk_index52 += 1
                            msgs_SnapshotFullRefresh52 = []

                    elif TemplateID == 53:

                        if (msgs_template is None) or (53 in msgs_template):
                            msgs_SnapshotFullRefreshOrderBook53.append(
                                main_template.SnapshotFullRefreshOrderBook53(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefreshOrderBook53) >= chunk_size:

                            msgs_SnapshotFullRefreshOrderBook53 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefreshOrderBook53))
                            msgs_SnapshotFullRefreshOrderBook53.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefreshOrderBook53}_{chunk_index53}.pkl")
                            chunk_index53 += 1
                            msgs_SnapshotFullRefreshOrderBook53 = []

                    elif TemplateID == 54:

                        if (msgs_template is None) or (54 in msgs_template):
                            msgs_MDInstrumentDefinitionFuture54.append(
                                main_template.MDInstrumentDefinitionFuture54(messages, BlockLength, Version, cme_packet))

                        if len(msgs_MDInstrumentDefinitionFuture54) >= chunk_size:

                            msgs_MDInstrumentDefinitionFuture54 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionFuture54))
                            msgs_MDInstrumentDefinitionFuture54.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionFuture54}_{chunk_index54}.pkl")
                            chunk_index54 += 1
                            msgs_MDInstrumentDefinitionFuture54 = []

                    elif TemplateID == 55:

                        if (msgs_template is None) or (55 in msgs_template):
                            msgs_MDInstrumentDefinitionOption55.append(
                                main_template.MDInstrumentDefinitionOption55(messages, BlockLength, Version, cme_packet))

                        if len(msgs_MDInstrumentDefinitionOption55) >= chunk_size:

                            msgs_MDInstrumentDefinitionOption55 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionOption55))
                            msgs_MDInstrumentDefinitionOption55.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionOption55}_{chunk_index55}.pkl")
                            chunk_index55 += 1
                            msgs_MDInstrumentDefinitionOption55 = []

                    elif TemplateID == 56:

                        if (msgs_template is None) or (56 in msgs_template):
                            msgs_MDInstrumentDefinitionSpread56.append(
                                main_template.MDInstrumentDefinitionSpread56(messages, BlockLength, Version, cme_packet))

                        if len(msgs_MDInstrumentDefinitionSpread56) >= chunk_size:

                            msgs_MDInstrumentDefinitionSpread56 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionSpread56))
                            msgs_MDInstrumentDefinitionSpread56.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionSpread56}_{chunk_index56}.pkl")
                            chunk_index56 += 1
                            msgs_MDInstrumentDefinitionSpread56 = []

                    elif TemplateID == 57:

                        if (msgs_template is None) or (57 in msgs_template):
                            msgs_MDInstrumentDefinitionFixedIncome57.append(
                                main_template.MDInstrumentDefinitionFixedIncome57(messages, BlockLength, cme_packet))

                        if len(msgs_MDInstrumentDefinitionFixedIncome57) >= chunk_size:

                            msgs_MDInstrumentDefinitionFixedIncome57 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionFixedIncome57))
                            msgs_MDInstrumentDefinitionFixedIncome57.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionFixedIncome57}_{chunk_index57}.pkl")
                            chunk_index57 += 1
                            msgs_MDInstrumentDefinitionFixedIncome57 = []

                    elif TemplateID == 58:

                        if (msgs_template is None) or (58 in msgs_template):
                            msgs_MDInstrumentDefinitionRepo58.append(
                                main_template.MDInstrumentDefinitionRepo58(messages, BlockLength, Version, cme_packet))

                        if len(msgs_MDInstrumentDefinitionRepo58) >= chunk_size:

                            msgs_MDInstrumentDefinitionRepo58 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionRepo58))
                            msgs_MDInstrumentDefinitionRepo58.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionRepo58}_{chunk_index58}.pkl")
                            chunk_index58 += 1
                            msgs_MDInstrumentDefinitionRepo58 = []

                    elif TemplateID == 59:

                        if (msgs_template is None) or (59 in msgs_template):
                            msgs_SnapshotRefreshTopOrders59.append(
                                main_template.SnapshotRefreshTopOrders59(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotRefreshTopOrders59) >= chunk_size:

                            msgs_SnapshotRefreshTopOrders59 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotRefreshTopOrders59))
                            msgs_SnapshotRefreshTopOrders59.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotRefreshTopOrders59}_{chunk_index59}.pkl")
                            chunk_index59 += 1
                            msgs_SnapshotRefreshTopOrders59 = []

                    elif TemplateID == 60:

                        if (msgs_template is None) or (60 in msgs_template):
                            msgs_SecurityStatusWorkup60.append(
                                main_template.SecurityStatusWorkup60(messages, BlockLength, cme_packet))

                        if len(msgs_SecurityStatusWorkup60) >= chunk_size:

                            msgs_SecurityStatusWorkup60 = pd.DataFrame(
                                chain.from_iterable(msgs_SecurityStatusWorkup60))
                            msgs_SecurityStatusWorkup60.to_pickle(
                                f"{save_file_path}/{msgs_SecurityStatusWorkup60}_{chunk_index60}.pkl")
                            chunk_index60 += 1
                            msgs_SecurityStatusWorkup60 = []

                    elif TemplateID == 61:

                        if (msgs_template is None) or (61 in msgs_template):
                            msgs_SnapshotFullRefreshTCP61.append(
                                main_template.SnapshotFullRefreshTCP61(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefreshTCP61) >= chunk_size:

                            msgs_SnapshotFullRefreshTCP61 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefreshTCP61))
                            msgs_SnapshotFullRefreshTCP61.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefreshTCP61}_{chunk_index61}.pkl")
                            chunk_index61 += 1
                            msgs_SnapshotFullRefreshTCP61 = []

                    elif TemplateID == 62:

                        if (msgs_template is None) or (62 in msgs_template):
                            msgs_CollateralMarketValue62.append(
                                main_template.CollateralMarketValue62(messages, BlockLength, cme_packet))

                        if len(msgs_CollateralMarketValue62) >= chunk_size:

                            msgs_CollateralMarketValue62 = pd.DataFrame(
                                chain.from_iterable(msgs_CollateralMarketValue62))
                            msgs_CollateralMarketValue62.to_pickle(
                                f"{save_file_path}/{msgs_CollateralMarketValue62}_{chunk_index62}.pkl")
                            chunk_index62 += 1
                            msgs_CollateralMarketValue62 = []

                    elif TemplateID == 63:

                        if (msgs_template is None) or (63 in msgs_template):
                            msgs_MDInstrumentDefinitionFX63.append(
                                main_template.MDInstrumentDefinitionFX63(messages, BlockLength, cme_packet))

                        if len(msgs_MDInstrumentDefinitionFX63) >= chunk_size:

                            msgs_MDInstrumentDefinitionFX63 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionFX63))
                            msgs_MDInstrumentDefinitionFX63.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionFX63}_{chunk_index63}.pkl")
                            chunk_index63 += 1
                            msgs_MDInstrumentDefinitionFX63 = []

                    elif TemplateID == 64:

                        if (msgs_template is None) or (64 in msgs_template):
                            msgs_MDIncrementalRefreshBookLongQty64.append(
                                main_template.MDIncrementalRefreshBookLongQty64(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshBookLongQty64) >= chunk_size:

                            msgs_MDIncrementalRefreshBookLongQty64 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshBookLongQty64))
                            msgs_MDIncrementalRefreshBookLongQty64.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshBookLongQty64}_{chunk_index64}.pkl")
                            chunk_index64 += 1
                            msgs_MDIncrementalRefreshBookLongQty64 = []

                    elif TemplateID == 65:

                        if (msgs_template is None) or (65 in msgs_template):
                            msgs_MDIncrementalRefreshTradeSummaryLongQty65.append(
                                main_template.MDIncrementalRefreshTradeSummaryLongQty65(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshTradeSummaryLongQty65) >= chunk_size:

                            msgs_MDIncrementalRefreshTradeSummaryLongQty65 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshTradeSummaryLongQty65))
                            msgs_MDIncrementalRefreshTradeSummaryLongQty65.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshTradeSummaryLongQty65}_{chunk_index65}.pkl")
                            chunk_index65 += 1
                            msgs_MDIncrementalRefreshTradeSummaryLongQty65 = []

                    elif TemplateID == 66:

                        if (msgs_template is None) or (66 in msgs_template):
                            msgs_MDIncrementalRefreshVolumeLongQty66.append(
                                main_template.MDIncrementalRefreshVolumeLongQty66(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshVolumeLongQty66) >= chunk_size:

                            msgs_MDIncrementalRefreshVolumeLongQty66 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshVolumeLongQty66))
                            msgs_MDIncrementalRefreshVolumeLongQty66.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshVolumeLongQty66}_{chunk_index66}.pkl")
                            chunk_index66 += 1
                            msgs_MDIncrementalRefreshVolumeLongQty66 = []

                    elif TemplateID == 67:

                        if (msgs_template is None) or (67 in msgs_template):
                            msgs_MDIncrementalRefreshSessionStatisticsLongQty67.append(
                                main_template.MDIncrementalRefreshSessionStatisticsLongQty67(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshSessionStatisticsLongQty67) >= chunk_size:

                            msgs_MDIncrementalRefreshSessionStatisticsLongQty67 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshSessionStatisticsLongQty67))
                            msgs_MDIncrementalRefreshSessionStatisticsLongQty67.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshSessionStatisticsLongQty67}_{chunk_index67}.pkl")
                            chunk_index67 += 1
                            msgs_MDIncrementalRefreshSessionStatisticsLongQty67 = []

                    elif TemplateID == 68:

                        if (msgs_template is None) or (68 in msgs_template):
                            msgs_SnapshotFullRefreshTCPLongQty68.append(
                                main_template.SnapshotFullRefreshTCPLongQty68(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefreshTCPLongQty68) >= chunk_size:

                            msgs_SnapshotFullRefreshTCPLongQty68 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefreshTCPLongQty68))
                            msgs_SnapshotFullRefreshTCPLongQty68.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefreshTCPLongQty68}_{chunk_index68}.pkl")
                            chunk_index68 += 1
                            msgs_SnapshotFullRefreshTCPLongQty68 = []

                    elif TemplateID == 69:

                        if (msgs_template is None) or (69 in msgs_template):
                            msgs_SnapshotFullRefreshLongQty69.append(
                                main_template.SnapshotFullRefreshLongQty69(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefreshLongQty69) >= chunk_size:

                            msgs_SnapshotFullRefreshLongQty69 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefreshLongQty69))
                            msgs_SnapshotFullRefreshLongQty69.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefreshLongQty69}_{chunk_index69}.pkl")
                            chunk_index69 += 1
                            msgs_SnapshotFullRefreshLongQty69 = []

                if notnull(max_read_packets):
                    read += 1
                    pbar.update(1)
                else:
                    read = end_pos
                    pbar.update(message_length + 4)

                f.seek(end_pos)

    # delte msgs that are not in the template

    if isnull(save_file_path):

        raise Exception('Path for saved files must be provided')

    for var_name, var_value in globals().items():
        if var_name.startswith("msgs_"):
            df = pd.DataFrame(chain.from_iterable(var_value))
            df.to_pickle(f"{save_file_path}/{var_name}.pkl")
            del globals()[var_name]
            print(' --> Dataframe: Success!')


def cme_parser_pcap(path, max_read_packets=None, msgs_template=None, cme_header=True,
                    save_file_path=None, disable_progress_bar=True, chunk_size=5000):
    """
    `cme_parser_pcap` is a binary pacaket capture (PCAP) data parser for 
    market data in the Chicago Mercantile Exchange (CME).
    This function applies to the real pcap data that contain both global header,
    packet header, etc. An overall data structure can be found as follows:

    Sequence | Time | Message Size | Block Length | Template ID | Schemal ID | Version | FIX header | FIX Message Body |
    (Packet Header) |   (2 bytes)  |........(Simplie Binary Header, 8 bytes)..........|.......(FIX message)...........|
    (12 bytes)      |..........................(message header)........................|
                    |.................................... MDP messages.................................................|

    Parameters
    ----------
    path : str
        The path of the raw data file.
    max_read_packets : int, optional
        The maximum number of packaets need to be processed, if None, 
        all packets are read. The default is None.
    msgs_template : list, optional
        Types of messages need to be returned. CME provides 
        numbers to different message templates, e.g., the channel reset messages
        are marked as 4. Users need to give the template numbers into a list. If None,
        all messages are returned. The default is None.
    cme_header : bool, optional
        Whether to parser the packet header, which includes the
        message sequence number and sending timestamps. The default is True.
    save_file_path : str, optional
        The path for the saving file. The default is None.
    disable_progress_bar : bool, optional
        Whether to disable the progress bar. The default is False.
    chunk_size : int
        The chunk size that needs to be saved.

    """
    template_id = [4, 16, 27, 29, 30, 32, 33, 34, 35, 36, 37, 38, 39, 41, 42, 43, 44, 46, 47, 48,
                   49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69]

    def packet_head(msgs_blocks):

        (time, time_ms, packet_length, orginal_length) = struct.unpack(
            '<IIII', msgs_blocks)

        msgs_blocks = [time, time_ms, packet_length, orginal_length]

        return msgs_blocks

    msgs_ChannelReset4 = []
    msgs_AdminLogout16 = []
    msgs_MDInstrumentDefinitionFuture27 = []
    msgs_MDInstrumentDefinitionSpread29 = []
    msgs_SecurityStatus30 = []
    msgs_MDIncrementalRefreshBook32 = []
    msgs_MDIncrementalRefreshDailyStatistics33 = []
    msgs_MDIncrementalRefreshLimitsBanding34 = []
    msgs_MDIncrementalRefreshSessionStatistics35 = []
    msgs_MDIncrementalRefreshTrade36 = []
    msgs_MDIncrementalRefreshVolume37 = []
    msgs_SnapshotFullRefresh38 = []
    msgs_QuoteRequest39 = []
    msgs_MDInstrumentDefinitionOption41 = []
    msgs_MDIncrementalRefreshTradeSummary42 = []
    msgs_MDIncrementalRefreshOrderBook43 = []
    msgs_SnapshotFullRefreshOrderBook44 = []
    msgs_MDIncrementalRefreshBook46 = []
    msgs_MDIncrementalRefreshOrderBook47 = []
    msgs_MDIncrementalRefreshTradeSummary48 = []
    msgs_MDIncrementalRefreshDailyStatistics49 = []
    msgs_MDIncrementalRefreshLimitsBanding50 = []
    msgs_MDIncrementalRefreshSessionStatistics51 = []
    msgs_SnapshotFullRefresh52 = []
    msgs_SnapshotFullRefreshOrderBook53 = []
    msgs_MDInstrumentDefinitionFuture54 = []
    msgs_MDInstrumentDefinitionOption55 = []
    msgs_MDInstrumentDefinitionSpread56 = []
    msgs_MDInstrumentDefinitionFixedIncome57 = []
    msgs_MDInstrumentDefinitionRepo58 = []
    msgs_SnapshotRefreshTopOrders59 = []
    msgs_SecurityStatusWorkup60 = []
    msgs_SnapshotFullRefreshTCP61 = []
    msgs_CollateralMarketValue62 = []
    msgs_MDInstrumentDefinitionFX63 = []
    msgs_MDIncrementalRefreshBookLongQty64 = []
    msgs_MDIncrementalRefreshTradeSummaryLongQty65 = []
    msgs_MDIncrementalRefreshVolumeLongQty66 = []
    msgs_MDIncrementalRefreshSessionStatisticsLongQty67 = []
    msgs_SnapshotFullRefreshTCPLongQty68 = []
    msgs_SnapshotFullRefreshLongQty69 = []

    chunk_index4 = 1
    chunk_index16 = 1
    chunk_index27 = 1
    chunk_index29 = 1
    chunk_index30 = 1
    chunk_index32 = 1
    chunk_index33 = 1
    chunk_index34 = 1
    chunk_index35 = 1
    chunk_index36 = 1
    chunk_index37 = 1
    chunk_index38 = 1
    chunk_index39 = 1
    chunk_index41 = 1
    chunk_index42 = 1
    chunk_index43 = 1
    chunk_index44 = 1
    chunk_index46 = 1
    chunk_index47 = 1
    chunk_index48 = 1
    chunk_index49 = 1
    chunk_index50 = 1
    chunk_index51 = 1
    chunk_index52 = 1
    chunk_index53 = 1
    chunk_index54 = 1
    chunk_index55 = 1
    chunk_index56 = 1
    chunk_index57 = 1
    chunk_index58 = 1
    chunk_index59 = 1
    chunk_index60 = 1
    chunk_index61 = 1
    chunk_index62 = 1
    chunk_index63 = 1
    chunk_index64 = 1
    chunk_index65 = 1
    chunk_index66 = 1
    chunk_index67 = 1
    chunk_index68 = 1
    chunk_index69 = 1

    if isnull(max_read_packets):
        print('maximum number of packets read does not provide. Read the whole file by default')
        max_read = os.path.getsize(path)
        print(f'Read total bytes: {max_read}')

    else:
        max_read = max_read_packets  # excluding the global header
        print(f'Read maximum number of packets {max_read_packets}')

    if notnull(max_read_packets):
        unit = 'packets'
    else:
        unit = 'bytes'

    # check whether the file is gzip

    with open(path, 'rb') as f:
        magic = f.read(2)

        if magic == b'\x1f\x8b':

            raise Exception(
                'Compressed file is not supported, please use uncompressed file')

    with open(path, "rb") as f:

        with tqdm(total=max_read, desc="Reading", ncols=100, unit=unit,
                  disable=True) as pbar:

            read = 0

            f.read(24)
            end_pos = 24

            if isnull(max_read_packets):
                read = 24
                pbar.update(24)

            while read < max_read:

                # skip the packet header -- 16 bytes
                # find the packet length

                packet_length = packet_head(f.read(16))[2]

                end_pos += (packet_length + 16)

                # skip networks header and UDP headers - 42 bytes

                f.read(42)

                # packet header defined by the CME

                if cme_header:

                    (MsgSeq, SendingTime) = struct.unpack('<IQ', f.read(12))
                    cme_packet = {'MsgSeq': MsgSeq,
                                  'SendingTime': SendingTime}

                else:

                    f.read(12)
                    cme_packet = False

                # parse message header
                """
                #---------------------------------------------------------------------
                Name        |  Type  |Description
                ----------------------------------------------------------------------
                MsgSize     | uInt16 |Length of entire message, including binary header
                            |        |in number of bytes
                ----------------------------------------------------------------------
                BlockLength | uInt16 |Length of the root of the FIX message contained
                            |        |before repeating groups or ariable/conditions fields
                ----------------------------------------------------------------------
                TemplateID  | uInt16 |Template ID used to encode the message
                ----------------------------------------------------------------------
                SchemaID    | uInt16 |ID of the system publishing the message
                ----------------------------------------------------------------------
                Version     | uInt16 |Schema version
                ----------------------------------------------------------------------
                """

                (MsgSize, BlockLength, TemplateID, SchemaID, Version) = struct.unpack(
                    '<HHHHH', f.read(10))

                # print

                # MsgSize =12, which is heartbeat
                # No action needed

                # print(TemplateID, SchemaID, Version)

                # One needs to find the template ID, Schema ID in a XML file of the correct version

                if BlockLength > 0:

                    # should be 16
                    messages = f.read(MsgSize-10)

                    # guding to the signle message

                    if TemplateID == 4:

                        if (msgs_template is None) or (4 in msgs_template):
                            msgs_ChannelReset4.append(main_template.ChannelReset4(
                                messages, BlockLength, cme_packet))

                        if len(msgs_ChannelReset4) >= chunk_size:

                            msgs_ChannelReset4 = pd.DataFrame(
                                chain.from_iterable(msgs_ChannelReset4))
                            msgs_ChannelReset4.to_pickle(
                                f"{save_file_path}/{msgs_ChannelReset4}_{chunk_index4}.pkl")
                            chunk_index4 += 1
                            msgs_ChannelReset4 = []

                    elif TemplateID == 16:

                        if (msgs_template is None) or (16 in msgs_template):
                            msgs_AdminLogout16.append(main_template.AdminLogout16(
                                messages, BlockLength, cme_packet))

                        if len(msgs_AdminLogout16) >= chunk_size:

                            msgs_AdminLogout16 = pd.DataFrame(
                                chain.from_iterable(msgs_AdminLogout16))
                            msgs_AdminLogout16.to_pickle(
                                f"{save_file_path}/{msgs_AdminLogout16}_{chunk_index16}.pkl")
                            chunk_index16 += 1
                            msgs_AdminLogout16 = []

                    elif TemplateID == 27:

                        if (msgs_template is None) or (27 in msgs_template):
                            msgs_MDInstrumentDefinitionFuture27.append(
                                main_template.MDInstrumentDefinitionFuture27(messages, BlockLength, Version, cme_packet))

                        if len(msgs_MDInstrumentDefinitionFuture27) >= chunk_size:

                            msgs_MDInstrumentDefinitionFuture27 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionFuture27))
                            msgs_MDInstrumentDefinitionFuture27.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionFuture27}_{chunk_index27}.pkl")
                            chunk_index27 += 1
                            msgs_MDInstrumentDefinitionFuture27 = []

                    elif TemplateID == 29:

                        if (msgs_template is None) or (29 in msgs_template):
                            msgs_MDInstrumentDefinitionSpread29.append(
                                main_template.MDInstrumentDefinitionSpread29(messages, BlockLength, Version, cme_packet))

                        if len(msgs_MDInstrumentDefinitionSpread29) >= chunk_size:

                            msgs_MDInstrumentDefinitionSpread29 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionSpread29))
                            msgs_MDInstrumentDefinitionSpread29.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionSpread29}_{chunk_index29}.pkl")
                            chunk_index29 += 1
                            msgs_MDInstrumentDefinitionSpread29 = []

                    elif TemplateID == 30:

                        if (msgs_template is None) or (30 in msgs_template):
                            msgs_SecurityStatus30.append(
                                main_template.SecurityStatus30(messages, BlockLength, cme_packet))

                        if len(msgs_SecurityStatus30) >= chunk_size:

                            msgs_SecurityStatus30 = pd.DataFrame(
                                chain.from_iterable(msgs_SecurityStatus30))
                            msgs_SecurityStatus30.to_pickle(
                                f"{save_file_path}/{msgs_SecurityStatus30}_{chunk_index30}.pkl")
                            chunk_index30 += 1
                            msgs_SecurityStatus30 = []

                    elif TemplateID == 32:

                        if (msgs_template is None) or (32 in msgs_template):
                            msgs_MDIncrementalRefreshBook32.append(
                                main_template.MDIncrementalRefreshBook32(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshBook32) >= chunk_size:

                            msgs_MDIncrementalRefreshBook32 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshBook32))
                            msgs_MDIncrementalRefreshBook32.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshBook32}_{chunk_index32}.pkl")
                            chunk_index32 += 1
                            msgs_MDIncrementalRefreshBook32 = []

                    elif TemplateID == 33:

                        if (msgs_template is None) or (33 in msgs_template):
                            msgs_MDIncrementalRefreshDailyStatistics33.append(
                                main_template.MDIncrementalRefreshDailyStatistics33(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshDailyStatistics33) >= chunk_size:

                            msgs_MDIncrementalRefreshDailyStatistics33 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshDailyStatistics33))
                            msgs_MDIncrementalRefreshDailyStatistics33.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshDailyStatistics33}_{chunk_index33}.pkl")
                            chunk_index33 += 1
                            msgs_MDIncrementalRefreshDailyStatistics33 = []

                    elif TemplateID == 34:

                        if (msgs_template is None) or (34 in msgs_template):
                            msgs_MDIncrementalRefreshLimitsBanding34.append(
                                main_template.MDIncrementalRefreshLimitsBanding34(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshLimitsBanding34) >= chunk_size:

                            msgs_MDIncrementalRefreshLimitsBanding34 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshLimitsBanding34))
                            msgs_MDIncrementalRefreshLimitsBanding34.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshLimitsBanding34}_{chunk_index34}.pkl")
                            chunk_index34 += 1
                            msgs_MDIncrementalRefreshLimitsBanding34 = []

                    elif TemplateID == 35:

                        if (msgs_template is None) or (35 in msgs_template):
                            msgs_MDIncrementalRefreshSessionStatistics35.append(
                                main_template.MDIncrementalRefreshSessionStatistics35(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshSessionStatistics35) >= chunk_size:

                            msgs_MDIncrementalRefreshSessionStatistics35 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshSessionStatistics35))
                            msgs_MDIncrementalRefreshSessionStatistics35.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshSessionStatistics35}_{chunk_index35}.pkl")
                            chunk_index35 += 1
                            msgs_MDIncrementalRefreshSessionStatistics35 = []

                    elif TemplateID == 36:

                        if (msgs_template is None) or (36 in msgs_template):
                            msgs_MDIncrementalRefreshTrade36.append(
                                main_template.MDIncrementalRefreshTrade36(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshTrade36) >= chunk_size:

                            msgs_MDIncrementalRefreshTrade36 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshTrade36))
                            msgs_MDIncrementalRefreshTrade36.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshTrade36}_{chunk_index36}.pkl")
                            chunk_index36 += 1
                            msgs_MDIncrementalRefreshTrade36 = []

                    elif TemplateID == 37:

                        if (msgs_template is None) or (37 in msgs_template):
                            msgs_MDIncrementalRefreshVolume37.append(
                                main_template.MDIncrementalRefreshVolume37(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshVolume37) >= chunk_size:

                            msgs_MDIncrementalRefreshVolume37 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshVolume37))
                            msgs_MDIncrementalRefreshVolume37.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshVolume37}_{chunk_index37}.pkl")
                            chunk_index37 += 1
                            msgs_MDIncrementalRefreshVolume37 = []

                    elif TemplateID == 38:

                        if (msgs_template is None) or (38 in msgs_template):
                            msgs_SnapshotFullRefresh38.append(
                                main_template.SnapshotFullRefresh38(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefresh38) >= chunk_size:

                            msgs_SnapshotFullRefresh38 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefresh38))
                            msgs_SnapshotFullRefresh38.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefresh38}_{chunk_index38}.pkl")
                            chunk_index38 += 1
                            msgs_SnapshotFullRefresh38 = []

                    elif TemplateID == 39:

                        if (msgs_template is None) or (39 in msgs_template):
                            msgs_QuoteRequest39.append(
                                main_template.QuoteRequest39(messages, BlockLength, cme_packet))

                        if len(msgs_QuoteRequest39) >= chunk_size:

                            msgs_QuoteRequest39 = pd.DataFrame(
                                chain.from_iterable(msgs_QuoteRequest39))
                            msgs_QuoteRequest39.to_pickle(
                                f"{save_file_path}/{msgs_QuoteRequest39}_{chunk_index39}.pkl")
                            chunk_index39 += 1
                            msgs_QuoteRequest39 = []

                    elif TemplateID == 41:

                        if (msgs_template is None) or (41 in msgs_template):
                            msgs_MDInstrumentDefinitionOption41.append(
                                main_template.MDInstrumentDefinitionOption41(messages, BlockLength, cme_packet))

                        if len(msgs_MDInstrumentDefinitionOption41) >= chunk_size:

                            msgs_MDInstrumentDefinitionOption41 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionOption41))
                            msgs_MDInstrumentDefinitionOption41.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionOption41}_{chunk_index41}.pkl")
                            chunk_index41 += 1
                            msgs_MDInstrumentDefinitionOption41 = []

                    elif TemplateID == 42:

                        if (msgs_template is None) or (42 in msgs_template):
                            msgs_MDIncrementalRefreshTradeSummary42.append(
                                main_template.MDIncrementalRefreshTradeSummary42(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshTradeSummary42) >= chunk_size:

                            msgs_MDIncrementalRefreshTradeSummary42 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshTradeSummary42))
                            msgs_MDIncrementalRefreshTradeSummary42.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshTradeSummary42}_{chunk_index42}.pkl")
                            chunk_index42 += 1
                            msgs_MDIncrementalRefreshTradeSummary42 = []

                    elif TemplateID == 43:

                        if (msgs_template is None) or (43 in msgs_template):
                            msgs_MDIncrementalRefreshOrderBook43.append(
                                main_template.MDIncrementalRefreshOrderBook43(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshOrderBook43) >= chunk_size:

                            msgs_MDIncrementalRefreshOrderBook43 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshOrderBook43))
                            msgs_MDIncrementalRefreshOrderBook43.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshOrderBook43}_{chunk_index43}.pkl")
                            chunk_index43 += 1
                            msgs_MDIncrementalRefreshOrderBook43 = []

                    elif TemplateID == 44:

                        if (msgs_template is None) or (44 in msgs_template):
                            msgs_SnapshotFullRefreshOrderBook44.append(
                                main_template.SnapshotFullRefreshOrderBook44(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefreshOrderBook44) >= chunk_size:

                            msgs_SnapshotFullRefreshOrderBook44 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefreshOrderBook44))
                            msgs_SnapshotFullRefreshOrderBook44.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefreshOrderBook44}_{chunk_index44}.pkl")
                            chunk_index44 += 1
                            msgs_SnapshotFullRefreshOrderBook44 = []

                    elif TemplateID == 46:

                        if (msgs_template is None) or (46 in msgs_template):
                            msgs_MDIncrementalRefreshBook46.append(
                                main_template.MDIncrementalRefreshBook46(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshBook46) >= chunk_size:

                            msgs_MDIncrementalRefreshBook46 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshBook46))
                            msgs_MDIncrementalRefreshBook46.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshBook46}_{chunk_index46}.pkl")
                            chunk_index46 += 1
                            msgs_MDIncrementalRefreshBook46 = []

                    elif TemplateID == 47:

                        if (msgs_template is None) or (47 in msgs_template):
                            msgs_MDIncrementalRefreshOrderBook47.append(
                                main_template.MDIncrementalRefreshOrderBook47(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshOrderBook47) >= chunk_size:

                            msgs_MDIncrementalRefreshOrderBook47 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshOrderBook47))
                            msgs_MDIncrementalRefreshOrderBook47.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshOrderBook47}_{chunk_index47}.pkl")
                            chunk_index47 += 1
                            msgs_MDIncrementalRefreshOrderBook47 = []

                    elif TemplateID == 48:

                        if (msgs_template is None) or (48 in msgs_template):
                            msgs_MDIncrementalRefreshTradeSummary48.append(
                                main_template.MDIncrementalRefreshTradeSummary48(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshTradeSummary48) >= chunk_size:

                            msgs_MDIncrementalRefreshTradeSummary48 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshTradeSummary48))
                            msgs_MDIncrementalRefreshTradeSummary48.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshTradeSummary48}_{chunk_index48}.pkl")
                            chunk_index48 += 1
                            msgs_MDIncrementalRefreshTradeSummary48 = []

                    elif TemplateID == 49:

                        if (msgs_template is None) or (49 in msgs_template):
                            msgs_MDIncrementalRefreshDailyStatistics49.append(
                                main_template.MDIncrementalRefreshDailyStatistics49(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshDailyStatistics49) >= chunk_size:

                            msgs_MDIncrementalRefreshDailyStatistics49 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshDailyStatistics49))
                            msgs_MDIncrementalRefreshDailyStatistics49.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshDailyStatistics49}_{chunk_index49}.pkl")
                            chunk_index49 += 1
                            msgs_MDIncrementalRefreshDailyStatistics49 = []

                    elif TemplateID == 50:

                        if (msgs_template is None) or (50 in msgs_template):
                            msgs_MDIncrementalRefreshLimitsBanding50.append(
                                main_template.MDIncrementalRefreshLimitsBanding50(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshLimitsBanding50) >= chunk_size:

                            msgs_MDIncrementalRefreshLimitsBanding50 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshLimitsBanding50))
                            msgs_MDIncrementalRefreshLimitsBanding50.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshLimitsBanding50}_{chunk_index50}.pkl")
                            chunk_index50 += 1
                            msgs_MDIncrementalRefreshLimitsBanding50 = []

                    elif TemplateID == 51:

                        if (msgs_template is None) or (51 in msgs_template):
                            msgs_MDIncrementalRefreshSessionStatistics51.append(
                                main_template.MDIncrementalRefreshSessionStatistics51(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshSessionStatistics51) >= chunk_size:

                            msgs_MDIncrementalRefreshSessionStatistics51 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshSessionStatistics51))
                            msgs_MDIncrementalRefreshSessionStatistics51.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshSessionStatistics51}_{chunk_index51}.pkl")
                            chunk_index51 += 1
                            msgs_MDIncrementalRefreshSessionStatistics51 = []

                    elif TemplateID == 52:

                        if (msgs_template is None) or (52 in msgs_template):
                            msgs_SnapshotFullRefresh52.append(
                                main_template.SnapshotFullRefresh52(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefresh52) >= chunk_size:

                            msgs_SnapshotFullRefresh52 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefresh52))
                            msgs_SnapshotFullRefresh52.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefresh52}_{chunk_index52}.pkl")
                            chunk_index52 += 1
                            msgs_SnapshotFullRefresh52 = []

                    elif TemplateID == 53:

                        if (msgs_template is None) or (53 in msgs_template):
                            msgs_SnapshotFullRefreshOrderBook53.append(
                                main_template.SnapshotFullRefreshOrderBook53(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefreshOrderBook53) >= chunk_size:

                            msgs_SnapshotFullRefreshOrderBook53 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefreshOrderBook53))
                            msgs_SnapshotFullRefreshOrderBook53.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefreshOrderBook53}_{chunk_index53}.pkl")
                            chunk_index53 += 1
                            msgs_SnapshotFullRefreshOrderBook53 = []

                    elif TemplateID == 54:

                        if (msgs_template is None) or (54 in msgs_template):
                            msgs_MDInstrumentDefinitionFuture54.append(
                                main_template.MDInstrumentDefinitionFuture54(messages, BlockLength, Version, cme_packet))

                        if len(msgs_MDInstrumentDefinitionFuture54) >= chunk_size:

                            msgs_MDInstrumentDefinitionFuture54 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionFuture54))
                            msgs_MDInstrumentDefinitionFuture54.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionFuture54}_{chunk_index54}.pkl")
                            chunk_index54 += 1
                            msgs_MDInstrumentDefinitionFuture54 = []

                    elif TemplateID == 55:

                        if (msgs_template is None) or (55 in msgs_template):
                            msgs_MDInstrumentDefinitionOption55.append(
                                main_template.MDInstrumentDefinitionOption55(messages, BlockLength, Version, cme_packet))

                        if len(msgs_MDInstrumentDefinitionOption55) >= chunk_size:

                            msgs_MDInstrumentDefinitionOption55 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionOption55))
                            msgs_MDInstrumentDefinitionOption55.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionOption55}_{chunk_index55}.pkl")
                            chunk_index55 += 1
                            msgs_MDInstrumentDefinitionOption55 = []

                    elif TemplateID == 56:

                        if (msgs_template is None) or (56 in msgs_template):
                            msgs_MDInstrumentDefinitionSpread56.append(
                                main_template.MDInstrumentDefinitionSpread56(messages, BlockLength, Version, cme_packet))

                        if len(msgs_MDInstrumentDefinitionSpread56) >= chunk_size:

                            msgs_MDInstrumentDefinitionSpread56 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionSpread56))
                            msgs_MDInstrumentDefinitionSpread56.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionSpread56}_{chunk_index56}.pkl")
                            chunk_index56 += 1
                            msgs_MDInstrumentDefinitionSpread56 = []

                    elif TemplateID == 57:

                        if (msgs_template is None) or (57 in msgs_template):
                            msgs_MDInstrumentDefinitionFixedIncome57.append(
                                main_template.MDInstrumentDefinitionFixedIncome57(messages, BlockLength, cme_packet))

                        if len(msgs_MDInstrumentDefinitionFixedIncome57) >= chunk_size:

                            msgs_MDInstrumentDefinitionFixedIncome57 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionFixedIncome57))
                            msgs_MDInstrumentDefinitionFixedIncome57.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionFixedIncome57}_{chunk_index57}.pkl")
                            chunk_index57 += 1
                            msgs_MDInstrumentDefinitionFixedIncome57 = []

                    elif TemplateID == 58:

                        if (msgs_template is None) or (58 in msgs_template):
                            msgs_MDInstrumentDefinitionRepo58.append(
                                main_template.MDInstrumentDefinitionRepo58(messages, BlockLength, Version, cme_packet))

                        if len(msgs_MDInstrumentDefinitionRepo58) >= chunk_size:

                            msgs_MDInstrumentDefinitionRepo58 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionRepo58))
                            msgs_MDInstrumentDefinitionRepo58.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionRepo58}_{chunk_index58}.pkl")
                            chunk_index58 += 1
                            msgs_MDInstrumentDefinitionRepo58 = []

                    elif TemplateID == 59:

                        if (msgs_template is None) or (59 in msgs_template):
                            msgs_SnapshotRefreshTopOrders59.append(
                                main_template.SnapshotRefreshTopOrders59(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotRefreshTopOrders59) >= chunk_size:

                            msgs_SnapshotRefreshTopOrders59 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotRefreshTopOrders59))
                            msgs_SnapshotRefreshTopOrders59.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotRefreshTopOrders59}_{chunk_index59}.pkl")
                            chunk_index59 += 1
                            msgs_SnapshotRefreshTopOrders59 = []

                    elif TemplateID == 60:

                        if (msgs_template is None) or (60 in msgs_template):
                            msgs_SecurityStatusWorkup60.append(
                                main_template.SecurityStatusWorkup60(messages, BlockLength, cme_packet))

                        if len(msgs_SecurityStatusWorkup60) >= chunk_size:

                            msgs_SecurityStatusWorkup60 = pd.DataFrame(
                                chain.from_iterable(msgs_SecurityStatusWorkup60))
                            msgs_SecurityStatusWorkup60.to_pickle(
                                f"{save_file_path}/{msgs_SecurityStatusWorkup60}_{chunk_index60}.pkl")
                            chunk_index60 += 1
                            msgs_SecurityStatusWorkup60 = []

                    elif TemplateID == 61:

                        if (msgs_template is None) or (61 in msgs_template):
                            msgs_SnapshotFullRefreshTCP61.append(
                                main_template.SnapshotFullRefreshTCP61(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefreshTCP61) >= chunk_size:

                            msgs_SnapshotFullRefreshTCP61 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefreshTCP61))
                            msgs_SnapshotFullRefreshTCP61.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefreshTCP61}_{chunk_index61}.pkl")
                            chunk_index61 += 1
                            msgs_SnapshotFullRefreshTCP61 = []

                    elif TemplateID == 62:

                        if (msgs_template is None) or (62 in msgs_template):
                            msgs_CollateralMarketValue62.append(
                                main_template.CollateralMarketValue62(messages, BlockLength, cme_packet))

                        if len(msgs_CollateralMarketValue62) >= chunk_size:

                            msgs_CollateralMarketValue62 = pd.DataFrame(
                                chain.from_iterable(msgs_CollateralMarketValue62))
                            msgs_CollateralMarketValue62.to_pickle(
                                f"{save_file_path}/{msgs_CollateralMarketValue62}_{chunk_index62}.pkl")
                            chunk_index62 += 1
                            msgs_CollateralMarketValue62 = []

                    elif TemplateID == 63:

                        if (msgs_template is None) or (63 in msgs_template):
                            msgs_MDInstrumentDefinitionFX63.append(
                                main_template.MDInstrumentDefinitionFX63(messages, BlockLength, cme_packet))

                        if len(msgs_MDInstrumentDefinitionFX63) >= chunk_size:

                            msgs_MDInstrumentDefinitionFX63 = pd.DataFrame(
                                chain.from_iterable(msgs_MDInstrumentDefinitionFX63))
                            msgs_MDInstrumentDefinitionFX63.to_pickle(
                                f"{save_file_path}/{msgs_MDInstrumentDefinitionFX63}_{chunk_index63}.pkl")
                            chunk_index63 += 1
                            msgs_MDInstrumentDefinitionFX63 = []

                    elif TemplateID == 64:

                        if (msgs_template is None) or (64 in msgs_template):
                            msgs_MDIncrementalRefreshBookLongQty64.append(
                                main_template.MDIncrementalRefreshBookLongQty64(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshBookLongQty64) >= chunk_size:

                            msgs_MDIncrementalRefreshBookLongQty64 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshBookLongQty64))
                            msgs_MDIncrementalRefreshBookLongQty64.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshBookLongQty64}_{chunk_index64}.pkl")
                            chunk_index64 += 1
                            msgs_MDIncrementalRefreshBookLongQty64 = []

                    elif TemplateID == 65:

                        if (msgs_template is None) or (65 in msgs_template):
                            msgs_MDIncrementalRefreshTradeSummaryLongQty65.append(
                                main_template.MDIncrementalRefreshTradeSummaryLongQty65(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshTradeSummaryLongQty65) >= chunk_size:

                            msgs_MDIncrementalRefreshTradeSummaryLongQty65 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshTradeSummaryLongQty65))
                            msgs_MDIncrementalRefreshTradeSummaryLongQty65.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshTradeSummaryLongQty65}_{chunk_index65}.pkl")
                            chunk_index65 += 1
                            msgs_MDIncrementalRefreshTradeSummaryLongQty65 = []

                    elif TemplateID == 66:

                        if (msgs_template is None) or (66 in msgs_template):
                            msgs_MDIncrementalRefreshVolumeLongQty66.append(
                                main_template.MDIncrementalRefreshVolumeLongQty66(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshVolumeLongQty66) >= chunk_size:

                            msgs_MDIncrementalRefreshVolumeLongQty66 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshVolumeLongQty66))
                            msgs_MDIncrementalRefreshVolumeLongQty66.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshVolumeLongQty66}_{chunk_index66}.pkl")
                            chunk_index66 += 1
                            msgs_MDIncrementalRefreshVolumeLongQty66 = []

                    elif TemplateID == 67:

                        if (msgs_template is None) or (67 in msgs_template):
                            msgs_MDIncrementalRefreshSessionStatisticsLongQty67.append(
                                main_template.MDIncrementalRefreshSessionStatisticsLongQty67(messages, BlockLength, cme_packet))

                        if len(msgs_MDIncrementalRefreshSessionStatisticsLongQty67) >= chunk_size:

                            msgs_MDIncrementalRefreshSessionStatisticsLongQty67 = pd.DataFrame(
                                chain.from_iterable(msgs_MDIncrementalRefreshSessionStatisticsLongQty67))
                            msgs_MDIncrementalRefreshSessionStatisticsLongQty67.to_pickle(
                                f"{save_file_path}/{msgs_MDIncrementalRefreshSessionStatisticsLongQty67}_{chunk_index67}.pkl")
                            chunk_index67 += 1
                            msgs_MDIncrementalRefreshSessionStatisticsLongQty67 = []

                    elif TemplateID == 68:

                        if (msgs_template is None) or (68 in msgs_template):
                            msgs_SnapshotFullRefreshTCPLongQty68.append(
                                main_template.SnapshotFullRefreshTCPLongQty68(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefreshTCPLongQty68) >= chunk_size:

                            msgs_SnapshotFullRefreshTCPLongQty68 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefreshTCPLongQty68))
                            msgs_SnapshotFullRefreshTCPLongQty68.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefreshTCPLongQty68}_{chunk_index68}.pkl")
                            chunk_index68 += 1
                            msgs_SnapshotFullRefreshTCPLongQty68 = []

                    elif TemplateID == 69:

                        if (msgs_template is None) or (69 in msgs_template):
                            msgs_SnapshotFullRefreshLongQty69.append(
                                main_template.SnapshotFullRefreshLongQty69(messages, BlockLength, cme_packet))

                        if len(msgs_SnapshotFullRefreshLongQty69) >= chunk_size:

                            msgs_SnapshotFullRefreshLongQty69 = pd.DataFrame(
                                chain.from_iterable(msgs_SnapshotFullRefreshLongQty69))
                            msgs_SnapshotFullRefreshLongQty69.to_pickle(
                                f"{save_file_path}/{msgs_SnapshotFullRefreshLongQty69}_{chunk_index69}.pkl")
                            chunk_index69 += 1
                            msgs_SnapshotFullRefreshLongQty69 = []

                if notnull(max_read_packets):
                    read += 1
                    pbar.update(1)
                else:
                    read = end_pos
                    pbar.update(packet_length + 16)

                f.seek(end_pos)

    if isnull(save_file_path):

        raise Exception('Path for saved files must be provided')

    for var_name, var_value in globals().items():
        if var_name.startswith("msgs_"):
            df = pd.DataFrame(chain.from_iterable(var_value))
            df.to_pickle(f"{save_file_path}/{var_name}.pkl")
            del globals()[var_name]
            print(' --> Dataframe: Success!')


def timestamp_conversion(msgs_data, USCentralTime=True, timezone=None):
    """
    Convert the timestamps, including SendingTime and TransactTime.

    Parameters
    ----------
    msgs_data : pandas DataFrame
        Message data need to convert timestamps.
    USCentralTime : str, optional
        Whether to convert the timestamp to the U.S. Central Time. The default is True.
    timezone : str, optional
        The target timezone needs to be converted. The default is None.

    Returns
    -------
    msgs_data : pandas DataFrame
        message data with converted timestamps.

    """

    msgs_data['SendingTime'] = pd.to_datetime(
        msgs_data['SendingTime'], utc=True, unit='ns')
    msgs_data['TransactTime'] = pd.to_datetime(
        msgs_data['TransactTime'], utc=True, unit='ns')

    if USCentralTime:

        msgs_data['SendingTime'] = msgs_data['SendingTime'].dt.tz_convert(
            'America/Chicago')
        msgs_data['TransactTime'] = msgs_data['TransactTime'].dt.tz_convert(
            'America/Chicago')

    else:

        if isnull(timezone):
            raise Exception('Targeted timezone must be provided')

        else:
            msgs_data['SendingTime'] = msgs_data['SendingTime'].dt.tz_convert(
                timezone)
            msgs_data['TransactTime'] = msgs_data['TransactTime'].dt.tz_convert(
                timezone)

    return msgs_data
