##########################
# Import Needed Packages #
##########################
import pandas as pd
import datetime as dt
import os
import gc
import time
import numpy as np
from collections import Counter
import copy
import time
import re
import wrds
from scipy import stats
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from dateutil.relativedelta import relativedelta
from Functions import Utilis as Util
import importlib
# importlib.reload(module)


############
# Settings #
############
"""
General
"""
printing = True
pd.options.display.float_format = '{:.4f}'.format
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


"""
Paths
"""
# Information to be changed by user
user = 'abiss'
basepath = '/Documents/Local Data Storage/'

# Basepaths
basepath = "C:/Users/" + user + basepath
fundsBasePath = basepath + "CRSP"

# Input paths and names
inputPath = fundsBasePath + "/Input/"
Util.createFolder(inputPath)
fund_summary_name = "fund_summary_full.csv"
CRSP_FUNDNO_WFICN_name = "Mflinks_crsp.csv"
FRONT_LOAD_name = "FrontLoads.csv"
REAR_LOAD_name = "RearLoads.csv"
monthly_return_name = "MonthlyReturns.csv"
CRSP_Holdings_name = "CRSPHoldings.csv"
map_fundno_portno_name = "Map_fundno_portno.csv"
stocks_monthly_name = "MonthlyStockFile.csv"
FUNDNO_WFICN_name = "Mflinks_thompson.csv"
thomson_Holdings_name = "thompsonmasterfile.csv"

# Output path
outputPath = fundsBasePath + "/dbcourse_output/"
Util.createFolder(outputPath)
fund_summary_ActiveEq_name = "fund_summary_cleaned_ActiveEq.csv"
fund_summary_ActiveEq_final_name = "fund_summary_cleaned_ActiveEq_Final.csv"
fund_summary_all_name = 'fund_summary_cleaned.csv'
missing_tna_name = 'tna_miss.csv'
historical_names_record = 'historical_fund_names.csv'
fund_summary_agg_ActiveEq_name = 'agg_fund_summary.csv'
fund_summary_agg_ActiveEq_final_name = 'agg_fund_summary_final.csv'
filled_map_name = 'MonthlyMap.csv'
filled_map_final_name = 'MonthlyMapFinal.csv'
filled_agg_map_name = 'MonthlyAggregatedMap.csv'
filled_agg_map_final_name = 'MonthlyAggregatedMapFinal.csv'
MTNA_name = 'MTNA.csv'
aggMTNA_name = 'aggMTNA.csv'
agg_monthly_return_name = 'agg_MonthlyReturns.csv'
agg_monthly_return_final_name = 'agg_MonthlyReturnsFinal.csv'
duplicate_wficn_name = 'wficn_duplicates.csv'
bond_holdings_name = 'bond_holdings.csv'
other_holdings_name = 'other_holdings.csv'
other_thomson_holdings_name = 'other_thomson_holdings.csv'
output_crsp_holdings_name = 'CRSP_holdings.csv'
filled_output_crsp_holdings_name = 'CRSP_filled_holdings.csv'
output_thomson_holdings_name = 'Thomson_holdings.csv'
filled_output_thomson_holdings_name = 'Thomson_filled_holdings.csv'
filled_output_holdings_name = 'filled_holdings.csv'
output_holdings_name = 'filled_holdings.csv'
freqReporting_holdings_name = 'freq_holdings_reporting.csv'
finalFundsNum_holdings_name = 'final_holdings_count.csv'
fuzzyNoGroup_name = "fuzzyMapping_noGroup.csv"
fuzzyMatch_name = "fuzzyMapping_wGroup.csv"
fuzzyMatch_all_name = "fuzzyMapping_wGroup_multiOptions.csv"
fuzzyMatchNoG_all_name = "fuzzyMapping_noGroup_multiOptions.csv"
groupsNotMapped_name = "groupsNotMapped.csv"
crsp_final_map_name = "crsp_final_map.csv"
final_monthly_name = 'final_monthly_panel.csv'
Compared_TNA_name = 'Compared_TNA.csv'
avail_pfolioIDs_name = "available_pfolioIDs.csv"


"""
Parameter Values
"""
maxRET = 20
maxPRC = 10000000000
percDetail = [0.01, 0.05, 0.10, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99]
percDetailPlus = [0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.01, 0.05, 0.10, 0.25, 0.5,
                         0.75, 0.90, 0.95, 0.99, 0.991, 0.992, 0.993, 0.994, 0.995, 0.996, 0.997, 0.998, 0.999]


"""
Lists
"""
# Index Fund Names
eliminated_content = ['Index', 'Ind', 'Idx', 'Indx', 'iShares', 'SPDR', 'HOLDRs', 'ETF',
                      'Exchange-Traded Fund', 'PowerShares', 'StreetTRACKS']

# Name Standardization Exceptions
exceptions = dict()
exceptions["AIM Investment Securities Funds (Invesco Investment Securities Funds)"] = \
    "aim investment securities funds (invesco investment securiti"
exceptions["AIM Investment Series"] = "aim investment funds (invesco investment funds)"
exceptions["AIM Investment Series (Invesco Investment Series)"] = "aim investment funds (invesco investment funds)"
exceptions["AIM Variable Insurance Funds (Invesco Variable Insurance Funds)"] = \
    "aim variable insurance funds (invesco variable insurance fun"
exceptions["American General Series Portfolio Company 2"] = "american general series portfolio co 2"
exceptions["AmSouth Funds"] = "amsouth mutual funds"
exceptions["Ariel Growth Fund (DBA"] = "ariel growth fund"
exceptions["ARK Funds"] = "ark funds ma"
exceptions["AXP Growth Series, Inc"] = "axp growth series inc mn"
exceptions["AXP Strategey Series, Inc"] = "axp strategy series inc"
exceptions["Baron Investment Funds Trust"] = "baron investment funds trust (f k a baron asset fund)"
exceptions["Baron Select Fund"] = "baron select funds"
exceptions["BBH Funds, Inc"] = "bbh fund inc"
exceptions["BPV Family Funds"] = "bpv family of funds"
exceptions["Brandywine Blue Midcap Growth Fund, Inc"] = "brandywine blue fund inc"
exceptions["BSG Funds"] = "diamond hill funds"
exceptions["BSG Funds"] = "calamos investment trust il"
exceptions["Calamos Investment Trust"] = "calamos investment trust il"
exceptions["Capstone Social Ethics and Religious Values Fund"] = "capstone social ethics & religious values fund"
exceptions["CitiTrust II"] = "Smith Barney Trust II"
exceptions["CNI Charter Funds Inc"] = "cni charter funds"
exceptions["Conventry Group"] = "coventry group"
exceptions["CornerCap Group of Funds"] = "cornercap group of funds  va"
exceptions["Credit Suisse Institutional Series Fund"] = "credit suisse institutional fund inc"
exceptions["Croft Funds Corporation"] = "croft funds corp"
exceptions["Delaware Group Equity Funds I, Inc."] = "delaware group equity funds i"
exceptions["Delaware Group Equity Funds II, Inc."] = "delaware group equity funds ii"
exceptions["Delaware Group Equity Funds III, Inc."] = "delaware group equity funds iii"
exceptions["Delaware Group Equity Funds IV, Inc."] = "delaware group equity funds iv"
exceptions["Dresdner RCM Capital Funds, Inc"] = "dresdner rcm global funds inc"
exceptions["Dreyfus Growth and Value Funds, Inc."] = "dreyfus growth & value funds inc"
exceptions["E*TRADE Funds"] = "e trade funds"
exceptions["Embarcadero Funds"] = "embarcadero funds inc"
exceptions["Endowments Trust"] = "endowments de"
exceptions["Evergreen Equity Funds"] = "evergreen equity trust de"
exceptions["Evergreen Equity Trust"] = "evergreen equity trust de"
exceptions["Exeter"] = "exeter fund inc ny"
exceptions["Exeter Fund, Inc"] = "exeter fund inc ny"
exceptions["Flex-funds Trust"] = "flex funds"
exceptions["Frank Russell Investment Company"] = "RUSSELL FRANK INVESTMENT CO"
exceptions["Galaxy Fund"] = "galaxy fund de"
exceptions["GE RSP Program Funds"] = "general electric rsp u s equity fund"
exceptions["GE S&S Program Funds"] = "general electric s&s program mutual fund"
exceptions["Great-West Fund, Inc"] = "great west funds inc"
exceptions["Hartford Mutual Funds, Inc"] = "hartford mutual funds inc ct"
exceptions["HighMark Funds"] = "highmark funds ma"
exceptions["IDEX Mutual Funds"] = "idex mutual fds"
exceptions["Infinity Mutual Funds, Inc."] = "infinity mutual funds inc md"
exceptions["Hartford Mutual Funds, Inc"] = "hartford mutual funds inc ct"
exceptions["HighMark Funds"] = "highmark funds ma"
exceptions["IDEX Mutual Funds"] = "idex mutual fds"
exceptions["Infinity Mutual Funds, Inc."] = "infinity mutual funds inc md"
exceptions["Institutional Equity Funds, Inc"] = "institutional domestic equity funds inc"
exceptions["INVESCO Advantage Series Funds, Inc"] = "INVESCO Counselor Series Funds, Inc"
exceptions["Ivy Funds Variable Insurance Portfolio, Inc"] = "Ivy Funds Variable Insurance Portfolios"
exceptions["J.P. Morgan Funds"] = "jp morgan funds"
exceptions["J.P. Morgan Series Trust"] = "jp morgan series trust"
exceptions["John Hancock Equity Trust"] = "HANCOCK JOHN EQUITY TRUST"
exceptions["John Hancock Institutional Series Trust"] = "hancock john institutional series trust"
exceptions["JP Morgan Mutual Fund Group"] = "JP MORGAN MUTUAL FUND SERIES"
exceptions["Liberty-Stein Roe Advisors Trust"] = "liberty stein roe advisor trust"
exceptions["Magna Funds"] = "magna funds ma"
exceptions["MainStay Institutional Funds, Inc"] = "MainStay Institutional Funds, Inc."
exceptions["Mairs & Power Growth Fund series of the Trust"] = "Mairs & Power Funds Trust"
exceptions["Managed Accounts Services Portfolio Trust"] = "PACE Select Advisors Trust"
exceptions["MAS Funds"] = "mas funds ma"
exceptions["Masters' Select Funds Trust"] = "MASTERS SELECT FUNDS TRUST"
exceptions["Morgan Stanley Dean Witter Instl Fund, Inc"] = "MORGAN STANLEY DEAN WITTER INSTITUTIONAL FUND INC"
exceptions["Mutual of America Investment Corporation"] = "mutual of america investment corp"
exceptions["Nationwide Investing Foundation III"] = "Gartmore Mutual Funds"
exceptions["Nuveen Investment Trust I"] = "nuveen investment trust"
exceptions["Oak Associates Fund"] = "oak associates funds"
exceptions["Old Mutual Advisor Funds, Inc"] = "old mutual advisor funds"
exceptions["Oppenheimer Quest Funds"] = "oppenheimer quest for value funds"
exceptions["Optique Funds, Inc"] = "optique funds"
exceptions["Pacific Advisors Fund Inc"] = "PACIFIC GLOBAL FUND INC"
exceptions["Payden Funds"] = "paydenfunds"
exceptions["Phoenix Duff & Phelps Instl Mutual Funds"] = "phoenix duff & phelps institutional mutual funds"
exceptions["Phoenix-Engemann Trust"] = "phoenix engemann funds"
exceptions["Pilgrim Investment Funds, Inc"] = "pilgrim investment funds inc md"
exceptions["Pilgrim Mutual Funds Trust"] = "PILGRIM EQUITY TRUST"
exceptions["Pioneer Variable Contracts Trust"] = "pioneer variable contracts trust ma"
exceptions["Premier Equity Funds, Inc."] = "DREYFUS PREMIER EQUITY FUNDS INC"
exceptions["Prudential Investment Series Fund, Inc"] = "PRUDENTIAL INVESTMENT PORTFOLIOS INC"
exceptions["Prudential Jennison Series Fund, Inc."] = "PRUDENTIAL INVESTMENT PORTFOLIOS INC"
exceptions["Prudential Series Fund, Inc"] = "PRUDENTIAL SERIES FUND"
exceptions["Ranger Funds Trust"] = "ranger funds investment trust"
exceptions["Regions Morgan Keegan Select Fund, Inc"] = "MORGAN KEEGAN SELECT FUND INC"
exceptions["Riverfront Funds"] = "RIVERFRONT FUNDS / NJ"
exceptions["Russell Investment Company"] = "RUSSELL INVESTMENT CO"
exceptions["SA Funds - Investment Trust"] = "sa funds investment trust"
exceptions["Security Large Cap Value Fund"] = "security large cap value fund ks"
exceptions["Smith Barney Trust III"] = "smith barney trust ii"
exceptions["State Farm Associates' Funds Trust"] = "state farm associates funds trusts"
exceptions["Stein Roe Advisor Trust"] = "LIBERTY STEIN ROE ADVISOR TRUST"
exceptions["Stralem Funds"] = "stralem fund"
exceptions["SunAmerica Style Select Series, Inc"] = "SUNAMERICA SERIES, INC."
exceptions["TIAA-CREF Mutual Funds"] = "TIAA CREF MUTUAL FUND"
exceptions["Touchstone Institutional Portfolios"] = "Touchstone Institutional Funds Trust"
exceptions["U.S. Global Investors Funds"] = "us global investors funds"
exceptions["US Global Accolade Funds"] = "US GLOBAL INVESTORS FUNDS"
exceptions["VALIC Company I"] = "valic co i"
exceptions["VALIC Company II"] = "valic co ii"
exceptions["Van Kampen World Port Srs Trust"] = "van kampen world portfolio series trust"
exceptions["Variable Insurance Products Fund III"] = "VARIABLE INSURANCE PRODUCTS III"
exceptions["Virtus Insight Funds Trust"] = "VIRTUS INSIGHT TRUST"
exceptions["Vision Group of Funds, Inc."] = "Vision Group of Funds, Inc"
exceptions["Voyageur Mutual Funds III, Inc."] = "VOYAGEUR MUTUAL FUNDS III INC /MN/"
exceptions["Waddell & Reed Advisors Funds, Inc"] = "WADDELL & REED ADVISORS SMALL CAP FUND INC"
exceptions["Weiss, Peck & Greer Funds Trust"] = "weiss peck & greer funds trust ma"
exceptions["Yacktman Funds, Inc"] = "yacktman fund inc"

# Group extraction exceptions
grpExceptions = dict()
grpExceptions["CARNEGIE-CAPPIELLO TRUST-GROWTH SERIES"] = "CARNEGIE-CAPPIELLO TRUST"
grpExceptions["CARNEGIE-CAPPIELLO TRUST-TOTAL RETURN SERIES"] = "CARNEGIE-CAPPIELLO TRUST"
grpExceptions["SMITH BARNEY FUNDS-CAPITAL APPRECIATION/C"] = "SMITH BARNEY FUNDS"
grpExceptions["INVESTMENT SERIES TRUST-CAPITAL GROWTH/INVEST"] = "INVESTMENT SERIES TRUST"
grpExceptions["FIRST AMERICAN INVESTMENT-REGIONAL EQUITY"] = "FIRST AMERICAN INVESTMENT"
grpExceptions["GIT EQUITY TRUST-SELECT GROWTH PORTFLIO"] = "GIT EQUITY TRUST"
grpExceptions["SMITH BARNEY FUNDS-CAPITAL APPRECIATION/A"] = "SMITH BARNEY FUNDS"
grpExceptions["SMITH BARNEY FUNDS-CAPITAL APPRECIATION/B"] = "SMITH BARNEY FUNDS"
grpExceptions["Fidelity Advisor Equity-Income Fund/Instl"] = "Fidelity Advisor Equity"
grpExceptions["Fidelity Advisor Equity-Income Fund/A"] = "Fidelity Advisor Equity"
grpExceptions["Fidelity Advisor Equity-Income Fund/B"] = "Fidelity Advisor Equity"
grpExceptions["MAS POOLED TRUST FUND-EMERGING GROWTH PORT"] = "MAS POOLED TRUST FUND"
grpExceptions["PERFORMANCE FUNDS TRUST-EQUITY FUND/INSTL"] = "PERFORMANCE FUNDS TRUST"
grpExceptions["ZWEIG SERIES TRUST-APPRECIATION FUND/A"] = "ZWEIG SERIES TRUST"
grpExceptions["ZWEIG SERIES TRUST-STRATEGY/B"] = "ZWEIG SERIES TRUST"
grpExceptions["ZWEIG SERIES TRUST-APPRECIATION/B"] = "ZWEIG SERIES TRUST"
grpExceptions["PAPP AMERICA-ABROAD FUND"] = "PAPP AMERICA"
grpExceptions["INTEGRITY-JORDAN AMERICAN VALUE PORTFOLIO"] = "INTEGRITY"
grpExceptions["INTEGRITY-JORDAN EMERGING AMERICA PORTFOLIO"] = "INTEGRITY"
grpExceptions["SMITH BARNEY FUNDS-CAPITAL APPRECIATION/C"] = "SMITH BARNEY FUNDS"

# Fund extraction exceptions
fundExceptions = dict()
fundExceptions["CARNEGIE-CAPPIELLO TRUST-GROWTH SERIES"] = "GROWTH SERIES"
fundExceptions["CARNEGIE-CAPPIELLO TRUST-TOTAL RETURN SERIES"] = "TOTAL RETURN SERIES"
fundExceptions["INVESTMENT SERIES TRUST-CAPITAL GRTH/A"] = "CAPITAL GRTH"
fundExceptions["INVESTMENT SERIES TRUST-CAPITAL GROWTH/INVEST"] = "CAPITAL GROWTH"
fundExceptions["FIRST AMERICAN INVESTMENT-REGIONAL EQUITY"] = "REGIONAL EQUITY"
fundExceptions["GIT EQUITY TRUST-SELECT GROWTH PORTFLIO"] = "SELECT GROWTH PORTFLIO"
fundExceptions["SMITH BARNEY FUNDS-CAPITAL APPRECIATION/A"] = "CAPITAL APPRECIATION"
fundExceptions["SMITH BARNEY FUNDS-CAPITAL APPRECIATION/B"] = "CAPITAL APPRECIATION"
fundExceptions["Fidelity Advisor Equity-Income Fund/Instl"] = "Income Fund"
fundExceptions["Fidelity Advisor Equity-Income Fund/A"] = "Income Fund"
fundExceptions["Fidelity Advisor Equity-Income Fund/B"] = "Income Fund"
fundExceptions["MAS POOLED TRUST FUND-EMERGING GROWTH PORT"] = "EMERGING GROWTH PORT"
fundExceptions["PERFORMANCE FUNDS TRUST-EQUITY FUND/INSTL"] = "EQUITY FUND/INSTL"
fundExceptions["ZWEIG SERIES TRUST-APPRECIATION FUND/A"] = "APPRECIATION FUND"
fundExceptions["ZWEIG SERIES TRUST-STRATEGY/B"] = "STRATEGY"
fundExceptions["ZWEIG SERIES TRUST-APPRECIATION/B"] = "APPRECIATION"
fundExceptions["PAPP AMERICA-ABROAD FUND"] = "ABROAD FUND"
fundExceptions["INTEGRITY-JORDAN AMERICAN VALUE PORTFOLIO"] = "JORDAN AMERICAN VALUE PORTFOLIO"
fundExceptions["INTEGRITY-JORDAN EMERGING AMERICA PORTFOLIO"] = "JORDAN EMERGING AMERICA PORTFOLIO"
fundExceptions["SMITH BARNEY FUNDS-CAPITAL APPRECIATION/C"] = "CAPITAL APPRECIATION"

# Variables that should be unique per group month for aggregation
features = ['adv_name', 'mgmt_name', 'mgr_dt', 'mgr_name', 'fund_name_short', 'wficn', 'crsp_obj_cd', 'lipper_obj_cd']

# Variables to which weighted mean should be applied for aggregation
# wtMeanList = ['per_cash', 'per_com', 'per_pref', 'per_conv', 'per_corp', 'per_muni', 'per_govt', 'per_oth',
#              'per_bond', 'per_abs', 'per_mbs', 'per_eq_oth', 'per_fi_oth', 'exp_ratio', 'turn_ratio',
#              'front_load', 'rear_load', 'load', 'mgmt_fee', 'actual_12b1']
wtMeanList = ['per_cash', 'per_com', 'per_pref', 'per_conv', 'per_corp', 'per_muni', 'per_govt', 'per_oth',
              'per_bond', 'per_abs', 'per_mbs', 'per_eq_oth', 'per_fi_oth', 'exp_ratio', 'turn_ratio',
              'mgmt_fee', 'actual_12b1']

# Mgr_name Typos
Typos = [{'crsp_cl_grp': 2001065, 'caldt': dt.datetime(2014, 9, 30),
          'Correct Mgr_name': 'Ferretti/Otto'},
         {'crsp_cl_grp': 2001065, 'caldt': dt.datetime(2014, 12, 31),
          'Correct Mgr_name': 'Ferretti/Otto'},
         {'crsp_cl_grp': 2001065, 'caldt': dt.datetime(2015, 3, 31),
          'Correct Mgr_name': 'Ferretti/Otto'},
         {'crsp_cl_grp': 2018710, 'caldt': dt.datetime(2013, 9, 30),
          'Correct Mgr_name': 'Wicker/Braverman/Trenum'},
         {'crsp_cl_grp': 2018710, 'caldt': dt.datetime(2013, 12, 31),
          'Correct Mgr_name': 'Wicker/Braverman/Trenum'},
         {'crsp_cl_grp': 2018710, 'caldt': dt.datetime(2014, 3, 31),
          'Correct Mgr_name': 'Wicker/Braverman/Trenum'},
         {'crsp_cl_grp': 2018710, 'caldt': dt.datetime(2014, 6, 30),
          'Correct Mgr_name': 'Wicker/Braverman/Trenum'},
         {'crsp_cl_grp': 2018710, 'caldt': dt.datetime(2014, 9, 30),
          'Correct Mgr_name': 'Wicker/Braverman/Trenum'},
         {'crsp_cl_grp': 2018710, 'caldt': dt.datetime(2014, 12, 31),
          'Correct Mgr_name': 'Wicker/Braverman/Trenum'},
         {'crsp_cl_grp': 2018710, 'caldt': dt.datetime(2015, 3, 31),
          'Correct Mgr_name': 'Wicker/Braverman/Trenum'},
         {'crsp_cl_grp': 2018710, 'caldt': dt.datetime(2015, 6, 30),
          'Correct Mgr_name': 'Wicker/Braverman/Trenum'},
         {'crsp_cl_grp': 2018710, 'caldt': dt.datetime(2015, 9, 30),
          'Correct Mgr_name': 'Wicker/Braverman/Trenum'},
         {'crsp_cl_grp': 2018710, 'caldt': dt.datetime(2015, 12, 31),
          'Correct Mgr_name': 'Wicker/Braverman/Trenum'},
         {'crsp_cl_grp': 2018710, 'caldt': dt.datetime(2016, 3, 31),
          'Correct Mgr_name': 'Wicker/Braverman/Trenum'},
         {'crsp_cl_grp': 2018710, 'caldt': dt.datetime(2016, 6, 30),
          'Correct Mgr_name': 'Wicker/Braverman/Trenum'}]

# Variables Type
fund_var_types = {'summary_period2': object, 'crsp_fundno': int, 'caldt': object, 'nav_latest': object,
                  'nav_latest_dt': object, 'tna_latest': float, 'tna_latest_dt': object, 'yield': float,
                  'div_ytd': float, 'cap_gains_ytd': float, 'nav_52w_h': float, 'nav_52w_h_dt': object,
                  'nav_52w_l': object, 'nav_52w_l_dt': object, 'unrealized_app_dep': float,
                  'unrealized_app_dt': object, 'asset_dt': object, 'per_com': float, 'per_pref': float,
                  'per_conv': float, 'per_corp': float, 'per_muni': float, 'per_govt': float,
                  'per_oth': float, 'per_cash': float, 'per_bond': float, 'per_abs': float,
                  'per_mbs': float, 'per_eq_oth': float, 'per_fi_oth': float, 'maturity': float,
                  'maturity_dt': object, 'cusip8': object, 'crsp_portno': float, 'crsp_cl_grp': float,
                  'fund_name_long': object, 'ticker': object, 'ncusip': object, 'mgmt_name': object,
                  'mgmt_cd': object, 'mgr_name': object, 'mgr_dt': object, 'adv_name': object,
                  'open_to_inv': object, 'retail_fund': object, 'inst_fund': object, 'm_fund': object,
                  'index_fund_flag': object, 'vau_fund': object, 'et_flag': float, 'delist_cd': object,
                  'first_offer_dt': object, 'end_dt': object, 'dead_flag': object, 'merge_fundno': float,
                  'actual_12b1': float, 'max_12b1': float, 'exp_ratio': float, 'mgmt_fee': float,
                  'turn_ratio': float, 'fiscal_yearend': float, 'crsp_obj_cd': object, 'si_obj_cd': object,
                  'accrual_fund': object, 'sales_restrict': object, 'wbrger_obj_cd': object, 'policy': object,
                  'lipper_class': object, 'lipper_class_name': object, 'lipper_obj_cd': object,
                  'lipper_obj_name': object, 'lipper_asset_cd': object, 'lipper_tax_cd': object,
                  'front_load': float, 'rear_load': float, 'load': float, 'wficn': float, 'share_class': object,
                  'fund_name_short': object, 'group_name': object, 'monthend': object}

agg_fund_var_types = {'crsp_cl_grp': float, 'caldt': object, 'adv_name': object, 'mgmt_name': object,
                      'mgr_dt': object, 'mgr_name': object, 'fund_name_short': object, 'wficn': float,
                      'tna_latest': float, 'crsp_portno': float, 'altervative_portno_1': float,
                      'altervative_portno_2': float, 'weighted_per_cash': float,
                      'weighted_exp_ratio': float, 'weighted_turn_ratio': float,
                      'weighted_front_load': float, 'weighted_rear_load': float,
                      'weighted_load': float, 'weighted_mgmt_fee': float,
                      'weighted_actual_12b1': float, 'equal_weight': float, 'monthend': object}

FUNDNO_WFICN_var_types = {'wficn': float, 'fdate': object, 'fundno': int, 'fundname': object,
                          'mgrcoab': object, 'rdate': object, 'assets': float, 'ioc': float,
                          'prdate': object, 'country': object, 'fundno_id': int, 'Num_Holdings': float,
                          'percentage_crsp_holding': float}

crsp_holdings_var_types = {'crsp_portno': float, 'report_dt': int, 'security_rank': int,
                           'eff_dt': int, 'percent_tna': float, 'nbr_shares': float,
                           'market_val': float, 'security_name': object, 'cusip': object,
                           'permno': float, 'permco': float, 'ticker': object, 'coupon': float, 'maturity_dt': float}

thomson_holdings_var_types = {'fdate': int, 'fundno': int, 'fundname': object, 'mgrcoab': object,
                              'rdate': int, 'assets': float, 'ioc': float, 'prdate': float, 'country': object,
                              'cusip': object, 'shares': float, 'change': float, 'stkname': object,
                              'ticker': object, 'exchcd': object, 'stkcd': object, 'indcode': float,
                              'stkcdesc': object, 'prc': float, 'shrout1': float, 'shrout2': float}

holdings_var_types = {'monthend': object, 'crsp_cl_grp': float, 'crsp_portno': float, 'wficn': float,
                      'permno': float, 'shares': float, 'cusip': object, 'date_original': object,
                      'adj_monthend': float, 'adj_date_original': float, 'shares_adj': float, 'prc': float,
                      'ret': float, 'value': float, 'total_value': float, 'weight': float, 'num_holdings': int}

fund_port_map_var_types = {'crsp_fundno': int, 'crsp_portno': int, 'begdt': int, 'enddt': int, 'cusip8': object,
                           'fund_name': object, 'ncusip': object, 'first_offer_dt': object, 'mgmt_name': object,
                           'mgmt_cd': object, 'mgr_name': object, 'mgr_dt': float, 'adv_name': object,
                           'open_to_inv': object, 'retail_fund': object, 'inst_fund': object,
                           'end_dt': float, 'dead_flag': object, 'delist_cd': object, 'merge_fundno': float}

stock_var_types = {'PERMNO': int, 'date': int, 'NAMEENDT': float, 'SHRCD': float,
                   'EXCHCD': float, 'SICCD': object, 'NCUSIP': object, 'TICKER': object, 'COMNAM': object,
                   'SHRCLS': object, 'TSYMBOL': object, 'NAICS': float, 'PRIMEXCH': object,
                   'TRDSTAT': object, 'SECSTAT': object, 'PERMCO': int, 'ISSUNO': int,
                   'HEXCD': int, 'HSICCD': object, 'CUSIP': object, 'DCLRDT': float,
                   'DLAMT': float, 'DLPDT': float, 'DLSTCD': float, 'NEXTDT': float,
                   'PAYDT': float, 'RCRDDT': float, 'SHRFLG': float, 'HSICMG': float,
                   'HSICIG': float, 'DISTCD': float, 'DIVAMT': float, 'FACPR': float,
                   'FACSHR': float, 'ACPERM': float, 'ACCOMP': float, 'SHRENDDT': float,
                   'NWPERM': float, 'DLRETX': object, 'DLPRC': float, 'DLRET': object,
                   'TRTSCD': float, 'NMSIND': float, 'MMCNT': float, 'NSDINX': float,
                   'BIDLO': float, 'ASKHI': float, 'PRC': float, 'VOL': float, 'RET': object,
                   'BID': float, 'ASK': float, 'SHROUT': float, 'CFACPR': float, 'CFACSHR': float,
                   'ALTPRC': float, 'SPREAD': float, 'ALTPRCDT': float, 'RETX': object,
                   'vwretd': float, 'vwretx': float, 'ewretd': float, 'ewretx': float, 'sprtrn': float}

return_var_types = {'caldt': int, 'crsp_fundno': int, 'mtna': object, 'mret': object, 'mnav': object}

aggMTNA_var_types = {'Unnamed: 0': int, 'crsp_cl_grp': float, 'monthend': object, 'mtna': float, 'lag_mtna': float}
