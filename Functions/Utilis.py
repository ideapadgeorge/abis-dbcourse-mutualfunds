##########################
# Import Needed Packages #
##########################
import re
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import datetime as dt
import string
from fuzzywuzzy import process
import copy
import os
import glob


#############
# Functions #
#############


def scan_folders(folder, extension):
    """
    Scans for files with the .txt extension and puts the names in a list
    :param folder: location of the folder in which to compute the search
    :return: list with names of files in folder with .txt extension
    """
    old_folder = os.getcwd()
    os.chdir(folder)
    files = []
    for name in glob.glob('*.' + extension):
        files.append(name)
    os.chdir(old_folder)
    return files


def get_share_class(s):
    """
    It extracts the share class name from the pre-processed fund name.
    :param s: pre-processed fund name
    :return: share class name
    """
    l = re.split('; *|/ *|\\\\', str(s))
    if len(l) > 1:
        if len(re.split(';|/|\\\\|,|:', l[-1])) == 1:
            return l[-1]
    l = re.split(r'[^A-Za-z ] *[c|C]lass', str(s))
    if len(l) > 1:
        return 'Class' + l[-1]
    else:
        if s == 'Alliance Quasar Fund, Inc.: Advisor Class Shares' or 'The Alliance Fund, Inc.: Advisor Class Shares':
            return 'Advisor Class Shares'
        else:
            return np.nan


def get_short_fundname(s):
    """
    It extracts the short fund name from the pre-processed fund name, which means eliminating the share class name.
    :param s: pre-processed fund name
    :return: short fund name
    """
    l = re.split('; *|/ *|\\\\', str(s))
    if len(l) == 2:
        return l[0]
    elif len(l) > 2:
        return ';'.join(l[:-1])
    else:
        l = re.split(r'[^A-Za-z ] *[c|C]lass', str(s))
        if len(l) == 2:
            return l[0]
        elif len(l) > 2:
            return ''.join(l[:-1])
        else:
            if s == 'Alliance Quasar Fund, Inc.: Advisor Class Shares':
                return 'Alliance Quasar Fund, Inc'
            elif s == 'The Alliance Fund, Inc.: Advisor Class Shares':
                return 'The Alliance Fund, Inc'
            else:
                return s


def get_group_name(s):
    """
    It extracts the group name from the original fund name.
    :param s: original fund name
    :return: group name of fund
    """
    l = re.split(':', str(s))
    if len(l) == 1:
        l = re.split('--', str(s))
        if len(l) == 1:
            return np.nan
        else:
            return l[0]
    else:
        return l[0]


def update_short_fundname(s):
    """
    It extracts the pre-processed fund name from the original fund name, which means eliminating the group name
    :param s: original fund name.
    :return: pre-processed fund name
    """
    l = re.split(':', str(s))
    if len(l) == 1:
        return s
    elif len(l) == 2:
        return l[1]
    elif len(l) > 2:
        return ':'.join(l[1:])


def correct_fund_name(s):
    """
    It corrects the fund name when it includes both group name and fund name separated by '--'
    :param s: old fund name of fund
    :param exceptions: dictionary containing all the mistakes needed to be corrected
    :return: correct group name of fund
    """
    l = re.split('--', str(s))
    if len(l) == 1:
            return l[0]
    else:
        return l[1]


def correct_name(s, exceptions, retNaN=True):
    """
    It corrects mistakes in group names using exceptions dictionary.
    :param s: group name of fund
    :param exceptions: dictionary containing all the mistakes needed to be corrected
    :return: correct group or fund name or NaN
    """
    if s in list(exceptions.keys()):
        return exceptions[s]
    else:
        if retNaN:
            return np.nan
        else:
            return s


def my_lower(s):
    """
    It returns a copy of the string in which all case-based characters have been lower-cased.
    Usually used for groupby function.
    :param s: input string
    :return: lower-cased input string
    """
    try:
        return s.lower()
    except:
        return s


def my_strip(s):
    """
    It returns a copy of the string with the leading and trailing characters removed.
    Usually used for groupby function.
    :param s: input string
    :return: a copy of the input string with the leading and trailing characters removed
    """
    try:
        return s.strip()
    except:
        return s


def standardize_name(s):
    """
    It standardizes the input string by eliminating some special patterns.
    :param s: input string
    :return: standardized input string
    """
    try:
        # Transform string to lower case
        s = s.lower()
        # Eliminate symbols
        s = re.sub('&', 'and', s)
        s = re.sub('\+', '', s)
        s = re.sub('[—<^>*&$#@{|~}(")•“”®]', ' ', s)
        # Add a space after a dot or a comma which are followed by a non-whitespace
        s = re.sub(r'(?<=[.,;:])(?=[^\s])', r' ', s)
        # Deal with common apostrophe abbreviations
        s = re.sub(r'\bint\'l\b', 'international', s)
        s = re.sub(r'\bu.s.\b', 'us', s)
        # Eliminate punctuation
        s = re.sub('[\W]+', ' ', s)
        s = re.sub(',', '', s)
        s = s.translate(str.maketrans('', '', string.punctuation))
        # Standardize common words
        s = re.sub(r'\bthe\b *', '', s)
        s = re.sub(r'\bincorporated\b', 'inc', s)
        s = s.rsplit('inc', 1)[0]
        s = re.sub(r'\binc\b', '', s)
        s = re.sub(r'\bcompany\b', 'co', s)
        s = re.sub(r'\bcorporation\b', 'corp', s)
        s = re.sub(r'\bportfolio\b', 'pfolio', s)
        s = re.sub(r'\bport\b', 'pfolio', s)
        s = re.sub(r'\bpfolio\b', '', s)
        s = re.sub(r'\bportfolios\b', 'pfolios', s)
        s = re.sub(r'\bpfolios\b', '', s)
        s = re.sub(r'\binst\b', 'institutional', s)
        s = re.sub(r'\binstl\b', 'institutional', s)
        s = re.sub(r'\binvestment\b', 'invest', s)
        s = re.sub(r'\binvestments\b', 'invest', s)
        s = re.sub(r'\binv\b', 'invest', s)
        s = re.sub(r'\binvestors\b', 'invest', s)
        s = re.sub(r'\binvestor\b', 'invest', s)
        s = re.sub(r'\bsrs\b', 'series', s)
        s = re.sub(r'\balloc\b', 'allocation', s)
        s = re.sub(r'\ballocations\b', 'allocation', s)
        s = re.sub(r'\bgrp\b', 'group', s)
        s = re.sub(r'\bmgr\b', 'manager', s)
        s = re.sub('j p morgan', 'jpm', s)
        s = re.sub(r'jpmorgan', 'jpm', s)
        s = re.sub(r'j hancock', 'hancock', s)
        s = re.sub(r'john hancock', 'hancock', s)
        s = re.sub(r'\bacct\b', 'account', s)
        s = re.sub(r'\baccounts\b', 'account', s)
        s = re.sub(r'\bu s\b', 'us', s)
        s = re.sub(r'\bmgmt\b', 'management', s)
        s = re.sub(r'\bgs\b', 'goldman sachs', s)
        s = re.sub(r'\bgs\b', 'goldman sachs', s)
        s = re.sub(r'\bgoldman\b', 'goldman sachs', s)
        s = re.sub(r'\bg s\b', 'goldman sachs', s)
        s = re.sub(r'\btr\b', 'trust', s)
        s = re.sub(r'\bmutuai\b', 'mutual', s)
        s = re.sub(r'\btrst\b', 'trust', s)
        s = re.sub(r'\btrusts\b', 'trust', s)
        s = re.sub(r'\btrust\b', '', s)
        s = re.sub(r'\bfd\b', 'fund', s)
        s = re.sub(r'\bfund\b', '', s)
        s = re.sub(r'\bfds\b', 'funds', s)
        s = re.sub(r'\bfunds\b', '', s)
        s = re.sub(r'\beqty\b', 'equity', s)
        s = re.sub(r'\beq\b', 'equity', s)
        s = re.sub(r'\bequities\b', 'equity', s)
        # Remove leading and trailing characters
        s = s.strip()
        if len(s.split()) >= 2:
            first, *middle, last = s.split()
            if (last == 'a') or (last == 'b') or (last == 'c'):
                s = s.rsplit(' ', 1)[0]
        # Remove leading and trailing characters
        s = s.strip()
        # Remove extra white spaces inside string
        " ".join(s.split())
        return s
    except:
        return s


def strStandardizer(s, oldChar, newChar, max):
    """
    Standardize strings. Method used in the edgar project. Useful to merge databases processed there.
    :param s: string to be standardized
    :param oldChar: list of characters to be eliminated/substituted
    :param newChar: corresponding list of characters to replace oldChar elements with
    :param max: maximum number of instances to be replaced/eliminated
    :return:
    """
    # Eliminate special characters
    if oldChar == []:
        oldChar = [',', ';', '.', '-', '_', '/', '&#174;']
    if newChar == []:
        newChar = ['', '', '', '', '', '', '']
    for i, new in enumerate(newChar):
        if max > 0:
            s = s.replace(oldChar[i - 1], newChar[i - 1], max)
        else:
            s = s.replace(oldChar[i - 1], newChar[i - 1])
    # Put all characters to lower case
    s = s.lower()
    # Remove leading and trailing characters
    s = s.strip()
    # Remove extra white spaces inside string
    " ".join(s.split())
    return s


def my_fuzzy(df, choiceDf, keyCol, matchCol, firstDateCol="", lastDateCol=""):
    """
    Creates a key variable to facilitate fuzzy match of two dataframes based on a string variable
    :param df: dataframe to be fuzzy merged
    :param choiceDf: dataframe to be fuzzy merged from which to choose options for the key variable
    :param keyCol: (str) name of key column to be added
    :param matchCol: (str) name of column to be used in the fuzzy merge - same in both dataframes
    :param firstDateCol: (str) name of column that defines the lower-bound of the dates range to be considered in
            choosing the key value. Default = ""
    :param lastDateCol: (str) name of column that defines the upper-bound of the dates range to be considered in
            choosing the key value. Default = ""
    :return:
    """
    for index, row in df.iterrows():
        tomatch = df[matchCol][index]
        if firstDateCol == "":
            choices = choiceDf[matchCol]
        else:
            condition = (choiceDf.monthend >= df[firstDateCol][index]) & \
                        (choiceDf.monthend <= df[lastDateCol][index])
            choices = choiceDf[matchCol].loc[condition]
        if len(choices) > 0:
            match = [process.extract(tomatch, choices, processor=lambda y: y)]
        else:
            match = np.nan
        df.loc[index, keyCol] = match
    df.loc[df[keyCol].notnull(), keyCol] = df.loc[df[keyCol].notnull(), keyCol].apply(lambda x: x[0])


def exceptions_tofile(inputdf, feature, folder):
    """
    It checks the non-uniqueness case for a specific feature in input fund_summary data frame.
    It saves the output to a csv file.
    :param inputdf: data frame for which uniqueness needs to be checked
    :param feature: one feature for which uniqueness needs to be checked
    :param folder: output folder where to save the output csv file
    :return: a dictionary recording all the non-unique instances,
             where the keys are crsp_cl_grp/caldt and value is the values of the specific non-unique feature
    """
    d = dict()
    df = inputdf.groupby(['crsp_cl_grp', 'caldt'])[feature].nunique()
    index = list(df[df > 1].index)
    for i in index:
        a = list(inputdf.loc[(inputdf['crsp_cl_grp'] == i[0]) & (inputdf['caldt'] == i[1]), feature])
        a = list(set(a))
        d[i] = a
    output = pd.DataFrame.from_dict(d, orient='index').reset_index()
    output['crsp_cl_grp'] = output['index'].apply(lambda x: x[0])
    output['caldt'] = output['index'].apply(lambda x: x[1])
    cols = output.columns.tolist()
    output = output[cols[-2:] + cols[1:-2]]
    output.to_csv(folder + feature + '_records.csv', index=False)
    return d


def wficn2crsp_cl_grp(wficn, d):
    """
    It maps wficn to crsp_cl_grp using dictionary d which has wficn as key and crsp_cl_grp as value
    :param wficn: Input wficn
    :param d: The dictionary mapping wficn to crsp_cl_grp
    :return: The corresponding crsp_cl_grp
    """
    try:
        return d[wficn]
    except:
        return np.nan


def crsp_cl_grp2wficn(crsp_cl_grp, d):
    """
    It maps wficn to crsp_cl_grp using dictionary d which has wficn as key and crsp_cl_grp as value
    :param crsp_cl_grp: Input crsp_cl_grp
    :param d: The dictionary mapping wficn to crsp_cl_grp
    :return: The corresponding wficn
    """
    try:
        return [key for (key, value) in d.items() if value == crsp_cl_grp][0]
    except:
        return np.nan


def mode_with_NA(x):
    """
    It returns the mode of the input array excluding NAs.
    :param x: input array
    :return: mode of the input array excluding NAs
    """
    try:
        return x.value_counts().index[0]
    except:
        return np.nan


def unique_non_NA(s):
    """
    It returns the sorted unique elements of an array excluding NAs.
    :param s: input list
    :return: sorted unique elements of the input array excluding NAs
    """
    return list(s.dropna().unique())


def weighted_mean(x, cols, w="tna_latest"):
    """
    It computes the weighted mean of the columns "cols" for the rows of "x" by using weights "w"
    N.B. the function uses a weighted average when "w" is available for ALL rows of interest, otherwise it uses equal
    weights. When weights are lagged variables, in a panel setting this means that the firs observation for an ID-time
    group is always equal weighted.
    The function appends all variables created to a dataframe
    :param x: Subset of a dataframe, generally obtained with a groupby statement
    :param cols: Columns to be averaged
    :param w: Dataframe column to be used as weights
    :return: Dataframe containing aggregated variable by groupby IDs & an index indicating when equal weights were used
    """
    if x[w].isnull().values.any() or sum(x[w]) == 0:
        result = pd.Series(np.average(x[cols], axis=0), [i for i in cols])
        result = result.append(pd.Series(True, index=['equal_weight']))
    else:
        result = pd.Series(np.average(x[cols], weights=x[w], axis=0), [i for i in cols])
        result = result.append(pd.Series(False, index=['equal_weight']))
    return result


def get_non_consecutive(df, month_column):
    """
    It checks continuousness of the month_column in the input data frame.
    If there is any gap longer than one year in the month_column, it will consider the observations as non-continuous.
    The function return two arrays recording the start and end of every continuous period.
    :param df: input data frame
    :param month_column: column name of time variable
    :return: Two arrays containing the start and end dates of every continuous period
    """
    date = list(set(list(df[month_column])))
    date.sort()
    date_diff = [x - date[i - 1] for i, x in enumerate(date)][1:]
    where = np.where([pd.Timedelta(x) < pd.Timedelta(days=365) for x in date_diff])[0]
    return np.array(date)[where], np.array(date)[where + 1]


def forward_fill(df, month_column):
    """
    It fills information forward to the monthly frequency if the time gap between continuous observations is less than
    one year (12 months).
    :param df: input data frame which needs to be filled
    :param month_column: column name of the time variable
    :return: filled data frame
    """
    df = df.copy()
    start_indices, end_indices = get_non_consecutive(df, month_column)
    for start_date, end_date in zip(start_indices, end_indices):
        add = df.loc[df[month_column] == start_date]
        cur_date = start_date
        add_date = []
        while cur_date < end_date:
            cur_date += relativedelta(months=1)
            # cur_date += relativedelta(day=31)
            cur_date += pd.tseries.offsets.DateOffset(day=31)
            add_date.append(cur_date)
        add_date = add_date[:-1]
        for cur_date in add_date:
            add[month_column] = cur_date
            df = pd.concat([df, add], axis=0)
    return df


def rolling_apply(df, period, func, min_periods=None):
    """
    Rolling apply the function to the data frame
    :param df: Input data frame
    :param period: Size of the moving window.
    :param func: Function to be use
    :param min_periods: Minimum number of observations in window required to have a value
    :return: An result array with the same index as input data frame
    """
    if min_periods is None:
        min_periods = abs(period)
    result = pd.Series(np.nan, index=df.index)
    for i in range(1, len(df) + 1):
        if period > 0:
            sub_df = df.iloc[max(i - period, 0):i, :]
            idx = sub_df.index[-1]
        else:
            sub_df = df.iloc[i:min(i + period, df.shape[0]), :]
            idx = sub_df.index[0]
        if len(sub_df) >= min_periods:
            result[idx] = func(sub_df)
    return result


def string2date(s, format):
    """
    convert string/int/float to datetime.datetime
    :param s: Input value
    :param format: Format of the input string.
    """
    try:
        if type(s) != str:
            s = str(int(s))
        s = dt.datetime.strptime(s, format)
        s += relativedelta(day=31)
        return s
    except:
        return np.nan


def s2datetime(s, format):
    """
    convert string/int/float to datetime.datetime
    :param s: Input value
    :param format: Format of the input string.
    """
    try:
        if type(s) != str:
            s = str(int(s))
        s = dt.datetime.strptime(s, format)
        return s
    except:
        return np.nan


def isnull_chk(df, col):
    """
    This function checks the number and percentage of missing values in a dataframe column
    :param df: dataframe
    :param col: str column name
    :return: prints output
    """
    print('The number of missing values in the column "', col, '" is: ', df[df[col].isnull()].shape[0])
    if df[df[col].isnull()].shape[0] > 0:
        print('Which in percentage of the total number of observations is: ',
              (df[df[col].isnull()].shape[0]/df.shape[0])*100, '%')


def date_range(df, group, datecol, newnames, how='first'):
    """
    Returns the start date, end date of both by groups in a dataframe
    :param df: input dataframe
    :param group: group name as string or group names as list
    :param datecol: date column of interest
    :param newnames: name of the new column as string or names of new columns as list
    :param how: 'first for first date only, 'last' for last date only, 'first&last' for both. Default: how='first
    :return: dataframe with new columns merged by group
    """
    if type(group) == str:
        group = [group]
    if how == 'first':
        agg = df.groupby(group)[datecol].min().reset_index()
        agg.rename(columns={datecol: newnames}, inplace=True)
        df = pd.merge(df, agg, how='left', on=group)
    elif how == 'last':
        agg = df.groupby(group)[datecol].max().reset_index()
        agg.rename(columns={datecol: newnames}, inplace=True)
        df = pd.merge(df, agg, how='left', on=group)
    elif how == 'first&last':
        agg = df.groupby(group)[datecol].min().reset_index()
        agg.rename(columns={datecol: newnames[0]}, inplace=True)
        df = pd.merge(df, agg, how='left', on=group)
        agg1 = df.groupby(group)[datecol].max().reset_index()
        agg1.rename(columns={datecol: newnames[1]}, inplace=True)
        df = pd.merge(df, agg1, how='left', on=group)
    else:
        raise ValueError('The only supported value for how are "first", "last" and "first&last"')
    return df


def last_day_of_month(any_day):
    """
    Take in any day of a month
    Return the end day of that month
    :param any_day: date of the month
    """
    next_month = any_day.replace(day=28) + dt.timedelta(days=4)
    return next_month - dt.timedelta(days=next_month.day)


def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print('Directory created:', directory)
        else:
            print("Existing directory:", directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)
