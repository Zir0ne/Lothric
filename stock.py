#!/usr/bin/env python3

"""
    purchase stock's data. First we need prepare mysql database with following statements.
    create database stock;
    create table a(
        company_code char(6)       UNIQUE NOT NULL,
        company_abbv varchar(50)   NOT NULL,
        company_name varchar(300),
        eng_name     varchar(100),
        address      varchar(1000),
        code         char(6)       UNIQUE NOT NULL,
        abbv         varchar(50)   NOT NULL,
        date         date          NOT NULL,
        capital      bigint        NOT NULL,
        flow         bigint        NOT NULL,
        area         char(30),
        province     char(30),
        city         char(30),
        industry     char(50),
        net          varchar(100)
    );
    create index code_index on a(code);
    create index company_code_index on a(company_code);

    create table history(
        code      char(6) NOT NULL,
        date      date    NOT NULL,
        close     float,
        high      float,
        low       float,
        open      float,
        yesterday float,
        amount    float,
        range_    float,
        rate      float,
        volume    float,
        deal      float,
        capital   float,
        flow      float
    );
    create index code_date on history(code, date);
    alter table history add constraint fk_code foreign key(code) references a(code);

    create table authority(
        code      char(6) NOT NULL,
        date      date    NOT NULL,
        open      float,
        high      float,
        close     float,
        low       float,
        volume    float,
        amount    float,
        authority float
    );
    create index code_date on authority(code, date);
    alter table authority add constraint fk_code foreign key(code) references a(code);
"""

import excel
import mysql.connector
import requests
import os
import time
import datetime

from urllib import parse
from scrapy.selector import Selector


database_config = {
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'stock',
    'user': 'root',
    'password': 'With-Never',
    'charset': 'utf8',
    'use_unicode': True,
    'get_warnings': True,
}
root_dir = "/Users/cuiwei/Documents/Lothric/data/"
stmt_a_insert = (
    "INSERT INTO a (company_code, company_abbv, company_name, eng_name, address, code, abbv, date, capital, flow, area, province, city, industry, net) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
)
stmt_history_trading_update = (
    "INSERT INTO history (code, date, close, high, low, open, yesterday, amount, range_, rate, volume, deal, capital, flow) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
)
stmt_authority_update = (
    "INSERT INTO authority (code, date, open, high, close, low, volume, amount, authority) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
)


def create_a_stock_table():
    """
        import all A stock into mysql database.
        this function only need to run once to create base a stock table in mysql database when in new environment
    """
    # 深圳交易所的A股
    fin = excel.OpenExcel(os.path.join(root_dir, 'shen_a.xlsx'))
    for i in range(2, 1789):
        row = fin.read(i)
        company_code = row[0]
        company_abbv = row[1]
        company_name = row[2]
        eng_name     = row[3]
        address      = row[4]
        code         = row[5]
        stock_abbv   = row[6]
        date         = row[7]
        capital      = row[8].replace(',', '')
        flow         = row[9].replace(',', '')
        area         = row[15]
        province     = row[16]
        city         = row[17]
        industry     = row[18]
        net          = row[19]

        try:
            cur.execute(stmt_a_insert, (company_code, company_abbv, company_name, eng_name, address, code, stock_abbv,
                                        date, capital, flow, area, province, city, industry, net))
        except mysql.connector.errors.DataError as e:
            print("  DataError - %s: %s" % (company_code, str(e.args)))
        except mysql.connector.errors.DatabaseError as e:
            print("  DatabaseError - %s: %s" % (company_code, str(e.args)))
    db.commit()

    # 上海交易所的A股
    fin = open(os.path.join(root_dir, 'shang_a.xls'), 'r', encoding='gb2312')
    fin.readline()
    for line in fin:
        row = line.split('\t')
        company_code = row[0].strip()
        company_abbv = row[1].strip()
        code         = row[2].strip()
        stock_abbv   = row[3].strip()
        date         = row[4].strip()
        capital      = row[5].replace(',', '').strip()
        flow         = row[6].replace(',', '').strip()

        try:
            cur.execute(stmt_a_insert, (company_code, company_abbv, None, None, None, code, stock_abbv, date, capital,
                                        flow, None, None, None, None, None))
        except mysql.connector.errors.DataError as e:
            print("  DataError - %s: %s" % (company_code, str(e.args)))
        except mysql.connector.errors.DatabaseError as e:
            print("  DatabaseError - %s: %s" % (company_code, str(e.args)))
    db.commit()


def update_stock_history_trading_data(start_date, end_date):
    """
        download history trading data from 163.com then update mysql database
        002752, 300208 has encountered error. I think the reason is download error
    """
    download_fields = 'TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
    download_url = 'http://quotes.money.163.com/service/chddata.html?code={0}{1}&start={2}&end={3}&fields={4}'

    try:
        cur.execute('select code, date from a')
        results = cur.fetchall()
        for result in results:
            # continue with 002752
            if result[0] < '002752':
                continue
            print('\rProcessing %s ->                                  ' % result[0], end='')

            # download latest data
            start_date = start_date.replace('-', '') if start_date else result[1].strftime('%Y%m%d')
            end_date = end_date.replace('-', '') if end_date else datetime.date.today().strftime('%Y%m%d')
            r = requests.get(download_url.format('0' if result[0] >= '600000' else '1',
                                                 result[0], start_date, end_date, download_fields))
            if r.status_code != 200:
                print('Error: Cannot download history trading data for {0} between {1} and {2}.'.format(
                    result[0], start_date, end_date))
                continue

            # save data
            try:
                fou = open(os.path.join(root_dir, '{0}.csv'.format(result[0])), 'w')
                fou.write(r.content.decode('GB2312'))
                fou.close()
                print('\rProcessing %s -> downloaded -> ' % result[0], end='')
            except UnicodeDecodeError:
                print('\rProcessing %s -> cannot decode using gb2312 ' % result[0])
                continue

            # update mysql database
            fin = open(os.path.join(root_dir, '{0}.csv'.format(result[0])), 'r')
            fin.readline()
            for line in fin:
                column  = line.split(',')
                date    = column[ 0]
                close   = column[ 3] if column[ 3] != 'None' and float(column[ 3]) != 0 else None
                high    = column[ 4] if column[ 4] != 'None' and float(column[ 4]) != 0 else None
                low     = column[ 5] if column[ 5] != 'None' and float(column[ 5]) != 0 else None
                open_   = column[ 6] if column[ 6] != 'None' and float(column[ 6]) != 0 else None
                before  = column[ 7] if column[ 7] != 'None' and float(column[ 7]) != 0 else None
                amount  = column[ 8] if column[ 8] != 'None' and float(column[ 8]) != 0 else None
                range_  = column[ 9] if column[ 9] != 'None' and float(column[ 9]) != 0 else None
                rate    = column[10] if column[10] != 'None' and float(column[10]) != 0 else None
                volume  = column[11] if column[11] != 'None' and float(column[11]) != 0 else None
                deal    = column[12] if column[12] != 'None' and float(column[12]) != 0 else None
                capital = column[13] if column[13] != 'None' and float(column[13]) != 0 else None
                flow    = column[14].replace('\n', '') if column[14] != '0\n' and column[14] != 'None\n' else None

                try:
                    cur.execute(stmt_history_trading_update, (result[0], date, close, high, low, open_, before, amount,
                                                              range_, rate, volume, deal, capital, flow))
                except mysql.connector.errors.DataError as e:
                    print("\rProcessing %s -> downloaded -> DataError - %s: %s" % (result[0], date, str(e.args)))
                except mysql.connector.errors.DatabaseError as e:
                    print("\rProcessing %s -> downloaded -> DatabaseError - %s: %s" % (result[0], date, str(e.args)))
            fin.close()
            os.remove(os.path.join(root_dir, '{0}.csv'.format(result[0])))
            db.commit()
            print('\rProcessing %s -> downloaded -> done ' % result[0], end='')
            time.sleep(1)

    except mysql.connector.errors.DataError:
        print("DataError - cannot retrieve stock code list")
    except mysql.connector.errors.DatabaseError:
        print("DatabaseError - cannot retrieve stock code list")


def update_answer_authority_data(years, quarters):
    """
        Download answer authority data from sina's financial page
    """
    url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vMS_FuQuanMarketHistory/stockid/{0}.phtml?year={1:d}&jidu={2:d}"

    try:
        cur.execute('select code, from a')
        results = cur.fetchall()
        for result in results:
            for year in years:
                for quarter in quarters:
                    print('\rProcessing %s year=%d quarter=%d ->                   ' % (result[0], year, quarter), end='')
                    r = requests.get(url.format(result[0], year, quarter))
                    if r.status_code != 200:
                        print('\rError: Cannot retrieve answer authority data for {0} in {1}:{2}.'.format(
                            result[0], year, quarter))
                        continue
                    print('\rProcessing %s year=%d quarter=%d -> retrieved ->      ' % (result[0], year, quarter), end='')

                    response = Selector(text=r.content.decode('gb2312'))
                    for sel in response.xpath('//table[@id="FundHoldSharesTable"]/tr'):
                        date = sel.xpath('.//td/div/a/text()').extract_first()
                        if date:
                            date = date.replace('\t', '').replace('\r', '').replace('\n', '')
                            data_list = list()
                            for data in sel.xpath('.//td/div/text()').extract():
                                try:
                                    test = float(data)
                                    data_list.append(data)
                                except ValueError:
                                    pass
                            if len(data_list) != 7:
                                print('Waring: in-complete answer authority value')

                            try:
                                cur.execute(stmt_history_trading_update, (result[0], date, data_list[0], data_list[1],
                                                                          data_list[2], data_list[3], data_list[4],
                                                                          data_list[5], data_list[6]))
                            except mysql.connector.errors.DataError as e:
                                print("  DataError - %s: %s" % (date, str(e.args)))
                            except mysql.connector.errors.DatabaseError as e:
                                print("  DatabaseError - %s: %s" % (date, str(e.args)))
                    db.commit()
                    print('\rProcessing %s year=%d quarter=%d -> retrieved -> done ' % (result[0], year, quarter), end='')
                    time.sleep(1)

    except mysql.connector.errors.DataError:
        print("  DataError - cannot retrieve stock code list")
    except mysql.connector.errors.DatabaseError:
        print("  DatabaseError - cannot retrieve stock code list")


if __name__ == '__main__':
    # connect to remote mysql server
    db = mysql.connector.Connect(**database_config)
    cur = db.cursor()
    print("Connected to MySQL server at %s:%d" % (database_config['host'], database_config['port']))
    update_stock_history_trading_data(None, None)
