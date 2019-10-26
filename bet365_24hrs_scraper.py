import csv

import re

import pdb

import requests

from lxml import etree

import json

import time

from selenium import webdriver

from selenium.webdriver.chrome.options import Options

from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.support.ui import WebDriverWait

from selenium.webdriver.common.by import By

import MySQLdb

import schedule

import datetime

from pyvirtualdisplay import Display

display = Display(visible=0, size=(800, 600))

display.start()

# load chrome driver and initialze webdriver object.
options = Options() 

# PROXY = "163.172.36.181:15002" # IP:PORT or HOST:PORT

# options.add_argument('--proxy-server=%s' % PROXY)

# driver = webdriver.Chrome('./BetScraper/chromedriver.exe', options=options)

PROXY = "103.9.188.236:30274"

options.add_argument('--proxy-server=%s' % PROXY)

options.add_argument('--no-sandbox')

driver = webdriver.Chrome('./chromedriver', options=options)

# init url of bet website.
base_url = 'https://www.bet365.com'

db = MySQLdb.connect(host="localhost", user="ubuntu", passwd="root", db="bet365_db")       

cur = db.cursor()

table_name = 'odd_table'

_SQL = """SHOW TABLES"""

cur.execute(_SQL)

results = cur.fetchall()

results_list = [item[0] for item in results] 

if table_name in results_list:

    print(table_name, 'was found!')

else:

    print(table_name, 'was NOT found!')
    _SQL = """CREATE TABLE %s (
            id int auto_increment not null primary key, 
            market_id varchar(20),
            sport_id varchar(10),
            start_time varchar(50),
            time_status varchar(10),
            league varchar(50), 
            teams varchar(50), 
            home varchar(50), 
            away varchar(50),
            ss varchar(10),
            market varchar(30),
            bet_slip_id varchar(30),
            updated_at varchar(50));
        """ %table_name

    cur.execute(_SQL)

watch_market_list = [
    'Asian Handicap',
    'Goal Line',    
    'Goals Over/Under',
    'First Team to Score',
    'Corners Race',
    'Alternative Corners'
]

local_today = datetime.datetime.now()

# validate item. elimiate spaces and ascill code. 
def validate(item):    

    if item == None:

        item = ''

    if type(item) == int or type(item) == float:

        item = str(item)

    if type(item) == list:

        item = ' '.join(item)

    return item.encode('ascii', 'ignore').encode("utf8").strip()


def parse_market_odd(league_title, start_time, market_list):

    for market in market_list:

        market_header = validate(market.find_element_by_xpath('.//span').text)        

        if market_header in watch_market_list:

            collapse = market.find_element_by_xpath('.//div[contains(@class, "gll-MarketGroupButton")]')

            if 'open' not in collapse.get_attribute('class').lower():

                collapse.click()

                time.sleep(1)

            odd_list = market.find_elements_by_xpath('.//div[contains(@class, "gll-Participant_General")]')

            for odd in odd_list:         

                try:

                    odd.click()

                    time.sleep(3)

                    # switch driver into iframe object
                    driver.switch_to.frame(driver.find_element_by_tag_name("iframe"))

                    output = []

                    odd_element = driver.find_elements_by_xpath('//li[contains(@class, "bs-Item bs-SingleItem")]')[-1]

                    # BetSlipID
                    bet_slip_id = odd_element.get_attribute('data-item-fpid')
                    
                    # MarketID
                    market_id = odd_element.get_attribute('data-fixtureid')

                    details = (driver.find_elements_by_xpath('//div[@class="bs-SelectionRow"]')[-1].text.split('\n'))

                    # details[1] : market

                    # details[2] : teams

                    updated_at = validate(int(time.mktime(datetime.datetime.utcnow().timetuple())))

                    team_list = details[2].split(' v ')

                    home = team_list[0]

                    away = team_list[1]

                    check_query = "select * from %s where bet_slip_id='%s'" %(table_name, bet_slip_id)

                    count = cur.execute(check_query)

                    db.cursor()

                    if count == 0:

                        sql = "INSERT INTO " + table_name

                        sql += "(market_id, sport_id, start_time, time_status, league, teams, home, away, ss, market, bet_slip_id, updated_at) "

                        sql += "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'); " %(market_id, "1", start_time, "0", league_title, details[2], home, away, 'null', details[1], bet_slip_id, updated_at)

                    else:

                        sql = "UPDATE %s SET market_id='%s', sport_id='%s', start_time='%s', time_status='%s', teams='%s', league='%s', home='%s', away='%s', ss='%s', market='%s', bet_slip_id='%s', updated_at='%s'  WHERE bet_slip_id='%s'"

                        sql = sql %(table_name, market_id, "1", start_time, "0", details[2], league_title, home, away, 'null', details[1], bet_slip_id, updated_at, bet_slip_id)

                    print(sql)

                    cur.execute(sql)

                    db.commit()

                    # switch driver back to root object.
                    driver.switch_to.default_content()

                    odd.click()

                    time.sleep(3)

                except Exception as e:

                    driver.switch_to.default_content()

                    break


def fetch_data():

    try:

        delay_time = 15

        # start with 
        driver.get(base_url)

        # store cookies from the base website and use it for next loading since bet365 keeps cookies everytime page id loaded
        cookies = driver.get_cookies()

        for cookie in cookies:

            if 'expiry' in cookie:

                del cookie['expiry']

            driver.add_cookie(cookie)

        url = 'https://www.bet365.com/#/AC/B1/C1/D13/E0/F2/J0/Q1/F^24'

        driver.get(url)

        section_length = 0

        try:

            section_list = WebDriverWait(driver, delay_time).until(EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "gll-MarketGroup ufm-MarketGroupUpcomingCompetition")]')))

            section_length = len(driver.find_elements_by_xpath('//div[contains(@class, "gll-MarketGroup ufm-MarketGroupUpcomingCompetition")]'))

        except:

            pass

        time.sleep(2)

        section_list = driver.find_elements_by_xpath('//div[contains(@class, "gll-MarketGroup ufm-MarketGroupUpcomingCompetition")]')
        
        for idx in range(0, len(section_list)):

            try:

                section_list_container = WebDriverWait(driver, delay_time).until(EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "gll-MarketGroup ufm-MarketGroupUpcomingCompetition")]')))
                
            except:

                print('section list has been not loaded yet')

                pass

            time.sleep(2)

            section_list = driver.find_elements_by_xpath('//div[contains(@class, "gll-MarketGroup ufm-MarketGroupUpcomingCompetition")]')

            league_collapse = section_list[idx].get_attribute('class')

            if 'collapsed' in league_collapse:

                section_list[idx].click()

                time.sleep(1)

            league_list_count = len(section_list[idx].find_elements_by_xpath('.//div[contains(@class, "sl-CouponParticipantWithBookCloses_NameContainer")]/div[contains(@class, "sl-CouponParticipantWithBookCloses_Name")]'))

            for l_idx in range(0, league_list_count):

                print('-------- pointer -------- ', idx, l_idx)

                try:

                    section_list_container = WebDriverWait(driver, delay_time).until(EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "gll-MarketGroup ufm-MarketGroupUpcomingCompetition")]')))
                    
                except:

                    print('section list has been not loaded yet')

                    pass

                time.sleep(3)

                section_list = driver.find_elements_by_xpath('//div[contains(@class, "gll-MarketGroup ufm-MarketGroupUpcomingCompetition")]')

                league_collapse = section_list[idx].get_attribute('class')

                if 'collapsed' in league_collapse:

                    section_list[idx].click()

                    time.sleep(1)

                league_list = section_list[idx].find_elements_by_xpath('.//div[contains(@class, "sl-CouponParticipantWithBookCloses_NameContainer")]/div[contains(@class, "sl-CouponParticipantWithBookCloses_Name")]')

                league_list[l_idx].click()

                try:

                    WebDriverWait(driver, delay_time).until(EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "gl-MarketGrid")]/div[contains(@class, "gll-MarketGroup")]')))

                    WebDriverWait(driver, delay_time).until(EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "BreadcrumbTrail_BreadcrumbTruncate")]')))                    
                    
                except:

                    print('market list has been not loaded yet')

                    pass

                time.sleep(3)

                league_title = validate(driver.find_element_by_xpath('//div[contains(@class, "cl-BreadcrumbTrail_BreadcrumbTruncate")]').text)

                temp_datetime = validate(local_today.year) + ' ' +  validate(driver.find_element_by_xpath('//div[contains(@class, "cm-MarketGroupExtraData_TimeStamp")]').text)                

                local_datetime = datetime.datetime.strptime(temp_datetime, '%Y %d %b %H:%M')

                utc_datetime = local_datetime - datetime.timedelta(hours=6)

                start_time = validate(int(time.mktime(utc_datetime.timetuple())))

                market_list = driver.find_elements_by_xpath('//div[contains(@class, "gl-MarketGrid")]/div[contains(@class, "gll-MarketGroup")]')

                parse_market_odd(league_title, start_time, market_list)

                market_tab_list = driver.find_elements_by_xpath('//div[contains(@class, "cl-MarketGroupNavBarButton")]')

                corners_idx = 0

                for idx, market_tab in enumerate(market_tab_list):

                    market_text = validate(market_tab.text)

                    if market_text == 'Corners':

                        corners_idx = idx

                        break                

                if corners_idx != 0:

                    market_tab_list[corners_idx].click()

                    try:

                        sub_market_list_container = WebDriverWait(driver, delay_time).until(EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "gl-MarketGrid")]/div[contains(@class, "gll-MarketGroup")]')))
                        
                    except:

                        print('market list has been not loaded yet')

                        pass

                    time.sleep(3)

                    sub_market_list = driver.find_elements_by_xpath('//div[contains(@class, "gl-MarketGrid")]/div[contains(@class, "gll-MarketGroup")]')

                    parse_market_odd(league_title, start_time, sub_market_list)

                    driver.execute_script("window.history.go(-2)")

                else:

                    driver.execute_script("window.history.go(-1)")
        
        db.close()
        
        driver.close()

    except Exception as e:

        print(e)

        pass

# start the process
fetch_data()

#schedule for running 
schedule.every(1).hour.do(fetch_data)

while True:

    schedule.run_pending()