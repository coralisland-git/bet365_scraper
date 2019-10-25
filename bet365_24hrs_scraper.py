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

from pyvirtualdisplay import Display

display = Display(visible=0, size=(800, 600))

display.start()

# load chrome driver and initialze webdriver object.
options = Options() 

# PROXY = "163.172.36.181:15002" # IP:PORT or HOST:PORT

# options.add_argument('--proxy-server=%s' % PROXY)

# driver = webdriver.Chrome('./BetScraper/chromedriver.exe', options=options)

driver = webdriver.Chrome('./chromedriver', options=options)

# init url of bet website.
base_url = 'https://www.bet365.com'

db = MySQLdb.connect(host="localhost", user="root", passwd="root", db="bet365_db")       

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

    _SQL = """CREATE TABLE %s (id int auto_increment not null primary key, team_match varchar(50), team_name varchar(50), league varchar(50), market varchar(30), odd varchar(20), bet_slip_id varchar(20), market_id varchar(20));""" %table_name

    cur.execute(_SQL)

watch_market_list = [
    'Asian Handicap',
    'Goal Line',
    'Goals Over/Under',
    'First Team to Score',
    'Corners Race',
    'Alternative Corners'
]

# validate item. elimiate spaces and ascill code. 
def validate(item):    

    if item == None:

        item = ''

    if type(item) == int or type(item) == float:

        item = str(item)

    if type(item) == list:

        item = ' '.join(item)

    return item.encode('ascii', 'ignore').encode("utf8").strip()


def parse_market_odd(league_title, market_list):

    for market in market_list:

        market_header = validate(market.find_element_by_xpath('.//span').text)        

        if market_header in watch_market_list:

            collapse = market.find_element_by_xpath('.//div[contains(@class, "gll-MarketGroupButton")]')

            if 'open' not in collapse.get_attribute('class').lower():

                collapse.click()

                time.sleep(1)

            odd_list = market.find_elements_by_xpath('.//div[contains(@class, "gll-Participant_General")]')

            for odd in odd_list:         

                # try:

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

                output.append(details[2])

                output.append(details[0])

                output.append(league_title)

                output.append(details[1])

                output.append(details[3])

                output.append(bet_slip_id)

                output.append(market_id)

                print(output)

                check_query = "select * from %s where bet_slip_id='%s'" %(table_name, bet_slip_id)

                count = cur.execute(check_query)

                db.cursor()

                if count == 0:

                    sql = "INSERT INTO " + table_name

                    sql += "(team_match, team_name, league, market, odd, bet_slip_id, market_id) "

                    sql += "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s'); " %(details[2], details[0], league_title, details[1], details[3], bet_slip_id, market_id)

                else:

                    sql = "UPDATE %s SET team_match='%s', team_name='%s', league='%s', market='%s', odd='%s', bet_slip_id='%s', market_id='%s' WHERE bet_slip_id='%s'"

                    sql = sql %(table_name, details[2], details[0], league_title, details[1], details[3], bet_slip_id, market_id, bet_slip_id)

                cur.execute(sql)

                db.commit()

                # switch driver back to root object.
                driver.switch_to.default_content()

                odd.click()

                time.sleep(2)

                # except Exception as e:

                #     pdb.set_trace()


def fetch_data():

    try:

        delay_time = 15

        # start with 
        driver.get(base_url)

        # store cookies from the base website and use it for next loading since bet365 keeps cookies everytime page id loaded
        cookies = driver.get_cookies()

        for cookie in cookies:

            driver.add_cookie(cookie)

        url = 'https://www.bet365.com/#/AC/B1/C1/D13/E0/F2/J0/Q1/F^24'

        driver.get(url)

        section_length = 0

        try:

            section_list = WebDriverWait(driver, delay_time).until(EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "gll-MarketGroup ufm-MarketGroupUpcomingCompetition")]')))

            section_length = len(driver.find_elements_by_xpath('//div[contains(@class, "gll-MarketGroup ufm-MarketGroupUpcomingCompetition")]'))

        except:

            pass


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

            for l_idx in range(0, league_list_count)[3:]:

                print('-------- pointer -------- ', idx, l_idx)

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

                league_list = section_list[idx].find_elements_by_xpath('.//div[contains(@class, "sl-CouponParticipantWithBookCloses_NameContainer")]/div[contains(@class, "sl-CouponParticipantWithBookCloses_Name")]')

                league_list[l_idx].click()

                try:

                    market_list_container = WebDriverWait(driver, delay_time).until(EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "gl-MarketGrid")]/div[contains(@class, "gll-MarketGroup")]')))
                    
                except:

                    print('market list has been not loaded yet')

                    pass

                time.sleep(2)

                league_title = validate(driver.find_element_by_xpath('//div[contains(@class, "cl-BreadcrumbTrail_BreadcrumbTruncate")]').text)

                market_list = driver.find_elements_by_xpath('//div[contains(@class, "gl-MarketGrid")]/div[contains(@class, "gll-MarketGroup")]')

                parse_market_odd(league_title, market_list)

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

                    time.sleep(2)

                    sub_market_list = driver.find_elements_by_xpath('//div[contains(@class, "gl-MarketGrid")]/div[contains(@class, "gll-MarketGroup")]')

                    parse_market_odd(league_title, sub_market_list)

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