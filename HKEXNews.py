#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@description:
1. class to scrap daily news from HKEX website: HKEXNewsManager
"""

import time
from bs4 import BeautifulSoup
from databaseManager import DatabaseManager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


class HKEXNewsManager:
    def __init__(self, host, user, passwd, name):
        self.base_url = 'https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en'
        self.table_name = 'HKEXNews'
        self.dbm = DatabaseManager()
        self.dbm.connect(host=host, user=user, passwd=passwd, name=name)
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

        # create table HKEXNews if not exits
        if not self.dbm.table_exists(self.table_name):
            self.dbm.cur.execute('DROP TABLE IF EXISTS HKEXNews')
            sql = """CREATE TABLE HKEXNews (
                     RELEASE_TIME VARCHAR(255),
                     STOCK_CODE VARCHAR(255),
                     STOCK_SHORT_NAME VARCHAR(255),  
                     HEADLINE_CATEGORY VARCHAR(255),
                     HEADLINE_CATEGORY_SECONDARY VARCHAR(255),
                     TITLE VARCHAR(255),
                     PDF_URL VARCHAR(255))"""
            self.dbm.cur.execute(sql)
            self.dbm.conn.commit()
            print('Create table HKEXNews')

    def scrollDown(self, distance):
        self.driver.execute_script("window.scrollBy(0, "+str(distance)+");")  # roll down by [distance] px

    def scrollDownToBottom(self):
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight*10);")  # roll down to bottom

    def getHKEXNews(self, checkDuplicate=False):
        self.driver.maximize_window()
        url = self.base_url
        self.driver.get(url)
        total = 0
        time.sleep(2)  # wait until the page is fully loaded
        searchbutton = self.driver.find_element(By.CLASS_NAME, "filter__btn-applyFilters-js.btn-blue")
        searchbutton.click()
        time.sleep(2)  # wait until the page is fully loaded
        try:
            if self.driver.title == 'Page Not Found':
                print('Page does not exists')
                return 0
            else:
                print('Page exists')

            num = self.storeNews(total, checkDuplicate)
            total = total + num
            while num > 0:
                print('{} news saved; {} news in total'.format(num, total))
                time.sleep(5)
                self.scrollDownToBottom()
                time.sleep(5)
                load_more_button = self.driver.find_element(By.CLASS_NAME, 'component-loadmore__link.component-loadmore__icon')
                WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, 'component-loadmore__link.component-loadmore__icon')))
                load_more_button.click()
                num = self.storeNews(total, checkDuplicate)
                total = total + num
                time.sleep(2)

            self.dbm.conn.commit()
            return total
        except Exception as ex:
            print(str(ex))
            return total

    def storeNews(self, start, checkDuplicate=False):
        updatenews_cnt = 0

        soup = BeautifulSoup(self.driver.page_source, "lxml")
        items = soup.select('tbody>tr')
        for item in items[start:]:  # skip the top [start] items; # start is the number of articles already collected
            # get url
            try:
                url = 'https://www1.hkexnews.hk' + item.a['href']
            except Exception as e:
                url = ""

            # get title
            try:
                title = item.a.get_text().strip()
            except Exception as e:
                print(str(e))
                title = ""

            # get time
            try:
                time = item.select('td.text-right.release-time')[0].get_text()[15:]
            except Exception as e:
                print(str(e))
                time = ""

            # get stock code
            try:
                code = str(item.select("td.text-right.stock-short-code")[0])
                extra_n = len('<td class="text-right stock-short-code"><span class="mobile-list-heading">Stock Code: </span>')
                if '<br/>' in code:
                    code = code[extra_n: -5].replace('<br/>', '\n')
                else:
                    code = code[extra_n: -5]
            except Exception as e:
                print(str(e))
                code = ""
            # print('code: ', code)

            # get stock name
            try:
                stock_name = str(item.select("td.stock-short-name")[0])
                extra_n = len('<td class="stock-short-name"><span class="mobile-list-heading">Stock Short Name: </span>')
                if '<br/>' in stock_name:
                    stock_name = stock_name[extra_n: -5].replace('<br/>', '\n')
                else:
                    stock_name = stock_name[extra_n: -5]
            except Exception as e:
                print(str(e))
                stock_name = ""
            # print('stock_name: ', stock_name)

            # get category
            try:
                category = item.div.get_text()
            except Exception as e:
                print(str(e))
                category = ''
            # print('category: ', category)

            # parse category
            if ' - ' in category:
                category1 = category.split(' - ')[0]
                category2 = category.split(' - ')[1][1:-2]
            else:
                category1 = category
                category2 = ''

            datapoint = {
                "RELEASE_TIME": time,
                "STOCK_CODE": code,
                "STOCK_SHORT_NAME": stock_name,
                "HEADLINE_CATEGORY": category1,
                "HEADLINE_CATEGORY_SECONDARY": category2,
                "TITLE": title,
                "PDF_URL": url
            }
            if checkDuplicate:
                if not self.dbm.exists(self.table_name, url):  # if not present in the database
                    updatenews_cnt = updatenews_cnt + self.dbm.insert_dict(datapoint, self.table_name)
            else:
                updatenews_cnt = updatenews_cnt + self.dbm.insert_dict(datapoint, self.table_name)

        self.dbm.conn.commit()
        return updatenews_cnt


if __name__ == '__main__':
    HKEX = HKEXNewsManager(host='localhost', user='root', passwd='Aa1999513', name='HKEX')
    HKEX.getHKEXNews()

