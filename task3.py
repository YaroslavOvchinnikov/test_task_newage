# -*- coding: utf-8 -*-
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

service_account = gspread.service_account(filename='file.json')
URL = "https://www.olx.ua/d/uk/nedvizhimost/kvartiry/"
all_apartment_data = []
links = []
count_of_parsing_page = 0
delay = 20
#write
work_sheet = service_account.open('3')


def start_script():
    flag = True
    global limit_pages_for_parsing
    while flag:
        try:
            limit_pages_for_parsing = int(input('Input count of page for parsing (1-25): '))
        except:
            print('Please, try again, your wrote incorrect symbols')
            continue
        if limit_pages_for_parsing in range(1, 26):
            print('Script started')
            flag = False
            startBrowser()
        else:
            print('Please, try again, your number not in range (1-25)')
            continue


def startBrowser():
    global driver
    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.get(URL)
    get_apartments_links()


def get_apartments_links():
    all_apartments_links_elements_on_page = WebDriverWait(driver, delay)\
        .until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[data-cy = 'l-card']>a")))
    for apartment_link in all_apartments_links_elements_on_page:
        link = apartment_link.get_attribute('href')
        links.append(link)
    click_change_page_button()


def click_change_page_button():
    global count_of_parsing_page
    global limit_pages_for_parsing
    count_of_parsing_page += 1
    if count_of_parsing_page == 25 or limit_pages_for_parsing == count_of_parsing_page:
        click_on_ads()
    else:
        try:
            new_page_button = WebDriverWait(driver, delay)\
                .until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='pagination-forward']")))
        except TimeoutException:
            print("page loading too long")
        driver.execute_script("arguments[0].scrollIntoView();", new_page_button)
        link_to_new_page = new_page_button.get_attribute("href")
        driver.get(link_to_new_page)
        get_apartments_links()


def click_on_ads():
    for apartment_link in links:
        driver.get(apartment_link)
        parsing_data(apartment_link)
    write_data(all_apartment_data)


def parsing_data(apartment_link):
    def take_part_data(item):
        return item.split(": ")[1]

    apartment = {
        "link": apartment_link,
        "price": "",
        "floor": "",
        "number of storeys": "",
        "area": "",
        "settlement": ""
    }
    try:
        price_elements = WebDriverWait(driver, delay)\
            .until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[data-testid ='ad-price-container']")))
    except TimeoutException:
        print("page loading too long")
    price = []
    for price_item in price_elements:
        price.append((price_item.text).replace('\n', ''))
    apartment["price"] = " ".join(price)

    settlement_element = WebDriverWait(driver, delay)\
        .until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="root"]/div[1]/div[3]/div[3]/div[2]/div[2]/div/section/div[1]/div')))
    settlement = []
    for settlement_item in settlement_element:
        settlement.append((settlement_item.text).replace('\n', ''))
    apartment["settlement"] = " ".join(settlement)

    apartment_elements = driver.find_elements(By.XPATH, '//*[@id="root"]/div[1]/div[3]/div[3]/div[1]/div[2]/ul/li')
    for item in apartment_elements:
        item = item.text
        if item.startswith("Поверх:"):
            apartment["floor"] = take_part_data(item)
        elif item.startswith("Поверховість"):
            apartment["number of storeys"] = take_part_data(item)
        elif item.startswith("Загальна площа:"):
            apartment["area"] = take_part_data(item)

    all_apartment_data.append(apartment)
    back_button = WebDriverWait(driver, delay)\
        .until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='to-back']")))
    back_button.click()


def write_data(all_apartment_data):
    df = pd.DataFrame.from_dict(all_apartment_data, orient='columns')
    set_with_dataframe(work_sheet.sheet1, df, include_column_header=True)
    return 1


start_script()

