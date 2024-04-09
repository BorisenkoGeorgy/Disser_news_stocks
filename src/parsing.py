import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import random
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
import shutil
import requests
import os
import time
import random
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from threading import Thread, Lock
import threading
import pickle
import datetime

######## Общие функция для парсинга ##########
def start_parsing(root, target, parse_from_link, links, n_threads=3, save_evry=15000):
    
    lock = Lock()
    threads = []
    run_event = threading.Event()
    run_event.set()

    for i in range(n_threads):
        t = Thread(target=target, args=(root, lock, parse_from_link, links, save_evry))
        t.start()
        time.sleep(2)
        threads.append(t)

    for thread in threads:
        thread.join()

def parse(root, lock, parse_from_link, links, save_evry):

    global data, total_news, current_value

    user_agent = user_agent_rotator.get_random_user_agent()

    while current_value < total_news:
        with lock:
            thread_value = current_value
            link = links[current_value]
            current_value += 1
        
        parsed = parse_from_link(link, user_agent, thread_value)
        if parsed == None:
            return

        with lock:
            data['link'].append(link)
            data['data_or_ex'].append(parsed)
            if thread_value % save_evry == 0:
                with open(f'{root}/{thread_value}.pkl', 'wb') as f:
                    pickle.dump(data, f)
                    print(f'{thread_value} saved')
                data.clear()
                data['link'] = []
                data['data_or_ex'] = []

        time.sleep(2)

    with open(f'{root}/{thread_value}.pkl', 'wb') as f:
        pickle.dump(data, f)

    return 'Конец'


############ РИА новости ###########
# Сохраняем всю ленту новостей
def get_source_html_ria(url, driver):
    counter = 0
    driver.get(url)

    time.sleep(30)

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    action = ActionChains(driver)
    el = driver.find_element(By.CLASS_NAME, 'list-more')
    action.move_to_element_with_offset(el, 5, 5)
    action.click()
    action.perform()

    time.sleep(0.5)

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    try:
        counter += 2

        while True:

            if counter >= 300 and counter % 20 == 0:
                with  open(f'ria_economy/raw_preview/news_ria{counter}.html', 'w') as file:
                    file.write(driver.page_source)

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)
            counter += 1

    except Exception as _ex:
        print(_ex)

    finally:
        driver.close()
        driver.quit()

# Сохраняем ссылки из ленты новостей

def get_file_urls_ria(file_path):
    import os
    files = sorted(os.listdir(file_path))[1:]

    urls = []
    c = 1
    for f in files:

        with open(file_path + f'/{f}') as file:
            src = file.read()
        
        soup = BeautifulSoup(src, 'lxml')
        items_divs = soup.find_all('div', class_='list-item')

        for item in items_divs:
            item_url = item.find('div', class_='list-item__content').find('a').get('href')
            if item_url not in urls:
                urls.append(item_url)

        print(f'Ссылки из файлов {c} из {len(files)} получены')
        c+=1

    with open(f'ria_economy/urls.txt', 'w') as file:
        for url in urls:
            file.write(f'{url}\n')

        print(f'Ссылки сохранены')
    return

# Пишем функцию для парсинга нужных данных с многопоточностью.

def parse_from_link_ria(link, user_agent, current_value):
    if current_value % 10 == 0:
        user_agent = user_agent_rotator.get_random_user_agent()

    try:
        with requests.session() as s:
            txt = s.get(link, headers={'User-Agent':user_agent}).text
            soup = BeautifulSoup(txt, 'lxml')
            try:
                timestamp = soup.find('div', class_='article__info-date').find('a').text
            except:
                timestamp = 'No time'
            
            try:
                title = soup.find('h1', class_='article__title').text
            except:
                title = 'No title'
            try:
                announce = soup.find('div', class_='article__announce-text').text
            except:
                announce = 'No announce'
            try:
                text = soup.find('div', class_='article__body js-mediator-article mia-analytics').text
            except:
                text = 'No text'
            return timestamp, title, announce, text

    except Exception as ex:
        print(link)
        print(ex)
        return ex

    except KeyboardInterrupt:
        return

########## Лента ############
# Получаем и сохраняем ссылки с ленты
def save_urls(url, urls, user_agent):
    page = 1
    while True:
        time.sleep(1.75)
        url = url + f'page/{page}/'
        data = requests.get(url, headers={'User-Agent':user_agent}).text
        soup = BeautifulSoup(data, 'lxml')
        items = soup.find_all('a', class_='card-full-news _archive')
        if items == []:
            break
        for item in items:
            item_url = 'https://lenta.ru/' + item.get('href')
            if item_url not in urls:
                urls.append(item_url)
        page += 1
    return urls
    


def get_lenta_urls(root):
    c = 0
    if c % 3 == 0:
        user_agent = user_agent_rotator.get_random_user_agent()
    urls = []
    for year in range(2018, 2023):
        for month in range(1, 13):
            if month in [1, 3, 5, 7, 8, 10, 12]:
                if len(str(month)) == 1:
                    month = '0' + str(month)
                for day in range(1, 32):
                    if len(str(day)) == 1:
                        day = '0' + str(day)
                    url = f'https://lenta.ru/rubrics/economics/{year}/{month}/{day}/'
                    urls = save_urls(url, urls, user_agent)
            elif month in [4, 6, 9, 11]:
                if len(str(month)) == 1:
                    month = '0' + str(month)
                for day in range(1, 31):
                    if len(str(day)) == 1:
                        day = '0' + str(day)
                    url = f'https://lenta.ru/rubrics/economics/{year}/{month}/{day}/'
                    urls = save_urls(url, urls, user_agent)
            else:
                if len(str(month)) == 1:
                    month = '0' + str(month)
                if year % 4 == 0:
                    for day in range(1, 30):
                        if len(str(day)) == 1:
                            day = '0' + str(day)
                        url = f'https://lenta.ru/rubrics/economics/{year}/{month}/{day}/'
                        urls = save_urls(url, urls, user_agent)
                else:
                    for day in range(1, 29):
                        if len(str(day)) == 1:
                            day = '0' + str(day)
                        url = f'https://lenta.ru/rubrics/economics/{year}/{month}/{day}/'
                        urls = save_urls(url, urls, user_agent)
                        
            with open(f'{root}/urls{year}-{month}.txt', 'w') as file:
                for url in urls:
                    file.write(f'{url}\n')
                urls = []
            c += 1          
            print(f'{year}-{month} готов')
    print('Все сохранено')

#Для парсинга
def parse_from_link_lenta(link, user_agent, current_value):
    if current_value % 10 == 0:
        user_agent = user_agent_rotator.get_random_user_agent()

    try:
        with requests.session() as s:
            txt = s.get(link, headers={'User-Agent':user_agent}).text
            soup = BeautifulSoup(txt, 'lxml')
            try:
                timestamp = soup.find('a', class_='topic-header__item topic-header__time').text
            except:
                timestamp = 'No time'
            
            try:
                title = soup.find('span', class_='topic-body__title').text
            except:
                title = 'No title'
            try:
                announce = soup.find('div', class_='topic-body__title-yandex').text
            except:
                announce = 'No announce'
            try:
                text = soup.find_all('p', class_='topic-body__content-text')
                text = ' '.join([el.text for el in text])
            except:
                text = 'No text'
            return timestamp, title, announce, text

    except Exception as ex:
        print(link)
        print(ex)
        return ex

    except KeyboardInterrupt:
        return

########### Ведомости #############
def get_file_urls(file_path):
    files = sorted(os.listdir(file_path))
    files = [file for file in files if 'html' in file]

    urls = []
    c = 1
    for f in files:

        with open(file_path + f'/{f}', encoding='utf-8') as file:
            src = file.read()
        
        soup = BeautifulSoup(src, 'lxml')
        items_divs = soup.find_all('a', class_="article-preview-item articles-preview-list__item")

        for item in items_divs:
            item_url = item.get('href')
            if item_url not in urls:
                urls.append(item_url)

        print(f'Ссылки из файлов {c} из {len(files)} получены')
        c+=1
        with open(f'vedomosti_invest/urls_{f[10:-5]}.txt', 'w') as file:
            for url in urls:
                file.write(f'{url}\n')
            urls = []

        print(f'Ссылки сохранены')

def parse_from_link_vedomosti(link, user_agent, current_value):
    if current_value % 10 == 0:
        user_agent = user_agent_rotator.get_random_user_agent()

    try:
        with requests.session() as s:
            txt = s.get(link, headers={'User-Agent':user_agent}).text
            soup = BeautifulSoup(txt, 'lxml')
            try:
                timestamp = soup.find('time', class_='article-meta__date').get('datetime')
            except:
                timestamp = 'No time'
            
            try:
                title = soup.find('h1', class_='article-headline__title').text
            except:
                title = 'No title'
            try:
                announce = soup.find('em', class_='article-headline__subtitle').text
            except:
                announce = 'No announce'
            try:
                text = soup.find_all('p', class_='box-paragraph__text')
                text = ' '.join([el.text for el in text])
            except:
                text = 'No text'
            return timestamp, title, announce, text

    except Exception as ex:
        print(link)
        print(ex)
        return ex

    except KeyboardInterrupt:
        return

############# Комерсант  ############
# Получаем ссылки
def save_komersant_urls(link):
    urls = []
    txt = requests.get(link).text
    soup = BeautifulSoup(txt, 'lxml')
    lst = soup.find_all('h2', class_='uho__name rubric_lenta__item_name')

    for elem in lst:
        urls.append('https://www.kommersant.ru/' + elem.find('a', class_='uho__link uho__link--overlay').get('href'))
        
    return urls

def load_links(rubric):
    rubrics = {'2': 'politics', '3': 'economics', '4': 'business', '40':'finance', '41': 'consumer_market'}
    urls = []

    for year in range(2022, 2009, -1):
        for month in range(12, 0, -1):
            if month in [1, 3, 5, 7, 8, 10, 12]:
                if len(str(month)) == 1:
                    month = '0' + str(month)
                for day in range(31, 0, -1):
                    if len(str(day)) == 1:
                        day = '0' + str(day)
                    link = f'https://www.kommersant.ru/archive/rubric/{rubric}/day/{year}-{month}-{day}'
                    urls.extend(save_komersant_urls(link))
                    time.sleep(1 + random.random())
            elif month in [4, 6, 9, 11]:
                if len(str(month)) == 1:
                    month = '0' + str(month)
                for day in range(30, 0, -1):
                    if len(str(day)) == 1:
                        day = '0' + str(day)
                    link = f'https://www.kommersant.ru/archive/rubric/{rubric}/day/{year}-{month}-{day}'
                    urls.extend(save_komersant_urls(link))
                    time.sleep(1 + random.random())
            else:
                if len(str(month)) == 1:
                    month = '0' + str(month)
                if year % 4 == 0:
                    for day in range(29, 0, -1):
                        if len(str(day)) == 1:
                            day = '0' + str(day)
                        link = f'https://www.kommersant.ru/archive/rubric/{rubric}/day/{year}-{month}-{day}'
                        urls.extend(save_komersant_urls(link))
                        time.sleep(1 + random.random())
                else:
                    for day in range(28, 0, -1):
                        if len(str(day)) == 1:
                            day = '0' + str(day)
                        link = f'https://www.kommersant.ru/archive/rubric/{rubric}/day/{year}-{month}-{day}'
                        urls.extend(save_komersant_urls(link))
                        time.sleep(1 + random.random())

            print(f'{month} {year} ссылки получены')
            with open(f'komersant_{rubrics[rubric]}/urls{year}-{month}.txt', 'w') as file:
                for url in urls:
                    file.write(f'{url}\n')

def parse_from_link_komersant(link, user_agent, current_value):
    if current_value % 10 == 0:
        user_agent = user_agent_rotator.get_random_user_agent()

    try:
        with requests.session() as s:
            txt = s.get(link, headers={'User-Agent':user_agent}).text
            soup = BeautifulSoup(txt, 'lxml')
            try:
                timestamp = soup.find('time', class_='doc_header__publish_time').get('datetime')
            except:
                timestamp = 'No time'
            
            try:
                title = soup.find('h1', class_='doc_header__name js-search-mark').text
            except:
                title = 'No title'
            try:
                announce = soup.find('h2', class_='doc_header__subheader').text
            except:
                announce = 'No announce'
            try:
                try:
                    text = soup.find('p', class_='doc__text doc__intro').text
                    txt = soup.find_all('p', class_='doc__text')
                    text = text + ' ' + ' '.join([el.text for el in txt])
                except:
                    txt = soup.find_all('p', class_='doc__text')
                    text = ' '.join([el.text for el in txt])
                    
                try:
                    txt = soup.find_all('p', class_='doc__thought')
                    text = text + ' ' + ' '.join([el.text for el in txt])
                except:
                    pass
            except:
                text = 'No text'
            return timestamp, title, announce, text

    except Exception as ex:
        print(link)
        print(ex)
        return ex

    except KeyboardInterrupt:
        return

######### Парсинг Финам ##########
def parse_finam(link, year, ticks={'1 мин.': 1, '5 мин.': 2, '10 мин.': 3, '15 мин.': 4, '30 мин.': 5, '1 час': 6, '1 день': 7}):

    # options = Options()
    # options.add_argument("--headless")
    display = Xvfb()
    display.start()
    driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(),
    #  options=options
    )
    driver.get(link)
    time.sleep(12.5)

    file_type = driver.find_element(by=By.ID, value='issuer-profile-export-file-ext')
    driver.execute_script("arguments[0].removeAttribute('style')", file_type)
    dd_file = driver.find_element(by=By.ID, value='issuer-profile-export-period')
    driver.execute_script("arguments[0].removeAttribute('style')", dd_file)

    for tick in ticks:
        action = ActionChains(driver) 
        file_type = driver.find_element(by=By.ID, value='issuer-profile-export-file-ext')
        dd_file = Select(file_type)
        dd_file.select_by_visible_text('.csv')
        tick_type = driver.find_element(by=By.ID, value='issuer-profile-export-period')
        dd_tick = Select(tick_type)
        dd_tick.select_by_index(ticks[tick])     

        from_time = driver.find_element(By.ID, 'issuer-profile-export-from')
        action.move_to_element_with_offset(from_time, 1, 1)
        action.click()
        action.perform()

        from_month = driver.find_element(by=By.CLASS_NAME, value='ui-datepicker-month')
        dd_file = Select(from_month)
        dd_file.select_by_index(0)
        from_year = driver.find_element(by=By.CLASS_NAME, value='ui-datepicker-year')
        dd_file = Select(from_year)
        dd_file.select_by_visible_text(f'{year}')
        from_day = driver.find_element(by=By.CLASS_NAME, value='ui-state-default')
        action.move_to_element_with_offset(from_day, 1, 1)
        action.click()
        action.perform()

 
        to_time = driver.find_element(By.ID, 'issuer-profile-export-to')
        action.move_to_element_with_offset(to_time, 1, 1)
        action.click()
        action.perform()

        to_month = driver.find_element(by=By.CLASS_NAME, value='ui-datepicker-month')
        dd_file = Select(to_month)
        dd_file.select_by_index(0)
        to_year = driver.find_element(by=By.CLASS_NAME, value='ui-datepicker-year')
        dd_file = Select(to_year)
        dd_file.select_by_visible_text(f'{year + 1}')
        to_day = driver.find_element(by=By.CLASS_NAME, value='ui-state-default')
        action.move_to_element_with_offset(to_day, 1, 1)
        action.click()
        action.perform()

        download = driver.find_element(by=By.CLASS_NAME, value='finam-ui-dialog-button-cancel')
        action.move_to_element_with_offset(download, 1, 1)
        action.click()
        action.perform()

        t = random.random()
        if tick == '1 мин.':
            time.sleep(20 + t)
        elif tick == '5 мин.':
            time.sleep(10 + t)
        elif tick == '10 мин.':
            time.sleep(8 + t)
        elif tick == '15 мин.':
            time.sleep(5 + t)
        elif tick == '30 мин.':
            time.sleep(3 + t)
        elif tick == '1 час':
            time.sleep(2 + t)
        else:
            time.sleep(1 + t)

        file = [f for f in os.listdir('/Users/eugenborisenko/Downloads') if f[0] != '.'][0]

        while '.crdownload' in file:
            time.sleep(1)
            file = [f for f in os.listdir('/Users/eugenborisenko/Downloads') if f[0] != '.'][0]
        
        print(file)
        shutil.move(f'/Users/eugenborisenko/Downloads/{file}', f'Стоимость акций/{tick}/{file}')

    driver.quit()
    display.stop()

def get_all_quotes(companies, year_start=2010):
    for company in companies:
        for year in range(year_start, 2023):
            try:
                link = f'https://www.finam.ru/profile/moex-akcii/{companies[company]}/export/'
                parse_finam(link, year)
                print(f'{company} {year} parsed')
            except KeyboardInterrupt:
                raise
            except Exception as ex:
                print(ex)
