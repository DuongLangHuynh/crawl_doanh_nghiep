import csv
import random

import psycopg2
import requests
from selenium import webdriver
from selenium.common import ElementNotInteractableException
from selenium.webdriver.common.by import By
from time import sleep
import os, time
import pandas as pd

if __name__ == "__main__":
    data_folder_path = os.path.join(os.getcwd(), 'data')
    url_file_driver = os.path.join('etc', 'chromedriver.exe')
    driver = webdriver.Chrome(executable_path=url_file_driver)
    driver.get('https://vnr500.com.vn/Charts/Index?chartId=1')
    sleep(random.randint(2, 4))
    driver.maximize_window()
    alo1 = driver.find_element("xpath", '/html/body/div[1]/div[2]/div[3]/div[2]/div[1]/label/select')
    alo1.click()
    sleep(2)
    driver.find_element("xpath", '/html/body/div[1]/div[2]/div[3]/div[2]/div[1]/label/select/option[4]').click()
    sleep(3)

    name_comment, ceo_comment, industry_comment, tax_count = [], [], [], []

    for i in range(1, 6):
        try:
            print("Crawl Page " + str(i))
            # get name
            elems = driver.find_elements(By.CSS_SELECTOR, '.name_1 a')
            name_comment = name_comment+[elem.text for elem in elems]
            # get ceo
            ceos = driver.find_elements(By.CSS_SELECTOR, '.ceo a')
            ceo_comment = ceo_comment+ [ceo1.text for ceo1 in ceos]
            # get nghành nghề
            industrys = driver.find_elements(By.CSS_SELECTOR,
                                             '.col-xs-12.col-sm-6.nganh-nghe span a:nth-of-type(2)')
            industry_comment = industry_comment+[nghanhnghe.text for nghanhnghe in industrys]

            # get mã số thuế
            taxs = driver.find_elements(By.CSS_SELECTOR, '.col-xs-12.col-sm-6.mst span:nth-of-type(2)')
            tax_count =  tax_count+[mst.text for mst in taxs]

            next_pagination_cmt = driver.find_element("xpath",
                                                      f"//a[@data-dt-idx='{i}']")
            next_pagination_cmt.click()
            print("Clicked on button next page!")
            sleep(7)
            print(name_comment)
        except ElementNotInteractableException:
            print("Element Not Interactable Exception!")
            break


    df4 = pd.DataFrame(list(zip(name_comment, ceo_comment, industry_comment, tax_count)),
                       columns=['name_comment', 'ceo_comment', 'industry_comment', 'tax_count'])
    print(df4)


    #tạo file csv
    data_folder_path = os.path.join(os.getcwd(), 'data')
    if not os.path.exists(data_folder_path):
        os.makedirs(data_folder_path)

    output_file_path = os.path.join(data_folder_path, 'output_data.csv')
    df4.to_csv(output_file_path, index=False, sep='\t')
    sleep(3)


    #kết nối telegram bot
    for data in df4.loc:
        text = f"Tên doanh nghiêp: {data['name_comment']}\nCEO: {data['ceo_comment']}\nNghành nghề: {data['industry_comment']}\nMã số thuế: {data['tax_count']}"
        url = "https://api.telegram.org/bot6806391224:AAFf3tV7tXxwzsbFyXymI6yHTvGSYCujn0I/sendMessage"
        payload = {
            "text": text,
            "chat_id":  '-1002094293536'
        }
        headers = {
            'authority': 'api.telegram.org',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'vi,en-US;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'cookie': 'stel_ln=en',
            'pragma': 'no-cache',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        try:
            requests.request("GET", url, headers=headers, data=payload)
        except Exception as error:
            print(f"Error sending Telegram message: {error}")
            print("Continuing with other tasks...")
        time.sleep(10)

        # Connect Database
        hostnamne = 'localhost'
        database = 'test'
        username = 'postgres'
        pwd = '343251679'
        port_id = '5432'

        try:
            conn = psycopg2.connect(
                host=hostnamne,
                dbname=database,
                user=username,
                password=pwd,
                port=port_id)
        except Exception as error:
            print(error)
            conn.rollback()  # Rollback nếu có lỗi kết nối
            exit()  # Thoát chương trình nếu kết nối thất bại

        cur = conn.cursor()

        # Thêm dữ liệu vào PostgreSQL
        output_file_path = os.path.join(data_folder_path, 'output_data.csv')
        with open(output_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Bỏ qua dòng tiêu đề (nếu có)

            for row in reader:
                if len(row) >= 4:
                    cur.execute(
                        "INSERT INTO doanhnghiepvn7(name, ceo, industry, tax) VALUES (%s, %s, %s, %s)",
                        (row[0], row[1], row[2], row[3])
                    )
                else:
                    print(f"Skipping row {row}, does not have enough columns.")



            conn.commit()
            cur.close()
            conn.close()

    driver.close()  # nhớ phải đóng lại nhé