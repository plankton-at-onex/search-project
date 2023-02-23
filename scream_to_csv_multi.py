# This file reorganizes the content scraped by screaming frog into a csv, on multiple cores

import re
import os
import csv
import time
import tldextract
import pandas as pd
from bs4 import BeautifulSoup

from multiprocessing import Process, cpu_count

def clean_url(x):
    return x.lstrip('original_').rstrip('.html').replace('https_', 'https://').replace('http_', 'http://').replace('_', '/')

def extract_domain(url):
    extracted = tldextract.extract(url)
    domain = extracted.domain
    suffix = extracted.suffix
    return f'{domain}.{suffix}'

def segmentorX(page):
    pattern = re.compile(r'<(\/)?(b|strong|i|em|u|mark|small|del|ins|sub|sup)\b[^>]*>')
    page = re.sub(pattern, '', page)

    soup = BeautifulSoup(page, 'html.parser')

    text = []
    for tag in soup.stripped_strings:
        text.append(tag)
    text = [' '.join(segment.strip().split()) for segment in text]
    return text

def worker(df_domain_index, files, write_to):
    with open(write_to, mode='w', newline='', encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(['ID', 'Page', 'Website', 'Manufacturer', 'Scientific Name', 'Specialty', 'Segments'])

        for i, f in enumerate(files):

            page = clean_url(f)
            d = extract_domain(page)
            id = df_domain_index.loc[d, 'ID']
            website = df_domain_index.loc[d, 'Website']
            name = df_domain_index.loc[d, 'Manufacturer']
            sn = df_domain_index.loc[d, 'Scientific Name']
            sp = df_domain_index.loc[d, 'Specialty']

            with open(os.path.join('C:\Hassan\Onex2\Screaming Frog Export\Export', f), encoding="utf-8") as fp:
                text = fp.read()
            segments = segmentorX(text)
            
            writer.writerow([id, page, website, name, sn, sp, segments])

def main():
    start = time.time()
    dir = r'C:\Hassan\Onex2\Screaming Frog Export\Export'
    files = os.listdir(dir)

    df = pd.read_csv('Embedding Sample With Extracted Domains.csv')
    # df[df['Domain'].duplicated(keep = False) == True].sort_values(by = ['Domain'], ascending= False)
    df = df.drop([112, 147, 84, 449, 451, 452, 468, 146, 184])

    df['Domain'] = df['Domain'].apply(lambda x: x.lower())
    df_domain_index = df.set_index('Domain')

    CPU_NUM = 14
    split = [len(files) * i // (CPU_NUM) for i in range(CPU_NUM)]

    thread1 = Process(target=worker, args=(df_domain_index, files[:split[1]], 'output.csv'))
    thread2 = Process(target=worker, args=(df_domain_index, files[split[1]:split[2]], 'output1.csv'))
    thread3 = Process(target=worker, args=(df_domain_index, files[split[2]:split[3]], 'output2.csv'))
    thread4 = Process(target=worker, args=(df_domain_index, files[split[3]:split[4]], 'output3.csv'))
    thread5 = Process(target=worker, args=(df_domain_index, files[split[4]:split[5]], 'output4.csv'))
    thread6 = Process(target=worker, args=(df_domain_index, files[split[5]:split[6]], 'output5.csv'))
    thread7 = Process(target=worker, args=(df_domain_index, files[split[6]:split[7]], 'output6.csv'))
    thread8 = Process(target=worker, args=(df_domain_index, files[split[7]:split[8]], 'output7.csv'))
    thread9 = Process(target=worker, args=(df_domain_index, files[split[8]:split[9]], 'output8.csv'))
    thread10 = Process(target=worker, args=(df_domain_index, files[split[9]:split[10]], 'output9.csv'))
    thread11 = Process(target=worker, args=(df_domain_index, files[split[10]:split[11]], 'output10.csv'))
    thread12 = Process(target=worker, args=(df_domain_index, files[split[11]:split[12]], 'output11.csv'))
    thread13 = Process(target=worker, args=(df_domain_index, files[split[12]:split[13]], 'output12.csv'))
    thread14 = Process(target=worker, args=(df_domain_index, files[split[13]:], 'output13.csv'))

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()
    thread7.start()
    thread8.start()
    thread9.start()
    thread10.start()
    thread11.start()
    thread12.start()
    thread13.start()
    thread14.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()
    thread7.join()
    thread8.join()
    thread9.join()
    thread10.join()
    thread11.join()
    thread12.join()
    thread13.join()
    thread14.join()

    print(time.perf_counter())

if __name__ == '__main__':
    main()

    # 5554.6889345