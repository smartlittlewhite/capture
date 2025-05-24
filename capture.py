import requests
from datetime import datetime, UTC
from bs4 import BeautifulSoup
import pandas as pd
import time, random

BASE_URL = 'https://pultegroup.wd1.myworkdayjobs.com'
JOB_LIST_API = BASE_URL + '/wday/cxs/pultegroup/PGI/jobs'
DETAIL_API = BASE_URL + '/wday/cxs/pultegroup/PGI'

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'application/json',
}

def get_job_list(offset, limit):
    payload = {
        "limit": limit,
        "offset": offset,
        "searchText": "",
        "appliedFacets": {}
    }
    try:
        resp = requests.post(JOB_LIST_API, json=payload, headers=headers)
        if resp.status_code == 200:
            return resp.json().get('jobPostings', [])
        else:
            print(f"职位列表请求失败：{resp.status_code}")
            return []
    except Exception as e:
        print(f"请求职位列表时出错: {e}")
        return []

def get_job_detail(externalPath):
    try:
        url = DETAIL_API + externalPath
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            print(f"请求详情失败 {resp.status_code}: {url}")
            return None
        data = resp.json()
        info = data.get('jobPostingInfo', {})
        description_html = info.get('jobDescription', '')
        description = BeautifulSoup(description_html, 'html.parser').get_text(separator='\n')
        return {
            'Job Title': info.get('title'),
            'Job URL': info.get('externalUrl'),
            'Job Description': description.strip(),
            'Posting Date': info.get('startDate'),
            'Company Name': data.get('hiringOrganization',{}).get('name'),
            'Scrape Time': datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        print(f"解析职位详情出错: {e}")
        return None

def main():
    offset = 0
    limit = 20
    all_jobs = []

    while True:
        job_list = get_job_list(offset, limit)
        if not job_list:
            break

        for job in job_list:
            title = job.get('title')
            externalPath = job.get('externalPath')
            if not title or not externalPath:
                continue

            time.sleep(random.uniform(2, 4))
            detail = get_job_detail(externalPath)
            if not detail:
                print(f"跳过职位 {title}，无法获取详情")
                continue

            all_jobs.append(detail)
            print(f"爬取职位: {detail['Job Title']}")

        if len(job_list) < limit:
            break
        offset += limit

    if all_jobs:
        df = pd.DataFrame(all_jobs)
        df.to_csv('pultegroup_jobs.csv', index=False, encoding='utf-8-sig')
        print("✅ 数据已保存到 pultegroup_jobs.csv")
    else:
        print("⚠️ 没有成功爬取任何职位。")

if __name__ == '__main__':
    main()