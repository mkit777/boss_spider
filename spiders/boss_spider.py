from typing import List, Optional, Any, Tuple

import requests
import re
import time
from lxml import etree
from requests import Response


class Position:
    def __init__(self):
        self.title = None
        self.salary = None
        self.detail_url = None
        self.detail = None
        self.address = None
        self.experience = None
        self.education = None
        self.company_name = None
        self.company_category = None
        self.employee_count = None
        self.finance = None

    def __str__(self):
        return f'{self.title} {self.salary} {self.address} {self.experience} {self.education} {self.company_name} ' \
            f'{self.company_category} {self.employee_count} {self.finance}'


class BossClient:

    headers = {
        'User-Agent': "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) "
                      "Version/5.1 Safari/534.50"}

    base = 'https://www.zhipin.com'

    def __init__(self, url):
        self.url_format = BossClient.__get_url_format(url)

    @staticmethod
    def __get_url_format(url: str):
        if url.find('?') > 0:
            return url + '&page={page}'
        return url+'?page={page}'

    def index_page_resp_iter(self, start, end):
        print(start, end)
        for page in range(start, end+1):
            resp = self.__do_get(self.url_format.format(page=page))
            if resp.text.find('立即沟通'):
                yield resp
            else:
                break

    def get_index_page_resp(self, url) -> Response:
        resp = self.__do_get(url)
        resp.encoding='utf-8'
        return resp

    def get_detail_page_resp(self, path) -> Response:
        return self.__do_get(BossClient.base + path)

    @staticmethod
    def __do_get(url):
        return requests.get(url, headers=BossClient.headers)


class DetailPageExtractor:

    @staticmethod
    def extract(text) -> str:
        detail = etree.HTML(text).xpath('//div[@class="job-sec"][h3[text()="职位描述"]]/div[@class="text"]/text()')
        detail = ''.join(map(str.strip, detail))
        detail = detail.replace(',', '，')
        return detail


class IndexPageExtractor:

    @classmethod
    def extract(cls, text) -> Tuple[List[Position], Optional[str]]:
        root = etree.HTML(text)
        job_primaries = root.xpath('//div[@class="job-primary"]')
        positions = []
        for job_primary in job_primaries:
            position = cls.__do_extract(job_primary)
            if position is not None:
                positions.append(position)
        # next_page_url = root.xpath('//a[@class="next"]/@href')[0]
        # return positions, next_page_url if len(next_page_url) == 0 else None
        return positions

    @classmethod
    def __do_extract(cls, node):
        position = Position()

        info_primary = node.xpath('./div[@class="info-primary"]')[0]

        position.title = info_primary.xpath('.//div[@class="job-title"]/text()')[0].replace(',', '/').strip()
        position.salary = info_primary.xpath('.//span[@class="red"]/text()')[0].strip()
        position.detail_url = info_primary.xpath('./h3[@class="name"]/a/@href')[0].strip()

        job_shortcut = info_primary.xpath('./p/text()')
        if len(job_shortcut) == 3:
            position.address, position.experience, position.education = map(str.strip, job_shortcut)
        else:
            return

        info_company = node.xpath('./div[@class="info-company"]')[0]
        position.company_name = info_company.xpath('.//h3/a/text()')[0]

        company_shortcut = info_company.xpath('.//p/text()')
        if len(company_shortcut) == 2:
            position.company_category, position.employee_count = map(str.strip, company_shortcut)
            position.finance = '#'
        elif len(company_shortcut) == 3:
            position.company_category, position.finance, position.employee_count = map(str.strip, company_shortcut)
        else:
            return

        return position


class PositionWriter:
    HEAD = ('职位', '工资', '地址', '工作经验', '学历', '公司名', '分类', '金融', '员工数', '详情')

    def __init__(self, path, mode):
        self.path = path
        self.f = open(path, mode=mode, errors='ignore', encoding='gbk')
        self.write_item(PositionWriter.HEAD)

    def write_item(self, item):
        if isinstance(item, Position):
            item = (item.title, item.salary, item.address, item.experience, item.education, item.company_name,
                    item.company_category, item.finance, item.employee_count, item.detail)
        s = ','.join(map(str, item)) + '\n'
        self.f.write(s)

    def write_items(self, items):
        for item in items:
            self.write_item(item)

    def close(self):
        self.f.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


INDEX_PAGE_URL_PATTEN = re.compile(r'(https://www.zhipin.com/.*/)\??')


def start_crawl(url, start, end, task=None):
    boss = BossClient(url)
    index_extractor = IndexPageExtractor()
    # detail_extractor = DetailPageExtractor()

    for count, index_resp in enumerate(boss.index_page_resp_iter(start, end)):
        positions = index_extractor.extract(index_resp.text)
        for position in positions:
            # detail_resp = boss.get_detail_page_resp(position.detail_url)
            # position.detail = detail_extractor.extract(detail_resp.text)
            yield position
        if task is not None:
            task.update_state(meta={'current': count, 'total': (end-start+1), 'end': False})
        time.sleep(1)
    task.update_state(meta={'current': count, 'total': (end - start + 1), 'end': True})


def main(url, start, end, path, task=None):
    with PositionWriter(path, 'w') as writer:
        for position in start_crawl(url, start, end, task=task):
            writer.write_item(position)
            print(position)


if __name__ == "__main__":
    main('https://www.zhipin.com/job_detail/?city=101010100&source=10&query=%E4%BA%A4%E9%80%9A&tdsourcetag=s_pcqq_aiomsg',
               1, 2, 'D:\postion.csv')
