from flask import Flask, request, make_response
from celery import Celery
from spiders import boss_spider
import json
import os
import logging
from redis import StrictRedis, ConnectionPool

app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'redis://123.206.71.208:6379/0'
app.config['CELERY_BACKEND_URL'] = 'redis://123.206.71.208:6379/0'
app.config['FILE_TMP_PATH'] = r'tmp'

REDIS_CONF = {
    'host': '123.206.71.208',
    'port': 6379,
    'db': 1
}

COUNTER_KEY = 'boss_download_count'
REDIS_POOL = ConnectionPool(**REDIS_CONF)
REDIS = StrictRedis(connection_pool=REDIS_POOL)
if not REDIS.exists(COUNTER_KEY):
    REDIS.set(COUNTER_KEY, 0)

handler = logging.FileHandler('log/flask.log', encoding='UTF-8')
handler.setLevel(logging.DEBUG)
logging_format = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)s - %(message)s')
handler.setFormatter(logging_format)
app.logger.addHandler(handler)

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'], backend=app.config['CELERY_BACKEND_URL'])
celery.conf.update(app.config)


@celery.task(bind=True)
def crawl_positions_from_boss(self, url, start, end):
    task_id = self.request.id
    start = int(start)
    end = int(end)
    file_path = f'{app.config["FILE_TMP_PATH"]}/{task_id}.csv'
    app.logger.info(f'Task Info URL:{url}  Start:{start}  End:{end} File:{file_path} Task_Id:{task_id}')
    boss_spider.main(url, start, end, file_path, task=self)
    return file_path


@app.route('/')
def index():
    return app.send_static_file('page/index.html')


@app.route('/boss_spider/create', methods=('POST',))
def create_task():
    url = request.form.get('url')
    start = request.form.get('start')
    end = request.form.get('end')
    if int(start) > int(end):
        return '无效页码，请重新输入', 400
    try:
        task = crawl_positions_from_boss.delay(url, start, end)
    except Exception:
        return '无效任务', 400

    app.logger.info(f'create task success, task_id:{task.id}')
    return task.id


@app.route('/boss_spider/status/<task_id>')
def task_status(task_id):
    task = crawl_positions_from_boss.AsyncResult(task_id)
    info = task.info
    if task.state == 'SUCCESS':
        return json.dumps({
            'end': True
        })
    return json.dumps({
        'current': info.get('current'),
        'total': info.get('total'),
        'end': info.get('end')
    })


@app.route('/boss_spider/positions/<task_id>')
def download(task_id):
    try:
        result = crawl_positions_from_boss.AsyncResult(task_id)
        file_path = result.get()
        if not os.path.exists(file_path):
            content = "文件失效，请重新爬取!".encode('gbk')
        else:
            r = StrictRedis(connection_pool=REDIS_POOL)
            r.incr(COUNTER_KEY, 1)
            with open(file_path, 'rb') as f:
                content = f.read()
            os.remove(file_path)
    except Exception:
        content = "无效的任务ID,请重新爬取".encode('gbk')

    resp = make_response(content)
    resp.headers['Content-Disposition'] = f'attachment; filename={file_path}.csv'
    resp.headers['Content-type'] = 'text/csv'
    return resp


@app.route('/boss_spider/download_count')
def get_download_count():
    r = StrictRedis(connection_pool=REDIS_POOL)
    return json.dumps({'download_count': int(r.get(COUNTER_KEY).decode('utf-8'))})


if __name__ == '__main__':
    app.run()
