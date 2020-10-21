import os

import multiprocessing

# debug = True
loglevel = 'debug'
bind = "0.0.0.0:9898"
pidfile = "log/gunicorn.pid"
accesslog = "log/access.log"
errorlog = "log/debug.log"
daemon = True 

# 启动的进程数
workers = multiprocessing.cpu_count()
x_forwarded_for_header = 'X-FORWARDED-FOR'
