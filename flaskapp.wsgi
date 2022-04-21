#!/usr/bin/env python3
import sys
import logging
logging.basicConfig(stream=sys.stderr)
sys.path.insert(0,"/var/www/flask_web_api/")
sys.path.insert(0,"/var/www/flask_web_api/api/Lib/site-packages")

from flask_web_api import app as application
application.secret_key = 'DS50project'