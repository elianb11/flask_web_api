from email.policy import default
import werkzeug # pip install werkzeug==2.0.3
werkzeug.cached_property = werkzeug.utils.cached_property
import flask.scaffold
flask.helpers._endpoint_from_view_func = flask.scaffold._endpoint_from_view_func

# main.py
from flask import Flask, request
from request_data import *
from request_filter import *
from flask_restplus import Api, Resource
from flask_cors import CORS, cross_origin
import json

app = Flask(__name__)
CORS(app, support_credentials=True)
api = Api(
    app, 
    version='v1', 
    title='DS50 Project API', 
    description='DS50 Project API',
    license='UTBM DS50',
    contact='Elian Belmonte',
    contact_email='elian.belmonte@utbm.fr',
    terms_url='https://www.utbm.fr/',
    default="DS50 Project API",
    default_label="",
    endpoint="DS50 Project API/swagger.json"
)

@api.route('/apis/DS50/Book/First1000')
class First1000Books(Resource):
    def get(self):
        return getFirst1000Books()

@api.route('/apis/DS50/Book/First1000/tag_name=<tag_name>')
class First1000BooksByTag(Resource):
    def get(self, tag_name):
        return getFirst1000BooksByTag(tag_name)

@api.route('/apis/DS50/Book/First1000/search=<search_text>')
class First1000BooksBySearch(Resource):
    def get(self, search_text):
        return getFirst1000BooksBySearch(search_text)

@api.route('/apis/DS50/Book/book_id=<book_id>')
class BookByBookId(Resource):
    def get(self, book_id):
        return getBookByBookId(book_id)

@api.route('/apis/DS50/BooksFromSameSerie/book_id=<book_id>')
class BooksFromSameSerie(Resource):
    def get(self, book_id):
        return getBooksFromSameSerie(book_id)

@api.route('/apis/DS50/Author/book_id=<book_id>')
class AuthorByBookId(Resource):
    def get(self, book_id):
        return getAuthorByBookId(book_id)

@api.route('/apis/DS50/Tag/AllTags')
class AllTags(Resource):
    def get(self):
        return getAllTags()

@api.route('/apis/DS50/Filtering/<method>/<item>')
class Filtering(Resource):
    def get(self, method, item):
        return getReco(method=method, filter_base=item)

if __name__ == "__main__":
    app.run()
