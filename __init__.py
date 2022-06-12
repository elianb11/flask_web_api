from email.policy import default
from bs4 import ResultSet
import werkzeug # pip install werkzeug==2.0.3
werkzeug.cached_property = werkzeug.utils.cached_property
import flask.scaffold
flask.helpers._endpoint_from_view_func = flask.scaffold._endpoint_from_view_func

# main.py
from flask import Flask, request
from werkzeug.exceptions import BadRequest
from request_data import *
from request_filter import *
from post_data import *
from flask_restplus import Api, Resource, fields
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

ns_user = api.namespace('users', description='users operations')

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

@api.route('/apis/DS50/Interaction/First100/book_id=<book_id>')
class InteractionsByBookId(Resource):
    def get(self, book_id):
        return getInteractionsByBookId(book_id)

@api.route('/apis/DS50/Filtering/<method>/<item>')
class Filtering(Resource):
    def get(self, method, item):
        return getReco(method=method, filter_base=item)

@api.route('/apis/DS50/User/mail=<mail>')
class User(Resource): 
    def get(self, mail):
        return getUserByMail(mail)

@api.route('/apis/DS50/Book/Stats/book_id=<book_id>')
class BookStats(Resource): 
    def get(self, book_id):
        return getStatsByBookId(book_id)

@api.route('/apis/DS50/User')
class User(Resource):
    user_data = ns_user.model(
    "user_data",
    {
        "first_name": fields.String(required=True),
        "last_name": fields.String(required=True),
        "username": fields.String(required=True),
        "password": fields.String(required=True),
        "mail": fields.String(required=True),
        "address": fields.String(required=True),
        "first_fav_category": fields.String(required=True),
        "second_fav_category": fields.String(required=True),
        "third_fav_category": fields.String(required=True),
    },
)
    @ns_user.expect(user_data)
    def post(self):
        return postUser(request.get_json())

@api.route('/apis/DS50/Interaction')
class Interaction(Resource):
    interaction_data = ns_user.model(
    "interaction_data",
    {
        "mail": fields.String(required=True),
        "book_id": fields.String(required=True),
        "rating": fields.String(required=True),
        "review_text": fields.String(required=True),
    },
)
    @ns_user.expect(interaction_data)
    def post(self):
        if int(request.get_json()['rating']) < 1 or int(request.get_json()['rating']) > 5 :
            raise BadRequest('Rating value must be between 0 and 5.')
        return postInteraction(request.get_json())

if __name__ == "__main__":
    app.run()
