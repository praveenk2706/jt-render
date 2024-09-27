import logging
import os
import traceback

import pandas as pd
import requests
from flask import Flask, flash, jsonify, redirect, render_template, request, url_for
from flask_caching import Cache

app = Flask(__name__)

app.config["SECRET_KEY"] = "T3mP$7r1nGf0RFl@sk@ppS3cr3tK3y"
