#!/usr/bin/env python

"""
Columbia's COMS W4111.003 Introduction to Databases
"""

import os
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from flask import Flask, request, render_template, g, redirect, Response
import re

tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)




@app.route('/')
def index(): 

  return render_template("OneML.html")


@app.route('/submit')
def another():
    sampling = request.form['sampling']
    preprocessing = request.form['preprocessing']
    mlalgo = request.form['mlalgo']
    opmetrics = request.form['opmetrics']
    return render_template("submit.html")
  
@app.route('/invalid')
def invalid():
  return render_template("invalid.html")


if __name__ == "__main__":
  import click

  @click.command()
  @click.option('--debug', is_flag=True)
  @click.option('--threaded', is_flag=True)
  @click.argument('HOST', default='0.0.0.0')
  @click.argument('PORT', default=8111, type=int)
  def run(debug, threaded, host, port):
    HOST, PORT = host, port
    print("running on %s:%d" % (HOST, PORT))
    app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)


  run()
